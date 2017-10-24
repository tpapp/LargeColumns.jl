module LargeColumns

using ArgCheck
using JLD

import Base: length, size, getindex, setindex!, push!, close, eltype, flush

import Base.Mmap: sync!

export MmappedColumns, SinkColumns

######################################################################
# utilities
######################################################################

"""
   fixed_Tuple_types(T)

Extract the parameters of a Tuple and verify that they have a fixed length.
"""
function fixed_Tuple_types(T::Type{<: Tuple})
    p = tuple(T.parameters...)
    @argcheck !(p[end] <: Vararg) "Not a tuple of fixed length."
    p
end

# FIXME use Base.write when functionality similar to
# https://github.com/JuliaLang/julia/pull/24234/ is merged
function bits_write(io::IO, x::T) where T
    @argcheck isbits(T)
    unsafe_write(io, Ref(x), sizeof(T))
end

######################################################################
# layout information
######################################################################

function representative_value(::Type{T}) where T
    @argcheck isbits(T)
    Vector{T}(1)[1]
end

const LAYOUT_FILE = "layout.jld"
const LAYOUT_N = "N"
const LAYOUT_SVAL = "S"
const LAYOUT_MAGIC = "magic"
const MAGIC = b"LargeCol-0.1"

checkdir(dir) = @argcheck isdir(dir) "Directory $dir does not exist."

function layout_path(dir)
    checkdir(dir)
    joinpath(dir, LAYOUT_FILE)
end

function write_layout(dir, N::Integer, S)
    jld = jldopen(layout_path(dir), "w")
    write(jld, LAYOUT_MAGIC, MAGIC)
    write(jld, LAYOUT_N, N)
    write(jld, LAYOUT_SVAL, representative_value(S))
    close(jld)
    nothing
end

function read_layout(dir)
    jld = jldopen(layout_path(dir), "r")
    @assert read(jld, LAYOUT_MAGIC) == MAGIC
    N = read(jld, LAYOUT_N)
    SVAL = read(jld, LAYOUT_SVAL)
    close(jld)
    N, typeof(SVAL)
end

binary_filename(dir, i::Int) = joinpath(dir, "$(i).bin")

function check_filesize(dir, N, i, T)
    fn = binary_filename(dir, i)
    size_T = sizeof(T)
    size_expected = size_T * N
    @assert(filesize(fn) == size_expected,
            "Inconsistent file size for $fn (should be $N × $(size_T) == $size_expected)")
end

######################################################################
# meta information
######################################################################

function meta_path(dir, filename = "meta.jld")
    checkdir(dir)
    @argcheck filename != LAYOUT_FILE "Conflict with the layout file."
    @argcheck !ismatch(r"\d+\.bin", filename) "Conflict with binary column file."
    joinpath(dir, filename)
end

######################################################################
# mmapped columns
######################################################################

const VectorTuple = Tuple{Vararg{Vector}}

struct MmappedColumns{T <: VectorTuple, S <: Tuple} <: AbstractVector{S}
    columns::T
    function MmappedColumns(columns::T) where {T <: VectorTuple}
        @argcheck !isempty(columns) "Need at least one column."
        N = length(first(columns))
        for v in Base.tail(columns)
            @argcheck length(v) == N "Column lengths need to match."
        end
        for c in columns
            @argcheck isbits(eltype(c)) "All columns need to be bits types."
        end
        S = Tuple{map(eltype, columns)...}
        new{T, S}(columns)
    end
end

length(A::MmappedColumns) = length(first(A.columns))

size(A::MmappedColumns) = (length(A),)

getindex(A::MmappedColumns, i) = map(c -> getindex(c, i), A.columns)

setindex!(A::MmappedColumns{T,S}, X::S, i) where {T,S} =
    map((c, x) -> setindex!(c, x, i), A.columns, X)

sync!(A::MmappedColumns) = foreach(sync!, A.columns)

function _mmap_column(dir::AbstractString, col_index::Integer, T::Type, N::Integer, mode)
    @argcheck isbits(T) "Type $T is not a bits type."
    checkdir(dir)
    io = open(binary_filename(dir, col_index), mode)
    Mmap.mmap(io, Vector{T}, N)
end

function MmappedColumns(dir::AbstractString)
    N, S = read_layout(dir)
    T = fixed_Tuple_types(S)
    columns = ntuple(i -> _mmap_column(dir, i, T[i], N, "r+"),
                     length(T))
    MmappedColumns(columns)
end

function MmappedColumns(dir::AbstractString, N, S::Type{<:Tuple})
    T = fixed_Tuple_types(S)
    columns = ntuple(i -> _mmap_column(dir, i, T[i], N, "w+"),
                     length(T))
    write_layout(dir, N, S)
    MmappedColumns(columns)
end

######################################################################
# sinks - writing an *ex ante* unknown number of elements
######################################################################

mutable struct SinkColumns{S <: Tuple, R <: Tuple, D <: AbstractString}
    dir::D
    sinks::R
    N::Int
    function SinkColumns(dir::D, S::Type{<: NTuple{Z, Any}}, sinks::R,
                         N::Int = 0) where {D, R <: NTuple{Z, IO}} where Z
        for T in fixed_Tuple_types(S)
            @argcheck isbits(T) "Type $(T) is not a bits type."
        end
        new{S, R, D}(dir, sinks, N)
    end
end

function _sink_streams(dir, S, mode)
    ntuple(i -> open(binary_filename(dir, i), mode),
           length(fixed_Tuple_types(S)))
end

"""
    SinkColumns(dir, S)

Open sinks for columns with the given type `S` in `dir`.
"""
SinkColumns(dir::AbstractString, S::Type{<: Tuple}) =
    SinkColumns(dir, S, _sink_streams(dir, S, "w"))

"""
    SinkColumns(dir, append = false)

Open sink columns with an existing layout in `dir`. When `append`, append to the
existing data (which is not checked for consistent length, it is assumed that it
was closed/flushed properly).
"""
function SinkColumns(dir::AbstractString, append::Bool)
    N, S = read_layout(dir)
    if append
        for (i, T) in enumerate(fixed_Tuple_types(S))
            check_filesize(dir, N, i, T)
        end
    end
    SinkColumns(dir, S,_sink_streams(dir, S, append ? "a" : "w"),
                append ? N : 0)
end

function push!(A::SinkColumns{S}, X::S) where S
    map(bits_write, A.sinks, X)
    A.N += 1
    A
end

push!(A::SinkColumns{S}, X) where S = push!(A, convert(S, X))

length(A::SinkColumns) = A.N

function flush(A::SinkColumns{S}) where S
    write_layout(A.dir, A.N, S)
    foreach(flush, A.sinks)
end

function close(A::SinkColumns)
    flush(A)
    foreach(close, A.sinks)
end

eltype(A::SinkColumns{S}) where S = S

end # module
