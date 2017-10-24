module LargeColumns

using ArgCheck
using JLD

import Base:
    length, size, getindex, setindex!,  eltype, # mmapped vectors
    push!, close, flush                         # written streams

import Base.Mmap: sync!

export MmappedColumns, SinkColumns, meta_path

######################################################################
# utilities
######################################################################

"""
   fixed_Tuple_types(T)

Extract the parameters of a Tuple and verify that they have a fixed length.

```jldocstest
julia> fixed_Tuple_types(Tuple{Int64,Float64})
(Int64, Float64)
```
"""
function fixed_Tuple_types(T::Type{<: Tuple})
    @argcheck !Base.isvatuple(T)
    tuple(T.parameters...)
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

"""
    representative_value(T)

Return (an otherwise unspecified) value of type `T`.

See [`write_layout`](@ref) for why this is needed.
"""
function representative_value(::Type{T}) where T
    @argcheck isbits(T)
    Vector{T}(1)[1]
end

"Name of the layout file."
const LAYOUT_FILE = "layout.jld"

"Key for the number of records."
const LAYOUT_N = "N"

"Key for representative values."
const LAYOUT_SVAL = "S"

"Key for the 'magic' constant."
const LAYOUT_MAGIC = "magic"

"A string that is checked for version consistency."
const MAGIC = b"LargeCol-0.1"

"""
    checkdir(dir)

Check that `dir` exists and is a directory.

!!! NOTE
    Currently a placeholder. Future checks may be more picky (empty directory etc).
"""
checkdir(dir) = @argcheck isdir(dir) "Directory $dir does not exist."

"""
    layout_path(dir)

Return the path for the layout file, also checking `dir`.
"""
function layout_path(dir)
    checkdir(dir)
    joinpath(dir, LAYOUT_FILE)
end

"""
    ensure_meta_directory(dir)

If the `meta` directory does not exist, it is created.
"""
function ensure_meta_directory(dir)
    meta = joinpath(dir, "meta")
    isdir(meta) || mkpath(meta)
end

"""
    write_layout(dir, N::Integer, S)

Write the layout information into the layout file in the directory `dir`.

`N` is the number of records, `S` is the type information (eg
`Tuple{Int,Float64`).

When the `dir` and `dir/meta` do not exist, they are created.

!!! NOTE

    Type information is written as a value that is of type `S`. The sanity
    checks implemented in JLD should ensure that changed definitions are caught
    this way; the actual value is not relevant.
"""
function write_layout(dir, N::Integer, S::Type{<:Tuple})
    ensure_meta_directory(dir)
    jld = jldopen(layout_path(dir), "w")
    write(jld, LAYOUT_MAGIC, MAGIC)
    write(jld, LAYOUT_N, N)
    write(jld, LAYOUT_SVAL, representative_value(S))
    close(jld)
    nothing
end

"""
    N, S = read_layout(dir)

Return data layout from directory. If not found, or cannot be read, throw an
error.
"""
function read_layout(dir)
    jld = jldopen(layout_path(dir), "r")
    @assert read(jld, LAYOUT_MAGIC) == MAGIC
    N = read(jld, LAYOUT_N)
    SVAL = read(jld, LAYOUT_SVAL)
    close(jld)
    ensure_meta_directory(dir)
    N, typeof(SVAL)
end

"""
    binary_filename(dir, i)

Filename for the binary data of column `i` in directory `dir`.
"""
binary_filename(dir, i::Int) = joinpath(dir, "$(i).bin")

"""
    check_filesize(dir, N, i, T)

Check that the size of file for column `i` in directory `dir` is consistent with
length `N` and type `T`.
"""
function check_filesize(dir, N, i, T)
    @argcheck isbits(T)
    fn = binary_filename(dir, i)
    size_T = sizeof(T)
    size_expected = size_T * N
    @assert(filesize(fn) == size_expected,
            "Inconsistent file size for $fn (should be $N × $(size_T) == $size_expected)")
end

######################################################################
# meta information
######################################################################

"""
    meta_path(dir, [filename = "meta.jld"])

Return a path for a metadata filename in `dir`. Arbitrary filenames are allowed,
as long as they are valid base file names.
"""
function meta_path(dir, filename = "meta.jld")
    checkdir(dir)
    @argcheck(basename(filename) == filename,
              "Filename $filename is not a base file name.")
    joinpath(dir, "meta", filename)
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

getindex(A::MmappedColumns, i::Int) = map(c -> getindex(c, i), A.columns)

getindex(A::MmappedColumns, I) = map(i -> getindex(A, i), to_indices(A, (I,))...)

setindex!(A::MmappedColumns{T,S}, X::S, i::Int) where {T,S} =
    map((c, x) -> setindex!(c, x, i), A.columns, X)

setindex!(A::MmappedColumns{T,S}, X, i::Int) where {T,S} =
    setindex!(A, convert(S, X), i)

function setindex!(A::MmappedColumns{T,S}, X, I) where {T,S}
    for (i, x) in zip(to_indices(A, (I,))[1], X)
        A[i] = x
    end
    X
end

sync!(A::MmappedColumns) = foreach(sync!, A.columns)

function _mmap_column(dir::AbstractString, col_index::Integer, T::Type, N::Integer, mode)
    @argcheck isbits(T) "Type $T is not a bits type."
    checkdir(dir)
    io = open(binary_filename(dir, col_index), mode)
    Mmap.mmap(io, Vector{T}, N)
end

"""
    MmappedColumns(dir, [N, S])

Open mmapped columns in `dir`, returning a wrapper object for them that can be
accessed as a vector of eltype `S`.

When `N` and `S` are provided, they are used for the number of items (length)
and the type (eg `Tuple{Int, Float64}`), and layout information is *created or
overwritten*.

When they are not provided, `dir` is supposed to contain layout information.
"""
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

Supported interface:

- `push!`: add a record
- `length`: number of records written
- `eltype`: return `S`
- `close`, `flush`: do what they are supposed to.
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
