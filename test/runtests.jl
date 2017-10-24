using LargeColumns
using Base.Test

import LargeColumns:
    # internals
    fixed_Tuple_types, representative_value, write_layout, read_layout

@testset "utilities" begin
    @test fixed_Tuple_types(Tuple{Int, Int}) ≡ (Int, Int)
    @test fixed_Tuple_types(Tuple{Int, Float64, Char}) ≡ (Int, Float64, Char)
    @test_throws ArgumentError fixed_Tuple_types(Tuple{Int, Vararg{Int}})

    @test representative_value(Int) isa Int
    @test representative_value(Float64) isa Float64
    @test representative_value(Date) isa Date
    @test representative_value(Tuple{Date,Int}) isa Tuple{Date,Int}
    @test_throws ArgumentError representative_value(Vector{Int})
end

@testset "layout information" begin
    dir = mktempdir()
    N = rand(1:10_000)
    S = Tuple{Date,Int}
    write_layout(dir, N, S)
    @test (touch(meta_path(dir)); true) # test that meta was created
    @test read_layout(dir) ≡ (N, S)
end

@testset "meta path" begin
    @test meta_path("/tmp/", "test") == "/tmp/meta/test"
    @test_throws ArgumentError meta_path("/tmp/", "../foo")
end

@testset "write values, get back as mmapped" begin
    dir = mktempdir()

    # write
    sink = SinkColumns(dir, Tuple{Int, Float64})
    for i in 1:9
        push!(sink, (i, Float64(i)))
    end
    push!(sink, (10, 10))       # test conversion
    @test length(sink) == 10
    @test eltype(sink) == Tuple{Int, Float64}
    flush(sink)     # NOTE calling both `flush` and `close` is not a strong test
    close(sink)

    # append
    sink = SinkColumns(dir, true)
    for i in 11:15
        push!(sink, (i, Float64(i)))
    end
    @test length(sink) == 15
    @test eltype(sink) == Tuple{Int, Float64}
    close(sink)

    # mmap
    cols = MmappedColumns(dir)
    @test eltype(cols) == Tuple{Int, Float64}
    @test length(cols) == 15
    @test cols[3] ≡ (3, 3.0)
    @test cols == [(i, Float64(i)) for i in 1:15]
end

@testset "mmap standalone, opened multiple times" begin
    dir = mktempdir()
    N = 39
    # create
    cols = MmappedColumns(dir, N, Tuple{Int}) # create
    col = cols.columns[1]
    col .= randperm(N)          # random permutation
    Mmap.sync!(cols)
    # reopen and sort
    cols = MmappedColumns(dir)
    sort!(cols.columns[1])
    Mmap.sync!(cols)
    # reopen and test
    cols = MmappedColumns(dir)
    @test cols == [(i,) for i in 1:N]
end

@testset "mmap getindex and setindex" begin
    dir = mktempdir()
    N = 10
    # setindex! — fill with values
    cols = MmappedColumns(dir, N, Tuple{Int, Char})
    for i in 1:10
        cols[i] = i, Char(i + 'a')
    end
    Mmap.sync!(cols)
    # getindex — reopen and check
    for i in 1:10
        @test cols[i] ≡ (i, Char(i + 'a'))
    end
end
