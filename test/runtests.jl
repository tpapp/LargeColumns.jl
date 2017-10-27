using LargeColumns
using Base.Test

import LargeColumns:
    # internals
    fixed_Tuple_types, representative_value, ensure_proper_subpath,
    write_layout, read_layout

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

@testset "meta subpath calculation" begin
    @test ensure_proper_subpath("/tmp", "foo") == "/tmp/foo"
    @test ensure_proper_subpath("/tmp/dir/", "foo") == "/tmp/dir/foo"
    @test_throws ArgumentError ensure_proper_subpath("/tmp/dir/", "../foo")
    @test ensure_proper_subpath("/tmp/", "dir/../foo") == "/tmp/foo"
    @test_throws ArgumentError ensure_proper_subpath("/tmp/", "dir/../../foo")
    @test_throws ArgumentError ensure_proper_subpath("/tmp", "/root")
end

@testset "layout information" begin
    dir = mktempdir()
    N = rand(1:10_000)
    S = Tuple{Date,Int}
    write_layout(dir, N, S)
    @test meta_path(dir, ".") == joinpath(dir, "meta/")
    @test meta_path(dir, "foo.jld2") == joinpath(dir, "meta/foo.jld2")
    @test isdir(meta_path(dir, ".")) # test that meta was created
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

    # test meta path
    @test meta_path(sink, "foo") == meta_path(dir, "foo")

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
    @test cols[3] ≡ (3, 3.0)                                        # getindex Int
    @test cols[(end-2):end] == [(13, 13.0), (14, 14.0), (15, 15.0)] # getindex to_indices
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
    # test meta path
    @test meta_path(cols, "foo") == meta_path(dir, "foo")
end

@testset "mmap getindex and setindex" begin
    dir = mktempdir()
    N = 10
    # setindex! — fill with values
    cols = MmappedColumns(dir, N, Tuple{Int, Char})
    for i in 1:N
        cols[i] = i, Char(i + 'a')
    end
    Mmap.sync!(cols)
    # getindex — reopen and check
    for i in 1:N
        @test cols[i] ≡ (i, Char(i + 'a'))
    end
    A = [(N+i, Char(i + 'a')) for i in 2:7]
    cols[3:8] = A
    @test cols[3:8] == A
end

@testset "path creation tests" begin
    dir = tempname()
    @test !isdir(dir)           # verify that it does not exist
    cols = MmappedColumns(dir, 10, Tuple{Float64, Date})
    # test that directories are created
    @test isdir(dir)
    @test isdir(joinpath(dir, "meta"))
    # test layout and data files
    @test isfile(joinpath(dir, "layout.jld2"))
    @test isfile(joinpath(dir, "1.bin"))
    @test isfile(joinpath(dir, "2.bin"))
    # test that no other files are created
    @test sort(readdir(dir)) == ["1.bin", "2.bin", "layout.jld2", "meta"]
    @test readdir(joinpath(dir, "meta")) == String[]
end
