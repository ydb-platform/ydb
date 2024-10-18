#include <library/cpp/disjoint_sets/disjoint_sets.h>
#include <random>
#include <util/stream/file.h>
#include <library/cpp/testing/gtest/gtest.h>
#include <util/system/datetime.h>

TEST(TTestDisjointSets, Benchmark)
{
    std::unordered_map<size_t, std::tuple<size_t, size_t>> expected_result;
    expected_result[32] = { 1, 32 };
    expected_result[256] = { 1, 256 };
    expected_result[2048] = { 2, 2047 };
    expected_result[16384] = { 5, 16380 };
    expected_result[131072] = { 24, 131049 };
    expected_result[1048576] = { 190, 1048387 };

    std::mt19937_64 rng; // fixed-seed prng
    for (size_t size = 16; size < 1024*1024; size *= 4) {
	    auto start = GetCycleCount();
	    TDisjointSets set(size);
	    for (size_t test = 0; test != size*4; ++test) {
		    size_t u = rng() & (size - 1);
		    size_t v = rng() & (size - 1);
		    set.UnionSets(u, v);
	    }
	    size *= 2;
	    set.Expand(size);
	    for (size_t test = 0; test != size*4; ++test) {
		    size_t u = rng() & (size - 1);
		    size_t v = rng() & (size - 1);
		    set.UnionSets(u, v);
	    }
	    auto end = GetCycleCount();
	    Cerr << size << ':' << (end - start) << Endl;
	    auto got = std::tuple { set.SetCount(), set.SizeOfSet(rng() & (size - 1)) };
#ifndef NDEBUG
	    Cerr << std::get<0>(got) << Endl;
	    Cerr << std::get<1>(got) << Endl;
#endif
	    EXPECT_EQ(expected_result[size], got);
    }
}
