#include <yql/essentials/minikql/mkql_runtime_version.h>
#include <yql/essentials/minikql/comp_nodes/ut/mkql_computation_node_ut.h>
#include <library/cpp/testing/unittest/registar.h>

#include <chrono>
#include <vector>
#include <set>
#include <random>

#include <util/system/fs.h>
#include <util/system/compiler.h>
#include <util/stream/null.h>
#include <util/system/mem_info.h>

#include <ydb/library/yql/minikql/comp_nodes/packed_tuple/hashes_calc.h>
#include <ydb/library/yql/minikql/comp_nodes/packed_tuple/tuple.h>

#include <yql/essentials/minikql/comp_nodes/mkql_rh_hash.h>

namespace NKikimr {
namespace NMiniKQL {
namespace NPackedTuple {

using namespace std::chrono_literals;

static volatile bool IsVerbose = true;
#define CTEST (IsVerbose ? Cerr : Cnull)

namespace {

template <typename TTraits>
void TestCalculateCRC32_Impl() {
    std::mt19937_64 rng; // fixed-seed (0) prng
    std::vector<ui64> v(1024);
    std::generate(v.begin(), v.end(), rng);

    ui64 nanoseconds = 0;
    ui64 totalBytes = 0;
    ui32 hash = 0;
    for (ui32 test = 0; test < 65535; ++test) {
        ui32 bytes = rng() % (sizeof(v[0])*v.size());

        std::chrono::steady_clock::time_point begin01 = std::chrono::steady_clock::now();
        hash = CalculateCRC32<TTraits>((const ui8 *) v.data(), bytes, hash);
        std::chrono::steady_clock::time_point end01 = std::chrono::steady_clock::now();

        nanoseconds += std::chrono::duration_cast<std::chrono::nanoseconds>(end01 - begin01).count();
        totalBytes += bytes;
    }
    CTEST << "Hash: "  << hash << Endl;
    UNIT_ASSERT_VALUES_EQUAL(hash, 80113928);
    CTEST << "Data Size: "  << totalBytes << Endl;
    CTEST  << "Time for hash: " << ((nanoseconds + 999)/1000)  << "[microseconds]" << Endl;
    CTEST  << "Calculating speed: " << totalBytes / ((nanoseconds + 999)/1000) << "MB/sec" << Endl;
}
}

Y_UNIT_TEST_SUITE(TestHash) {

Y_UNIT_TEST(TestCalculateCRC32Fallback) {
    TestCalculateCRC32_Impl<NSimd::TSimdFallbackTraits>();
}

Y_UNIT_TEST(TestCalculateCRC32SSE42) {
    if (NX86::HaveSSE42())
        TestCalculateCRC32_Impl<NSimd::TSimdSSE42Traits>();
    else
        CTEST << "Skipped SSE42 test\n";
}

Y_UNIT_TEST(TestCalculateCRC32AVX2) {
    if (NX86::HaveAVX2())
        TestCalculateCRC32_Impl<NSimd::TSimdAVX2Traits>();
    else
        CTEST << "Skipped AVX2 test\n";
}

}

Y_UNIT_TEST_SUITE(TupleLayout) {
Y_UNIT_TEST(CreateLayout) {

    TColumnDesc kc1, kc2,  pc1, pc2, pc3;

    kc1.Role = EColumnRole::Key;
    kc1.DataSize = 8;

    kc2.Role = EColumnRole::Key;
    kc2.DataSize = 4;

    pc1.Role = EColumnRole::Payload;
    pc1.DataSize = 16;

    pc2.Role = EColumnRole::Payload;
    pc2.DataSize = 4;

    pc3.Role = EColumnRole::Payload;
    pc3.DataSize = 8;

    std::vector<TColumnDesc> columns{kc1, kc2, pc1, pc2, pc3};

    auto tl = TTupleLayout::Create(columns);
    UNIT_ASSERT(tl->TotalRowSize == 45);
}

Y_UNIT_TEST(Pack) {

    TScopedAlloc alloc(__LOCATION__);

    TColumnDesc kc1, kc2,  pc1, pc2;

    kc1.Role = EColumnRole::Key;
    kc1.DataSize = 8;

    kc2.Role = EColumnRole::Key;
    kc2.DataSize = 4;

    pc1.Role = EColumnRole::Payload;
    pc1.DataSize = 8;

    pc2.Role = EColumnRole::Payload;
    pc2.DataSize = 4;

    std::vector<TColumnDesc> columns{kc1, kc2, pc1, pc2};

    auto tl = TTupleLayout::Create(columns);
    UNIT_ASSERT(tl->TotalRowSize == 29);

    const ui64 NTuples1 = 10e6;

    const ui64 Tuples1DataBytes = (tl->TotalRowSize) * NTuples1;

    std::vector<ui64> col1(NTuples1, 0);
    std::vector<ui32> col2(NTuples1, 0);
    std::vector<ui64> col3(NTuples1, 0);
    std::vector<ui32> col4(NTuples1, 0);

    for (ui32 i = 0; i < NTuples1; ++i) {
        col1[i] = i;
        col2[i] = i;
        col3[i] = i;
        col4[i] = i;
    }

    const ui8* cols[4];

    cols[0] = (ui8*) col1.data();
    cols[1] = (ui8*) col2.data();
    cols[2] = (ui8*) col3.data();
    cols[3] = (ui8*) col4.data();

    
    std::vector<ui8> colValid1((NTuples1 + 7)/8, ~0);
    std::vector<ui8> colValid2((NTuples1 + 7)/8, ~0);
    std::vector<ui8> colValid3((NTuples1 + 7)/8, ~0);
    std::vector<ui8> colValid4((NTuples1 + 7)/8, ~0);
    const ui8 *colsValid[4] = {
        colValid1.data(),
        colValid2.data(),
        colValid3.data(),
        colValid4.data(),
    };
    
    std::vector<ui8, TMKQLAllocator<ui8>> overflow;

    std::vector<ui8> res(Tuples1DataBytes, 0);
    std::chrono::steady_clock::time_point begintp = std::chrono::steady_clock::now();
    tl->Pack(cols, colsValid, res.data(), overflow, 0, NTuples1);
    std::chrono::steady_clock::time_point endtp = std::chrono::steady_clock::now();
    ui64 microseconds = std::chrono::duration_cast<std::chrono::microseconds>(endtp - begintp).count();
    if (microseconds == 0) microseconds = 1;

    CTEST  << "Time for " << (NTuples1) << " transpose (external cycle)= " << microseconds  << "[microseconds]" << Endl;
    CTEST  << "Data size = " << Tuples1DataBytes / (1024 * 1024) << "[MB]" << Endl;
    CTEST  << "Calculating pack speed = " << Tuples1DataBytes / microseconds << "MB/sec" << Endl;
    CTEST  << Endl;

    static const ui32 NLogBuckets = 3;

    auto resesData = [&]{
        std::array<std::vector<ui8, TMKQLAllocator<ui8>>, 1u << NLogBuckets> result;
        for (auto& bres : result) {
            bres.resize((Tuples1DataBytes >> NLogBuckets) * 9 / 8, 0);
            bres.resize(0);
        }
        return result;
    }();
    std::array<std::vector<ui8, TMKQLAllocator<ui8>>, 1u << NLogBuckets> overflowsData;

    auto reses = TPaddedPtr(resesData.data(), sizeof(resesData[0]));
    auto overflows = TPaddedPtr(overflowsData.data(), sizeof(overflowsData[0]));

    begintp = std::chrono::steady_clock::now();
    tl->BucketPack(cols, colsValid, reses, overflows, 0, NTuples1, NLogBuckets);
    endtp = std::chrono::steady_clock::now();
    microseconds = std::chrono::duration_cast<std::chrono::microseconds>(endtp - begintp).count();
    if (microseconds == 0) microseconds = 1;

    CTEST  << "Time for " << (NTuples1) << " transpose in " << (1u << NLogBuckets) << "(external cycle)= " << microseconds  << "[microseconds]" << Endl;
    CTEST  << "Data size = " << Tuples1DataBytes / (1024 * 1024) << "[MB]" << Endl;
    CTEST  << "Calculating bucketed pack speed = " << Tuples1DataBytes / microseconds << "MB/sec" << Endl;
    CTEST  << Endl;

    const size_t bressize = std::accumulate(resesData.begin(), resesData.end(), 0, [](size_t lhs, const auto& rhs) {
        return lhs + rhs.size();
    });
    UNIT_ASSERT_EQUAL(res.size(), bressize);

    ui32 hashsum = 0;
    for (size_t i = 0; i < res.size(); i += tl->TotalRowSize) {
        hashsum += ReadUnaligned<ui32>(res.data() + i);
    }
    ui32 bhashsum = 0;
    for (const auto& bres : resesData) {
        for (size_t i = 0; i < bres.size(); i += tl->TotalRowSize) {
            bhashsum += ReadUnaligned<ui32>(bres.data() + i);
        }
    }
    UNIT_ASSERT_EQUAL(hashsum, bhashsum);
}

Y_UNIT_TEST(PackMany) {

    TScopedAlloc alloc(__LOCATION__);

    constexpr size_t cols_types_num = 4;
    constexpr size_t cols_cnts[cols_types_num] = {0, 20, 10, 6}; // both Key and Payload
    const ui64 NTuples1 = 1e6 * 2;

    constexpr size_t cols_sizes[cols_types_num] = {1, 2, 4, 8};
    constexpr size_t cols_num =
        2 * [&]<size_t... Is>(const std::index_sequence<Is...> &) {
            return (... + cols_cnts[Is]);
        }(std::make_index_sequence<cols_types_num>{});

    [&]<size_t... Is>(const std::index_sequence<Is...>&) {
        CTEST << "Row: [crc32]";
        ((cols_cnts[Is] ? CTEST << " [" << cols_sizes[Is] << "]x" <<  cols_cnts[Is] : CTEST), ...);
        CTEST << " [mask byte]x" << (cols_num + 7) / 8;
        ((cols_cnts[Is] ? CTEST << " [" << cols_sizes[Is] << "]x" <<  cols_cnts[Is] : CTEST), ...);
        CTEST << Endl;
    }(std::make_index_sequence<cols_types_num>{});
    
    std::vector<TColumnDesc> columns;
    for (size_t ind = 0; ind != cols_types_num; ++ind) {
        TColumnDesc desc;
        desc.DataSize = cols_sizes[ind];

        desc.Role = EColumnRole::Key;
        for (size_t cnt = 0; cnt != cols_cnts[ind]; ++cnt) {
            columns.push_back(desc);
        }
        desc.Role = EColumnRole::Payload;
        for (size_t cnt = 0; cnt != cols_cnts[ind]; ++cnt) {
            columns.push_back(desc);
        }
    }

    UNIT_ASSERT_EQUAL(cols_num, columns.size());

    auto tl = TTupleLayout::Create(columns);
    CTEST << "Row size: " << tl->TotalRowSize << Endl;

    const ui64 Tuples1DataBytes = (tl->TotalRowSize) * NTuples1;

    std::vector<ui8> cols_vecs[cols_num];
    for (size_t col = 0; col != cols_num; ++col) {
        const size_t size = columns[col].DataSize * NTuples1;
        cols_vecs[col].resize(size, 0);
        for (ui32 i = 0; i < size; ++i) {
            cols_vecs[col][i] = i;
        }
        for (ui32 i = 0; i < NTuples1; ++i) {
            cols_vecs[col][i * columns[col].DataSize] = i;
        }
    }

    const ui8* cols[cols_num];
    [&]<size_t... Is>(const std::index_sequence<Is...>&) {
        ((cols[Is] = cols_vecs[Is].data()), ...);
    }(std::make_index_sequence<cols_num>{});

    std::vector<ui8> cols_valid_vecs[cols_num];
    for (size_t col = 0; col != cols_num; ++col) {
        cols_valid_vecs[col].resize((NTuples1 + 7)/8, ~0);
    }

    const ui8* cols_valid[cols_num];
    [&]<size_t... Is>(const std::index_sequence<Is...>&) {
        ((cols_valid[Is] = cols_valid_vecs[Is].data()), ...);
    }(std::make_index_sequence<cols_num>{});

    std::vector<ui8> res(Tuples1DataBytes + 64, 0);
    std::vector<ui8, TMKQLAllocator<ui8>> overflow;

    std::chrono::steady_clock::time_point begin02 = std::chrono::steady_clock::now();
    tl->Pack(cols, cols_valid, res.data(), overflow, 0, NTuples1);
    std::chrono::steady_clock::time_point end02 = std::chrono::steady_clock::now();

    ui64 microseconds = std::chrono::duration_cast<std::chrono::microseconds>(end02 - begin02).count();
    if (microseconds == 0) microseconds = 1;

    for (size_t ind = 0; ind != NTuples1; ++ind) {
        UNIT_ASSERT_EQUAL((ui8)ind, res.data()[tl->TotalRowSize * ind + 4]);
    }

    CTEST  << "Time for " << (NTuples1) << " transpose (external cycle)= " << microseconds  << "[microseconds]" << Endl;
    CTEST  << "Data size =  " << Tuples1DataBytes / (1024 * 1024) << "[MB]" << Endl;
    CTEST  << "Calculating pack-many speed = " << Tuples1DataBytes / microseconds << "[MB/sec]" << Endl;
    CTEST  << Endl;

    UNIT_ASSERT(true);

}

Y_UNIT_TEST(UnpackMany) {

    TScopedAlloc alloc(__LOCATION__);

    constexpr size_t cols_types_num = 4;
    constexpr size_t cols_cnts[cols_types_num] = {0, 20, 10, 6}; // both Key and Payload
    const ui64 NTuples1 = 1e6 * 2;

    constexpr size_t cols_sizes[cols_types_num] = {1, 2, 4, 8};
    constexpr size_t cols_num =
        2 * [&]<size_t... Is>(const std::index_sequence<Is...> &) {
            return (... + cols_cnts[Is]);
        }(std::make_index_sequence<cols_types_num>{});

    [&]<size_t... Is>(const std::index_sequence<Is...>&) {
        CTEST << "Row: [crc32]";
        ((cols_cnts[Is] ? CTEST << " [" << cols_sizes[Is] << "]x" <<  cols_cnts[Is] : CTEST), ...);
        CTEST << " [mask byte]x" << (cols_num + 7) / 8;
        ((cols_cnts[Is] ? CTEST << " [" << cols_sizes[Is] << "]x" <<  cols_cnts[Is] : CTEST), ...);
        CTEST << Endl;
    }(std::make_index_sequence<cols_types_num>{});
    
    std::vector<TColumnDesc> columns;
    for (size_t ind = 0; ind != cols_types_num; ++ind) {
        TColumnDesc desc;
        desc.DataSize = cols_sizes[ind];

        desc.Role = EColumnRole::Key;
        for (size_t cnt = 0; cnt != cols_cnts[ind]; ++cnt) {
            columns.push_back(desc);
        }
        desc.Role = EColumnRole::Payload;
        for (size_t cnt = 0; cnt != cols_cnts[ind]; ++cnt) {
            columns.push_back(desc);
        }
    }

    UNIT_ASSERT_EQUAL(cols_num, columns.size());

    auto tl = TTupleLayout::Create(columns);
    CTEST << "Row size: " << tl->TotalRowSize << Endl;

    const ui64 Tuples1DataBytes = (tl->TotalRowSize) * NTuples1;

    std::vector<ui8> cols_vecs[cols_num];
    for (size_t col = 0; col != cols_num; ++col) {
        const size_t size = columns[col].DataSize * NTuples1;
        cols_vecs[col].resize(size, 0);
        for (ui32 i = 0; i < size; ++i) {
            cols_vecs[col][i] = i;
        }
    }

    const ui8* cols[cols_num];
    [&]<size_t... Is>(const std::index_sequence<Is...>&) {
        ((cols[Is] = cols_vecs[Is].data()), ...);
    }(std::make_index_sequence<cols_num>{});

    std::vector<ui8> cols_valid_vecs[cols_num];
    for (size_t col = 0; col != cols_num; ++col) {
        cols_valid_vecs[col].resize((NTuples1 + 7)/8, ~0);
    }

    const ui8* cols_valid[cols_num];
    [&]<size_t... Is>(const std::index_sequence<Is...>&) {
        ((cols_valid[Is] = cols_valid_vecs[Is].data()), ...);
    }(std::make_index_sequence<cols_num>{});

    std::vector<ui8> res(Tuples1DataBytes + 64, 0);
    std::vector<ui8, TMKQLAllocator<ui8>> overflow;
    tl->Pack(cols, cols_valid, res.data(), overflow, 0, NTuples1);

    std::vector<ui8> cols_new_vecs[cols_num];
    for (size_t col = 0; col != cols_num; ++col) {
        const size_t size = columns[col].DataSize * NTuples1;
        cols_new_vecs[col].resize(size, 13);
    }

    ui8* cols_new[cols_num];
    [&]<size_t... Is>(const std::index_sequence<Is...>&) {
        ((cols_new[Is] = cols_new_vecs[Is].data()), ...);
    }(std::make_index_sequence<cols_num>{});

    std::vector<ui8> cols_new_valid_vecs[cols_num];
    for (size_t col = 0; col != cols_num; ++col) {
        cols_new_valid_vecs[col].resize((NTuples1 + 7)/8, 13);
    }

    ui8* cols_new_valid[cols_num];
    [&]<size_t... Is>(const std::index_sequence<Is...>&) {
        ((cols_new_valid[Is] = cols_new_valid_vecs[Is].data()), ...);
    }(std::make_index_sequence<cols_num>{});


    std::chrono::steady_clock::time_point begin02 = std::chrono::steady_clock::now();
    tl->Unpack(cols_new, cols_new_valid, res.data(), overflow, 0, NTuples1);
    std::chrono::steady_clock::time_point end02 = std::chrono::steady_clock::now();
    ui64 microseconds = std::chrono::duration_cast<std::chrono::microseconds>(end02 - begin02).count();

    if (microseconds == 0) microseconds = 1;

    CTEST  << "Time for " << (NTuples1) << " transpose (external cycle)= " << microseconds  << "[microseconds]" << Endl;
    CTEST  << "Data size =  " << Tuples1DataBytes / (1024 * 1024) << "[MB]" << Endl;
    CTEST  << "Calculating unpack-many speed = " << Tuples1DataBytes / microseconds << "[MB/sec]" << Endl;
    CTEST  << Endl;

    [&]<size_t... Is>(const std::index_sequence<Is...>&) {
        const bool data_check = ((std::memcmp(cols[Is], cols_new[Is], cols_vecs[Is].size()) == 0) && ...);
        UNIT_ASSERT(data_check);
        const bool valid_check = ((std::memcmp(cols_valid[Is], cols_new_valid[Is], cols_valid_vecs[Is].size()) == 0) && ...);
        UNIT_ASSERT(valid_check); 
    }(std::make_index_sequence<cols_num>{});
}

Y_UNIT_TEST(Unpack) {

    TScopedAlloc alloc(__LOCATION__);

    TColumnDesc kc1, kc2,  pc1, pc2;

    kc1.Role = EColumnRole::Key;
    kc1.DataSize = 8;

    kc2.Role = EColumnRole::Key;
    kc2.DataSize = 4;

    pc1.Role = EColumnRole::Payload;
    pc1.DataSize = 8;

    pc2.Role = EColumnRole::Payload;
    pc2.DataSize = 4;

    std::vector<TColumnDesc> columns{kc1, kc2, pc1, pc2};

    auto tl = TTupleLayout::Create(columns);
    UNIT_ASSERT(tl->TotalRowSize == 29);

    const ui64 NTuples1 = 10e6;

    const ui64 Tuples1DataBytes = (tl->TotalRowSize) * NTuples1;

    std::vector<ui64> col1(NTuples1, 0);
    std::vector<ui32> col2(NTuples1, 0);
    std::vector<ui64> col3(NTuples1, 0);
    std::vector<ui32> col4(NTuples1, 0);

    std::vector<ui8> res(Tuples1DataBytes + 64, 0);

    for (ui32 i = 0; i < NTuples1; ++i) {
        col1[i] = i;
        col2[i] = i ^ 1;
        col3[i] = i ^ 7;
        col4[i] = i ^ 13;
    }

    const ui8* cols[4];

    cols[0] = (ui8*) col1.data();
    cols[1] = (ui8*) col2.data();
    cols[2] = (ui8*) col3.data();
    cols[3] = (ui8*) col4.data();

    std::vector<ui8> colValid1((NTuples1 + 7)/8, ~0);
    std::vector<ui8> colValid2((NTuples1 + 7)/8, ~0);
    std::vector<ui8> colValid3((NTuples1 + 7)/8, ~0);
    std::vector<ui8> colValid4((NTuples1 + 7)/8, ~0);
    const ui8 *colsValid[4] = {
            colValid1.data(),
            colValid2.data(),
            colValid3.data(),
            colValid4.data(),
    };

    std::vector<ui8, TMKQLAllocator<ui8>> overflow;
    tl->Pack(cols, colsValid, res.data(), overflow, 0, NTuples1);

    std::vector<ui64> col1_new(NTuples1, 0);
    std::vector<ui32> col2_new(NTuples1, 0);
    std::vector<ui64> col3_new(NTuples1, 0);
    std::vector<ui32> col4_new(NTuples1, 0);

    ui8* cols_new[4];
    cols_new[0] = (ui8*) col1_new.data();
    cols_new[1] = (ui8*) col2_new.data();
    cols_new[2] = (ui8*) col3_new.data();
    cols_new[3] = (ui8*) col4_new.data();

    std::vector<ui8> colValid1_new((NTuples1 + 7)/8, 0);
    std::vector<ui8> colValid2_new((NTuples1 + 7)/8, 0);
    std::vector<ui8> colValid3_new((NTuples1 + 7)/8, 0);
    std::vector<ui8> colValid4_new((NTuples1 + 7)/8, 0);

    ui8 *colsValid_new[4] = {
        colValid1_new.data(),
        colValid2_new.data(),
        colValid3_new.data(),
        colValid4_new.data(),
    };

    std::chrono::steady_clock::time_point begin02 = std::chrono::steady_clock::now();
    tl->Unpack(cols_new, colsValid_new, res.data(), overflow, 0, NTuples1);
    std::chrono::steady_clock::time_point end02 = std::chrono::steady_clock::now();
    ui64 microseconds = std::chrono::duration_cast<std::chrono::microseconds>(end02 - begin02).count();

    if (microseconds == 0) microseconds = 1;

    CTEST  << "Time for " << (NTuples1) << " transpose (external cycle)= " << microseconds  << "[microseconds]" << Endl;
    CTEST  << "Data size =  " << Tuples1DataBytes / (1024 * 1024) << "[MB]" << Endl;
    CTEST  << "Calculating unpack speed = " << Tuples1DataBytes / microseconds << "MB/sec" << Endl;
    CTEST  << Endl;

    UNIT_ASSERT(std::memcmp(col1.data(), col1_new.data(), sizeof(ui64) * col1.size()) == 0);
    UNIT_ASSERT(std::memcmp(col2.data(), col2_new.data(), sizeof(ui32) * col2.size()) == 0);
    UNIT_ASSERT(std::memcmp(col3.data(), col3_new.data(), sizeof(ui64) * col3.size()) == 0);
    UNIT_ASSERT(std::memcmp(col4.data(), col4_new.data(), sizeof(ui32) * col4.size()) == 0);

    UNIT_ASSERT(std::memcmp(colValid1.data(), colValid1_new.data(), colValid1.size()) == 0);
    UNIT_ASSERT(std::memcmp(colValid2.data(), colValid2_new.data(), colValid2.size()) == 0);
    UNIT_ASSERT(std::memcmp(colValid3.data(), colValid3_new.data(), colValid3.size()) == 0);
    UNIT_ASSERT(std::memcmp(colValid4.data(), colValid4_new.data(), colValid4.size()) == 0);
}

Y_UNIT_TEST(PackVarSize) {

    TScopedAlloc alloc(__LOCATION__);

    TColumnDesc kc1, kcv1, kcv2, kc2,  pc1, pc2;

    kc1.Role = EColumnRole::Key;
    kc1.DataSize = 8;

    kc2.Role = EColumnRole::Key;
    kc2.DataSize = 4;

    pc1.Role = EColumnRole::Payload;
    pc1.DataSize = 8;

    pc2.Role = EColumnRole::Payload;
    pc2.DataSize = 4;

    kcv1.Role = EColumnRole::Key;
    kcv1.DataSize = 8;
    kcv1.SizeType = EColumnSizeType::Variable;

    kcv2.Role = EColumnRole::Key;
    kcv2.DataSize = 16;
    kcv2.SizeType = EColumnSizeType::Variable;

    pc1.Role = EColumnRole::Payload;
    pc1.DataSize = 8;

    pc2.Role = EColumnRole::Payload;
    pc2.DataSize = 4;

    std::vector<TColumnDesc> columns{kc1, kc2, kcv1, kcv2, pc1, pc2};

    auto tl = TTupleLayout::Create(columns);
    CTEST << "TotalRowSize = " << tl->TotalRowSize << Endl;
    UNIT_ASSERT_VALUES_EQUAL(tl->TotalRowSize, 54);

    const ui64 NTuples1 = 3;

    const ui64 Tuples1DataBytes = (tl->TotalRowSize) * NTuples1;

    std::vector<ui64> col1(NTuples1, 0);
    std::vector<ui32> col2(NTuples1, 0);
    std::vector<ui64> col3(NTuples1, 0);
    std::vector<ui32> col4(NTuples1, 0);

    std::vector<ui32> vcol1(1, 0);

    std::vector<ui8> vcol1data;
    std::vector<ui32> vcol2(1, 0);
    std::vector<ui8> vcol2data;

    std::vector<ui8> res(Tuples1DataBytes + 64, 0);
    std::vector<TString> vcol1str {
        "abc",
        "ABCDEFGHIJKLMNO",
        "ZYXWVUTSPR"
    };
    std::vector<TString> vcol2str {
        "ABC",
        "abcdefghijklmno",
        "zyxwvutspr"
    };
    for (auto &&str: vcol1str) {
        for (auto c: str)
            vcol1data.push_back(c);
        vcol1.push_back(vcol1data.size());
    }
    UNIT_ASSERT_VALUES_EQUAL(vcol1.size(), NTuples1 + 1);
    for (auto &&str: vcol2str) {
        for (auto c: str)
            vcol2data.push_back(c);
        vcol2.push_back(vcol2data.size());
    }
    UNIT_ASSERT_VALUES_EQUAL(vcol2.size(), NTuples1 + 1);
    for (ui32 i = 0; i < NTuples1; ++i) {
        col1[i] = (1ull<<(sizeof(col1[0])*8 - 4)) + i + 1;
        col2[i] = (2ull<<(sizeof(col2[0])*8 - 4)) + i + 1;
        col3[i] = (3ull<<(sizeof(col3[0])*8 - 4)) + i + 1;
        col4[i] = (4ull<<(sizeof(col4[0])*8 - 4)) + i + 1;
    }

    const ui8* cols[4 + 2*2];

    cols[0] = (ui8*) col1.data();
    cols[1] = (ui8*) col2.data();
    cols[2] = (ui8*) vcol1.data();
    cols[3] = (ui8*) vcol1data.data();
    cols[4] = (ui8*) vcol2.data();
    cols[5] = (ui8*) vcol2data.data();
    cols[6] = (ui8*) col3.data();
    cols[7] = (ui8*) col4.data();

    std::vector<ui8, TMKQLAllocator<ui8>> overflow;
    std::vector<ui8> colValid((NTuples1 + 7)/8, ~0);
    const ui8 *colsValid[8] = {
            colValid.data(),
            colValid.data(),
            colValid.data(),
            nullptr,
            colValid.data(),
            nullptr,
            colValid.data(),
            colValid.data(),
    };

    auto begintp = std::chrono::steady_clock::now();
    tl->Pack(cols, colsValid, res.data(), overflow, 0, NTuples1);
    auto endtp = std::chrono::steady_clock::now();
    ui64 microseconds = std::chrono::duration_cast<std::chrono::microseconds>(endtp - begintp).count();

    if (microseconds == 0)
        microseconds = 1;

    CTEST  << "Time for " << (NTuples1) << " transpose (external cycle)= " << microseconds  << "[microseconds]" << Endl;

    constexpr ui32 NLogBuckets = 2;

    std::array<std::vector<ui8, TMKQLAllocator<ui8>>, 1u << NLogBuckets> resesData;
    std::array<std::vector<ui8, TMKQLAllocator<ui8>>, 1u << NLogBuckets> overflowsData;

    auto reses = TPaddedPtr(resesData.data(), sizeof(resesData[0]));
    auto overflows = TPaddedPtr(overflowsData.data(), sizeof(overflowsData[0]));

    begintp = std::chrono::steady_clock::now();
    tl->BucketPack(cols, colsValid, reses, overflows, 0, NTuples1, NLogBuckets);
    endtp = std::chrono::steady_clock::now();
    microseconds = std::chrono::duration_cast<std::chrono::microseconds>(endtp - begintp).count();

    if (microseconds == 0)
        microseconds = 1;

    CTEST  << "Time for " << (NTuples1) << " transpose in " << (1u << NLogBuckets) << " (external cycle)= " << microseconds  << "[microseconds]" << Endl;

    #ifndef NDEBUG
    CTEST << "Result size = " << Tuples1DataBytes << Endl;
    CTEST << "Result = ";
    for (ui32 i = 0; i < Tuples1DataBytes; ++i)
        CTEST << int(res[i]) << ' ';
    CTEST << Endl;
    CTEST << "Overflow size = " << overflow.size() << Endl;
    CTEST << "Overflow = ";
    for (auto c: overflow)
        CTEST << int(c) << ' ';
    CTEST << Endl;
#endif
    static const ui8 expected_data[54*3] = {
        // row1

        0x29,0x8f,0xdf,0xc0, // hash
        0x1, 0, 0, 0x20, // col1
        0x1, 0, 0, 0, 0, 0, 0, 0x10, // col2
        0x3, 0x61, 0x62, 0x63, 0, 0, 0, 0, 0, // vcol1
        0x3, 0x41, 0x42, 0x43, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // vcol2
        0xff, //NULL bitmap
        0x1, 0, 0, 0x40, // col3
        0x1, 0, 0, 0, 0, 0, 0, 0x30, // col4
        // row2
        0x42, 0xb2, 0x58, 0xc0, // hash
        0x2, 0, 0, 0x20, // col1
        0x2, 0, 0, 0, 0, 0, 0, 0x10, // col2
        0xff,  0, 0, 0, 0,  0xf, 0, 0, 0, // vcol1 [overflow offset, overflow size]
        0xf, 0x61, 0x62, 0x63, 0x64, 0x65, 0x66, 0x67, 0x68, 0x69, 0x6a, 0x6b, 0x6c, 0x6d, 0x6e, 0x6f, // vcol2
        0xff, // NULL bitmap
        0x2, 0, 0, 0x40, // col3
        0x2, 0, 0, 0, 0, 0, 0, 0x30, // col4
        // row3
        0xc3, 0xc9, 0xc4, 0x64, // hash
        0x3, 0, 0, 0x20, // col1
        0x3, 0, 0, 0, 0, 0, 0, 0x10, // col2
        0xff,  0xf, 0, 0, 0,  0xa, 0, 0, 0, // vcol1 [overflow offset, overflow size]
        0xa, 0x7a, 0x79, 0x78, 0x77, 0x76, 0x75, 0x74, 0x73, 0x70, 0x72,  0, 0, 0, 0, 0, // vcol2
        0xff, // NULL bitmap
        0x3, 0, 0, 0x40, // col3
        0x3, 0, 0, 0, 0, 0, 0, 0x30, // col4
    };
    static const ui8 expected_overflow[25] = {
        0x41, 0x42, 0x43, 0x44, 0x45, 0x46, 0x47, 0x48, 0x49, 0x4a, 0x4b, 0x4c, 0x4d, 0x4e, 0x4f,
        0x5a, 0x59, 0x58, 0x57, 0x56, 0x55, 0x54, 0x53, 0x50, 0x52,
    };

    UNIT_ASSERT_VALUES_EQUAL(sizeof(expected_data), tl->TotalRowSize*NTuples1);
    UNIT_ASSERT_VALUES_EQUAL(overflow.size(), sizeof(expected_overflow));
    for (ui32 i = 0; i < sizeof(expected_data); ++i)
        UNIT_ASSERT_VALUES_EQUAL(expected_data[i], res[i]);
    for (ui32 i = 0; i < sizeof(expected_overflow); ++i)
        UNIT_ASSERT_VALUES_EQUAL(expected_overflow[i], overflow[i]);


    UNIT_ASSERT_VALUES_EQUAL(resesData[0].size(), 0);
    UNIT_ASSERT_VALUES_EQUAL(overflowsData[0].size(), 0);
    UNIT_ASSERT_VALUES_EQUAL(resesData[2].size(), 0);
    UNIT_ASSERT_VALUES_EQUAL(overflowsData[2].size(), 0);

    UNIT_ASSERT_VALUES_EQUAL(resesData[1].size(), tl->TotalRowSize);
    UNIT_ASSERT_VALUES_EQUAL(overflowsData[1].size(), 10);
    for (ui32 i = 0; i < 54; ++i) if (!(i >= 17 && i <= 20))
        UNIT_ASSERT_VALUES_EQUAL(expected_data[2 * tl->TotalRowSize + i], resesData[1][i]);
    for (ui32 i = 0; i < 10; ++i)
        UNIT_ASSERT_VALUES_EQUAL(expected_overflow[15 + i], overflowsData[1][i]);

    UNIT_ASSERT_VALUES_EQUAL(resesData[3].size(), 2 * tl->TotalRowSize);
    UNIT_ASSERT_VALUES_EQUAL(overflowsData[3].size(), 15);
    for (ui32 i = 0; i < 2 * 54; ++i) if (!(i % 54 >= 17 && i % 54 <= 20))
        UNIT_ASSERT_VALUES_EQUAL(expected_data[0 * tl->TotalRowSize + i], resesData[3][i]);
    for (ui32 i = 0; i < 15; ++i)
        UNIT_ASSERT_VALUES_EQUAL(expected_overflow[0 + i], overflowsData[3][i]);
}

Y_UNIT_TEST(UnpackVarSize) {

    TScopedAlloc alloc(__LOCATION__);

    TColumnDesc kc1, kcv1, kcv2, kc2,  pc1, pc2;

    kc1.Role = EColumnRole::Key;
    kc1.DataSize = 8;

    kc2.Role = EColumnRole::Key;
    kc2.DataSize = 4;

    pc1.Role = EColumnRole::Payload;
    pc1.DataSize = 8;

    pc2.Role = EColumnRole::Payload;
    pc2.DataSize = 4;

    kcv1.Role = EColumnRole::Key;
    kcv1.DataSize = 8;
    kcv1.SizeType = EColumnSizeType::Variable;

    kcv2.Role = EColumnRole::Key;
    kcv2.DataSize = 16;
    kcv2.SizeType = EColumnSizeType::Variable;

    pc1.Role = EColumnRole::Payload;
    pc1.DataSize = 8;

    pc2.Role = EColumnRole::Payload;
    pc2.DataSize = 4;

    std::vector<TColumnDesc> columns{kc1, kc2, kcv1, kcv2, pc1, pc2};

    auto tl = TTupleLayout::Create(columns);
    CTEST << "TotalRowSize = " << tl->TotalRowSize << Endl;
    UNIT_ASSERT_VALUES_EQUAL(tl->TotalRowSize, 54);

    const ui64 NTuples1 = 3;

    const ui64 Tuples1DataBytes = (tl->TotalRowSize) * NTuples1;

    std::vector<ui64> col1(NTuples1, 0);
    std::vector<ui32> col2(NTuples1, 0);
    std::vector<ui64> col3(NTuples1, 0);
    std::vector<ui32> col4(NTuples1, 0);
    
    std::vector<ui32> vcol1(1, 0);
    std::vector<ui8> vcol1data;
    std::vector<ui32> vcol2(1, 0);
    std::vector<ui8> vcol2data;

    std::vector<ui8> res(Tuples1DataBytes + 64, 0);
    std::vector<TString> vcol1str {
        "abc",
        "ABCDEFGHIJKLMNO",
        "ZYXWVUTSPR"
    };
    std::vector<TString> vcol2str {
        "ABC",
        "abcdefghijklmno",
        "zyxwvutspr"
    };
    for (auto &&str: vcol1str) {
        for (auto c: str)
            vcol1data.push_back(c);
        vcol1.push_back(vcol1data.size());
    }
    UNIT_ASSERT_VALUES_EQUAL(vcol1.size(), NTuples1 + 1);
    for (auto &&str: vcol2str) {
        for (auto c: str)
            vcol2data.push_back(c);
        vcol2.push_back(vcol2data.size());
    }
    UNIT_ASSERT_VALUES_EQUAL(vcol2.size(), NTuples1 + 1);
    for (ui32 i = 0; i < NTuples1; ++i) {
        col1[i] = (1ull<<(sizeof(col1[0])*8 - 4)) + i + 1;
        col2[i] = (2ull<<(sizeof(col2[0])*8 - 4)) + i + 1;
        col3[i] = (3ull<<(sizeof(col3[0])*8 - 4)) + i + 1;
        col4[i] = (4ull<<(sizeof(col4[0])*8 - 4)) + i + 1;
    }

    const ui8* cols[4 + 2*2];

    cols[0] = (ui8*) col1.data();
    cols[1] = (ui8*) col2.data();
    cols[2] = (ui8*) vcol1.data();
    cols[3] = (ui8*) vcol1data.data();
    cols[4] = (ui8*) vcol2.data();
    cols[5] = (ui8*) vcol2data.data();
    cols[6] = (ui8*) col3.data();
    cols[7] = (ui8*) col4.data();
 
    std::vector<ui8, TMKQLAllocator<ui8>> overflow;
    std::vector<ui8> colValid((NTuples1 + 7)/8, ~0);
    const ui8 *colsValid[8] = {
            colValid.data(),
            nullptr,
            colValid.data(),
            nullptr,
            colValid.data(),
            nullptr,
            colValid.data(),
            colValid.data(),
    };

    tl->Pack(cols, colsValid, res.data(), overflow, 0, NTuples1);

    std::vector<ui64> col1_new(NTuples1, 0);
    std::vector<ui32> col2_new(NTuples1, 0);
    std::vector<ui64> col3_new(NTuples1, 0);
    std::vector<ui32> col4_new(NTuples1, 0);
    
    std::vector<ui32> vcol1_new(NTuples1 + 1, 0);
    std::vector<ui8>  vcol1data_new(vcol1data.size());
    std::vector<ui32> vcol2_new(NTuples1 + 1, 0);
    std::vector<ui8>  vcol2data_new(vcol2data.size());

    ui8* cols_new[4 + 2 * 2];
    cols_new[0] = (ui8*) col1_new.data();
    cols_new[1] = (ui8*) col2_new.data();
    cols_new[2] = (ui8*) vcol1_new.data();
    cols_new[3] = (ui8*) vcol1data_new.data();
    cols_new[4] = (ui8*) vcol2_new.data();
    cols_new[5] = (ui8*) vcol2data_new.data();
    cols_new[6] = (ui8*) col3_new.data();
    cols_new[7] = (ui8*) col4_new.data();

    std::vector<ui8> colValid1_new((NTuples1 + 7)/8, 0);
    colValid1_new.back() = ~0;
    std::vector<ui8> colValid2_new((NTuples1 + 7)/8, 0);
    colValid2_new.back() = ~0;
    std::vector<ui8> colValid3_new((NTuples1 + 7)/8, 0);
    colValid3_new.back() = ~0;
    std::vector<ui8> colValid4_new((NTuples1 + 7)/8, 0);
    colValid4_new.back() = ~0;
    std::vector<ui8> colValid5_new((NTuples1 + 7)/8, 0);
    colValid5_new.back() = ~0;
    std::vector<ui8> colValid6_new((NTuples1 + 7)/8, 0);
    colValid6_new.back() = ~0;

    ui8 *colsValid_new[8] = {
        colValid1_new.data(),
        colValid2_new.data(),
        colValid3_new.data(),
        nullptr,
        colValid4_new.data(),
        nullptr,
        colValid5_new.data(),
        colValid6_new.data(),
    };

    std::chrono::steady_clock::time_point begin02 = std::chrono::steady_clock::now();
    tl->Unpack(cols_new, colsValid_new, res.data(), overflow, 0, NTuples1);
    std::chrono::steady_clock::time_point end02 = std::chrono::steady_clock::now();
    ui64 microseconds = std::chrono::duration_cast<std::chrono::microseconds>(end02 - begin02).count();

    if (microseconds == 0)
        microseconds = 1;

    CTEST  << "Time for " << (NTuples1) << " transpose (external cycle)= " << microseconds  << "[microseconds]" << Endl;
#ifndef NDEBUG
    CTEST << "Result size = " << Tuples1DataBytes << Endl;
    CTEST << "Result = ";
    for (ui32 i = 0; i < Tuples1DataBytes; ++i)
        CTEST << int(res[i]) << ' ';
    CTEST << Endl;
    CTEST << "Overflow size = " << overflow.size() << Endl;
    CTEST << "Overflow = ";
    for (auto c: overflow)
        CTEST << int(c) << ' ';
    CTEST << Endl;
#endif

    UNIT_ASSERT(std::memcmp(cols[0], cols_new[0], sizeof(ui64) * col1.size()) == 0);
    UNIT_ASSERT(std::memcmp(cols[1], cols_new[1], sizeof(ui32) * col2.size()) == 0);
    UNIT_ASSERT(std::memcmp(cols[2], cols_new[2], sizeof(ui32) * vcol1.size()) == 0);
    UNIT_ASSERT(std::memcmp(cols[3], cols_new[3], vcol1data.size()) == 0);
    UNIT_ASSERT(std::memcmp(cols[4], cols_new[4], sizeof(ui32) * vcol2.size()) == 0);
    UNIT_ASSERT(std::memcmp(cols[5], cols_new[5], vcol1data.size()) == 0);
    UNIT_ASSERT(std::memcmp(cols[6], cols_new[6], sizeof(ui64) * col3.size()) == 0);
    UNIT_ASSERT(std::memcmp(cols[7], cols_new[7], sizeof(ui32) * col4.size()) == 0);

    UNIT_ASSERT(std::memcmp(colValid.data(), colValid1_new.data(), colValid.size()) == 0);
    UNIT_ASSERT(std::memcmp(colValid.data(), colValid2_new.data(), colValid.size()) == 0);
    UNIT_ASSERT(std::memcmp(colValid.data(), colValid3_new.data(), colValid.size()) == 0);
    UNIT_ASSERT(std::memcmp(colValid.data(), colValid4_new.data(), colValid.size()) == 0);
    UNIT_ASSERT(std::memcmp(colValid.data(), colValid5_new.data(), colValid.size()) == 0);
    UNIT_ASSERT(std::memcmp(colValid.data(), colValid6_new.data(), colValid.size()) == 0);
}

Y_UNIT_TEST(PackVarSizeBig) {

    TScopedAlloc alloc(__LOCATION__);

    TColumnDesc kc1, kc2, kcv1;

    kc1.Role = EColumnRole::Key;
    kc1.DataSize = 1;

    kc2.Role = EColumnRole::Key;
    kc2.DataSize = 2;

    kcv1.Role = EColumnRole::Key;
    kcv1.DataSize = 1000;
    kcv1.SizeType = EColumnSizeType::Variable;

    std::vector<TColumnDesc> columns{kc1, kc2, kcv1 };

    auto tl = TTupleLayout::Create(columns);
    //CTEST << "TotalRowSize = " << tl->TotalRowSize << Endl;
    UNIT_ASSERT_VALUES_EQUAL(tl->TotalRowSize, 263);

    const ui64 NTuples1 = 2;

    const ui64 Tuples1DataBytes = (tl->TotalRowSize) * NTuples1;

    std::vector<ui8> col1(NTuples1, 0);
    std::vector<ui16> col2(NTuples1, 0);

    std::vector<ui32> vcol1(1, 0);

    std::vector<ui8> vcol1data;

    std::vector<ui8> res(Tuples1DataBytes + 64, 0);
    std::vector<TString> vcol1str {
        "zaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaabbb"
        "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbabcdefghijklnmorstuvwxy",
        "zaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaabbb"
        "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbrstuv",
    };
    for (auto &&str: vcol1str) {
        for (auto c: str)
            vcol1data.push_back(c);
        vcol1.push_back(vcol1data.size());
    }
    UNIT_ASSERT_VALUES_EQUAL(vcol1.size(), NTuples1 + 1);
    for (ui32 i = 0; i < NTuples1; ++i) {
        col1[i] = (1ull<<(sizeof(col1[0])*8 - 4)) + i + 1;
        col2[i] = (2ull<<(sizeof(col2[0])*8 - 4)) + i + 1;
    }

    const ui8* cols[2 + 1*2];

    cols[0] = (ui8*) col1.data();
    cols[1] = (ui8*) col2.data();
    cols[2] = (ui8*) vcol1.data();
    cols[3] = (ui8*) vcol1data.data();

    std::vector<ui8> colValid((NTuples1 + 7)/8, ~0);
    std::vector<ui8> colInvalid((NTuples1 + 7)/8, 0);
    const ui8 *colsValid[2 + 1*2] = {
            nullptr,
            colInvalid.data(),
            colValid.data(),
            nullptr,
    };
    std::vector<ui8, TMKQLAllocator<ui8>> overflow;

    std::chrono::steady_clock::time_point begin02 = std::chrono::steady_clock::now();
    tl->Pack(cols, colsValid, res.data(), overflow, 0, NTuples1);
    std::chrono::steady_clock::time_point end02 = std::chrono::steady_clock::now();
    ui64 microseconds = std::chrono::duration_cast<std::chrono::microseconds>(end02 - begin02).count();

    CTEST  << "Time for " << (NTuples1) << " transpose (external cycle)= " << microseconds  << "[microseconds]" << Endl;
#ifndef NDEBUG
    CTEST << "Result size = " << Tuples1DataBytes << Endl;
    CTEST << "Result = ";
    for (ui32 i = 0; i < Tuples1DataBytes; ++i)
        CTEST << int(res[i]) << ' ';
    CTEST << Endl;
    CTEST << "Overflow size = " << overflow.size() << Endl;
    CTEST << "Overflow = ";
    for (auto c: overflow)
        CTEST << int(c) << ' ';
    CTEST << Endl;
#endif
    static const ui8 expected_data[263*2] = {
            // row1
            0x27,0xd1,0xce,0x49, // hash
            0x11, // col1
            0x1, 0x20, // col2
            0xff,  0, 0, 0, 0,   0xb, 0, 0, 0, // vcol2 [ overflow offset, overflow size ]
            0x7a, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
            0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
            0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
            0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
            0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
            0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
            0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
            0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
            0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
            0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
            0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
            0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
            0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
            0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
            0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
            0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
            0x61, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62,
            0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62,
            0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62,
            0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62,
            0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62,
            0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62,
            0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62,
            0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62,
            0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62,
            0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62,
            0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62,
            0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62,
            0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62,
            0x62, 0x62, 0x61, 0x62, 0x63, 0x64, 0x65, 0x66,
            0x67, 0x68, 0x69, 0x6a, 0x6b, 0x6c,
            ui8(~0x2), // NULL bitmap
            // row 2
            0x96,0x27,0x88,0xaa, // hash
            0x12, // col1
            0x2, 0x20, // col2
            0xfe, 0x7a, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
            0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
            0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
            0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
            0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
            0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
            0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
            0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
            0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
            0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
            0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
            0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
            0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
            0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
            0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
            0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61,
            0x61, 0x61, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62,
            0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62,
            0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62,
            0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62,
            0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62,
            0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62,
            0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62,
            0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62,
            0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62,
            0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62,
            0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62,
            0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62,
            0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62,
            0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62,
            0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62, 0x62,
            0x62, 0x62, 0x72, 0x73, 0x74, 0x75, 0x76,
            ui8(~0x2), // NULLs bitmap
    };
    static const ui8 expected_overflow[11] = {
            0x6e, 0x6d, 0x6f, 0x72, 0x73, 0x74, 0x75, 0x76, 0x77, 0x78, 0x79,
    };
    UNIT_ASSERT_VALUES_EQUAL(sizeof(expected_data), tl->TotalRowSize*NTuples1);
    UNIT_ASSERT_VALUES_EQUAL(overflow.size(), sizeof(expected_overflow));
    for (ui32 i = 0; i < sizeof(expected_data); ++i)
        UNIT_ASSERT_VALUES_EQUAL(expected_data[i], res[i]);
    for (ui32 i = 0; i < sizeof(expected_overflow); ++i)
        UNIT_ASSERT_VALUES_EQUAL(expected_overflow[i], overflow[i]);
}
Y_UNIT_TEST(PackIsValidFuzz) {

    TScopedAlloc alloc(__LOCATION__);

    std::mt19937 rng; // fixed-seed (0) prng
    std::vector<TColumnDesc> columns;
    std::vector<std::vector<ui8>> colsdata;
    std::vector<const ui8*> colsptr;
    std::vector<std::vector<ui8>> isValidData;
    std::vector<const ui8*> isValidPtr;

    ui64 totalNanoseconds = 0;
    ui64 totalSize = 0;
    ui64 totalRows = 0;
    for (ui32 test = 0; test < 10; ++test) {
        ui32 rows = 1 + (rng() % 1000);
        ui32 cols = 1 + (rng() % 100);
        columns.resize(cols);
        colsdata.resize(cols);
        colsptr.resize(cols);
        isValidData.resize(cols);
        isValidPtr.resize(cols);
        ui32 isValidSize = (rows + 7)/8;
        totalRows += rows;
        for (ui32 j = 0; j < cols; ++j) {
            auto &col = columns[j];
            col.Role = (rng() % 10 < 1) ? EColumnRole::Key : EColumnRole::Payload;
            col.DataSize = 1u <<(rng() % 16);
            col.SizeType = EColumnSizeType::Fixed;
            colsdata[j].resize(rows*col.DataSize);
            colsptr[j] = colsdata[j].data();
            isValidData[j].resize(isValidSize);
            isValidPtr[j] = isValidData[j].data();
            std::generate(isValidData[j].begin(), isValidData[j].end(), rng);
        }
        auto tl = TTupleLayout::Create(columns);
        std::vector<ui8> res;
        for (ui32 subtest = 0; subtest < 20; ++subtest) {
            ui32 subRows = 1 + (rows ? rng() % (rows - 1) : 0);
            ui32 off = subRows != rows ? rng() % (rows - subRows) : 0;
            std::vector<ui8, TMKQLAllocator<ui8>> overflow;
            totalSize += subRows*tl->TotalRowSize;
            res.resize(subRows*tl->TotalRowSize);

            std::chrono::steady_clock::time_point begin01 = std::chrono::steady_clock::now();
            tl->Pack(colsptr.data(), isValidPtr.data(), res.data(), overflow, off, subRows);
            std::chrono::steady_clock::time_point end01 = std::chrono::steady_clock::now();
            totalNanoseconds += std::chrono::duration_cast<std::chrono::nanoseconds>(end01 - begin01).count();

            UNIT_ASSERT_VALUES_EQUAL(overflow.size(), 0);
            auto resptr = res.data();
            for (ui32 row = 0; row < subRows; ++row, resptr += tl->TotalRowSize) {
                for (ui32 j = 0; j < cols; ++j) {
                    auto &col = tl->Columns[j];
                    UNIT_ASSERT_VALUES_EQUAL(((resptr[tl->BitmaskOffset + (j / 8)] >> (j % 8)) & 1), ((isValidData[col.OriginalIndex][(off + row) / 8] >> ((off + row) % 8)) & 1));
                }
            }
        }
    }

    if (totalNanoseconds == 0) totalNanoseconds = 1;

    CTEST  << "Time for " << totalRows << " transpose (external cycle)= " << (totalNanoseconds + 999)/1000  << "[microseconds]" << Endl;
    CTEST  << "Data size =  " << totalSize / (1024 * 1024) << "[MB]" << Endl;
    CTEST  << "Calculating speed = " << totalSize / ((totalNanoseconds + 999)/1000) << "MB/sec" << Endl;
    CTEST  << Endl;
}
}



}
} // namespace NMiniKQL
} // namespace NKikimr
