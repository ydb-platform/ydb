#include <ydb/library/yql/minikql/mkql_runtime_version.h>
#include <ydb/library/yql/minikql/comp_nodes/ut/mkql_computation_node_ut.h>
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

#include <ydb/library/yql/minikql/comp_nodes/mkql_rh_hash.h>

namespace NKikimr {
namespace NMiniKQL {
namespace NPackedTuple {

using namespace std::chrono_literals;

static volatile bool IsVerbose = false;
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

    std::vector<ui8> res(Tuples1DataBytes + 64, 0);

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

    std::chrono::steady_clock::time_point begin02 = std::chrono::steady_clock::now();

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
    std::chrono::steady_clock::time_point end02 = std::chrono::steady_clock::now();
    ui64 microseconds = std::chrono::duration_cast<std::chrono::microseconds>(end02 - begin02).count();
    if (microseconds == 0) microseconds = 1;

    CTEST  << "Time for " << (NTuples1) << " transpose (external cycle)= " << microseconds  << "[microseconds]" << Endl;
    CTEST  << "Data size =  " << Tuples1DataBytes / (1024 * 1024) << "[MB]" << Endl;
    CTEST  << "Calculating speed = " << Tuples1DataBytes / microseconds << "MB/sec" << Endl;
    CTEST  << Endl;

    UNIT_ASSERT(true);

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

    std::chrono::steady_clock::time_point begin02 = std::chrono::steady_clock::now();
    tl->Pack(cols, colsValid, res.data(), overflow, 0, NTuples1);
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
    static const ui8 expected_data[54*3] = {
        // row1

        0xe2,0x47,0x16,0x6c, // hash
        0x1, 0, 0, 0x20, // col1
        0x1, 0, 0, 0, 0, 0, 0, 0x10, // col2
        0x3, 0x61, 0x62, 0x63, 0, 0, 0, 0, 0, // vcol1
        0x3, 0x41, 0x42, 0x43, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // vcol2
        0x3f, //NULL bitmap
        0x1, 0, 0, 0x40, // col3
        0x1, 0, 0, 0, 0, 0, 0, 0x30, // col4
        // row2
        0xc2, 0x1c, 0x1b, 0xa8, // hash
        0x2, 0, 0, 0x20, // col1
        0x2, 0, 0, 0, 0, 0, 0, 0x10, // col2
        0xff,  0, 0, 0, 0,  0xf, 0, 0, 0, // vcol1 [overflow offset, overflow size]
        0xf, 0x61, 0x62, 0x63, 0x64, 0x65, 0x66, 0x67, 0x68, 0x69, 0x6a, 0x6b, 0x6c, 0x6d, 0x6e, 0x6f, // vcol2
        0x3f, // NULL bitmap
        0x2, 0, 0, 0x40, // col3
        0x2, 0, 0, 0, 0, 0, 0, 0x30, // col4
        // row3
        0xfa, 0x49, 0x5, 0xe9, // hash
        0x3, 0, 0, 0x20, // col1
        0x3, 0, 0, 0, 0, 0, 0, 0x10, // col2
        0xff,  0xf, 0, 0, 0,  0xa, 0, 0, 0, // vcol1 [overflow offset, overflow size]
        0xa, 0x7a, 0x79, 0x78, 0x77, 0x76, 0x75, 0x74, 0x73, 0x70, 0x72,  0, 0, 0, 0, 0, // vcol2
        0x3f, // NULL bitmap
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
    const ui8 *colsValid[2 + 1*2] = {
            colValid.data(),
            colValid.data(),
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
            0xe1,0x22,0x63,0xf5, // hash
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
            0x7, // NULL bitmap
            // row 2
            0xab,0xa5,0x5f,0xd4, // hash
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
            0x7, // NULLs bitmap
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
