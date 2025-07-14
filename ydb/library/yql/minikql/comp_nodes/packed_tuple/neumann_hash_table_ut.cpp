#include <yql/essentials/minikql/mkql_runtime_version.h>
#include <yql/essentials/minikql/comp_nodes/ut/mkql_computation_node_ut.h>
#include <library/cpp/testing/unittest/registar.h>

#include <chrono>
#include <vector>
#include <random>

#include <util/system/fs.h>
#include <util/system/compiler.h>
#include <util/stream/null.h>
#include <util/system/mem_info.h>

#include <ydb/library/yql/minikql/comp_nodes/packed_tuple/neumann_hash_table.h>
#include <ydb/library/yql/minikql/comp_nodes/packed_tuple/tuple.h>

namespace NKikimr {
namespace NMiniKQL {
namespace NPackedTuple {

using namespace std::chrono_literals;

static volatile bool IsVerbose = false;
#define CTEST (IsVerbose ? Cerr : Cnull)

namespace {

std::vector<std::pair<ui32, const ui8*>> GetRandomTuples(ui32 count, ui32 nTuples, const ui8* data, const TTupleLayout* layout) {
    std::mt19937_64 rng;
    std::vector<ui64> indexes(count);
    std::vector<std::pair<ui32, const ui8*>> result;
    std::generate(indexes.begin(), indexes.end(), rng);
    for (auto& index: indexes) {
        index %= nTuples;
    }

    for (auto index: indexes) {
        auto tuple = data + index * layout->TotalRowSize;
        result.emplace_back(ReadUnaligned<ui32>(tuple), tuple + layout->KeyColumnsOffset);
    }

    return result;
}

// -----------------------------------------------------------------
ui64 TUPLE_COUNTER = 0;

Y_FORCE_INLINE void IncCounter(const ui8* tuple) {
    if (tuple != nullptr) {
        TUPLE_COUNTER++;
    }
}

ui8* KEY_TO_CHECK = nullptr;
ui64 KEY_LEN = 0;
ui64 KEY_OFFSET = 0;
bool WAS_FOUND = false;

Y_FORCE_INLINE void CheckEq(const ui8* tuple) {
    if (std::equal(KEY_TO_CHECK, KEY_TO_CHECK + KEY_LEN, tuple + KEY_OFFSET)) {
        WAS_FOUND = true;
    }
}

} // namespace

// -----------------------------------------------------------------
Y_UNIT_TEST_SUITE(NeumannHashTable) {

Y_UNIT_TEST(CreateNeumannHashTable) {
    TScopedAlloc alloc(__LOCATION__);

    TColumnDesc kc1, kc2, pc1, pc2, pc3;

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
    auto ht = TNeumannHashTable(tl.Get());
} // Y_UNIT_TEST(CreateNeumannHashTable)

Y_UNIT_TEST(ApplyNeumannHashTable) {
    TScopedAlloc alloc(__LOCATION__);

    TColumnDesc kc1, kc2, pc1, pc2;

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
    const ui64 NTuples1 = 100;

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

    auto ht = TNeumannHashTable(tl.Get());

    ht.Build(res.data(), overflow.data(), NTuples1);

    auto tuple = res.data() + 42 /* index */ * tl->TotalRowSize;
    KEY_TO_CHECK = tuple + tl->KeyColumnsOffset;
    KEY_OFFSET = tl->KeyColumnsOffset;
    KEY_LEN = tl->KeyColumnsSize;

    ht.Apply(tuple, overflow.data(), CheckEq);

    UNIT_ASSERT(WAS_FOUND == true);
} // Y_UNIT_TEST(CreateNeumannHashTable)

Y_UNIT_TEST(BenchNeumannHashTable_SmallTuple) { // Tuple size <= 16
    TScopedAlloc alloc(__LOCATION__);

    TColumnDesc kc1, pc1;

    kc1.Role = EColumnRole::Key;
    kc1.DataSize = 4;

    pc1.Role = EColumnRole::Payload;
    pc1.DataSize = 4;

    std::vector<TColumnDesc> columns{kc1, pc1};

    auto tl = TTupleLayout::Create(columns);

    const ui64 NTuples1 = 10e6;

    const ui64 Tuples1DataBytes = (tl->TotalRowSize) * NTuples1;

    std::vector<ui32> col1(NTuples1, 0);
    std::vector<ui32> col2(NTuples1, 0);

    std::vector<ui8> res(Tuples1DataBytes + 64, 0);

    for (ui32 i = 0; i < NTuples1; ++i) {
        col1[i] = i;
        col2[i] = i;
    }

    const ui8* cols[2];

    cols[0] = (ui8*) col1.data();
    cols[1] = (ui8*) col2.data();

    std::vector<ui8> colValid1((NTuples1 + 7)/8, ~0);
    std::vector<ui8> colValid2((NTuples1 + 7)/8, ~0);
    const ui8 *colsValid[2] = {
        colValid1.data(),
        colValid2.data(),
    };

    std::vector<ui8, TMKQLAllocator<ui8>> overflow;
    tl->Pack(cols, colsValid, res.data(), overflow, 0, NTuples1);

    auto NSelected = NTuples1 / 10;
    auto NSelectedBytes = NSelected * tl->TotalRowSize;
    auto probes = GetRandomTuples(NSelected, NTuples1, res.data(), tl.Get());
    auto ht = TNeumannHashTable(tl.Get());

    TUPLE_COUNTER = 0;
    std::chrono::steady_clock::time_point buildBegin = std::chrono::steady_clock::now();
    ht.Build(res.data(), overflow.data(), NTuples1);
    std::chrono::steady_clock::time_point buildEnd = std::chrono::steady_clock::now();
    ui64 buildUs = std::chrono::duration_cast<std::chrono::microseconds>(buildEnd - buildBegin).count();
    if (buildUs == 0) buildUs = 1;

    TUPLE_COUNTER = 0;
    std::chrono::steady_clock::time_point singleFindBegin = std::chrono::steady_clock::now();
    {
        auto [hash, key] = probes.front();
        ht.Apply(key - 4, overflow.data(), IncCounter);
    }
    std::chrono::steady_clock::time_point singleFindEnd = std::chrono::steady_clock::now();
    ui64 singleFindNs = std::chrono::duration_cast<std::chrono::nanoseconds>(singleFindEnd - singleFindBegin).count();
    if (singleFindNs == 0) singleFindNs = 1;
    UNIT_ASSERT(TUPLE_COUNTER == 1);

    TUPLE_COUNTER = 0;
    std::chrono::steady_clock::time_point findBegin = std::chrono::steady_clock::now();
    for (auto [hash, key]: probes) {
        ht.Apply(key - 4, overflow.data(), IncCounter);
    }
    std::chrono::steady_clock::time_point findEnd = std::chrono::steady_clock::now();
    ui64 findUs = std::chrono::duration_cast<std::chrono::microseconds>(findEnd - findBegin).count();
    if (findUs == 0) findUs = 1;
    UNIT_ASSERT(TUPLE_COUNTER == NSelected);

    CTEST << "[Small] Build time for " << NTuples1 << " tuples = " << buildUs << "[microseconds]" << Endl;
    CTEST << "[Small] Data size = " << Tuples1DataBytes / (1024 * 1024) << "[MB]" << Endl;
    CTEST << "[Small] Processing speed = " << Tuples1DataBytes / buildUs << "MB/sec" << Endl;
    CTEST << Endl;
    CTEST << "[Small] Single find time = " << singleFindNs << "[nanoseconds]" << Endl;
    CTEST << Endl;
    CTEST << "[Small] Massive find time for " << NSelected << " attempts = " << findUs << "[microseconds]" << Endl;
    CTEST << "[Small] Selected data size = " << NSelectedBytes / (1024 * 1024) << "[MB]" << Endl;
    CTEST << "[Small] Processing speed = " << NSelectedBytes / (findUs) << "MB/sec" << Endl;
    CTEST << "-------------------" << Endl;
    CTEST << Endl;
} // Y_UNIT_TEST(BenchNeumannHashTable_SmallTuple)

Y_UNIT_TEST(BenchNeumannHashTable_LargeTuple) { // Tuple size > 16
    TScopedAlloc alloc(__LOCATION__);

    TColumnDesc kc1, kc2, pc1, pc2;

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

    auto NSelected = NTuples1 / 10;
    auto NSelectedBytes = NSelected * tl->TotalRowSize;
    auto probes = GetRandomTuples(NSelected, NTuples1, res.data(), tl.Get());
    auto ht = TNeumannHashTable(tl.Get());

    TUPLE_COUNTER = 0;
    std::chrono::steady_clock::time_point buildBegin = std::chrono::steady_clock::now();
    ht.Build(res.data(), overflow.data(), NTuples1);
    std::chrono::steady_clock::time_point buildEnd = std::chrono::steady_clock::now();
    ui64 buildUs = std::chrono::duration_cast<std::chrono::microseconds>(buildEnd - buildBegin).count();
    if (buildUs == 0) buildUs = 1;

    TUPLE_COUNTER = 0;
    std::chrono::steady_clock::time_point singleFindBegin = std::chrono::steady_clock::now();
    {
        auto [hash, key] = probes.front();
        ht.Apply(key - 4, overflow.data(), IncCounter);
    }
    std::chrono::steady_clock::time_point singleFindEnd = std::chrono::steady_clock::now();
    ui64 singleFindNs = std::chrono::duration_cast<std::chrono::nanoseconds>(singleFindEnd - singleFindBegin).count();
    if (singleFindNs == 0) singleFindNs = 1;
    UNIT_ASSERT(TUPLE_COUNTER == 1);

    TUPLE_COUNTER = 0;
    std::chrono::steady_clock::time_point findBegin = std::chrono::steady_clock::now();
    for (auto [hash, key]: probes) {
        ht.Apply(key - 4, overflow.data(), IncCounter);
    }
    std::chrono::steady_clock::time_point findEnd = std::chrono::steady_clock::now();
    ui64 findUs = std::chrono::duration_cast<std::chrono::microseconds>(findEnd - findBegin).count();
    if (findUs == 0) findUs = 1;
    UNIT_ASSERT(TUPLE_COUNTER == NSelected);

    CTEST << "[Large] Build time for " << NTuples1 << " tuples = " << buildUs << "[microseconds]" << Endl;
    CTEST << "[Large] Data size = " << Tuples1DataBytes / (1024 * 1024) << "[MB]" << Endl;
    CTEST << "[Large] Processing speed = " << Tuples1DataBytes / buildUs << "MB/sec" << Endl;
    CTEST << Endl;
    CTEST << "[Large] Single find time = " << singleFindNs << "[nanoseconds]" << Endl;
    CTEST << Endl;
    CTEST << "[Large] Massive find time for " << NSelected << " attempts = " << findUs << "[microseconds]" << Endl;
    CTEST << "[Large] Selected data size = " << NSelectedBytes / (1024 * 1024) << "[MB]" << Endl;
    CTEST << "[Large] Processing speed = " << NSelectedBytes / (findUs) << "MB/sec" << Endl;
    CTEST << "-------------------" << Endl;
    CTEST << Endl;
} // Y_UNIT_TEST(BenchNeumannHashTable_LargeTuple)

Y_UNIT_TEST(BenchNeumannHashTable_VarTuple){

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

    TNeumannHashTable ht(tl.Get());

    ht.Build(res.data(), overflow.data(), NTuples1, 18);

    {
        int result = 0;
        auto onMatchCall = [&result] (const ui8* row) {
            ++result;
            CTEST << "[Var]  - found: " << *(ui32*)row << ' ' << (int)row[16] << Endl;
        };

        ui8 inv_row[60];
        std::memcpy(inv_row, res.data(), sizeof(inv_row));
        inv_row[17] = 'z';

        ht.Apply(inv_row, overflow.data(), onMatchCall);
        CTEST << "[Var] HT nonpresent key search: " << (result == 0 ? "OK" : "BAD")  << Endl;

        CTEST << "[Var] KEY: " << *(ui32*)res.data() << ' ' << (int)res[16] << Endl;
        result = 0;
        ht.Apply(res.data(), overflow.data(), onMatchCall);
        CTEST << "[Var] HT present key search: " << (result > 0 ? "OK" : "BAD")  << Endl;

        CTEST << "[Var] KEY: " << *(ui32*)(res.data() + tl->TotalRowSize) << ' ' << (int)(res.data() + tl->TotalRowSize)[16] << Endl;
        result = 0;
        ht.Apply(res.data() + tl->TotalRowSize, overflow.data(), onMatchCall);
        CTEST << "[Var] HT present key search: " << (result > 0 ? "OK" : "BAD")  << Endl;

        CTEST << "[Var] KEY: " << *(ui32*)(res.data() + 2 * tl->TotalRowSize) << ' ' << (int)(res.data() + 2 * tl->TotalRowSize)[16] << Endl;
        result = 0;
        ht.Apply(res.data() + 2 * tl->TotalRowSize, overflow.data(), onMatchCall);
        CTEST << "[Var] HT present key search: " << (result > 0 ? "OK" : "BAD")  << Endl;

        CTEST << "-------------------" << Endl;
    }
}

} // Y_UNIT_TEST_SUITE(NeumannHashTable)


}
} // namespace NMiniKQL
} // namespace NKikimr
