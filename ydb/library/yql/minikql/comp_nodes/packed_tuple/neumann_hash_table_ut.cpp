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

static volatile bool IsVerbose = true;
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

    ht.Build(res.data(), NTuples1);

    auto tuple = res.data() + 42 /* index */ * tl->TotalRowSize;
    auto hash = ReadUnaligned<ui32>(tuple);
    KEY_TO_CHECK = tuple + tl->KeyColumnsOffset;
    KEY_OFFSET = tl->KeyColumnsOffset;
    KEY_LEN = tl->KeyColumnsSize;

    ht.Apply(hash, KEY_TO_CHECK, CheckEq);

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
    ht.Build(res.data(), NTuples1);
    std::chrono::steady_clock::time_point buildEnd = std::chrono::steady_clock::now();
    ui64 buildUs = std::chrono::duration_cast<std::chrono::microseconds>(buildEnd - buildBegin).count();
    if (buildUs == 0) buildUs = 1;

    TUPLE_COUNTER = 0;
    std::chrono::steady_clock::time_point singleFindBegin = std::chrono::steady_clock::now();
    {
        auto [hash, key] = probes.front();
        ht.Apply(hash, key, IncCounter);
    }
    std::chrono::steady_clock::time_point singleFindEnd = std::chrono::steady_clock::now();
    ui64 singleFindNs = std::chrono::duration_cast<std::chrono::nanoseconds>(singleFindEnd - singleFindBegin).count();
    if (singleFindNs == 0) singleFindNs = 1;
    UNIT_ASSERT(TUPLE_COUNTER == 1);

    TUPLE_COUNTER = 0;
    std::chrono::steady_clock::time_point findBegin = std::chrono::steady_clock::now();
    for (auto [hash, key]: probes) {
        ht.Apply(hash, key, IncCounter);
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
    ht.Build(res.data(), NTuples1);
    std::chrono::steady_clock::time_point buildEnd = std::chrono::steady_clock::now();
    ui64 buildUs = std::chrono::duration_cast<std::chrono::microseconds>(buildEnd - buildBegin).count();
    if (buildUs == 0) buildUs = 1;

    TUPLE_COUNTER = 0;
    std::chrono::steady_clock::time_point singleFindBegin = std::chrono::steady_clock::now();
    {
        auto [hash, key] = probes.front();
        ht.Apply(hash, key, IncCounter);
    }
    std::chrono::steady_clock::time_point singleFindEnd = std::chrono::steady_clock::now();
    ui64 singleFindNs = std::chrono::duration_cast<std::chrono::nanoseconds>(singleFindEnd - singleFindBegin).count();
    if (singleFindNs == 0) singleFindNs = 1;
    UNIT_ASSERT(TUPLE_COUNTER == 1);

    TUPLE_COUNTER = 0;
    std::chrono::steady_clock::time_point findBegin = std::chrono::steady_clock::now();
    for (auto [hash, key]: probes) {
        ht.Apply(hash, key, IncCounter);
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

} // Y_UNIT_TEST_SUITE(NeumannHashTable)


}
} // namespace NMiniKQL
} // namespace NKikimr
