// TODO: POSSIBLY DEPRECATED AND NEED TO BE DELETED DUE TO PERFORMANCE ISSUES
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

#include <ydb/library/yql/minikql/comp_nodes/packed_tuple/page_hash_table.h>
#include <ydb/library/yql/minikql/comp_nodes/packed_tuple/tuple.h>

namespace NKikimr {
namespace NMiniKQL {
namespace NPackedTuple {

using namespace std::chrono_literals;

static volatile bool IsVerbose = false;
#define CTEST (IsVerbose ? Cerr : Cnull)

namespace {

std::vector<ui8> GetRandomTuples(ui32 count, ui32 nTuples, const ui8* data, const TTupleLayout* layout) {
    std::mt19937 rng(std::random_device{}());
    std::vector<ui32> indexes(count);
    std::generate(indexes.begin(), indexes.end(), rng);

    std::vector<ui8> result(count * layout->TotalRowSize, 0);
    for (ui32 i = 0; i < indexes.size(); ++i) {
        auto index = indexes[i] % nTuples;
        auto tuple = data + index * layout->TotalRowSize;
        std::memcpy(result.data() + i * layout->TotalRowSize, tuple, layout->TotalRowSize);
    }

    return result;
}

std::vector<ui8> GetNonExistentTuples(ui32 count, ui32 nTuples, const ui8* data, const TTupleLayout* layout) {
    std::mt19937 rng(std::random_device{}());
    std::unordered_set<ui32> hashes;
    for (ui32 i = 0; i < nTuples; ++i) {
        auto tuple = data + i * layout->TotalRowSize;
        hashes.insert(ReadUnaligned<ui32>(tuple));
    }

    std::vector<ui8> result(count * layout->TotalRowSize, 1);
    ui32 counter = 0;
    ui32 nextHash = 0;
    while (counter < count) {
        if (!hashes.count(nextHash)) {
            WriteUnaligned<ui8>(result.data() + counter * layout->TotalRowSize, 0);
            ++counter;
        }
        nextHash = rng();
    }

    return result;
}

} // namespace

// -----------------------------------------------------------------
Y_UNIT_TEST_SUITE(PageHashTable) {

Y_UNIT_TEST(Create) {
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
    auto ht = TPageHashTable::Create(tl.Get(), 100);

    UNIT_ASSERT(ht.Get() != nullptr);
} // Y_UNIT_TEST(Create)

Y_UNIT_TEST(BenchBuild) {
    TScopedAlloc alloc(__LOCATION__);

    TColumnDesc kc1, pc1;

    kc1.Role = EColumnRole::Key;
    kc1.DataSize = 4;

    pc1.Role = EColumnRole::Payload;
    pc1.DataSize = 4;

    std::vector<TColumnDesc> columns{kc1, pc1};

    auto tl = TTupleLayout::Create(columns);

    const ui64 NTuples1 = 128e5;

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

    CTEST << "=============\nBench Build\n=============" << Endl;
    {
        ui64 tuples = 5e3;
        ui64 bytes = (tl->TotalRowSize) * tuples;

        std::chrono::steady_clock::time_point createBegin = std::chrono::steady_clock::now();
        auto ht = TPageHashTable::Create(tl.Get(), tuples);
        std::chrono::steady_clock::time_point createEnd = std::chrono::steady_clock::now();
        ui64 createUs = std::chrono::duration_cast<std::chrono::microseconds>(createEnd - createBegin).count();
        if (createUs == 0) createUs = 1;

        std::chrono::steady_clock::time_point buildBegin = std::chrono::steady_clock::now();
        ht->Build(res.data(), overflow.data(), tuples);
        std::chrono::steady_clock::time_point buildEnd = std::chrono::steady_clock::now();
        ui64 buildUs = std::chrono::duration_cast<std::chrono::microseconds>(buildEnd - buildBegin).count();
        if (buildUs == 0) buildUs = 1;

        CTEST << "[<L1] Data size = " << bytes / 1024 << "[KB]" << Endl;
        CTEST << "[<L1] Create time for " << tuples << " tuples = " << createUs << "[microseconds]" << Endl;
        CTEST << "[<L1] Create speed = " << bytes / createUs << "[MB/sec]" << Endl;
        CTEST << "[<L1] Build time for " << tuples << " tuples = " << buildUs << "[microseconds]" << Endl;
        CTEST << "[<L1] Build speed = " << bytes / buildUs << "[MB/sec]" << Endl;
        CTEST << "-------------------" << Endl;
    }

    {
        ui64 tuples = 8e4;
        ui64 bytes = (tl->TotalRowSize) * tuples;

        std::chrono::steady_clock::time_point createBegin = std::chrono::steady_clock::now();
        auto ht = TPageHashTable::Create(tl.Get(), tuples);
        std::chrono::steady_clock::time_point createEnd = std::chrono::steady_clock::now();
        ui64 createUs = std::chrono::duration_cast<std::chrono::microseconds>(createEnd - createBegin).count();
        if (createUs == 0) createUs = 1;

        std::chrono::steady_clock::time_point buildBegin = std::chrono::steady_clock::now();
        ht->Build(res.data(), overflow.data(), tuples);
        std::chrono::steady_clock::time_point buildEnd = std::chrono::steady_clock::now();
        ui64 buildUs = std::chrono::duration_cast<std::chrono::microseconds>(buildEnd - buildBegin).count();
        if (buildUs == 0) buildUs = 1;

        CTEST << "[<L2] Data size = " << bytes / 1024 << "[KB]" << Endl;
        CTEST << "[<L2] Create time for " << tuples << " tuples = " << createUs << "[microseconds]" << Endl;
        CTEST << "[<L2] Create speed = " << bytes / createUs << "[MB/sec]" << Endl;
        CTEST << "[<L2] Build time for " << tuples << " tuples = " << buildUs << "[microseconds]" << Endl;
        CTEST << "[<L2] Build speed = " << bytes / buildUs << "[MB/sec]" << Endl;
        CTEST << "-------------------" << Endl;
    }

    {
        ui64 tuples = 4e5;
        ui64 bytes = (tl->TotalRowSize) * tuples;

        std::chrono::steady_clock::time_point createBegin = std::chrono::steady_clock::now();
        auto ht = TPageHashTable::Create(tl.Get(), tuples);
        std::chrono::steady_clock::time_point createEnd = std::chrono::steady_clock::now();
        ui64 createUs = std::chrono::duration_cast<std::chrono::microseconds>(createEnd - createBegin).count();
        if (createUs == 0) createUs = 1;

        std::chrono::steady_clock::time_point buildBegin = std::chrono::steady_clock::now();
        ht->Build(res.data(), overflow.data(), tuples);
        std::chrono::steady_clock::time_point buildEnd = std::chrono::steady_clock::now();
        ui64 buildUs = std::chrono::duration_cast<std::chrono::microseconds>(buildEnd - buildBegin).count();
        if (buildUs == 0) buildUs = 1;

        CTEST << "[<L3] Data size = " << bytes / (1024 * 1024) << "[MB]" << Endl;
        CTEST << "[<L3] Create time for " << tuples << " tuples = " << createUs << "[microseconds]" << Endl;
        CTEST << "[<L3] Create speed = " << bytes / createUs << "[MB/sec]" << Endl;
        CTEST << "[<L3] Build time for " << tuples << " tuples = " << buildUs << "[microseconds]" << Endl;
        CTEST << "[<L3] Build speed = " << bytes / buildUs << "[MB/sec]" << Endl;
        CTEST << "-------------------" << Endl;
    }

    {
        ui64 tuples = 128e5;
        ui64 bytes = (tl->TotalRowSize) * tuples;

        std::chrono::steady_clock::time_point createBegin = std::chrono::steady_clock::now();
        auto ht = TPageHashTable::Create(tl.Get(), tuples);
        std::chrono::steady_clock::time_point createEnd = std::chrono::steady_clock::now();
        ui64 createUs = std::chrono::duration_cast<std::chrono::microseconds>(createEnd - createBegin).count();
        if (createUs == 0) createUs = 1;

        std::chrono::steady_clock::time_point buildBegin = std::chrono::steady_clock::now();
        ht->Build(res.data(), overflow.data(), tuples);
        std::chrono::steady_clock::time_point buildEnd = std::chrono::steady_clock::now();
        ui64 buildUs = std::chrono::duration_cast<std::chrono::microseconds>(buildEnd - buildBegin).count();
        if (buildUs == 0) buildUs = 1;

        CTEST << "[>L3] Data size = " << bytes / (1024 * 1024) << "[MB]" << Endl;
        CTEST << "[>L3] Create time for " << tuples << " tuples = " << createUs << "[microseconds]" << Endl;
        CTEST << "[>L3] Create speed = " << bytes / createUs << "[MB/sec]" << Endl;
        CTEST << "[>L3] Build time for " << tuples << " tuples = " << buildUs << "[microseconds]" << Endl;
        CTEST << "[>L3] Build speed = " << bytes / buildUs << "[MB/sec]" << Endl;
        CTEST << "-------------------" << Endl;
    }

    CTEST << Endl;
    UNIT_ASSERT(true);
} // Y_UNIT_TEST(BenchBuild)

Y_UNIT_TEST(Find) {
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

    auto ht = TPageHashTable::Create(tl.Get(), NTuples1);
    ht->Build(res.data(), overflow.data(), NTuples1);

    auto tuple = res.data() + 42 /* index */ * tl->TotalRowSize;
    auto count = ht->FindMatches(tl.Get(), tuple, overflow, 1);
    UNIT_ASSERT(count == 1);
} // Y_UNIT_TEST(Find)

Y_UNIT_TEST(BenchPositiveFind) { // find existent tuples
    TScopedAlloc alloc(__LOCATION__);

    TColumnDesc kc1, pc1;

    kc1.Role = EColumnRole::Key;
    kc1.DataSize = 4;

    pc1.Role = EColumnRole::Payload;
    pc1.DataSize = 4;

    std::vector<TColumnDesc> columns{kc1, pc1};

    auto tl = TTupleLayout::Create(columns);

    const ui64 NTuples1 = 128e5;

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

    CTEST << "=============\nBench Positive Find\n=============" << Endl;
    {
        ui64 tuples = 5e3;
        ui64 bytes = (tl->TotalRowSize) * tuples;

        auto ht = TPageHashTable::Create(tl.Get(), tuples);
        ht->Build(res.data(), overflow.data(), tuples);

        auto selected = tuples / 10;
        auto selectedBytes = selected * tl->TotalRowSize;
        auto probes = GetRandomTuples(selected, tuples, res.data(), tl.Get());

        std::chrono::steady_clock::time_point findBegin = std::chrono::steady_clock::now();
        auto count = ht->FindMatches(tl.Get(), probes.data(), overflow, selected);
        std::chrono::steady_clock::time_point findEnd = std::chrono::steady_clock::now();
        ui64 findUs = std::chrono::duration_cast<std::chrono::microseconds>(findEnd - findBegin).count();
        if (findUs == 0) findUs = 1;

        UNIT_ASSERT(count == selected);
        CTEST << "[<L1] Hash table data size = " << bytes / 1024 << "[KB]" << Endl;
        CTEST << "[<L1] Selected data size = " << selectedBytes / 1024 << "[KB]" << Endl;
        CTEST << "[<L1] Find time for " << selected << " tuples = " << findUs << "[microseconds]" << Endl;
        CTEST << "[<L1] Find speed = " << selectedBytes / findUs << "[MB/sec]" << Endl;
        CTEST << "[<L1] Avg find time = " << findUs * 1000 / selected << "[nanoseconds/iter]" << Endl;
        CTEST << "-------------------" << Endl;
    }

    {
        ui64 tuples = 8e4;
        ui64 bytes = (tl->TotalRowSize) * tuples;

        auto ht = TPageHashTable::Create(tl.Get(), tuples);
        ht->Build(res.data(), overflow.data(), tuples);

        auto selected = tuples / 10;
        auto selectedBytes = selected * tl->TotalRowSize;
        auto probes = GetRandomTuples(selected, tuples, res.data(), tl.Get());

        std::chrono::steady_clock::time_point findBegin = std::chrono::steady_clock::now();
        auto count = ht->FindMatches(tl.Get(), probes.data(), overflow, selected);
        std::chrono::steady_clock::time_point findEnd = std::chrono::steady_clock::now();
        ui64 findUs = std::chrono::duration_cast<std::chrono::microseconds>(findEnd - findBegin).count();
        if (findUs == 0) findUs = 1;

        UNIT_ASSERT(count == selected);
        CTEST << "[<L2] Hash table data size = " << bytes / 1024 << "[KB]" << Endl;
        CTEST << "[<L2] Selected data size = " << selectedBytes / 1024 << "[KB]" << Endl;
        CTEST << "[<L2] Find time for " << selected << " tuples = " << findUs << "[microseconds]" << Endl;
        CTEST << "[<L2] Find speed = " << selectedBytes / findUs << "[MB/sec]" << Endl;
        CTEST << "[<L2] Avg find time = " << findUs * 1000 / selected << "[nanoseconds/iter]" << Endl;
        CTEST << "-------------------" << Endl;
    }

    {
        ui64 tuples = 4e5;
        ui64 bytes = (tl->TotalRowSize) * tuples;

        auto ht = TPageHashTable::Create(tl.Get(), tuples);
        ht->Build(res.data(), overflow.data(), tuples);

        auto selected = tuples / 10;
        auto selectedBytes = selected * tl->TotalRowSize;
        auto probes = GetRandomTuples(selected, tuples, res.data(), tl.Get());

        std::chrono::steady_clock::time_point findBegin = std::chrono::steady_clock::now();
        auto count = ht->FindMatches(tl.Get(), probes.data(), overflow, selected);
        std::chrono::steady_clock::time_point findEnd = std::chrono::steady_clock::now();
        ui64 findUs = std::chrono::duration_cast<std::chrono::microseconds>(findEnd - findBegin).count();
        if (findUs == 0) findUs = 1;

        UNIT_ASSERT(count == selected);
        CTEST << "[<L3] Hash table data size = " << bytes / (1024 * 1024) << "[MB]" << Endl;
        CTEST << "[<L3] Selected data size = " << selectedBytes / 1024 << "[KB]" << Endl;
        CTEST << "[<L3] Find time for " << selected << " tuples = " << findUs << "[microseconds]" << Endl;
        CTEST << "[<L3] Find speed = " << selectedBytes / findUs << "[MB/sec]" << Endl;
        CTEST << "[<L3] Avg find time = " << findUs * 1000 / selected << "[nanoseconds/iter]" << Endl;
        CTEST << "-------------------" << Endl;
    }

    {
        ui64 tuples = 128e5;
        ui64 bytes = (tl->TotalRowSize) * tuples;

        auto ht = TPageHashTable::Create(tl.Get(), tuples);
        ht->Build(res.data(), overflow.data(), tuples);

        auto selected = tuples / 10;
        auto selectedBytes = selected * tl->TotalRowSize;
        auto probes = GetRandomTuples(selected, tuples, res.data(), tl.Get());

        std::chrono::steady_clock::time_point findBegin = std::chrono::steady_clock::now();
        auto count = ht->FindMatches(tl.Get(), probes.data(), overflow, selected);
        std::chrono::steady_clock::time_point findEnd = std::chrono::steady_clock::now();
        ui64 findUs = std::chrono::duration_cast<std::chrono::microseconds>(findEnd - findBegin).count();
        if (findUs == 0) findUs = 1;

        UNIT_ASSERT(count == selected);
        CTEST << "[>L3] Hash table data size = " << bytes / (1024 * 1024) << "[MB]" << Endl;
        CTEST << "[>L3] Selected data size = " << selectedBytes / (1024 * 1024) << "[MB]" << Endl;
        CTEST << "[>L3] Find time for " << selected << " tuples = " << findUs << "[microseconds]" << Endl;
        CTEST << "[>L3] Find speed = " << selectedBytes / findUs << "[MB/sec]" << Endl;
        CTEST << "[>L3] Avg find time = " << findUs * 1000 / selected << "[nanoseconds/iter]" << Endl;
        CTEST << "-------------------" << Endl;
    }

    CTEST << Endl;
} // Y_UNIT_TEST(BenchPositiveFind)

Y_UNIT_TEST(BenchNegativeFind) { // find non-existent tuples
    TScopedAlloc alloc(__LOCATION__);

    TColumnDesc kc1, pc1;

    kc1.Role = EColumnRole::Key;
    kc1.DataSize = 4;

    pc1.Role = EColumnRole::Payload;
    pc1.DataSize = 4;

    std::vector<TColumnDesc> columns{kc1, pc1};

    auto tl = TTupleLayout::Create(columns);

    const ui64 NTuples1 = 128e5;

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

    CTEST << "=============\nBench Negative Find\n=============" << Endl;
    {
        ui64 tuples = 5e3;
        ui64 bytes = (tl->TotalRowSize) * tuples;

        auto ht = TPageHashTable::Create(tl.Get(), tuples);
        ht->Build(res.data(), overflow.data(), tuples);

        auto selected = tuples / 10;
        auto selectedBytes = selected * tl->TotalRowSize;
        auto probes = GetNonExistentTuples(selected, tuples, res.data(), tl.Get());

        std::chrono::steady_clock::time_point findBegin = std::chrono::steady_clock::now();
        auto count = ht->FindMatches(tl.Get(), probes.data(), overflow, selected);
        std::chrono::steady_clock::time_point findEnd = std::chrono::steady_clock::now();
        ui64 findUs = std::chrono::duration_cast<std::chrono::microseconds>(findEnd - findBegin).count();
        if (findUs == 0) findUs = 1;

        UNIT_ASSERT(count == 0);
        CTEST << "[<L1] Hash table data size = " << bytes / 1024 << "[KB]" << Endl;
        CTEST << "[<L1] Selected data size = " << selectedBytes / 1024 << "[KB]" << Endl;
        CTEST << "[<L1] Find time for " << selected << " tuples = " << findUs << "[microseconds]" << Endl;
        CTEST << "[<L1] Find speed = " << selectedBytes / findUs << "[MB/sec]" << Endl;
        CTEST << "[<L1] Avg find time = " << findUs * 1000 / selected << "[nanoseconds/iter]" << Endl;
        CTEST << "-------------------" << Endl;
    }

    {
        ui64 tuples = 8e4;
        ui64 bytes = (tl->TotalRowSize) * tuples;

        auto ht = TPageHashTable::Create(tl.Get(), tuples);
        ht->Build(res.data(), overflow.data(), tuples);

        auto selected = tuples / 10;
        auto selectedBytes = selected * tl->TotalRowSize;
        auto probes = GetNonExistentTuples(selected, tuples, res.data(), tl.Get());

        std::chrono::steady_clock::time_point findBegin = std::chrono::steady_clock::now();
        auto count = ht->FindMatches(tl.Get(), probes.data(), overflow, selected);
        std::chrono::steady_clock::time_point findEnd = std::chrono::steady_clock::now();
        ui64 findUs = std::chrono::duration_cast<std::chrono::microseconds>(findEnd - findBegin).count();
        if (findUs == 0) findUs = 1;

        UNIT_ASSERT(count == 0);
        CTEST << "[<L2] Hash table data size = " << bytes / 1024 << "[KB]" << Endl;
        CTEST << "[<L2] Selected data size = " << selectedBytes / 1024 << "[KB]" << Endl;
        CTEST << "[<L2] Find time for " << selected << " tuples = " << findUs << "[microseconds]" << Endl;
        CTEST << "[<L2] Find speed = " << selectedBytes / findUs << "[MB/sec]" << Endl;
        CTEST << "[<L2] Avg find time = " << findUs * 1000 / selected << "[nanoseconds/iter]" << Endl;
        CTEST << "-------------------" << Endl;
    }

    {
        ui64 tuples = 4e5;
        ui64 bytes = (tl->TotalRowSize) * tuples;

        auto ht = TPageHashTable::Create(tl.Get(), tuples);
        ht->Build(res.data(), overflow.data(), tuples);

        auto selected = tuples / 10;
        auto selectedBytes = selected * tl->TotalRowSize;
        auto probes = GetNonExistentTuples(selected, tuples, res.data(), tl.Get());

        std::chrono::steady_clock::time_point findBegin = std::chrono::steady_clock::now();
        auto count = ht->FindMatches(tl.Get(), probes.data(), overflow, selected);
        std::chrono::steady_clock::time_point findEnd = std::chrono::steady_clock::now();
        ui64 findUs = std::chrono::duration_cast<std::chrono::microseconds>(findEnd - findBegin).count();
        if (findUs == 0) findUs = 1;

        UNIT_ASSERT(count == 0);
        CTEST << "[<L3] Hash table data size = " << bytes / (1024 * 1024) << "[MB]" << Endl;
        CTEST << "[<L3] Selected data size = " << selectedBytes / 1024 << "[KB]" << Endl;
        CTEST << "[<L3] Find time for " << selected << " tuples = " << findUs << "[microseconds]" << Endl;
        CTEST << "[<L3] Find speed = " << selectedBytes / findUs << "[MB/sec]" << Endl;
        CTEST << "[<L3] Avg find time = " << findUs * 1000 / selected << "[nanoseconds/iter]" << Endl;
        CTEST << "-------------------" << Endl;
    }

    {
        ui64 tuples = 128e5;
        ui64 bytes = (tl->TotalRowSize) * tuples;

        auto ht = TPageHashTable::Create(tl.Get(), tuples);
        ht->Build(res.data(), overflow.data(), tuples);

        auto selected = tuples / 10;
        auto selectedBytes = selected * tl->TotalRowSize;
        auto probes = GetNonExistentTuples(selected, tuples, res.data(), tl.Get());

        std::chrono::steady_clock::time_point findBegin = std::chrono::steady_clock::now();
        auto count = ht->FindMatches(tl.Get(), probes.data(), overflow, selected);
        std::chrono::steady_clock::time_point findEnd = std::chrono::steady_clock::now();
        ui64 findUs = std::chrono::duration_cast<std::chrono::microseconds>(findEnd - findBegin).count();
        if (findUs == 0) findUs = 1;

        UNIT_ASSERT(count == 0);
        CTEST << "[>L3] Hash table data size = " << bytes / (1024 * 1024) << "[MB]" << Endl;
        CTEST << "[>L3] Selected data size = " << selectedBytes / (1024 * 1024) << "[MB]" << Endl;
        CTEST << "[>L3] Find time for " << selected << " tuples = " << findUs << "[microseconds]" << Endl;
        CTEST << "[>L3] Find speed = " << selectedBytes / findUs << "[MB/sec]" << Endl;
        CTEST << "[>L3] Avg find time = " << findUs * 1000 / selected << "[nanoseconds/iter]" << Endl;
        CTEST << "-------------------" << Endl;
    }

    CTEST << Endl;
} // Y_UNIT_TEST(BenchNegativeFind)

Y_UNIT_TEST(BenchPositiveFindLargeTuple) { // Tuple size > 16
    TScopedAlloc alloc(__LOCATION__);

    TColumnDesc kc1, kc2, pc1, pc2, pc3, pc4;

    kc1.Role = EColumnRole::Key;
    kc1.DataSize = 8;

    kc2.Role = EColumnRole::Key;
    kc2.DataSize = 4;

    pc1.Role = EColumnRole::Payload;
    pc1.DataSize = 8;

    pc2.Role = EColumnRole::Payload;
    pc2.DataSize = 4;

    pc3.Role = EColumnRole::Payload;
    pc3.DataSize = 8;

    pc4.Role = EColumnRole::Payload;
    pc4.DataSize = 4;

    std::vector<TColumnDesc> columns{kc1, kc2, pc1, pc2, pc3, pc4};

    auto tl = TTupleLayout::Create(columns);

    const ui64 NTuples1 = 64e5;

    const ui64 Tuples1DataBytes = (tl->TotalRowSize) * NTuples1;

    std::vector<ui64> col1(NTuples1, 0);
    std::vector<ui32> col2(NTuples1, 0);
    std::vector<ui64> col3(NTuples1, 0);
    std::vector<ui32> col4(NTuples1, 0);
    std::vector<ui64> col5(NTuples1, 0);
    std::vector<ui32> col6(NTuples1, 0);

    std::vector<ui8> res(Tuples1DataBytes + 64, 0);

    for (ui32 i = 0; i < NTuples1; ++i) {
        col1[i] = i;
        col2[i] = i;
        col3[i] = i;
        col4[i] = i;
        col5[i] = i;
        col6[i] = i;
    }

    const ui8* cols[6];

    cols[0] = (ui8*) col1.data();
    cols[1] = (ui8*) col2.data();
    cols[2] = (ui8*) col3.data();
    cols[3] = (ui8*) col4.data();
    cols[4] = (ui8*) col5.data();
    cols[5] = (ui8*) col6.data();

    std::vector<ui8> colValid1((NTuples1 + 7)/8, ~0);
    std::vector<ui8> colValid2((NTuples1 + 7)/8, ~0);
    std::vector<ui8> colValid3((NTuples1 + 7)/8, ~0);
    std::vector<ui8> colValid4((NTuples1 + 7)/8, ~0);
    std::vector<ui8> colValid5((NTuples1 + 7)/8, ~0);
    std::vector<ui8> colValid6((NTuples1 + 7)/8, ~0);
    const ui8 *colsValid[6] = {
        colValid1.data(),
        colValid2.data(),
        colValid3.data(),
        colValid4.data(),
        colValid5.data(),
        colValid6.data(),
    };

    std::vector<ui8, TMKQLAllocator<ui8>> overflow;
    tl->Pack(cols, colsValid, res.data(), overflow, 0, NTuples1);

    CTEST << "=============\nLarge Bench Positive Find\n=============" << Endl;
    {
        ui64 tuples = 2e3;
        ui64 bytes = (tl->TotalRowSize) * tuples;

        auto ht = TPageHashTable::Create(tl.Get(), tuples);
        ht->Build(res.data(), overflow.data(), tuples);

        auto selected = tuples / 10;
        auto selectedBytes = selected * tl->TotalRowSize;
        auto probes = GetRandomTuples(selected, tuples, res.data(), tl.Get());

        std::chrono::steady_clock::time_point findBegin = std::chrono::steady_clock::now();
        auto count = ht->FindMatches(tl.Get(), probes.data(), overflow, selected);
        std::chrono::steady_clock::time_point findEnd = std::chrono::steady_clock::now();
        ui64 findUs = std::chrono::duration_cast<std::chrono::microseconds>(findEnd - findBegin).count();
        if (findUs == 0) findUs = 1;

        UNIT_ASSERT(count == selected);
        CTEST << "[<L1] Hash table data size = " << bytes / 1024 << "[KB]" << Endl;
        CTEST << "[<L1] Selected data size = " << selectedBytes / 1024 << "[KB]" << Endl;
        CTEST << "[<L1] Find time for " << selected << " tuples = " << findUs << "[microseconds]" << Endl;
        CTEST << "[<L1] Find speed = " << selectedBytes / findUs << "[MB/sec]" << Endl;
        CTEST << "[<L1] Avg find time = " << findUs * 1000 / selected << "[nanoseconds/iter]" << Endl;
        CTEST << "-------------------" << Endl;
    }

    {
        ui64 tuples = 3e4;
        ui64 bytes = (tl->TotalRowSize) * tuples;

        auto ht = TPageHashTable::Create(tl.Get(), tuples);
        ht->Build(res.data(), overflow.data(), tuples);

        auto selected = tuples / 10;
        auto selectedBytes = selected * tl->TotalRowSize;
        auto probes = GetRandomTuples(selected, tuples, res.data(), tl.Get());

        std::chrono::steady_clock::time_point findBegin = std::chrono::steady_clock::now();
        auto count = ht->FindMatches(tl.Get(), probes.data(), overflow, selected);
        std::chrono::steady_clock::time_point findEnd = std::chrono::steady_clock::now();
        ui64 findUs = std::chrono::duration_cast<std::chrono::microseconds>(findEnd - findBegin).count();
        if (findUs == 0) findUs = 1;

        UNIT_ASSERT(count == selected);
        CTEST << "[<L2] Hash table data size = " << bytes / 1024 << "[KB]" << Endl;
        CTEST << "[<L2] Selected data size = " << selectedBytes / 1024 << "[KB]" << Endl;
        CTEST << "[<L2] Find time for " << selected << " tuples = " << findUs << "[microseconds]" << Endl;
        CTEST << "[<L2] Find speed = " << selectedBytes / findUs << "[MB/sec]" << Endl;
        CTEST << "[<L2] Avg find time = " << findUs * 1000 / selected << "[nanoseconds/iter]" << Endl;
        CTEST << "-------------------" << Endl;
    }

    {
        ui64 tuples = 2e5;
        ui64 bytes = (tl->TotalRowSize) * tuples;

        auto ht = TPageHashTable::Create(tl.Get(), tuples);
        ht->Build(res.data(), overflow.data(), tuples);

        auto selected = tuples / 10;
        auto selectedBytes = selected * tl->TotalRowSize;
        auto probes = GetRandomTuples(selected, tuples, res.data(), tl.Get());

        std::chrono::steady_clock::time_point findBegin = std::chrono::steady_clock::now();
        auto count = ht->FindMatches(tl.Get(), probes.data(), overflow, selected);
        std::chrono::steady_clock::time_point findEnd = std::chrono::steady_clock::now();
        ui64 findUs = std::chrono::duration_cast<std::chrono::microseconds>(findEnd - findBegin).count();
        if (findUs == 0) findUs = 1;

        UNIT_ASSERT(count == selected);
        CTEST << "[<L3] Hash table data size = " << bytes / (1024 * 1024) << "[MB]" << Endl;
        CTEST << "[<L3] Selected data size = " << selectedBytes / 1024 << "[KB]" << Endl;
        CTEST << "[<L3] Find time for " << selected << " tuples = " << findUs << "[microseconds]" << Endl;
        CTEST << "[<L3] Find speed = " << selectedBytes / findUs << "[MB/sec]" << Endl;
        CTEST << "[<L3] Avg find time = " << findUs * 1000 / selected << "[nanoseconds/iter]" << Endl;
        CTEST << "-------------------" << Endl;
    }

    {
        ui64 tuples = 64e5;
        ui64 bytes = (tl->TotalRowSize) * tuples;

        auto ht = TPageHashTable::Create(tl.Get(), tuples);
        ht->Build(res.data(), overflow.data(), tuples);

        auto selected = tuples / 10;
        auto selectedBytes = selected * tl->TotalRowSize;
        auto probes = GetRandomTuples(selected, tuples, res.data(), tl.Get());

        std::chrono::steady_clock::time_point findBegin = std::chrono::steady_clock::now();
        auto count = ht->FindMatches(tl.Get(), probes.data(), overflow, selected);
        std::chrono::steady_clock::time_point findEnd = std::chrono::steady_clock::now();
        ui64 findUs = std::chrono::duration_cast<std::chrono::microseconds>(findEnd - findBegin).count();
        if (findUs == 0) findUs = 1;

        UNIT_ASSERT(count == selected);
        CTEST << "[>L3] Hash table data size = " << bytes / (1024 * 1024) << "[MB]" << Endl;
        CTEST << "[>L3] Selected data size = " << selectedBytes / (1024 * 1024) << "[MB]" << Endl;
        CTEST << "[>L3] Find time for " << selected << " tuples = " << findUs << "[microseconds]" << Endl;
        CTEST << "[>L3] Find speed = " << selectedBytes / findUs << "[MB/sec]" << Endl;
        CTEST << "[>L3] Avg find time = " << findUs * 1000 / selected << "[nanoseconds/iter]" << Endl;
        CTEST << "-------------------" << Endl;
    }

    CTEST << Endl;
} // Y_UNIT_TEST(BenchPositiveFindLargeTuple)

Y_UNIT_TEST(BenchNegativeFindLargeTuple) { // Tuple size > 16
    TScopedAlloc alloc(__LOCATION__);

    TColumnDesc kc1, kc2, pc1, pc2, pc3, pc4;

    kc1.Role = EColumnRole::Key;
    kc1.DataSize = 8;

    kc2.Role = EColumnRole::Key;
    kc2.DataSize = 4;

    pc1.Role = EColumnRole::Payload;
    pc1.DataSize = 8;

    pc2.Role = EColumnRole::Payload;
    pc2.DataSize = 4;

    pc3.Role = EColumnRole::Payload;
    pc3.DataSize = 8;

    pc4.Role = EColumnRole::Payload;
    pc4.DataSize = 4;

    std::vector<TColumnDesc> columns{kc1, kc2, pc1, pc2, pc3, pc4};

    auto tl = TTupleLayout::Create(columns);

    const ui64 NTuples1 = 64e5;

    const ui64 Tuples1DataBytes = (tl->TotalRowSize) * NTuples1;

    std::vector<ui64> col1(NTuples1, 0);
    std::vector<ui32> col2(NTuples1, 0);
    std::vector<ui64> col3(NTuples1, 0);
    std::vector<ui32> col4(NTuples1, 0);
    std::vector<ui64> col5(NTuples1, 0);
    std::vector<ui32> col6(NTuples1, 0);

    std::vector<ui8> res(Tuples1DataBytes + 64, 0);

    for (ui32 i = 0; i < NTuples1; ++i) {
        col1[i] = i;
        col2[i] = i;
        col3[i] = i;
        col4[i] = i;
        col5[i] = i;
        col6[i] = i;
    }

    const ui8* cols[6];

    cols[0] = (ui8*) col1.data();
    cols[1] = (ui8*) col2.data();
    cols[2] = (ui8*) col3.data();
    cols[3] = (ui8*) col4.data();
    cols[4] = (ui8*) col5.data();
    cols[5] = (ui8*) col6.data();

    std::vector<ui8> colValid1((NTuples1 + 7)/8, ~0);
    std::vector<ui8> colValid2((NTuples1 + 7)/8, ~0);
    std::vector<ui8> colValid3((NTuples1 + 7)/8, ~0);
    std::vector<ui8> colValid4((NTuples1 + 7)/8, ~0);
    std::vector<ui8> colValid5((NTuples1 + 7)/8, ~0);
    std::vector<ui8> colValid6((NTuples1 + 7)/8, ~0);
    const ui8 *colsValid[6] = {
        colValid1.data(),
        colValid2.data(),
        colValid3.data(),
        colValid4.data(),
        colValid5.data(),
        colValid6.data(),
    };

    std::vector<ui8, TMKQLAllocator<ui8>> overflow;
    tl->Pack(cols, colsValid, res.data(), overflow, 0, NTuples1);

    CTEST << "=============\nLarge Bench Negative Find\n=============" << Endl;
    {
        ui64 tuples = 2e3;
        ui64 bytes = (tl->TotalRowSize) * tuples;

        auto ht = TPageHashTable::Create(tl.Get(), tuples);
        ht->Build(res.data(), overflow.data(), tuples);

        auto selected = tuples / 10;
        auto selectedBytes = selected * tl->TotalRowSize;
        auto probes = GetNonExistentTuples(selected, tuples, res.data(), tl.Get());

        std::chrono::steady_clock::time_point findBegin = std::chrono::steady_clock::now();
        auto count = ht->FindMatches(tl.Get(), probes.data(), overflow, selected);
        std::chrono::steady_clock::time_point findEnd = std::chrono::steady_clock::now();
        ui64 findUs = std::chrono::duration_cast<std::chrono::microseconds>(findEnd - findBegin).count();
        if (findUs == 0) findUs = 1;

        UNIT_ASSERT(count == 0);
        CTEST << "[<L1] Hash table data size = " << bytes / 1024 << "[KB]" << Endl;
        CTEST << "[<L1] Selected data size = " << selectedBytes / 1024 << "[KB]" << Endl;
        CTEST << "[<L1] Find time for " << selected << " tuples = " << findUs << "[microseconds]" << Endl;
        CTEST << "[<L1] Find speed = " << selectedBytes / findUs << "[MB/sec]" << Endl;
        CTEST << "[<L1] Avg find time = " << findUs * 1000 / selected << "[nanoseconds/iter]" << Endl;
        CTEST << "-------------------" << Endl;
    }

    {
        ui64 tuples = 3e4;
        ui64 bytes = (tl->TotalRowSize) * tuples;

        auto ht = TPageHashTable::Create(tl.Get(), tuples);
        ht->Build(res.data(), overflow.data(), tuples);

        auto selected = tuples / 10;
        auto selectedBytes = selected * tl->TotalRowSize;
        auto probes = GetNonExistentTuples(selected, tuples, res.data(), tl.Get());

        std::chrono::steady_clock::time_point findBegin = std::chrono::steady_clock::now();
        auto count = ht->FindMatches(tl.Get(), probes.data(), overflow, selected);
        std::chrono::steady_clock::time_point findEnd = std::chrono::steady_clock::now();
        ui64 findUs = std::chrono::duration_cast<std::chrono::microseconds>(findEnd - findBegin).count();
        if (findUs == 0) findUs = 1;

        UNIT_ASSERT(count == 0);
        CTEST << "[<L2] Hash table data size = " << bytes / 1024 << "[KB]" << Endl;
        CTEST << "[<L2] Selected data size = " << selectedBytes / 1024 << "[KB]" << Endl;
        CTEST << "[<L2] Find time for " << selected << " tuples = " << findUs << "[microseconds]" << Endl;
        CTEST << "[<L2] Find speed = " << selectedBytes / findUs << "[MB/sec]" << Endl;
        CTEST << "[<L2] Avg find time = " << findUs * 1000 / selected << "[nanoseconds/iter]" << Endl;
        CTEST << "-------------------" << Endl;
    }

    {
        ui64 tuples = 2e5;
        ui64 bytes = (tl->TotalRowSize) * tuples;

        auto ht = TPageHashTable::Create(tl.Get(), tuples);
        ht->Build(res.data(), overflow.data(), tuples);

        auto selected = tuples / 10;
        auto selectedBytes = selected * tl->TotalRowSize;
        auto probes = GetNonExistentTuples(selected, tuples, res.data(), tl.Get());

        std::chrono::steady_clock::time_point findBegin = std::chrono::steady_clock::now();
        auto count = ht->FindMatches(tl.Get(), probes.data(), overflow, selected);
        std::chrono::steady_clock::time_point findEnd = std::chrono::steady_clock::now();
        ui64 findUs = std::chrono::duration_cast<std::chrono::microseconds>(findEnd - findBegin).count();
        if (findUs == 0) findUs = 1;

        UNIT_ASSERT(count == 0);
        CTEST << "[<L3] Hash table data size = " << bytes / (1024 * 1024) << "[MB]" << Endl;
        CTEST << "[<L3] Selected data size = " << selectedBytes / 1024 << "[KB]" << Endl;
        CTEST << "[<L3] Find time for " << selected << " tuples = " << findUs << "[microseconds]" << Endl;
        CTEST << "[<L3] Find speed = " << selectedBytes / findUs << "[MB/sec]" << Endl;
        CTEST << "[<L3] Avg find time = " << findUs * 1000 / selected << "[nanoseconds/iter]" << Endl;
        CTEST << "-------------------" << Endl;
    }

    {
        ui64 tuples = 64e5;
        ui64 bytes = (tl->TotalRowSize) * tuples;

        auto ht = TPageHashTable::Create(tl.Get(), tuples);
        ht->Build(res.data(), overflow.data(), tuples);

        auto selected = tuples / 10;
        auto selectedBytes = selected * tl->TotalRowSize;
        auto probes = GetNonExistentTuples(selected, tuples, res.data(), tl.Get());

        std::chrono::steady_clock::time_point findBegin = std::chrono::steady_clock::now();
        auto count = ht->FindMatches(tl.Get(), probes.data(), overflow, selected);
        std::chrono::steady_clock::time_point findEnd = std::chrono::steady_clock::now();
        ui64 findUs = std::chrono::duration_cast<std::chrono::microseconds>(findEnd - findBegin).count();
        if (findUs == 0) findUs = 1;

        UNIT_ASSERT(count == 0);
        CTEST << "[>L3] Hash table data size = " << bytes / (1024 * 1024) << "[MB]" << Endl;
        CTEST << "[>L3] Selected data size = " << selectedBytes / (1024 * 1024) << "[MB]" << Endl;
        CTEST << "[>L3] Find time for " << selected << " tuples = " << findUs << "[microseconds]" << Endl;
        CTEST << "[>L3] Find speed = " << selectedBytes / findUs << "[MB/sec]" << Endl;
        CTEST << "[>L3] Avg find time = " << findUs * 1000 / selected << "[nanoseconds/iter]" << Endl;
        CTEST << "-------------------" << Endl;
    }

    CTEST << Endl;
} // Y_UNIT_TEST(BenchNegativeFindLargeTuple)

Y_UNIT_TEST(BenchVariableSizedKeyPositiveFind) { // key is variable sized
    TScopedAlloc alloc(__LOCATION__);
    std::mt19937 gen(std::random_device{}());
    const char alphanum[] =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";

    TColumnDesc kc1, pc1;

    kc1.Role = EColumnRole::Key;
    kc1.DataSize = 16;
    kc1.SizeType = EColumnSizeType::Variable;
    std::uniform_int_distribution<> distr(kc1.DataSize - 10, kc1.DataSize + 2);

    pc1.Role = EColumnRole::Payload;
    pc1.DataSize = 4;

    std::vector<TColumnDesc> columns{kc1, pc1};

    auto tl = TTupleLayout::Create(columns);

    const ui64 NTuples1 = 64e5;

    const ui64 Tuples1DataBytes = (tl->TotalRowSize) * NTuples1;

    std::vector<ui32> icol1(1, 0);
    std::vector<ui8>  scol1;
    std::vector<ui32> col2(NTuples1, 0);

    std::vector<ui8> res(Tuples1DataBytes + 64, 0);

    std::unordered_set<TString> used;
    for (ui32 i = 0; i < NTuples1; ++i) {
        while (true) {
            ui32 len = distr(gen);
            TString str(len, 0);
            for (auto& c: str) {
                c = alphanum[gen() % (sizeof(alphanum) - 1)];
            }
            if (!used.count(str)) {
                used.insert(str);
                for (auto c: str) {
                    scol1.push_back(c);
                }
                icol1.push_back(scol1.size());
                break;
            }
        }
        col2[i] = i;
    }

    const ui8* cols[3];

    cols[0] = (ui8*) icol1.data();
    cols[1] = (ui8*) scol1.data();
    cols[2] = (ui8*) col2.data();

    std::vector<ui8> colValid((NTuples1 + 7)/8, ~0);
    const ui8 *colsValid[3] = {
        colValid.data(),
        nullptr,
        colValid.data(),
    };

    std::vector<ui8, TMKQLAllocator<ui8>> overflow;
    tl->Pack(cols, colsValid, res.data(), overflow, 0, NTuples1);

    CTEST << "=============\nBench Variable sized key Positive Find\n=============" << Endl;
    {
        ui64 tuples = 3e3;
        ui64 bytes = (tl->TotalRowSize) * tuples;

        auto ht = TPageHashTable::Create(tl.Get(), tuples);
        ht->Build(res.data(), overflow.data(), tuples);

        auto selected = tuples / 10;
        auto selectedBytes = selected * tl->TotalRowSize;
        auto probes = GetRandomTuples(selected, tuples, res.data(), tl.Get());

        std::chrono::steady_clock::time_point findBegin = std::chrono::steady_clock::now();
        auto count = ht->FindMatches(tl.Get(), probes.data(), overflow, selected);
        std::chrono::steady_clock::time_point findEnd = std::chrono::steady_clock::now();
        ui64 findUs = std::chrono::duration_cast<std::chrono::microseconds>(findEnd - findBegin).count();
        if (findUs == 0) findUs = 1;

        UNIT_ASSERT(count == selected);
        CTEST << "[<L1] Hash table data size = " << bytes / 1024 << "[KB]" << Endl;
        CTEST << "[<L1] Selected data size = " << selectedBytes / 1024 << "[KB]" << Endl;
        CTEST << "[<L1] Find time for " << selected << " tuples = " << findUs << "[microseconds]" << Endl;
        CTEST << "[<L1] Find speed = " << selectedBytes / findUs << "[MB/sec]" << Endl;
        CTEST << "[<L1] Avg find time = " << findUs * 1000 / selected << "[nanoseconds/iter]" << Endl;
        CTEST << "-------------------" << Endl;
    }

    {
        ui64 tuples = 4e4;
        ui64 bytes = (tl->TotalRowSize) * tuples;

        auto ht = TPageHashTable::Create(tl.Get(), tuples);
        ht->Build(res.data(), overflow.data(), tuples);

        auto selected = tuples / 10;
        auto selectedBytes = selected * tl->TotalRowSize;
        auto probes = GetRandomTuples(selected, tuples, res.data(), tl.Get());

        std::chrono::steady_clock::time_point findBegin = std::chrono::steady_clock::now();
        auto count = ht->FindMatches(tl.Get(), probes.data(), overflow, selected);
        std::chrono::steady_clock::time_point findEnd = std::chrono::steady_clock::now();
        ui64 findUs = std::chrono::duration_cast<std::chrono::microseconds>(findEnd - findBegin).count();
        if (findUs == 0) findUs = 1;

        UNIT_ASSERT(count == selected);
        CTEST << "[<L2] Hash table data size = " << bytes / 1024 << "[KB]" << Endl;
        CTEST << "[<L2] Selected data size = " << selectedBytes / 1024 << "[KB]" << Endl;
        CTEST << "[<L2] Find time for " << selected << " tuples = " << findUs << "[microseconds]" << Endl;
        CTEST << "[<L2] Find speed = " << selectedBytes / findUs << "[MB/sec]" << Endl;
        CTEST << "[<L2] Avg find time = " << findUs * 1000 / selected << "[nanoseconds/iter]" << Endl;
        CTEST << "-------------------" << Endl;
    }

    {
        ui64 tuples = 2e5;
        ui64 bytes = (tl->TotalRowSize) * tuples;

        auto ht = TPageHashTable::Create(tl.Get(), tuples);
        ht->Build(res.data(), overflow.data(), tuples);

        auto selected = tuples / 10;
        auto selectedBytes = selected * tl->TotalRowSize;
        auto probes = GetRandomTuples(selected, tuples, res.data(), tl.Get());

        std::chrono::steady_clock::time_point findBegin = std::chrono::steady_clock::now();
        auto count = ht->FindMatches(tl.Get(), probes.data(), overflow, selected);
        std::chrono::steady_clock::time_point findEnd = std::chrono::steady_clock::now();
        ui64 findUs = std::chrono::duration_cast<std::chrono::microseconds>(findEnd - findBegin).count();
        if (findUs == 0) findUs = 1;

        UNIT_ASSERT(count == selected);
        CTEST << "[<L3] Hash table data size = " << bytes / (1024 * 1024) << "[MB]" << Endl;
        CTEST << "[<L3] Selected data size = " << selectedBytes / 1024 << "[KB]" << Endl;
        CTEST << "[<L3] Find time for " << selected << " tuples = " << findUs << "[microseconds]" << Endl;
        CTEST << "[<L3] Find speed = " << selectedBytes / findUs << "[MB/sec]" << Endl;
        CTEST << "[<L3] Avg find time = " << findUs * 1000 / selected << "[nanoseconds/iter]" << Endl;
        CTEST << "-------------------" << Endl;
    }

    {
        ui64 tuples = 64e5;
        ui64 bytes = (tl->TotalRowSize) * tuples;

        auto ht = TPageHashTable::Create(tl.Get(), tuples);
        ht->Build(res.data(), overflow.data(), tuples);

        auto selected = tuples / 10;
        auto selectedBytes = selected * tl->TotalRowSize;
        auto probes = GetRandomTuples(selected, tuples, res.data(), tl.Get());

        std::chrono::steady_clock::time_point findBegin = std::chrono::steady_clock::now();
        auto count = ht->FindMatches(tl.Get(), probes.data(), overflow, selected);
        std::chrono::steady_clock::time_point findEnd = std::chrono::steady_clock::now();
        ui64 findUs = std::chrono::duration_cast<std::chrono::microseconds>(findEnd - findBegin).count();
        if (findUs == 0) findUs = 1;

        UNIT_ASSERT(count == selected);
        CTEST << "[>L3] Hash table data size = " << bytes / (1024 * 1024) << "[MB]" << Endl;
        CTEST << "[>L3] Selected data size = " << selectedBytes / (1024 * 1024) << "[MB]" << Endl;
        CTEST << "[>L3] Find time for " << selected << " tuples = " << findUs << "[microseconds]" << Endl;
        CTEST << "[>L3] Find speed = " << selectedBytes / findUs << "[MB/sec]" << Endl;
        CTEST << "[>L3] Avg find time = " << findUs * 1000 / selected << "[nanoseconds/iter]" << Endl;
        CTEST << "-------------------" << Endl;
    }

    CTEST << Endl;
} // Y_UNIT_TEST(BenchVariableSizedKeyPositiveFind)

} // Y_UNIT_TEST_SUITE(PageHashTable)


}
} // namespace NMiniKQL
} // namespace NKikimr
