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

#include <ydb/library/yql/minikql/comp_nodes/packed_tuple/accumulator.h>
#include <ydb/library/yql/minikql/comp_nodes/packed_tuple/tuple.h>

namespace NKikimr {
namespace NMiniKQL {
namespace NPackedTuple {

using namespace std::chrono_literals;

static volatile bool IsVerbose = false;
#define CTEST (IsVerbose ? Cerr : Cnull)

Y_UNIT_TEST_SUITE(Accumulator) {

Y_UNIT_TEST(CreateAccumulator) {
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

    auto accum = TAccumulator::Create(tl.Get());
    auto info = accum->GetBucket(0);

    UNIT_ASSERT(info.Data == nullptr);
    UNIT_ASSERT(info.Elements == 0);
} // Y_UNIT_TEST(CreateAccumulator)


Y_UNIT_TEST(BenchAccumulator) {
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

    auto accum = TAccumulator::Create(tl.Get());
    std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
    accum->AddData(res.data(), NTuples1);
    std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
    ui64 microseconds = std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count();
    if (microseconds == 0) microseconds = 1;

    CTEST  << "Time for " << (NTuples1) << " accumulate = " << microseconds  << "[microseconds]" << Endl;
    CTEST  << "Data size =  " << Tuples1DataBytes / (1024 * 1024) << "[MB]" << Endl;
    CTEST  << "Calculating speed = " << Tuples1DataBytes / microseconds << "MB/sec" << Endl;
    CTEST  << Endl;

    UNIT_ASSERT(true);
} // Y_UNIT_TEST(BenchAccumulator)


Y_UNIT_TEST(MultipleAddToAccumulator) {
    TScopedAlloc alloc(__LOCATION__);

    TColumnDesc kc1, pc1;

    kc1.Role = EColumnRole::Key;
    kc1.DataSize = 8;

    pc1.Role = EColumnRole::Payload;
    pc1.DataSize = 8;

    std::vector<TColumnDesc> columns{kc1, pc1};

    auto tl = TTupleLayout::Create(columns);

    const ui64 NTuples1 = 1000;

    const ui64 Tuples1DataBytes = (tl->TotalRowSize) * NTuples1;

    std::vector<ui64> col1(NTuples1, 0);
    std::vector<ui64> col2(NTuples1, 0);

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

    auto accum = TAccumulator::Create(tl.Get());
    accum->AddData(res.data(), NTuples1); // + 1000 elements
    accum->AddData(res.data(), NTuples1); // + 1000 elements

    ui32 totalCount = 0;
    ui32 nBuckets = TAccumulator::DEFAULT_BUCKETS_COUNT;
    for (ui32 i = 0; i < nBuckets; ++i) {
        auto info = accum->GetBucket(i);
        totalCount += info.Elements;
    }

    UNIT_ASSERT(totalCount == 2 * NTuples1);
} // Y_UNIT_TEST(MultipleAddToAccumulator)


Y_UNIT_TEST(AccumulatorFuzz) {
    TScopedAlloc alloc(__LOCATION__);

    std::mt19937 rng; // fixed-seed (0) prng
    std::vector<TColumnDesc> columns;
    std::vector<std::vector<ui8>> colsdata;
    std::vector<const ui8*> colsptr;
    std::vector<std::vector<ui8>> isValidData;
    std::vector<const ui8*> isValidPtr;

    for (ui32 test = 0; test < 10; ++test) {
        ui32 rows = 1 + (rng() % 1000);
        ui32 cols = 1 + (rng() % 100);
        columns.resize(cols);
        colsdata.resize(cols);
        colsptr.resize(cols);
        isValidData.resize(cols);
        isValidPtr.resize(cols);
        ui32 isValidSize = (rows + 7)/8;
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
            res.resize(subRows*tl->TotalRowSize);
            tl->Pack(colsptr.data(), isValidPtr.data(), res.data(), overflow, off, subRows);

            auto accum = TAccumulator::Create(tl.Get());
            accum->AddData(res.data(), subRows);

            ui32 totalCount = 0;
            ui32 nBuckets = TAccumulator::DEFAULT_BUCKETS_COUNT;
            for (ui32 i = 0; i < nBuckets; ++i) {
                auto info = accum->GetBucket(i);
                const ui8* Bucket = info.Data;

                ui32 offset = 0;
                if (info.Elements > 0) {
                    for (ui32 count = 0; count < info.Elements; ++count) {
                        auto suffix = (*reinterpret_cast<const ui32*>(Bucket + offset)) & (nBuckets - 1);
                        UNIT_ASSERT(suffix == i);
                        offset += info.Layout->TotalRowSize;
                        totalCount++;
                    }
                }
            }
            UNIT_ASSERT_VALUES_EQUAL(totalCount, subRows);
        }
    }
} // Y_UNIT_TEST(AccumulatorFuzz)

} // Y_UNIT_TEST_SUITE(Accumulator)


}
} // namespace NMiniKQL
} // namespace NKikimr
