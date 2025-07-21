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
#include <ydb/library/yql/minikql/comp_nodes/packed_tuple/histogram.h>

#include <arrow/util/bit_util.h>

namespace NKikimr {
namespace NMiniKQL {
namespace NPackedTuple {

using namespace std::chrono_literals;

static volatile bool IsVerbose = true;
#define CTEST (IsVerbose ? Cerr : Cnull)

namespace {

TVector<TString> GenerateValues(size_t level) {
    constexpr size_t alphaSize = 'Z' - 'A' + 1;
    if (level == 1) {
        TVector<TString> alphabet(alphaSize);
        std::iota(alphabet.begin(), alphabet.end(), 'A');
        return alphabet;
    }
    const auto subValues = GenerateValues(level - 1);
    TVector<TString> values;
    values.reserve(alphaSize * subValues.size());
    for (char ch = 'A'; ch <= 'Z'; ch++) {
        for (const auto& tail : subValues) {
            values.emplace_back(ch + tail);
        }
    }
    return values;
}

static const TVector<TString> threeLetterValues = GenerateValues(3);

bool RunFixSizedAccumulatorBench(ui64 nTuples, ui64 nCols, ui64 log2Buckets) {
    auto nBuckets = 1 << log2Buckets;
    CTEST << "============ BENCH BEGIN ============" << Endl;
    CTEST << "Test config:" << Endl;
    CTEST << ">  nTuples: " << nTuples << Endl;
    CTEST << ">  nCols: " << nCols << Endl;
    CTEST << ">  nBuckets: " << nBuckets << Endl;

    ui64 totalTime = 0;

    TScopedAlloc alloc(__LOCATION__);
    using TBuffer = TAccumulator::TBuffer;

    TColumnDesc kc;
    kc.Role = EColumnRole::Key;
    kc.DataSize = 8;

    std::vector<TColumnDesc> columns(nCols, kc);
    auto tl = TTupleLayout::Create(columns);
    const ui64 TuplesDataBytes = (tl->TotalRowSize) * nTuples;
    CTEST << ">  Dataset size: " << TuplesDataBytes / (1024 * 1024) << "[MB]" << Endl;
    CTEST << " " << Endl;

    std::vector<ui64> col(nTuples, 0);
    std::iota(col.begin(), col.end(), 0);
    auto colsData = std::vector(nCols, col);
    std::vector<const ui8*> cols(nCols);
    for (size_t ind = 0; ind != nCols; ind++) {
        cols[ind] = (ui8*)colsData[ind].data();
    }
    std::vector<ui8> colValid((nTuples + 7)/8, ~0);
    auto colsValidData = std::vector(nCols, colValid);
    std::vector<const ui8*> colsValid(nCols);
    for (size_t ind = 0; ind != nCols; ind++) {
        colsValid[ind] = (ui8*)colsValidData[ind].data();
    }

    auto resesData = std::vector<std::vector<ui8, TMKQLAllocator<ui8>>>(1u << log2Buckets);
    std::vector<std::vector<ui8, TMKQLAllocator<ui8>>> overflowsData(1u << log2Buckets);

    auto reses = TPaddedPtr(resesData.data(), sizeof(resesData[0]));
    auto overflows = TPaddedPtr(overflowsData.data(), sizeof(overflowsData[0]));

    tl->BucketPack(cols.data(), colsValid.data(), reses, overflows, 0, nTuples, log2Buckets);
    for (auto& bres : resesData) {
        bres.resize(0);
    }

    auto begintp = std::chrono::steady_clock::now();
    tl->BucketPack(cols.data(), colsValid.data(), reses, overflows, 0, nTuples, log2Buckets);
    auto endtp = std::chrono::steady_clock::now();
    auto bucketPackTime = std::chrono::duration_cast<std::chrono::microseconds>(endtp - begintp).count();
    if (bucketPackTime == 0) bucketPackTime = 1;

    CTEST << "Bucket pack time: " << bucketPackTime  << "[microseconds]" << Endl;
    CTEST << "Bucket pack speed: " << TuplesDataBytes / bucketPackTime << "[MB/sec]" << Endl;
    CTEST << " " << Endl;

    std::vector<ui8> res(TuplesDataBytes, 0);
    for (ui32 i = 0; i < nTuples; ++i) {
        col[i] = i;
    }
    std::vector<ui8, TMKQLAllocator<ui8>> overflow;
    tl->Pack(cols.data(), colsValid.data(), res.data(), overflow, 0, nTuples);

    begintp = std::chrono::steady_clock::now();
    tl->Pack(cols.data(), colsValid.data(), res.data(), overflow, 0, nTuples);
    endtp = std::chrono::steady_clock::now();
    auto packTime = std::chrono::duration_cast<std::chrono::microseconds>(endtp - begintp).count();
    if (packTime == 0) packTime = 1;
    totalTime += packTime;

    CTEST << "Pack time: " << packTime  << "[microseconds]" << Endl;
    CTEST << "Pack speed: " << TuplesDataBytes / packTime << "[MB/sec]" << Endl;
    CTEST << " " << Endl;

    ui32 bitsCount = arrow::BitUtil::NumRequiredBits(nBuckets - 1);
    ui32 shift = 32 - bitsCount;
    ui32 mask = (1 << bitsCount) - 1;

    std::vector<std::pair<ui64, ui64>, TMKQLAllocator<std::pair<ui64, ui64>>> sizes(nBuckets, {0, 0});

    {
        // std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();

        Histogram hist;
        hist.AddData(tl.Get(), res.data(), nTuples, TAccumulator::GetBucketId, shift, mask);
        hist.EstimateSizes(sizes);

        // std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
        // ui64 microseconds = std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count();
        // if (microseconds == 0) microseconds = 1;
        // // totalTime += microseconds;

        // CTEST << "Histogram build time: " << microseconds  << "[microseconds]" << Endl;
        // CTEST << "Histogram build speed: " << TuplesDataBytes / microseconds << "[MB/sec]" << Endl;
        // CTEST << " " << Endl;
    }

    std::vector<TBuffer, TMKQLAllocator<TBuffer>> PackedTupleBuckets(nBuckets);
    std::vector<TBuffer, TMKQLAllocator<TBuffer>> OverflowBuckets(nBuckets);

    {
        // std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();

        for (ui32 i = 0; i < sizes.size(); ++i) {
            auto [TLsize, Osize] = sizes[i];
            PackedTupleBuckets[i].resize(TLsize);
            std::memset(PackedTupleBuckets[i].data(), 0, TLsize);
            OverflowBuckets[i].resize(Osize);
            std::memset(OverflowBuckets[i].data(), 0, Osize);
        }

        // std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
        // ui64 microseconds = std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count();
        // if (microseconds == 0) microseconds = 1;
        // // totalTime += microseconds;

        // CTEST << "Memory allocation time: " << microseconds  << "[microseconds]" << Endl;
        // CTEST << "Memory allocation speed: " << TuplesDataBytes / microseconds << "[MB/sec]" << Endl;
        // CTEST << " " << Endl;
    }

    std::array<THolder<TAccumulator>, 2> accums;
    accums[0] = MakeHolder<TAccumulatorImpl>(
        tl.Get(), 0, log2Buckets, std::vector(PackedTupleBuckets), std::vector(OverflowBuckets));
    accums[1] = MakeHolder<TSMBAccumulatorImpl>(
        tl.Get(), 0, log2Buckets, std::vector(PackedTupleBuckets), std::vector(OverflowBuckets));

    for (auto &accum : accums)
    {
        std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();

        accum->AddData(res.data(), overflow.data(), nTuples);

        std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
        ui64 microseconds = std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count();
        if (microseconds == 0) microseconds = 1;
        // totalTime += microseconds;
    
        CTEST << "------------------" << Endl << " " << Endl;

        CTEST << "Accumulation time: " << microseconds  << "[microseconds]" << Endl;
        CTEST << "Accumulation speed: " << TuplesDataBytes / microseconds << "[MB/sec]" << Endl;
        CTEST << " " << Endl;

        CTEST << "Total time: " << totalTime + microseconds  << "[microseconds]" << Endl;
        CTEST << "Resulting speed: " << TuplesDataBytes / (totalTime + microseconds) << "[MB/sec]" << Endl;
        CTEST << " " << Endl;
    }

    CTEST << "============= BENCH END =============" << Endl << " " << Endl;

    return accums[0]->GetBucket(0).NTuples > 0;
}

bool RunVarSizedAccumulatorBench(ui64 nTuples, ui64 nCols, ui64 log2Buckets) {
    auto nBuckets = 1 << log2Buckets;
    TScopedAlloc alloc(__LOCATION__);
    using TBuffer = TAccumulator::TBuffer;

    ui64 totalTime = 0;

    TColumnDesc kc;
    kc.Role = EColumnRole::Key;
    kc.SizeType = EColumnSizeType::Variable;
    kc.DataSize = 8;

    std::vector<TColumnDesc> columns(nCols, kc);
    auto tl = TTupleLayout::Create(columns);
    ui64 TuplesDataBytes = tl->TotalRowSize * nTuples;

    std::vector<ui32> col(1, 0);
    std::vector<ui8> colData;

    for (ui64 i = 0; i < nTuples; ++i) {
        const auto& str = threeLetterValues[i % threeLetterValues.size()];
        for (ui8 k = 0; k < 6; ++k) {
            for (auto c: str) {
                colData.push_back(c);
            }
        }
        col.push_back(colData.size());
    }

    std::vector<const ui8*> cols(2 * nCols);
    for (ui64 i = 0; i < nCols; i++) {
        cols[2 * i] = (ui8*) col.data();
        cols[2 * i + 1] = (ui8*) colData.data();
    }

    std::vector<ui8> colValid((nTuples + 7)/8, ~0);
    std::vector<const ui8*> colsValid(2 * nCols);
    for (ui64 i = 0; i < nCols; i++) {
        colsValid[2 * i] = colValid.data();
        colsValid[2 * i + 1] = nullptr;
    }

    std::vector<ui8> res(TuplesDataBytes, 0);
    std::vector<ui8, TMKQLAllocator<ui8>> overflow;

    tl->Pack(cols.data(), colsValid.data(), res.data(), overflow, 0, nTuples);
    TuplesDataBytes += overflow.size();
    overflow.resize(0);

    CTEST << "============ BENCH BEGIN ============" << Endl;
    CTEST << "Test config:" << Endl;
    CTEST << ">  nTuples: " << nTuples << Endl;
    CTEST << ">  nCols: " << nCols << Endl;
    CTEST << ">  nBuckets: " << nBuckets << Endl;
    CTEST << ">  Dataset size: " << TuplesDataBytes / (1024 * 1024) << "[MB]" << Endl;
    CTEST << " " << Endl;

    auto resesData = std::vector<std::vector<ui8, TMKQLAllocator<ui8>>>(1u << log2Buckets);
    std::vector<std::vector<ui8, TMKQLAllocator<ui8>>> overflowsData(1u << log2Buckets);

    auto reses = TPaddedPtr(resesData.data(), sizeof(resesData[0]));
    auto overflows = TPaddedPtr(overflowsData.data(), sizeof(overflowsData[0]));
    
    tl->BucketPack(cols.data(), colsValid.data(), reses, overflows, 0, nTuples, log2Buckets);
    for (auto& bres : resesData) {
        bres.resize(0);
    }
    for (auto& boverflow : overflowsData) {
        boverflow.resize(0);
    }

    auto begintp = std::chrono::steady_clock::now();
    tl->BucketPack(cols.data(), colsValid.data(), reses, overflows, 0, nTuples, log2Buckets);
    auto endtp = std::chrono::steady_clock::now();
    auto bucketPackTime = std::chrono::duration_cast<std::chrono::microseconds>(endtp - begintp).count();
    if (bucketPackTime == 0) bucketPackTime = 1;

    CTEST << "Bucket pack time: " << bucketPackTime  << "[microseconds]" << Endl;
    CTEST << "Bucket pack speed: " << TuplesDataBytes / bucketPackTime << "[MB/sec]" << Endl;
    CTEST << " " << Endl;

    begintp = std::chrono::steady_clock::now();
    tl->Pack(cols.data(), colsValid.data(), res.data(), overflow, 0, nTuples);
    endtp = std::chrono::steady_clock::now();
    auto packTime = std::chrono::duration_cast<std::chrono::microseconds>(endtp - begintp).count();
    if (packTime == 0) packTime = 1;
    totalTime += packTime;

    CTEST << "Pack time: " << packTime  << "[microseconds]" << Endl;
    CTEST << "Pack speed: " << TuplesDataBytes / packTime << "[MB/sec]" << Endl;
    CTEST << " " << Endl;

    ui32 bitsCount = arrow::BitUtil::NumRequiredBits(nBuckets - 1);
    ui32 shift = 32 - bitsCount;
    ui32 mask = (1 << bitsCount) - 1;

    std::vector<std::pair<ui64, ui64>, TMKQLAllocator<std::pair<ui64, ui64>>> sizes(nBuckets, {0, 0});

    {
        std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();

        Histogram hist;
        hist.AddData(tl.Get(), res.data(), nTuples, TAccumulator::GetBucketId, shift, mask);
        hist.EstimateSizes(sizes);

        std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
        ui64 microseconds = std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count();
        if (microseconds == 0) microseconds = 1;
        // totalTime += microseconds;

        CTEST << "Histogram build time: " << microseconds  << "[microseconds]" << Endl;
        CTEST << "Histogram build speed: " << TuplesDataBytes / microseconds << "[MB/sec]" << Endl;
        CTEST << " " << Endl;
    }

    std::vector<TBuffer, TMKQLAllocator<TBuffer>> PackedTupleBuckets(nBuckets);
    std::vector<TBuffer, TMKQLAllocator<TBuffer>> OverflowBuckets(nBuckets);

    {
        std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();

        for (ui32 i = 0; i < sizes.size(); ++i) {
            auto [TLsize, Osize] = sizes[i];
            PackedTupleBuckets[i].resize(TLsize);
            std::memset(PackedTupleBuckets[i].data(), 0, TLsize);
            OverflowBuckets[i].resize(Osize);
            std::memset(OverflowBuckets[i].data(), 0, Osize);
        }

        std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
        ui64 microseconds = std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count();
        if (microseconds == 0) microseconds = 1;
        // totalTime += microseconds;

        CTEST << "Memory allocation time: " << microseconds  << "[microseconds]" << Endl;
        CTEST << "Memory allocation speed: " << TuplesDataBytes / microseconds << "[MB/sec]" << Endl;
        CTEST << " " << Endl;
    }

    std::array<THolder<TAccumulator>, 2> accums;
    accums[0] = MakeHolder<TAccumulatorImpl>(
        tl.Get(), 0, log2Buckets, std::vector(PackedTupleBuckets), std::vector(OverflowBuckets));
    accums[1] = MakeHolder<TSMBAccumulatorImpl>(
        tl.Get(), 0, log2Buckets, std::vector(PackedTupleBuckets), std::vector(OverflowBuckets));

    for (auto &accum : accums)
    {
        std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();

        accum->AddData(res.data(), overflow.data(), nTuples);

        std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
        ui64 microseconds = std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count();
        if (microseconds == 0) microseconds = 1;
        // totalTime += microseconds;
    
        CTEST << "------------------" << Endl << " " << Endl;

        CTEST << "Accumulation time: " << microseconds  << "[microseconds]" << Endl;
        CTEST << "Accumulation speed: " << TuplesDataBytes / microseconds << "[MB/sec]" << Endl;
        CTEST << " " << Endl;

        CTEST << "Total time: " << totalTime + microseconds  << "[microseconds]" << Endl;
        CTEST << "Resulting speed: " << TuplesDataBytes / (totalTime + microseconds) << "[MB/sec]" << Endl;
        CTEST << " " << Endl;
    }

    CTEST << "============= BENCH END =============" << Endl << " " << Endl;

    return accums[0]->GetBucket(0).NTuples > 0;
}

} // namespace

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

    auto accum = TAccumulator::Create(tl.Get(), 0, 1);
    auto info = accum->GetBucket(0);

    UNIT_ASSERT(info.NTuples == 0);
    CTEST << "";
} // Y_UNIT_TEST(CreateAccumulator)

Y_UNIT_TEST(MultipleAddToAccumulator) {
    TScopedAlloc alloc(__LOCATION__);

    TColumnDesc kc;
    kc.Role = EColumnRole::Key;
    kc.DataSize = 8;

    std::vector<TColumnDesc> columns{kc};
    auto tl = TTupleLayout::Create(columns);
    const ui64 NTuples = 1000;
    const ui64 TuplesDataBytes = (tl->TotalRowSize) * NTuples;

    std::vector<ui64> col(NTuples, 0);
    std::vector<ui8> res(TuplesDataBytes, 0);
    for (ui32 i = 0; i < NTuples; ++i) {
        col[i] = i;
    }
    const ui8* cols[1];
    cols[0] = (ui8*) col.data();

    std::vector<ui8> colValid((NTuples + 7)/8, ~0);
    const ui8 *colsValid[] = {
        colValid.data(),
    };

    std::vector<ui8, TMKQLAllocator<ui8>> overflow;
    tl->Pack(cols, colsValid, res.data(), overflow, 0, NTuples);

    auto accum = TAccumulator::Create(tl.Get(), 0, 3);

    for (int i = 0; i < 2; ++i) {
        accum->AddData(res.data(), overflow.data(), NTuples); // + 1000 elements
    }

    ui32 totalCount = 0;
    for (ui32 i = 0; i < 8; ++i) {
        auto info = accum->GetBucket(i);
        totalCount += info.NTuples;
        UNIT_ASSERT(info.NTuples > 0);
    }

    UNIT_ASSERT(totalCount == 2 * NTuples);
} // Y_UNIT_TEST(MultipleAddToAccumulator)

Y_UNIT_TEST(AddVarSizedDataToAccumulator) {
    TScopedAlloc alloc(__LOCATION__);

    TColumnDesc kc;
    kc.Role = EColumnRole::Key;
    kc.SizeType = EColumnSizeType::Variable;
    kc.DataSize = 8;

    std::vector<TColumnDesc> columns{kc};
    auto tl = TTupleLayout::Create(columns);
    const ui64 NTuples = 2;
    const ui64 TuplesDataBytes = (tl->TotalRowSize) * NTuples;
    std::vector<ui8> res(TuplesDataBytes, 0);

    std::vector<ui32> col(1, 0);
    std::vector<ui8> colData;
    std::vector<TString> strs{
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
    };

    for (const auto& str: strs) {
        for (auto c: str) {
            colData.push_back(c);
        }
        col.push_back(colData.size());
    }
    UNIT_ASSERT_VALUES_EQUAL(col.size(), NTuples + 1);

    const ui8* cols[2];
    cols[0] = (ui8*) col.data();
    cols[1] = (ui8*) colData.data();

    std::vector<ui8> colValid((NTuples + 7)/8, ~0);
    const ui8 *colsValid[] = {
        colValid.data(),
        nullptr,
    };

    std::vector<ui8, TMKQLAllocator<ui8>> overflow;
    tl->Pack(cols, colsValid, res.data(), overflow, 0, NTuples);

    auto accum = TAccumulator::Create(tl.Get(), 0, 5);
    accum->AddData(res.data(), overflow.data(), NTuples);

    ui32 total = 0;
    for (ui32 i = 0; i < 32; ++i) {
        auto info = accum->GetBucket(i);
        UNIT_ASSERT(info.NTuples <= 1);
        total += info.NTuples;
    }
    UNIT_ASSERT(total == 2);
} // Y_UNIT_TEST(AddVarSizedDataToAccumulator)

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
        ui32 cols = 1 + (rng() % 20);
        columns.resize(cols);
        colsdata.resize(cols);
        colsptr.resize(cols);
        isValidData.resize(cols);
        isValidPtr.resize(cols);
        ui32 isValidSize = (rows + 7)/8;
        for (ui32 j = 0; j < cols; ++j) {
            auto &col = columns[j];
            col.Role = (rng() % 10 < 1) ? EColumnRole::Key : EColumnRole::Payload;
            col.DataSize = 1u << (rng() % 10);
            col.SizeType = EColumnSizeType::Fixed;
            colsdata[j].resize(rows*col.DataSize);
            colsptr[j] = colsdata[j].data();
            isValidData[j].resize(isValidSize);
            isValidPtr[j] = isValidData[j].data();
            std::generate(isValidData[j].begin(), isValidData[j].end(), rng);
        }
        auto tl = TTupleLayout::Create(columns);
        std::vector<ui8> res;
        for (ui32 subtest = 0; subtest < 10; ++subtest) {
            ui32 log2Buckets = rng() % 11;
            ui32 nBuckets = (1 << log2Buckets);
            ui32 bitsCount = arrow::BitUtil::NumRequiredBits(nBuckets - 1);
            ui32 shift = 32 - bitsCount;
            ui32 mask = (1 << bitsCount) - 1;
            ui32 subRows = 1 + (rows ? rng() % (rows - 1) : 0);
            ui32 off = subRows != rows ? rng() % (rows - subRows) : 0;
            std::vector<ui8, TMKQLAllocator<ui8>> overflow;
            res.resize(subRows*tl->TotalRowSize);
            tl->Pack(colsptr.data(), isValidPtr.data(), res.data(), overflow, off, subRows);

            auto accum = TAccumulator::Create(tl.Get(), 0, log2Buckets);
            accum->AddData(res.data(), overflow.data(), subRows);

            ui32 totalCount = 0;
            for (ui32 i = 0; i < nBuckets; ++i) {
                auto info = accum->GetBucket(i);
                const ui8* Bucket = info.PackedTuples->data();

                ui32 offset = 0;
                if (info.NTuples > 0) {
                    UNIT_ASSERT(info.PackedTuples->size() > 0);
                    UNIT_ASSERT(info.PackedTuples->size() / tl->TotalRowSize == info.NTuples);
                    for (ui32 count = 0; count < info.NTuples; ++count) {
                        auto maskedHash = TAccumulator::GetBucketId(ReadUnaligned<ui32>(Bucket + offset), shift, mask);
                        UNIT_ASSERT(maskedHash == i);
                        offset += info.Layout->TotalRowSize;
                        totalCount++;
                    }
                }
            }
            UNIT_ASSERT_VALUES_EQUAL(totalCount, subRows);
        }
    }
} // Y_UNIT_TEST(AccumulatorFuzz)

Y_UNIT_TEST(BenchAccumulator_VariateNTuples) {
    Cerr << ">>>>>>>>>>>>>>>>>>>>>>> BenchAccumulator_VariateNTuples <<<<<<<<<<<<<<<<<<<<<<<<<<<<" << Endl;
    UNIT_ASSERT(RunFixSizedAccumulatorBench(1e5, 2, 3));
    UNIT_ASSERT(RunFixSizedAccumulatorBench(1e6, 2, 3));
    UNIT_ASSERT(RunFixSizedAccumulatorBench(1e7, 2, 3));
    UNIT_ASSERT(RunFixSizedAccumulatorBench(1e8, 2, 3));
    Cerr << ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>><<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<" << Endl << " " << Endl;
} // Y_UNIT_TEST(BenchAccumulator_VariateNTuples)

Y_UNIT_TEST(BenchAccumulator_VariateNColumns) {
    Cerr << ">>>>>>>>>>>>>>>>>>>>>>> BenchAccumulator_VariateNColumns <<<<<<<<<<<<<<<<<<<<<<<<<<<<" << Endl;
    UNIT_ASSERT(RunFixSizedAccumulatorBench(1e7,  1, 4));
    UNIT_ASSERT(RunFixSizedAccumulatorBench(1e7,  2, 4));
    UNIT_ASSERT(RunFixSizedAccumulatorBench(1e7,  4, 4));
    UNIT_ASSERT(RunFixSizedAccumulatorBench(1e6,  8, 4));
    UNIT_ASSERT(RunFixSizedAccumulatorBench(1e6, 16, 4));
    UNIT_ASSERT(RunFixSizedAccumulatorBench(1e6, 24, 4));
    UNIT_ASSERT(RunFixSizedAccumulatorBench(1e6, 32, 4));
    Cerr << ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>><<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<" << Endl << " " << Endl;
} // Y_UNIT_TEST(BenchAccumulator_VariateNColumns)

Y_UNIT_TEST(BenchAccumulator_VariateNBuckets) {
    Cerr << ">>>>>>>>>>>>>>>>>>>>>>> BenchAccumulator_VariateNBuckets <<<<<<<<<<<<<<<<<<<<<<<<<<<<" << Endl;
    UNIT_ASSERT(RunFixSizedAccumulatorBench(1e7, 4,  2));
    UNIT_ASSERT(RunFixSizedAccumulatorBench(1e7, 4,  3));
    UNIT_ASSERT(RunFixSizedAccumulatorBench(1e7, 4,  4));
    UNIT_ASSERT(RunFixSizedAccumulatorBench(1e7, 4,  5));
    UNIT_ASSERT(RunFixSizedAccumulatorBench(1e7, 4,  6));
    UNIT_ASSERT(RunFixSizedAccumulatorBench(1e7, 4,  7));
    UNIT_ASSERT(RunFixSizedAccumulatorBench(1e7, 4,  8));
    UNIT_ASSERT(RunFixSizedAccumulatorBench(1e7, 4,  9));
    UNIT_ASSERT(RunFixSizedAccumulatorBench(1e7, 4, 10));
    UNIT_ASSERT(RunFixSizedAccumulatorBench(1e7, 4, 11));
    UNIT_ASSERT(RunFixSizedAccumulatorBench(1e7, 4, 12));
    UNIT_ASSERT(RunFixSizedAccumulatorBench(1e7, 4, 13));
    Cerr << ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>><<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<" << Endl << " " << Endl;
} // Y_UNIT_TEST(BenchAccumulator_VariateNBuckets)

Y_UNIT_TEST(VarSized_BenchAccumulator_VariateNTuples) {
    Cerr << ">>>>>>>>>>>>>>>>>>>>>>> VarSized_BenchAccumulator_VariateNTuples <<<<<<<<<<<<<<<<<<<<<<<<<<<<" << Endl;
    UNIT_ASSERT(RunVarSizedAccumulatorBench(1e5, 2, 3));
    UNIT_ASSERT(RunVarSizedAccumulatorBench(1e6, 2, 3));
    UNIT_ASSERT(RunVarSizedAccumulatorBench(1e7, 2, 3));
    Cerr << ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>><<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<" << Endl << " " << Endl;
} // Y_UNIT_TEST(VarSized_BenchAccumulator_VariateNTuples)

Y_UNIT_TEST(VarSized_BenchAccumulator_VariateNColumns) {
    Cerr << ">>>>>>>>>>>>>>>>>>>>>>> VarSized_BenchAccumulator_VariateNColumns <<<<<<<<<<<<<<<<<<<<<<<<<<<<" << Endl;
    UNIT_ASSERT(RunVarSizedAccumulatorBench(1e7,  1, 4));
    UNIT_ASSERT(RunVarSizedAccumulatorBench(1e7,  2, 4));
    UNIT_ASSERT(RunVarSizedAccumulatorBench(1e7,  4, 4));
    UNIT_ASSERT(RunVarSizedAccumulatorBench(1e7,  8, 4));
    Cerr << ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>><<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<" << Endl << " " << Endl;
} // Y_UNIT_TEST(VarSized_BenchAccumulator_VariateNColumns)

Y_UNIT_TEST(VarSized_BenchAccumulator_VariateNBuckets) {
    Cerr << ">>>>>>>>>>>>>>>>>>>>>>> VarSized_BenchAccumulator_VariateNBuckets <<<<<<<<<<<<<<<<<<<<<<<<<<<<" << Endl;
    UNIT_ASSERT(RunVarSizedAccumulatorBench(1e7, 2,  2));
    UNIT_ASSERT(RunVarSizedAccumulatorBench(1e7, 2,  3));
    UNIT_ASSERT(RunVarSizedAccumulatorBench(1e7, 2,  4));
    UNIT_ASSERT(RunVarSizedAccumulatorBench(1e7, 2,  5));
    UNIT_ASSERT(RunVarSizedAccumulatorBench(1e7, 2,  6));
    UNIT_ASSERT(RunVarSizedAccumulatorBench(1e7, 2,  7));
    UNIT_ASSERT(RunVarSizedAccumulatorBench(1e7, 2,  8));
    UNIT_ASSERT(RunVarSizedAccumulatorBench(1e7, 2,  9));
    UNIT_ASSERT(RunVarSizedAccumulatorBench(1e7, 2, 10));
    UNIT_ASSERT(RunVarSizedAccumulatorBench(1e7, 2, 11));
    UNIT_ASSERT(RunVarSizedAccumulatorBench(1e7, 2, 12));
    UNIT_ASSERT(RunVarSizedAccumulatorBench(1e7, 2, 13));
    Cerr << ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>><<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<" << Endl << " " << Endl;
} // Y_UNIT_TEST(VarSized_BenchAccumulator_VariateNBuckets)

} // Y_UNIT_TEST_SUITE(Accumulator)


}
} // namespace NMiniKQL
} // namespace NKikimr
