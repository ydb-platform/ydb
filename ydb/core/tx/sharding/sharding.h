#pragma once

#include "hash.h"

#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/formats/arrow/hash/calcer.h>
#include <ydb/core/formats/arrow/size_calcer.h>

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/conclusion/result.h>
#include <ydb/library/conclusion/status.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/api.h>
#include <contrib/libs/xxhash/xxhash.h>

#include <util/random/random.h>

#include <type_traits>

namespace NKikimr::NSharding {

struct TExternalTableColumn;

class TShardingBase {
private:
    YDB_READONLY(ui32, ShardsCount, 0);
public:
    using TColumn = TExternalTableColumn;
public:
    static TConclusion<std::unique_ptr<TShardingBase>> BuildShardingOperator(const NKikimrSchemeOp::TColumnTableSharding& shardingInfo);

    virtual std::vector<ui32> MakeSharding(const std::shared_ptr<arrow::RecordBatch>& batch) const = 0;

    TConclusion<THashMap<ui64, std::vector<NArrow::TSerializedBatch>>> SplitByShards(const std::shared_ptr<arrow::RecordBatch>& batch, const std::vector<ui64>& shardIds, const ui64 chunkBytesLimit) {
        auto sharding = MakeSharding(batch);
        std::vector<std::shared_ptr<arrow::RecordBatch>> chunks;
        if (ShardsCount == 1) {
            chunks = {batch};
        } else {
            chunks = NArrow::ShardingSplit(batch, sharding, shardIds.size());
        }
        AFL_VERIFY(chunks.size() == ShardsCount);
        AFL_VERIFY(shardIds.size() == ShardsCount);
        NArrow::TBatchSplitttingContext context(chunkBytesLimit);
        THashMap<ui64, std::vector<NArrow::TSerializedBatch>> result;
        for (ui32 i = 0; i < chunks.size(); ++i) {
            if (!chunks[i]) {
                continue;
            }
            auto blobsSplittedConclusion = NArrow::SplitByBlobSize(chunks[i], context);
            if (blobsSplittedConclusion.IsFail()) {
                return TConclusionStatus::Fail("cannot split batch in according to limits: " + blobsSplittedConclusion.GetErrorMessage());
            }
            result.emplace(shardIds[i], blobsSplittedConclusion.DetachResult());
        }
        return result;
    }

    virtual TString DebugString() const;

    TShardingBase(const ui32 shardsCount)
        : ShardsCount(shardsCount)
    {
        Y_ABORT_UNLESS(ShardsCount);
    }
    virtual ~TShardingBase() = default;
};

class THashShardingImpl: public TShardingBase {
private:
    using TBase = TShardingBase;
    const ui64 Seed;
    const NArrow::NHash::TXX64 HashCalcer;
protected:
    const std::vector<TString> ShardingColumns;
public:
    THashShardingImpl(const ui32 shardsCount, const std::vector<TString>& columnNames, ui64 seed = 0)
        : TBase(shardsCount)
        , Seed(seed)
        , HashCalcer(columnNames, NArrow::NHash::TXX64::ENoColumnPolicy::Verify, Seed)
        , ShardingColumns(columnNames) {
    }

    virtual TString DebugString() const override {
        return TBase::DebugString() + ";Columns: " + JoinSeq(", ", GetShardingColumns());
    }

    virtual std::vector<ui64> MakeHashes(const std::shared_ptr<arrow::RecordBatch>& batch) const {
        return HashCalcer.Execute(batch).value_or(Default<std::vector<ui64>>());
    }

    template <typename T>
    static ui64 CalcHash(const T value, const ui32 seed = 0) {
        static_assert(std::is_arithmetic<T>::value);
        return XXH64(&value, sizeof(value), seed);
    }

    virtual const std::vector<TString>& GetShardingColumns() const {
        return ShardingColumns;
    }
};

class THashSharding : public THashShardingImpl {
private:
    using TBase = THashShardingImpl;
public:
    THashSharding(ui32 shardsCount, const std::vector<TString>& columnNames, ui64 seed = 0)
        : TBase(shardsCount, columnNames, seed)
    {}

    virtual std::vector<ui32> MakeSharding(const std::shared_ptr<arrow::RecordBatch>& batch) const override;

    template <typename T>
    static ui32 ShardNo(const T value, const ui32 shardsCount, const ui32 seed = 0) {
        Y_ASSERT(shardsCount);
        return CalcHash(value, seed) % shardsCount;
    }
};

class TRandomSharding: public TShardingBase {
private:
    using TBase = TShardingBase;
public:
    using TBase::TBase;

    virtual std::vector<ui32> MakeSharding(const std::shared_ptr<arrow::RecordBatch>& batch) const override {
        return std::vector<ui32>(batch->num_rows(), RandomNumber<ui32>(GetShardsCount()));
    }

};

class TConsistencySharding64: public THashSharding {
private:
    using TBase = THashSharding;

    static ui32 CalcShardIdImpl(const ui64 hash, const ui32 shardsCount) {
        return hash / (Max<ui64>() / shardsCount);
    }

public:
    TConsistencySharding64(ui32 shardsCount, const std::vector<TString>& columnNames, ui64 seed = 0)
        : TBase(shardsCount, columnNames, seed){
    }

    virtual std::vector<ui32> MakeSharding(const std::shared_ptr<arrow::RecordBatch>& batch) const override {
        auto hashes = MakeHashes(batch);
        std::vector<ui32> result;
        result.reserve(hashes.size());
        for (auto&& i : hashes) {
            result.emplace_back(CalcShardIdImpl(i, GetShardsCount()));
        }
        return result;
    }

    template <typename T>
    static ui32 ShardNo(const T value, const ui32 shardsCount, const ui32 seed = 0) {
        Y_ASSERT(shardsCount);
        return CalcShardIdImpl(CalcHash(value, seed), shardsCount);
    }
};

// KIKIMR-11529
class TLogsSharding : public THashShardingImpl {
private:
    using TBase = THashShardingImpl;
    ui32 NumActive;
    ui64 TsMin;
    ui64 ChangePeriod;
public:
    static constexpr ui32 DEFAULT_ACITVE_SHARDS = 10;
    static constexpr TDuration DEFAULT_CHANGE_PERIOD = TDuration::Minutes(5);

    TLogsSharding(ui32 shardsCountTotal, const std::vector<TString>& columnNames, ui32 shardsCountActive, TDuration changePeriod = DEFAULT_CHANGE_PERIOD)
        : TBase(shardsCountTotal, columnNames)
        , NumActive(Min<ui32>(shardsCountActive, GetShardsCount()))
        , TsMin(0)
        , ChangePeriod(changePeriod.MicroSeconds())
    {}

    // tsMin = GetTsMin(tabletIdsMap, timestamp);
    // tabletIds = GetTableIdsByTs(tabletIdsMap, timestamp);
    // numIntervals = tabletIds.size() / nActive;
    // tsInterval = (timestamp - tsMin) / changePeriod;
    // shardNo = (hash(uid) % nActive) + (tsInterval % numIntervals) * nActive;
    // tabletId = tabletIds[shardNo];
    ui32 ShardNo(ui64 timestamp, const ui64 uidHash) const {
        ui32 tsInterval = (timestamp - TsMin) / ChangePeriod;
        ui32 numIntervals = GetShardsCount() / NumActive;
        return ((uidHash % NumActive) + (tsInterval % numIntervals) * NumActive) % GetShardsCount();
    }

    virtual std::vector<ui32> MakeSharding(const std::shared_ptr<arrow::RecordBatch>& batch) const override;

};

}
