#pragma once

#include "hash.h"

#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <ydb/library/accessor/accessor.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/api.h>
#include <contrib/libs/xxhash/xxhash.h>

#include <type_traits>

namespace NYql::NUdf {
class TUnboxedValue;
}

namespace NKikimr::NSharding {

class TUnboxedValueReader;
struct TExternalTableColumn;

class TShardingBase {
private:
    YDB_READONLY(ui32, ShardsCount, 0);
public:
    using TColumn = TExternalTableColumn;

protected:
    static void AppendField(const std::shared_ptr<arrow::Array>& array, int row, IHashCalcer& hashCalcer);
public:
    static std::unique_ptr<TShardingBase> BuildShardingOperator(const NKikimrSchemeOp::TColumnTableSharding& shardingInfo);

    virtual const std::vector<TString>& GetShardingColumns() const = 0;
    ui32 CalcShardId(const NYql::NUdf::TUnboxedValue& value, const TUnboxedValueReader& readerInfo) const {
        return CalcHash(value, readerInfo) % ShardsCount;
    }
    virtual ui64 CalcHash(const NYql::NUdf::TUnboxedValue& value, const TUnboxedValueReader& readerInfo) const = 0;

    virtual std::vector<ui32> MakeSharding(const std::shared_ptr<arrow::RecordBatch>& batch) const;
    virtual std::vector<ui64> MakeHashes(const std::shared_ptr<arrow::RecordBatch>& batch) const = 0;

    virtual TString DebugString() const;

    TShardingBase(const ui32 shardsCount)
        : ShardsCount(shardsCount)
    {

    }
    virtual ~TShardingBase() = default;
};

class THashSharding : public TShardingBase {
private:
    using TBase = TShardingBase;
    ui64 Seed;
    std::vector<TString> ShardingColumns;
public:
    THashSharding(ui32 shardsCount, const std::vector<TString>& columnNames, ui64 seed = 0)
        : TBase(shardsCount)
        , Seed(seed)
        , ShardingColumns(columnNames)
    {}

    virtual std::vector<ui64> MakeHashes(const std::shared_ptr<arrow::RecordBatch>& batch) const override;

    template <typename T>
    static ui64 CalcHash(const T value, const ui32 seed = 0) {
        static_assert(std::is_arithmetic<T>::value);
        return XXH64(&value, sizeof(value), seed);
    }

    template <typename T>
    static ui32 ShardNo(const T value, const ui32 shardsCount, const ui32 seed = 0) {
        Y_ASSERT(shardsCount);
        return CalcHash(value, seed) % shardsCount;
    }
    virtual const std::vector<TString>& GetShardingColumns() const override {
        return ShardingColumns;
    }

    virtual ui64 CalcHash(const NYql::NUdf::TUnboxedValue& value, const TUnboxedValueReader& readerInfo) const override;
};

// KIKIMR-11529
class TLogsSharding : public TShardingBase {
private:
    using TBase = TShardingBase;
    ui32 NumActive;
    ui64 TsMin;
    ui64 ChangePeriod;
    const std::vector<TString> ShardingColumns;
public:
    static constexpr ui32 DEFAULT_ACITVE_SHARDS = 10;
    static constexpr TDuration DEFAULT_CHANGE_PERIOD = TDuration::Minutes(5);

    TLogsSharding(ui32 shardsCountTotal, const std::vector<TString>& columnNames, ui32 shardsCountActive, TDuration changePeriod = DEFAULT_CHANGE_PERIOD)
        : TBase(shardsCountTotal)
        , NumActive(Min<ui32>(shardsCountActive, GetShardsCount()))
        , TsMin(0)
        , ChangePeriod(changePeriod.MicroSeconds())
        , ShardingColumns(columnNames)
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
    virtual std::vector<ui64> MakeHashes(const std::shared_ptr<arrow::RecordBatch>& batch) const override;

    virtual const std::vector<TString>& GetShardingColumns() const override {
        return ShardingColumns;
    }

    virtual ui64 CalcHash(const NYql::NUdf::TUnboxedValue& value, const TUnboxedValueReader& readerInfo) const override;

};

}
