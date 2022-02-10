#pragma once
#include "arrow_helpers.h"

#include <contrib/libs/xxhash/xxhash.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/api.h>

#include <type_traits>

namespace NKikimr::NArrow {

class THashSharding {
public:
    THashSharding(ui32 shardsCount)
        : ShardsCount(shardsCount)
    {}

    std::vector<ui32> MakeSharding(const std::shared_ptr<arrow::RecordBatch>& batch,
                                   const TVector<TString>& shardingColumns) const
    {
        if (shardingColumns.size() != 1) {
            return {};
        }

        auto array = batch->GetColumnByName(shardingColumns[0]);
        if (!array) {
            return {};
        }

        std::vector<ui32> out(batch->num_rows());

        SwitchType(array->type_id(), [&](const auto& type) {
            using TWrap = std::decay_t<decltype(type)>;
            using TArray = typename arrow::TypeTraits<typename TWrap::T>::ArrayType;

            auto& column = static_cast<const TArray&>(*array);

            for (int row = 0; row < batch->num_rows(); ++row) {
                out[row] = ShardNo(column.GetView(row));
            }
            return true;
        });

        return out;
    }

private:
    ui32 ShardsCount;

    template <typename T, std::enable_if_t<std::is_arithmetic<T>::value, bool> = true>
    ui32 ShardNo(T value) const {
        return XXH64(&value, sizeof(value), 0) % ShardsCount;
    }

    ui32 ShardNo(arrow::util::string_view value) const {
        return XXH64(value.data(), value.size(), 0) % ShardsCount;
    }
};

// KIKIMR-11529
class TLogsSharding {
public:
    static constexpr ui32 DEFAULT_ACITVE_SHARDS = 10;
    static constexpr TDuration DEFAULT_CHANGE_PERIOD = TDuration::Minutes(5);

    // TODO
    TLogsSharding(ui32 shardsCountTotal, ui32 shardsCountActive = DEFAULT_ACITVE_SHARDS, TDuration changePeriod = DEFAULT_CHANGE_PERIOD)
        : ShardsCount(shardsCountTotal)
        , NumActive(Min<ui32>(shardsCountActive, ShardsCount))
        , TsMin(0)
        , ChangePeriod(changePeriod.MicroSeconds())
    {}

    // tsMin = GetTsMin(tabletIdsMap, timestamp);
    // tabletIds = GetTableIdsByTs(tabletIdsMap, timestamp);
    // numIntervals = tabletIds.size() / nActive;
    // tsInterval = (timestamp - tsMin) / changePeriod;
    // shardNo = (hash(uid) % nActive) + (tsInterval % numIntervals) * nActive;
    // tabletId = tabletIds[shardNo];
    ui32 ShardNo(ui64 timestamp, arrow::util::string_view uid) const {
        ui64 uidHash = XXH64(uid.data(), uid.size(), 0);
        ui32 tsInterval = (timestamp - TsMin) / ChangePeriod;
        ui32 numIntervals = ShardsCount / NumActive;
        return ((uidHash % NumActive) + (tsInterval % numIntervals) * NumActive) % ShardsCount;
    }

    std::vector<ui32> MakeSharding(const std::shared_ptr<arrow::RecordBatch>& batch,
                                   const TVector<TString>& shardingColumns) const
    {
        if (shardingColumns.size() != 2) {
            return {};
        }

        auto tsFiled = batch->schema()->GetFieldByName(shardingColumns[0]);
        auto uidFiled = batch->schema()->GetFieldByName(shardingColumns[1]);
        auto tsArray = batch->GetColumnByName(shardingColumns[0]);
        auto uidArray = batch->GetColumnByName(shardingColumns[1]);
        if (!tsArray || !uidArray || !tsFiled || !uidFiled ||
            (tsFiled->type()->id() != arrow::Type::TIMESTAMP) ||
            (uidFiled->type()->id() != arrow::Type::STRING)) {
            return {};
        }

        auto tsColumn = std::static_pointer_cast<arrow::TimestampArray>(tsArray);
        auto uidColumn = std::static_pointer_cast<arrow::StringArray>(uidArray);
        std::vector<ui32> out;
        out.reserve(batch->num_rows());

        for (int row = 0; row < batch->num_rows(); ++row) {
            ui32 shardNo = ShardNo(tsColumn->Value(row), uidColumn->GetView(row));
            Y_VERIFY(shardNo < ShardsCount);

            out.emplace_back(shardNo);
        }

        return out;
    }

private:
    ui32 ShardsCount;
    ui32 NumActive;
    ui64 TsMin;
    ui64 ChangePeriod;
};

}
