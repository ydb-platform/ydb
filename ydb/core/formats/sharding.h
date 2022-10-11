#pragma once
#include "arrow_helpers.h"

#include <contrib/libs/xxhash/xxhash.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/api.h>

#include <type_traits>

namespace NKikimr::NArrow {

class TShardingBase {
protected:
    static void AppendField(const std::shared_ptr<arrow::Array>& array, int row, std::string& concat) {
        NArrow::SwitchType(array->type_id(), [&](const auto& type) {
            using TWrap = std::decay_t<decltype(type)>;
            using T = typename TWrap::T;
            using TArray = typename arrow::TypeTraits<T>::ArrayType;

            if (!array->IsNull(row)) {
                auto& typedArray = static_cast<const TArray&>(*array);
                auto value = typedArray.GetView(row);
                if constexpr (arrow::has_string_view<T>()) {
                    concat.append(value.data(), value.size());
                } else if constexpr (arrow::has_c_type<T>()) {
                    if constexpr (arrow::is_physical_integer_type<T>()) {
                        concat.append(reinterpret_cast<const char*>(&value), sizeof(value));
                    } else {
                        // Do not use bool or floats for sharding
                        static_assert(arrow::is_boolean_type<T>() || arrow::is_floating_type<T>());
                    }
                } else {
                    static_assert(arrow::is_decimal_type<T>());
                }
            }
            return true;
        });
    }
};

class THashSharding : public TShardingBase {
public:
    THashSharding(ui32 shardsCount, ui64 seed = 0)
        : ShardsCount(shardsCount)
        , Seed(seed)
    {}

    std::vector<ui32> MakeSharding(const std::shared_ptr<arrow::RecordBatch>& batch,
                                   const TVector<TString>& shardingColumns) const
    {
        std::vector<std::shared_ptr<arrow::Array>> columns;
        columns.reserve(shardingColumns.size());

        for (auto& colName : shardingColumns) {
            auto array = batch->GetColumnByName(colName);
            if (!array) {
                return {};
            }
            columns.emplace_back(array);
        }

        std::vector<ui32> out(batch->num_rows());

        if (columns.size() == 1) {
            auto& array = columns[0];
            SwitchType(array->type_id(), [&](const auto& type) {
                using TWrap = std::decay_t<decltype(type)>;
                using TArray = typename arrow::TypeTraits<typename TWrap::T>::ArrayType;

                auto& column = static_cast<const TArray&>(*array);

                for (int row = 0; row < batch->num_rows(); ++row) {
                    out[row] = ShardNo(column.GetView(row));
                }
                return true;
            });
        } else {
            std::string concat;
            for (int row = 0; row < batch->num_rows(); ++row) {
                concat.clear();
                for (auto& column : columns) {
                    AppendField(column, row, concat);
                }

                out[row] = ShardNo(concat);
            }
        }

        return out;
    }

    template <typename T, std::enable_if_t<std::is_arithmetic<T>::value, bool> = true>
    ui32 ShardNo(T value) const {
        return XXH64(&value, sizeof(value), Seed) % ShardsCount;
    }

private:
    ui32 ShardsCount;
    ui64 Seed;

    ui32 ShardNo(arrow::util::string_view value) const {
        return XXH64(value.data(), value.size(), Seed) % ShardsCount;
    }
};

// KIKIMR-11529
class TLogsSharding : public TShardingBase {
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
        if (shardingColumns.size() < 2) {
            return {};
        }

        auto tsArray = batch->GetColumnByName(shardingColumns[0]);
        if (!tsArray || tsArray->type_id() != arrow::Type::TIMESTAMP) {
            return {};
        }

        std::vector<std::shared_ptr<arrow::Array>> extraColumns;
        extraColumns.reserve(shardingColumns.size() - 1);

        for (size_t i = 1; i < shardingColumns.size(); ++i) {
            auto array = batch->GetColumnByName(shardingColumns[i]);
            if (!array) {
                return {};
            }
            extraColumns.emplace_back(array);
        }

        auto tsColumn = std::static_pointer_cast<arrow::TimestampArray>(tsArray);
        std::vector<ui32> out;
        out.reserve(batch->num_rows());

        if (extraColumns.size() == 1 && extraColumns[0]->type_id() == arrow::Type::STRING) {
            auto column = std::static_pointer_cast<arrow::StringArray>(extraColumns[0]);

            for (int row = 0; row < batch->num_rows(); ++row) {
                ui32 shardNo = ShardNo(tsColumn->Value(row), column->GetView(row));
                out.emplace_back(shardNo);
            }
        } else {
            std::string concat;
            for (int row = 0; row < batch->num_rows(); ++row) {
                concat.clear();
                for (auto& column : extraColumns) {
                    AppendField(column, row, concat);
                }

                ui32 shardNo = ShardNo(tsColumn->Value(row), concat);
                out.emplace_back(shardNo);
            }
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
