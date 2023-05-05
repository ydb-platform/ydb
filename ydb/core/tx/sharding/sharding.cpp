#include "sharding.h"
#include "xx_hash.h"
#include "unboxed_reader.h"
#include <ydb/library/yql/utils/yql_panic.h>
#include <util/string/join.h>

namespace NKikimr::NSharding {

void TShardingBase::AppendField(const std::shared_ptr<arrow::Array>& array, int row, IHashCalcer& hashCalcer) {
    NArrow::SwitchType(array->type_id(), [&](const auto& type) {
        using TWrap = std::decay_t<decltype(type)>;
        using T = typename TWrap::T;
        using TArray = typename arrow::TypeTraits<T>::ArrayType;

        if (!array->IsNull(row)) {
            auto& typedArray = static_cast<const TArray&>(*array);
            auto value = typedArray.GetView(row);
            if constexpr (arrow::has_string_view<T>()) {
                hashCalcer.Update((const ui8*)value.data(), value.size());
            } else if constexpr (arrow::has_c_type<T>()) {
                if constexpr (arrow::is_physical_integer_type<T>()) {
                    hashCalcer.Update(reinterpret_cast<const ui8*>(&value), sizeof(value));
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

std::unique_ptr<TShardingBase> TShardingBase::BuildShardingOperator(const NKikimrSchemeOp::TColumnTableSharding& sharding) {
    if (sharding.HasHashSharding()) {
        auto& hashSharding = sharding.GetHashSharding();
        std::vector<TString> columnNames(hashSharding.GetColumns().begin(), hashSharding.GetColumns().end());
        if (hashSharding.GetFunction() == NKikimrSchemeOp::TColumnTableSharding::THashSharding::HASH_FUNCTION_MODULO_N) {
            return std::make_unique<THashSharding>(sharding.GetColumnShards().size(), columnNames, 0);
        } else if (hashSharding.GetFunction() == NKikimrSchemeOp::TColumnTableSharding::THashSharding::HASH_FUNCTION_CLOUD_LOGS) {
            ui32 activeShards = TLogsSharding::DEFAULT_ACITVE_SHARDS;
            if (hashSharding.HasActiveShardsCount()) {
                activeShards = hashSharding.GetActiveShardsCount();
            }
            return std::make_unique<TLogsSharding>(sharding.GetColumnShards().size(), columnNames, activeShards);
        }
    }
    return nullptr;
}

TString TShardingBase::DebugString() const {
    return "Columns: " + JoinSeq(", ", GetShardingColumns());
}

std::vector<ui32> TShardingBase::MakeSharding(const std::shared_ptr<arrow::RecordBatch>& batch) const {
    std::vector<ui64> hashes = MakeHashes(batch);
    std::vector<ui32> result;
    result.reserve(hashes.size());
    for (auto&& i : hashes) {
        result.emplace_back(i % ShardsCount);
    }
    return result;
}

std::vector<ui64> THashSharding::MakeHashes(const std::shared_ptr<arrow::RecordBatch>& batch) const {
    std::vector<std::shared_ptr<arrow::Array>> columns;
    columns.reserve(ShardingColumns.size());

    for (auto& colName : ShardingColumns) {
        auto array = batch->GetColumnByName(colName);
        if (!array) {
            return {};
        }
        columns.emplace_back(array);
    }

    std::vector<ui64> out(batch->num_rows());

    TStreamStringHashCalcer hashCalcer(Seed);

    for (int row = 0; row < batch->num_rows(); ++row) {
        hashCalcer.Start();
        for (auto& column : columns) {
            AppendField(column, row, hashCalcer);
        }
        out[row] = hashCalcer.Finish();
    }

    return out;
}

ui64 THashSharding::CalcHash(const NYql::NUdf::TUnboxedValue& value, const TUnboxedValueReader& readerInfo) const {
    TStreamStringHashCalcer hashCalcer(Seed);
    hashCalcer.Start();
    readerInfo.BuildStringForHash(value, hashCalcer);
    return hashCalcer.Finish();
}

std::vector<ui32> TLogsSharding::MakeSharding(const std::shared_ptr<arrow::RecordBatch>& batch) const {
    if (ShardingColumns.size() < 2) {
        return {};
    }

    auto tsArray = batch->GetColumnByName(ShardingColumns[0]);
    if (!tsArray || tsArray->type_id() != arrow::Type::TIMESTAMP) {
        return {};
    }

    const std::vector<ui64> hashes = MakeHashes(batch);
    if (hashes.empty()) {
        return {};
    }

    auto tsColumn = std::static_pointer_cast<arrow::TimestampArray>(tsArray);
    std::vector<ui32> out;
    out.reserve(batch->num_rows());

    TStreamStringHashCalcer hashCalcer(0);
    for (int row = 0; row < batch->num_rows(); ++row) {
        out.emplace_back(ShardNo(tsColumn->Value(row), hashes[row]));
    }

    return out;
}

std::vector<ui64> TLogsSharding::MakeHashes(const std::shared_ptr<arrow::RecordBatch>& batch) const {
    if (ShardingColumns.size() < 2) {
        return {};
    }

    std::vector<std::shared_ptr<arrow::Array>> extraColumns;
    extraColumns.reserve(ShardingColumns.size() - 1);

    for (size_t i = 1; i < ShardingColumns.size(); ++i) {
        auto array = batch->GetColumnByName(ShardingColumns[i]);
        if (!array) {
            return {};
        }
        extraColumns.emplace_back(array);
    }

    std::vector<ui64> out;
    out.reserve(batch->num_rows());

    TStreamStringHashCalcer hashCalcer(0);
    for (int row = 0; row < batch->num_rows(); ++row) {
        hashCalcer.Start();
        for (auto& column : extraColumns) {
            AppendField(column, row, hashCalcer);
        }
        out.emplace_back(hashCalcer.Finish());
    }

    return out;
}

ui64 TLogsSharding::CalcHash(const NYql::NUdf::TUnboxedValue& /*value*/, const TUnboxedValueReader& /*readerInfo*/) const {
    YQL_ENSURE(false);
    return 0;
}

}
