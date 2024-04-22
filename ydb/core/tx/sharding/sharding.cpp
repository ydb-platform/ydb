#include "sharding.h"
#include <ydb/library/yql/utils/yql_panic.h>
#include <util/string/join.h>

namespace NKikimr::NSharding {

TConclusion<std::unique_ptr<TShardingBase>> TShardingBase::BuildShardingOperator(const NKikimrSchemeOp::TColumnTableSharding& sharding) {
    if (sharding.HasRandomSharding()) {
        return std::make_unique<TRandomSharding>(sharding.GetColumnShards().size());
    }
    if (sharding.HasHashSharding()) {
        auto& hashSharding = sharding.GetHashSharding();
        std::vector<TString> columnNames(hashSharding.GetColumns().begin(), hashSharding.GetColumns().end());
        if (hashSharding.GetFunction() == NKikimrSchemeOp::TColumnTableSharding::THashSharding::HASH_FUNCTION_CONSISTENCY_64) {
            return std::make_unique<TConsistencySharding64>(sharding.GetColumnShards().size(), columnNames, 0);
        } else if (hashSharding.GetFunction() == NKikimrSchemeOp::TColumnTableSharding::THashSharding::HASH_FUNCTION_MODULO_N) {
            return std::make_unique<THashSharding>(sharding.GetColumnShards().size(), columnNames, 0);
        } else if (hashSharding.GetFunction() == NKikimrSchemeOp::TColumnTableSharding::THashSharding::HASH_FUNCTION_CLOUD_LOGS) {
            ui32 activeShards = TLogsSharding::DEFAULT_ACITVE_SHARDS;
            if (hashSharding.HasActiveShardsCount()) {
                activeShards = hashSharding.GetActiveShardsCount();
            }
            return std::make_unique<TLogsSharding>(sharding.GetColumnShards().size(), columnNames, activeShards);
        }
    }
    return TConclusionStatus::Fail("not determined sharding type");
}

TString TShardingBase::DebugString() const {
    return "SHARDING";
}

std::vector<ui32> THashSharding::MakeSharding(const std::shared_ptr<arrow::RecordBatch>& batch) const {
    std::vector<ui64> hashes = MakeHashes(batch);
    std::vector<ui32> result;
    result.reserve(hashes.size());
    for (auto&& i : hashes) {
        result.emplace_back(i % GetShardsCount());
    }
    return result;
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

    for (int row = 0; row < batch->num_rows(); ++row) {
        out.emplace_back(ShardNo(tsColumn->Value(row), hashes[row]));
    }

    return out;
}

}
