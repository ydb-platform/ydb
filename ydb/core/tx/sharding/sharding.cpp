#include "sharding.h"
#include <ydb/library/yql/utils/yql_panic.h>
#include <util/string/join.h>

namespace NKikimr::NSharding {

TConclusionStatus TShardingBase::ValidateBehaviour(const NSchemeShard::TOlapSchema& schema, const NKikimrSchemeOp::TColumnTableSharding& shardingInfo) {
    auto copy = shardingInfo;
    if (copy.GetColumnShards().size() == 0) {
        copy.AddColumnShards(1);
    }
    auto fakeResult = BuildFromProto(schema, copy);
    if (fakeResult.IsFail()) {
        return fakeResult;
    }
    return TConclusionStatus::Success();
}

TConclusion<std::unique_ptr<TShardingBase>> TShardingBase::BuildFromProto(const NSchemeShard::TOlapSchema& schema, const NKikimrSchemeOp::TColumnTableSharding& shardingProto) {
    if (!shardingProto.GetColumnShards().size()) {
        return TConclusionStatus::Fail("config is incorrect for construct sharding behaviour");
    }
    std::vector<ui64> shardIds(shardingProto.GetColumnShards().begin(), shardingProto.GetColumnShards().end());
    if (shardingProto.HasRandomSharding()) {
        return std::make_unique<TRandomSharding>(shardIds);
    }
    if (shardingProto.HasHashSharding()) {
        auto& hashSharding = shardingProto.GetHashSharding();
        std::vector<TString> columnNames;
        if (hashSharding.GetColumns().empty()) {
            return TConclusionStatus::Fail("no columns for hash calculation");
        } else {
            for (auto&& i : hashSharding.GetColumns()) {
                columnNames.emplace_back(i);
                if (!schema.GetColumns().GetByName(i)) {
                    return TConclusionStatus::Fail("incorrect sharding column name: " + i);
                }
                if (!schema.GetColumns().GetByName(i)->IsKeyColumn()) {
                    return TConclusionStatus::Fail("sharding column name have to been primary key column: " + i);
                }
            }
        }
        if (hashSharding.GetFunction() == NKikimrSchemeOp::TColumnTableSharding::THashSharding::HASH_FUNCTION_CONSISTENCY_64) {
            return std::make_unique<TConsistencySharding64>(shardIds, columnNames, 0);
        } else if (hashSharding.GetFunction() == NKikimrSchemeOp::TColumnTableSharding::THashSharding::HASH_FUNCTION_MODULO_N) {
            return std::make_unique<THashShardingModuloN>(shardIds, columnNames, 0);
        } else if (hashSharding.GetFunction() == NKikimrSchemeOp::TColumnTableSharding::THashSharding::HASH_FUNCTION_CLOUD_LOGS) {
            ui32 activeShards = TLogsSharding::DEFAULT_ACITVE_SHARDS;
            if (hashSharding.HasActiveShardsCount()) {
                activeShards = hashSharding.GetActiveShardsCount();
            }
            return std::make_unique<TLogsSharding>(shardIds, columnNames, activeShards);
        }
    }
    return TConclusionStatus::Fail("not determined sharding type");
}

TString TShardingBase::DebugString() const {
    return "SHARDING";
}

std::vector<ui32> THashShardingModuloN::MakeSharding(const std::shared_ptr<arrow::RecordBatch>& batch) const {
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
