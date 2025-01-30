#include "hash_slider.h"

namespace NKikimr::NSharding {

THashMap<ui64, std::vector<ui32>> TLogsSharding::MakeSharding(const std::shared_ptr<arrow::RecordBatch>& batch) const {
    if (GetShardingColumns().size() < 2) {
        return {};
    }

    auto tsArray = batch->GetColumnByName(GetShardingColumns()[0]);
    if (!tsArray || tsArray->type_id() != arrow::Type::TIMESTAMP) {
        return {};
    }

    const std::vector<ui64> hashes = MakeHashes(batch);
    if (hashes.empty()) {
        return {};
    }

    auto tsColumn = std::static_pointer_cast<arrow::TimestampArray>(tsArray);

    std::vector<std::vector<ui32>> result;
    result.resize(GetOrderedShardIds().size());
    for (auto&& i : result) {
        i.reserve(hashes.size());
    }

    for (int row = 0; row < batch->num_rows(); ++row) {
        result[ShardNo(tsColumn->Value(row), hashes[row])].emplace_back(row);
    }

    THashMap<ui64, std::vector<ui32>> resultHash;
    for (ui32 i = 0; i < result.size(); ++i) {
        if (result[i].size()) {
            resultHash.emplace(GetShardIdByOrderIdx(i), std::move(result[i]));
        }
    }

    return resultHash;
}

NKikimr::TConclusionStatus TLogsSharding::DoDeserializeFromProto(const NKikimrSchemeOp::TColumnTableSharding& proto) {
    auto conclusion = TBase::DoDeserializeFromProto(proto);
    if (conclusion.IsFail()) {
        return conclusion;
    }
    AFL_VERIFY(proto.GetHashSharding().GetFunction() == NKikimrSchemeOp::TColumnTableSharding::THashSharding::HASH_FUNCTION_CLOUD_LOGS);
    if (proto.GetHashSharding().HasActiveShardsCount()) {
        NumActive = proto.GetHashSharding().GetActiveShardsCount();
    }

    return TConclusionStatus::Success();
}

}
