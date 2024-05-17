#include "sharding.h"
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/library/yql/utils/yql_panic.h>
#include <util/string/join.h>
#include "hash_intervals.h"
#include "hash_modulo.h"
#include "hash_slider.h"
#include "random.h"

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

TConclusion<std::unique_ptr<TShardingBase>> TShardingBase::BuildFromProto(const NSchemeShard::TOlapSchema* schema, const NKikimrSchemeOp::TColumnTableSharding& shardingProto) {
    if (!shardingProto.GetColumnShards().size()) {
        return TConclusionStatus::Fail("config is incorrect for construct sharding behaviour");
    }
    std::unique_ptr<TShardingBase> result;
    if (shardingProto.HasRandomSharding()) {
        result = std::make_unique<TRandomSharding>();
    } else if (shardingProto.HasHashSharding()) {
        auto& hashSharding = shardingProto.GetHashSharding();
        std::vector<TString> columnNames;
        if (hashSharding.GetColumns().empty()) {
            return TConclusionStatus::Fail("no columns for hash calculation");
        } else {
            for (auto&& i : hashSharding.GetColumns()) {
                columnNames.emplace_back(i);
                if (schema) {
                    if (!schema->GetColumns().GetByName(i)) {
                        return TConclusionStatus::Fail("incorrect sharding column name: " + i);
                    }
                    if (!schema->GetColumns().GetByName(i)->IsKeyColumn()) {
                        return TConclusionStatus::Fail("sharding column name have to been primary key column: " + i);
                    }
                }
            }
        }
        if (hashSharding.GetFunction() == NKikimrSchemeOp::TColumnTableSharding::THashSharding::HASH_FUNCTION_CONSISTENCY_64) {
            result = std::make_unique<NConsistency::TConsistencySharding64>();
        } else if (hashSharding.GetFunction() == NKikimrSchemeOp::TColumnTableSharding::THashSharding::HASH_FUNCTION_MODULO_N) {
            result = std::make_unique<NModulo::THashShardingModuloN>();
        } else if (hashSharding.GetFunction() == NKikimrSchemeOp::TColumnTableSharding::THashSharding::HASH_FUNCTION_CLOUD_LOGS) {
            result = std::make_unique<TLogsSharding>();
        }
    }
    if (!result) {
        return TConclusionStatus::Fail("not determined type of sharding");
    }
    {
        auto conclusion = result->DeserializeFromProto(shardingProto);
        if (conclusion.IsFail()) {
            return conclusion;
        }
        return result;
    }
}

TString TShardingBase::DebugString() const {
    return "SHARDING";
}

NKikimr::TConclusionStatus TShardingBase::DeserializeFromProto(const NKikimrSchemeOp::TColumnTableSharding& proto) {
    std::set<ui64> shardIdsSetOriginal;
    {
        AFL_VERIFY(ShardIds.empty());
        for (auto&& i : proto.GetColumnShards()) {
            AFL_VERIFY(shardIdsSetOriginal.emplace(i).second);
            ShardIds.emplace_back(i);
        }
    }
    if (proto.GetClosedWritingShardIds().size()) {
        std::set<ui64> shardIdsSet = shardIdsSetOriginal;
        for (auto&& i : proto.GetClosedWritingShardIds()) {
            if (!shardIdsSet.erase(i)) {
                return TConclusionStatus::Fail("incorrect shardIds in ClosedWritingShardIds");
            }
            ClosedWritingShardIds.emplace(i);
        }
    }
    if (proto.GetClosedReadingShardIds().size()) {
        std::set<ui64> shardIdsSet = shardIdsSetOriginal;
        for (auto&& i : proto.GetClosedReadingShardIds()) {
            if (!shardIdsSet.erase(i)) {
                return TConclusionStatus::Fail("incorrect shardIds in ClosedReadingShardIds");
            }
            ClosedReadingShardIds.emplace(i);
        }
    }
    {
        auto conclusion = DoDeserializeFromProto(proto);
        if (conclusion.IsFail()) {
            return conclusion;
        }
    }
    return TConclusionStatus::Success();
}

NKikimr::TConclusionStatus TShardingBase::ApplyModification(const NKikimrSchemeOp::TShardingModification& proto) {
    {
        auto conclusion = OnBeforeModification();
        if (conclusion.IsFail()) {
            return conclusion;
        }
    }
    for (auto&& i : proto.GetNewShardIds()) {
        if (IsShardExists(i)) {
            return TConclusionStatus::Fail("shard id from NewShardIds duplication with current full shardIds list");
        }
        ShardIds.emplace_back(i);
    }
    for (auto&& i : proto.GetCloseWriteIds()) {
        if (!IsShardExists(i)) {
            return TConclusionStatus::Fail("incorrect shard id from CloseWriteIds - not exists in full scope");
        }
        ClosedWritingShardIds.emplace(i);
    }
    for (auto&& i : proto.GetCloseReadIds()) {
        if (!IsShardExists(i)) {
            return TConclusionStatus::Fail("incorrect shard id from CloseReadIds - not exists in full scope");
        }
        ClosedReadingShardIds.emplace(i);
    }
    for (auto&& i : proto.GetOpenWriteIds()) {
        if (!IsShardExists(i)) {
            return TConclusionStatus::Fail("incorrect shard id from OpenWriteIds - not exists in full scope");
        }
        ClosedWritingShardIds.erase(i);
    }
    for (auto&& i : proto.GetOpenReadIds()) {
        if (!IsShardExists(i)) {
            return TConclusionStatus::Fail("incorrect shard id from OpenReadIds - not exists in full scope");
        }
        ClosedReadingShardIds.erase(i);
    }
    {
        auto conclusion = DoApplyModification(proto);
        if (conclusion.IsFail()) {
            return conclusion;
        }
    }
    {
        auto conclusion = OnAfterModification();
        if (conclusion.IsFail()) {
            return conclusion;
        }
    }
    return TConclusionStatus::Success();
}

NKikimr::TConclusion<std::vector<NKikimrSchemeOp::TAlterShards>> TShardingBase::BuildAddShardsModifiers(const std::vector<ui64>& newTabletIds) const {
    NKikimrSchemeOp::TShardingModification startModification;
    for (auto&& i : newTabletIds) {
        startModification.AddNewShardIds(i);
        startModification.AddCloseReadIds(i);
        startModification.AddCloseWriteIds(i);
    }

    TConclusion<std::vector<NKikimrSchemeOp::TAlterShards>> infosConclusion = BuildSplitShardsModifiers(newTabletIds);
    if (infosConclusion.IsFail()) {
        return infosConclusion;
    }
    std::vector<NKikimrSchemeOp::TAlterShards> result;
    {
        NKikimrSchemeOp::TAlterShards startAlter;
        *startAlter.MutableModification() = std::move(startModification);
        result.emplace_back(std::move(startAlter));
    }
    for (auto&& i : *infosConclusion) {
        result.emplace_back(std::move(i));
    }
    return result;
}

NKikimrSchemeOp::TColumnTableSharding TShardingBase::SerializeToProto() const {
    NKikimrSchemeOp::TColumnTableSharding result;
    result.SetVersion(1);
    AFL_VERIFY(ShardIds.size());
    for (auto&& i : ShardIds) {
        result.AddColumnShards(i);
    }
    for (auto&& i : ClosedReadingShardIds) {
        AFL_VERIFY(IsShardExists(i));
        result.AddClosedReadingShardIds(i);
    }
    for (auto&& i : ClosedWritingShardIds) {
        AFL_VERIFY(IsShardExists(i));
        result.AddClosedWritingShardIds(i);
    }
    DoSerializeToProto(result);
    return result;
}

NKikimr::TConclusion<THashMap<ui64, std::vector<NKikimr::NArrow::TSerializedBatch>>> TShardingBase::SplitByShards(const std::shared_ptr<arrow::RecordBatch>& batch, const ui64 chunkBytesLimit) {
    THashMap<ui64, std::vector<ui32>> sharding = MakeSharding(batch);
    THashMap<ui64, std::shared_ptr<arrow::RecordBatch>> chunks;
    if (sharding.size() == 1) {
        AFL_VERIFY(chunks.emplace(sharding.begin()->first, batch).second);
    } else {
        chunks = NArrow::ShardingSplit(batch, sharding);
    }
    AFL_VERIFY(chunks.size() == sharding.size());
    NArrow::TBatchSplitttingContext context(chunkBytesLimit);
    THashMap<ui64, std::vector<NArrow::TSerializedBatch>> result;
    for (auto&& [tabletId, chunk]: chunks) {
        if (!chunk) {
            continue;
        }
        auto blobsSplittedConclusion = NArrow::SplitByBlobSize(chunk, context);
        if (blobsSplittedConclusion.IsFail()) {
            return TConclusionStatus::Fail("cannot split batch in according to limits: " + blobsSplittedConclusion.GetErrorMessage());
        }
        result.emplace(tabletId, blobsSplittedConclusion.DetachResult());
    }
    return result;
}

}
