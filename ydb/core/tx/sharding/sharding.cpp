#include "sharding.h"
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/library/yql/utils/yql_panic.h>
#include <ydb/core/tx/columnshard/common/protos/snapshot.pb.h>
#include <util/string/join.h>
#include "hash_intervals.h"
#include "hash_modulo.h"
#include "hash_slider.h"
#include "random.h"

namespace NKikimr::NSharding {

TConclusionStatus IShardingBase::ValidateBehaviour(const NSchemeShard::TOlapSchema& schema, const NKikimrSchemeOp::TColumnTableSharding& shardingInfo) {
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

TConclusion<std::unique_ptr<IShardingBase>> IShardingBase::BuildFromProto(const NSchemeShard::TOlapSchema* schema, const NKikimrSchemeOp::TColumnTableSharding& shardingProto) {
    if (!shardingProto.GetColumnShards().size()) {
        return TConclusionStatus::Fail("config is incorrect for construct sharding behaviour");
    }
    std::unique_ptr<IShardingBase> result;
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

TString IShardingBase::DebugString() const {
    return "SHARDING";
}

NKikimr::TConclusionStatus IShardingBase::DeserializeFromProto(const NKikimrSchemeOp::TColumnTableSharding& proto) {
    std::set<ui64> shardIdsSetOriginal;
    if (proto.GetShardsInfo().empty()) {
        std::vector<ui64> orderedTabletIds(proto.GetColumnShards().begin(), proto.GetColumnShards().end());
        InitializeFromOrdered(orderedTabletIds);
    } else {
        std::map<ui32, ui64> sortedTabletIds;
        for (auto&& i : proto.GetShardsInfo()) {
            TConclusion<TShardInfo> shardInfo = TShardInfo::BuildFromProto(i);
            if (shardInfo.IsFail()) {
                return shardInfo;
            }
            AFL_VERIFY(sortedTabletIds.emplace(shardInfo->GetSequenceIdx(), shardInfo->GetTabletId()).second);
            AFL_VERIFY(Shards.emplace(shardInfo->GetTabletId(), shardInfo.GetResult()).second);
        }
        for (auto&& i : sortedTabletIds) {
            OrderedShardIds.emplace_back(i.second);
        }
    }
    if (shardIdsSetOriginal.size()) {
        return TConclusionStatus::Fail("not for all shard in sequence we have info");
    }
    {
        auto conclusion = DoDeserializeFromProto(proto);
        if (conclusion.IsFail()) {
            return conclusion;
        }
    }
    return TConclusionStatus::Success();
}

NKikimr::TConclusionStatus IShardingBase::ApplyModification(const NKikimrSchemeOp::TShardingModification& proto) {
    {
        auto conclusion = OnBeforeModification();
        if (conclusion.IsFail()) {
            return conclusion;
        }
    }
    for (auto&& i : proto.GetNewShardIds()) {
        if (!Shards.emplace(i, TShardInfo(i, OrderedShardIds.size())).second) {
            return TConclusionStatus::Fail("shard id from NewShardIds duplication with current full shardIds list");
        }
        OrderedShardIds.emplace_back(i);
    }
    for (auto&& i : proto.GetCloseWriteIds()) {
        auto* shardInfo = GetShardInfo(i);
        if (!shardInfo) {
            return TConclusionStatus::Fail("incorrect shard id from CloseWriteIds - not exists in full scope");
        }
        shardInfo->SetIsOpenForWrite(false);
    }
    for (auto&& i : proto.GetCloseReadIds()) {
        auto* shardInfo = GetShardInfo(i);
        if (!shardInfo) {
            return TConclusionStatus::Fail("incorrect shard id from CloseReadIds - not exists in full scope");
        }
        shardInfo->SetIsOpenForRead(false);
    }
    for (auto&& i : proto.GetOpenWriteIds()) {
        auto* shardInfo = GetShardInfo(i);
        if (!shardInfo) {
            return TConclusionStatus::Fail("incorrect shard id from OpenWriteIds - not exists in full scope");
        }
        shardInfo->SetIsOpenForWrite(true);
    }
    for (auto&& i : proto.GetOpenReadIds()) {
        auto* shardInfo = GetShardInfo(i);
        if (!shardInfo) {
            return TConclusionStatus::Fail("incorrect shard id from OpenReadIds - not exists in full scope");
        }
        shardInfo->SetIsOpenForRead(true);
    }
    {
        std::set<ui64> deleteIds;
        for (auto&& i : proto.GetDeleteShardIds()) {
            if (!Shards.erase(i)) {
                return TConclusionStatus::Fail("shard id from DeleteShardIds absent with current full shardIds list");
            }
            deleteIds.emplace(i);
        }
        const auto pred = [&deleteIds](const ui64 tabletId) {
            return deleteIds.contains(tabletId);
        };
        OrderedShardIds.erase(std::remove_if(OrderedShardIds.begin(), OrderedShardIds.end(), pred), OrderedShardIds.end());
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

NKikimr::TConclusion<std::vector<NKikimrSchemeOp::TAlterShards>> IShardingBase::BuildAddShardsModifiers(const std::vector<ui64>& newTabletIds) const {
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

NKikimr::TConclusion<std::vector<NKikimrSchemeOp::TAlterShards>> IShardingBase::BuildReduceShardsModifiers(const std::vector<ui64>& newTabletIds) const {
    NKikimrSchemeOp::TShardingModification startModification;
    for (auto&& i : newTabletIds) {
        startModification.AddNewShardIds(i);
        startModification.AddCloseReadIds(i);
        startModification.AddCloseWriteIds(i);
    }

    TConclusion<std::vector<NKikimrSchemeOp::TAlterShards>> infosConclusion = BuildMergeShardsModifiers(newTabletIds);
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
    {
        NKikimrSchemeOp::TShardingModification finishModification;
        for (auto&& i : GetOrderedShardIds()) {
            finishModification.AddDeleteShardIds(i);
        }
        NKikimrSchemeOp::TAlterShards finishAlter;
        *finishAlter.MutableModification() = std::move(finishModification);
        result.emplace_back(std::move(finishAlter));
    }
    return result;
}

NKikimrSchemeOp::TColumnTableSharding IShardingBase::SerializeToProto() const {
    NKikimrSchemeOp::TColumnTableSharding result;
    AFL_VERIFY(Shards.size() == OrderedShardIds.size());
    AFL_VERIFY(OrderedShardIds.size());
    for (auto&& i : OrderedShardIds) {
        *result.AddShardsInfo() = GetShardInfoVerified(i).SerializeToProto();
        result.AddColumnShards(i);
    }
    DoSerializeToProto(result);
    return result;
}

THashMap<ui64, std::shared_ptr<arrow::RecordBatch>> IShardingBase::SplitByShardsToArrowBatches(const std::shared_ptr<arrow::RecordBatch>& batch) {
    THashMap<ui64, std::vector<ui32>> sharding = MakeSharding(batch);
    THashMap<ui64, std::shared_ptr<arrow::RecordBatch>> chunks;
    if (sharding.size() == 1) {
        AFL_VERIFY(chunks.emplace(sharding.begin()->first, batch).second);
    } else {
        chunks = NArrow::ShardingSplit(batch, sharding);
    }
    AFL_VERIFY(chunks.size() == sharding.size());
    return chunks;
}

TConclusion<THashMap<ui64, std::vector<NArrow::TSerializedBatch>>> IShardingBase::SplitByShards(const std::shared_ptr<arrow::RecordBatch>& batch, const ui64 chunkBytesLimit) {
    auto splitted = SplitByShardsToArrowBatches(batch);
    NArrow::TBatchSplitttingContext context(chunkBytesLimit);
    THashMap<ui64, std::vector<NArrow::TSerializedBatch>> result;
    for (auto&& [tabletId, chunk] : splitted) {
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

void IShardingBase::InitializeFromOrdered(const std::vector<ui64>& orderedIds) {
    AFL_VERIFY(orderedIds.size());
    std::set<ui64> shardIdsSetOriginal;
    OrderedShardIds.clear();
    Shards.clear();
    for (auto&& i : orderedIds) {
        AFL_VERIFY(shardIdsSetOriginal.emplace(i).second);
        Shards.emplace(i, TShardInfo(i, OrderedShardIds.size()));
        OrderedShardIds.emplace_back(i);
    }
}

NKikimrSchemeOp::TGranuleShardInfo TShardInfo::SerializeToProto() const {
    NKikimrSchemeOp::TGranuleShardInfo result;
    result.SetTabletId(TabletId);
    result.SetSequenceIdx(SequenceIdx);
    result.SetIsOpenForWrite(IsOpenForWrite);
    result.SetIsOpenForRead(IsOpenForRead);
    if (OpenForWriteSnapshot) {
        *result.MutableOpenForWriteSnapshot() = OpenForWriteSnapshot->SerializeToProto();
    }
    result.SetShardingVersion(ShardingVersion);
    return result;
}

NKikimr::TConclusionStatus TShardInfo::DeserializeFromProto(const NKikimrSchemeOp::TGranuleShardInfo& proto) {
    TabletId = proto.GetTabletId();
    SequenceIdx = proto.GetSequenceIdx();
    IsOpenForRead = proto.GetIsOpenForRead();
    IsOpenForWrite = proto.GetIsOpenForWrite();
    if (proto.HasOpenForWriteSnapshot()) {
        NKikimr::NOlap::TSnapshot ss = NKikimr::NOlap::TSnapshot::Zero();
        auto conclusion = ss.DeserializeFromProto(proto.GetOpenForWriteSnapshot());
        if (conclusion.IsFail()) {
            return conclusion;
        }
        OpenForWriteSnapshot = ss;
    }
    AFL_VERIFY(proto.HasShardingVersion());
    ShardingVersion = proto.GetShardingVersion();
    if (!TabletId) {
        return TConclusionStatus::Fail("incorrect TabletId for TShardInfo proto");
    }
    return TConclusionStatus::Success();
}

}
