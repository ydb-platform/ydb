#include "hash_intervals.h"

namespace NKikimr::NSharding::NConsistency {

NKikimr::TConclusion<std::vector<NKikimrSchemeOp::TAlterShards>> TConsistencySharding64::DoBuildSplitShardsModifiers(const std::vector<ui64>& newTabletIds) const {
    if (newTabletIds.size() != GetShardIds().size()) {
        return TConclusionStatus::Fail("can multiple 2 only for add shards count");
    }
    if (!!SpecialShardingInfo) {
        return TConclusionStatus::Fail("not unified shards distribution for consistency intervals modification");
    }
    const TSpecificShardingInfo shardingInfo = SpecialShardingInfo ? *SpecialShardingInfo : TSpecificShardingInfo(GetShardIds());
    std::vector<NKikimrSchemeOp::TAlterShards> result;
    {
        ui32 idx = 0;
        for (auto&& i : GetShardIds()) {
            {
                NKikimrSchemeOp::TAlterShards alter;
                auto specSharding = shardingInfo.GetShardingTabletVerified(i);
                specSharding.SetTabletId(newTabletIds[idx]);
                specSharding.CutHalfIntervalFromStart();
                alter.MutableModification()->AddOpenWriteIds(newTabletIds[idx]);
                *alter.MutableModification()->MutableConsistency()->AddShards() = specSharding.SerializeToProto();
                result.emplace_back(alter);
            }
            {
                NKikimrSchemeOp::TAlterShards alter;
                auto& transfer = *alter.MutableTransfer()->AddTransfers();
                transfer.SetDestinationTabletId(newTabletIds[idx]);
                transfer.AddSourceTabletIds(i);
                result.emplace_back(alter);
            }
            {
                NKikimrSchemeOp::TAlterShards alter;
                alter.MutableModification()->AddOpenReadIds(newTabletIds[idx]);
                auto specSharding = shardingInfo.GetShardingTabletVerified(i);
                specSharding.CutHalfIntervalToEnd();
                *alter.MutableModification()->MutableConsistency()->AddShards() = specSharding.SerializeToProto();
                result.emplace_back(alter);
            }
            ++idx;
        }
    }
    return result;
}

NKikimr::TConclusionStatus TConsistencySharding64::DoApplyModification(const NKikimrSchemeOp::TShardingModification& proto) {
    if (!proto.HasConsistency()) {
        return TConclusionStatus::Success();
    }

    for (auto&& i : proto.GetConsistency().GetShards()) {
        TSpecificShardingInfo::TConsistencyShardingTablet info;
        {
            auto conclusion = info.DeserializeFromProto(i);
            if (conclusion.IsFail()) {
                return conclusion;
            }
        }
        if (!UpdateShardInfo(info)) {
            return TConclusionStatus::Fail("no shard with same id on update consistency interval");
        }
    }
    return TConclusionStatus::Success();
}

NKikimr::TConclusionStatus TConsistencySharding64::DoDeserializeFromProto(const NKikimrSchemeOp::TColumnTableSharding& proto) {
    auto conclusion = TBase::DoDeserializeFromProto(proto);
    if (conclusion.IsFail()) {
        return conclusion;
    }
    if (!proto.HasHashSharding()) {
        return TConclusionStatus::Fail("no data for consistency sharding");
    }
    AFL_VERIFY(proto.GetHashSharding().GetFunction() == NKikimrSchemeOp::TColumnTableSharding::THashSharding::HASH_FUNCTION_CONSISTENCY_64);
    {
        TSpecificShardingInfo specSharding;
        auto parseResult = specSharding.DeserializeFromProto(proto, GetClosedWritingShardIds(), GetClosedReadingShardIds());
        if (parseResult.IsFail()) {
            return parseResult;
        }
        if (!specSharding.IsEmpty()) {
            SpecialShardingInfo = std::move(specSharding);
        }
    }
    return TConclusionStatus::Success();
}

NKikimr::TConclusionStatus TConsistencySharding64::DoOnAfterModification() {
    AFL_VERIFY(!!SpecialShardingInfo);
    auto result = SpecialShardingInfo->BuildActivityIndex(GetClosedWritingShardIds(), GetClosedReadingShardIds());
    if (result.IsFail()) {
        return result;
    }

    std::vector<ui64> shardIds;
    if (SpecialShardingInfo->CheckUnifiedDistribution(GetShardIds().size(), shardIds)) {
        SetShardIds(shardIds);
        SpecialShardingInfo.reset();
    }

    return TConclusionStatus::Success();
}

THashMap<ui64, std::vector<ui32>> TConsistencySharding64::MakeSharding(const std::shared_ptr<arrow::RecordBatch>& batch) const {
    std::vector<ui64> hashes = MakeHashes(batch);

    if (!SpecialShardingInfo) {
        std::vector<std::vector<ui32>> result;
        result.resize(GetShardsCount());
        for (auto&& i : result) {
            i.reserve(hashes.size());
        }
        ui32 idx = 0;
        for (auto&& hash : hashes) {
            result[std::min<ui32>(hash / (Max<ui64>() / result.size()), result.size() - 1)].emplace_back(idx);
            ++idx;
        }
        THashMap<ui64, std::vector<ui32>> resultHash;
        for (ui32 i = 0; i < result.size(); ++i) {
            if (result[i].size()) {
                resultHash.emplace(GetShardId(i), std::move(result[i]));
            }
        }
        return resultHash;
    } else {
        return SpecialShardingInfo->MakeShardingWrite(hashes);
    }
}

}
