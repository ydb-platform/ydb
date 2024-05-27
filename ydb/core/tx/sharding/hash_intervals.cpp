#include "hash_intervals.h"

namespace NKikimr::NSharding::NConsistency {

NKikimr::TConclusion<std::vector<NKikimrSchemeOp::TAlterShards>> TConsistencySharding64::DoBuildSplitShardsModifiers(const std::vector<ui64>& newTabletIds) const {
    if (newTabletIds.size() != GetOrderedShardIds().size()) {
        return TConclusionStatus::Fail("can multiple 2 only for add shards count");
    }
    if (!!SpecialShardingInfo) {
        return TConclusionStatus::Fail("not unified shards distribution for consistency intervals modification");
    }
    const TSpecificShardingInfo shardingInfo = SpecialShardingInfo ? *SpecialShardingInfo : TSpecificShardingInfo(GetOrderedShardIds());
    std::vector<NKikimrSchemeOp::TAlterShards> result;
    {
        ui32 idx = 0;
        for (auto&& i : GetOrderedShardIds()) {
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

NKikimr::TConclusion<std::vector<NKikimrSchemeOp::TAlterShards>> TConsistencySharding64::DoBuildMergeShardsModifiers(const std::vector<ui64>& newTabletIds) const {
    if (newTabletIds.size() * 2 != GetOrderedShardIds().size()) {
        return TConclusionStatus::Fail("can div 2 only for reduce shards count");
    }
    if (!!SpecialShardingInfo) {
        return TConclusionStatus::Fail("not unified shards distribution for consistency intervals modification");
    }
    const TSpecificShardingInfo shardingInfo = SpecialShardingInfo ? *SpecialShardingInfo : TSpecificShardingInfo(GetOrderedShardIds());
    std::vector<NKikimrSchemeOp::TAlterShards> result;
    {
        ui32 idx = 0;
        for (auto&& i : newTabletIds) {
            const ui64 from1 = GetOrderedShardIds()[2 * idx];
            const ui64 from2 = GetOrderedShardIds()[2 * idx + 1];
            auto source1 = shardingInfo.GetShardingTabletVerified(from1);
            auto source2 = shardingInfo.GetShardingTabletVerified(from2);
            AFL_VERIFY(source1.GetHashIntervalRightOpened() == source2.GetHashIntervalLeftClosed());
            {
                NKikimrSchemeOp::TAlterShards alter;
                TSpecificShardingInfo::TConsistencyShardingTablet newInterval(i, source1.GetHashIntervalLeftClosed(), source2.GetHashIntervalRightOpened());
                alter.MutableModification()->AddOpenWriteIds(i);
                *alter.MutableModification()->MutableConsistency()->AddShards() = newInterval.SerializeToProto();
                result.emplace_back(alter);
            }
            {
                NKikimrSchemeOp::TAlterShards alter;
                auto& transfer = *alter.MutableTransfer()->AddTransfers();
                transfer.SetDestinationTabletId(newTabletIds[idx]);
                transfer.AddSourceTabletIds(from1);
                transfer.AddSourceTabletIds(from2);
                transfer.SetMoving(true);
                result.emplace_back(alter);
            }
            {
                NKikimrSchemeOp::TAlterShards alter;
                alter.MutableModification()->AddOpenReadIds(i);
                alter.MutableModification()->AddCloseWriteIds(from1);
                alter.MutableModification()->AddCloseWriteIds(from2);
                alter.MutableModification()->AddCloseReadIds(from1);
                alter.MutableModification()->AddCloseReadIds(from2);
                result.emplace_back(alter);
            }
            ++idx;
        }
    }
    return result;
}

NKikimr::TConclusionStatus TConsistencySharding64::DoApplyModification(const NKikimrSchemeOp::TShardingModification& proto) {
    AFL_VERIFY(!!SpecialShardingInfo);
    for (auto&& i : proto.GetDeleteShardIds()) {
        if (!DeleteShardInfo(i)) {
            return TConclusionStatus::Fail("no shard with same id on delete consistency interval");
        }
    }

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
    if (SpecialShardingInfo->CheckUnifiedDistribution(GetOrderedShardIds().size(), shardIds)) {
        SetOrderedShardIds(shardIds);
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
                resultHash.emplace(GetShardIdByOrderIdx(i), std::move(result[i]));
            }
        }
        return resultHash;
    } else {
        return SpecialShardingInfo->MakeShardingWrite(hashes);
    }
}

std::shared_ptr<NKikimr::NSharding::IGranuleShardingLogic> TConsistencySharding64::DoGetTabletShardingInfoOptional(const ui64 tabletId) const {
    if (SpecialShardingInfo) {
        return std::make_shared<TGranuleSharding>(GetShardingColumns(), SpecialShardingInfo->GetShardingTabletVerified(tabletId));
    } else {
        TSpecificShardingInfo info(GetOrderedShardIds());
        return std::make_shared<TGranuleSharding>(GetShardingColumns(), info.GetShardingTabletVerified(tabletId));
    }
}

}
