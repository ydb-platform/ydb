#include "hash_modulo.h"

namespace NKikimr::NSharding::NModulo {

THashMap<ui64, std::vector<ui32>> THashShardingModuloN::MakeSharding(const std::shared_ptr<arrow::RecordBatch>& batch) const {
    const std::vector<ui64> hashes = MakeHashes(batch);
    if (!SpecialShardingInfo) {
        THashMap<ui64, std::vector<ui32>> resultHash;
        std::vector<std::vector<ui32>> result;
        result.resize(GetShardsCount());
        for (auto&& i : result) {
            i.reserve(hashes.size());
        }
        ui32 idx = 0;
        for (auto&& i : hashes) {
            result[i % GetShardsCount()].emplace_back(idx++);
        }
        for (ui32 i = 0; i < result.size(); ++i) {
            if (result[i].size()) {
                resultHash[GetOrderedShardIds()[i]] = std::move(result[i]);
            }
        }
        return resultHash;
    } else {
        return SpecialShardingInfo->MakeShardingWrite(hashes);
    }
}

NKikimr::TConclusion<std::vector<NKikimrSchemeOp::TAlterShards>> THashShardingModuloN::DoBuildSplitShardsModifiers(const std::vector<ui64>& newTabletIds) const {
    if (newTabletIds.size() != GetOrderedShardIds().size()) {
        return TConclusionStatus::Fail("can multiple 2 only for add shards count");
    }
    if (!!SpecialShardingInfo) {
        return TConclusionStatus::Fail("not unified shards distribution for module");
    }
    TSpecificShardingInfo info(GetOrderedShardIds());
    std::vector<NKikimrSchemeOp::TAlterShards> result;
    {
        NKikimrSchemeOp::TAlterShards alter;
        alter.MutableModification()->MutableModulo()->SetPartsCount(2 * GetOrderedShardIds().size());
        for (auto&& i : GetOrderedShardIds()) {
            auto specSharding = info.GetShardingTabletVerified(i);
            AFL_VERIFY(specSharding.MutableAppropriateMods().size() == 1);
            AFL_VERIFY(specSharding.MutableAppropriateMods().emplace(*specSharding.MutableAppropriateMods().begin() + GetOrderedShardIds().size()).second);
            *alter.MutableModification()->MutableModulo()->AddShards() = specSharding.SerializeToProto();
        }

        result.emplace_back(std::move(alter));
    }
    {
        ui32 idx = 0;
        for (auto&& i : GetOrderedShardIds()) {
            {
                NKikimrSchemeOp::TAlterShards alter;
                alter.MutableModification()->AddOpenWriteIds(newTabletIds[idx]);
                auto specSharding = info.GetShardingTabletVerified(i);
                specSharding.SetTabletId(newTabletIds[idx]);
                AFL_VERIFY(specSharding.MutableAppropriateMods().size() == 1);
                *alter.MutableModification()->MutableModulo()->AddShards() = specSharding.SerializeToProto();
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
                auto specSharding = info.GetShardingTabletVerified(i);
                AFL_VERIFY(specSharding.MutableAppropriateMods().size() == 1);
                const ui32 original = *specSharding.MutableAppropriateMods().begin();
                specSharding.MutableAppropriateMods().erase(original);
                specSharding.MutableAppropriateMods().emplace(original + GetOrderedShardIds().size());
                *alter.MutableModification()->MutableModulo()->AddShards() = specSharding.SerializeToProto();
                result.emplace_back(alter);
            }
            ++idx;
        }
    }
    return result;
}

NKikimr::TConclusionStatus THashShardingModuloN::DoApplyModification(const NKikimrSchemeOp::TShardingModification& proto) {
    AFL_VERIFY(!!SpecialShardingInfo);
    for (auto&& i : proto.GetDeleteShardIds()) {
        if (!DeleteShardInfo(i)) {
            return TConclusionStatus::Fail("no shard with same id on delete modulo interval");
        }
    }

    if (!proto.HasModulo()) {
        return TConclusionStatus::Success();
    }

    if (proto.GetModulo().HasPartsCount()) {
        SpecialShardingInfo->SetPartsCount(proto.GetModulo().GetPartsCount());
    }

    for (auto&& i : proto.GetModulo().GetShards()) {
        TSpecificShardingInfo::TModuloShardingTablet info;
        {
            auto conclusion = info.DeserializeFromProto(i);
            if (conclusion.IsFail()) {
                return conclusion;
            }
        }
        if (!UpdateShardInfo(info)) {
            return TConclusionStatus::Fail("no shard with same id on update modulo");
        }
    }
    return TConclusionStatus::Success();
}

NKikimr::TConclusionStatus THashShardingModuloN::DoOnAfterModification() {
    AFL_VERIFY(!!SpecialShardingInfo);

    auto result = SpecialShardingInfo->BuildActivityIndex(GetClosedWritingShardIds(), GetClosedReadingShardIds());
    if (result.IsFail()) {
        return result;
    }

    std::vector<ui64> shardIdsOrdered;
    if (SpecialShardingInfo->CheckUnifiedDistribution(GetOrderedShardIds().size(), shardIdsOrdered)) {
        SetOrderedShardIds(shardIdsOrdered);
        SpecialShardingInfo.reset();
    }

    return TConclusionStatus::Success();
}

NKikimr::TConclusionStatus THashShardingModuloN::DoDeserializeFromProto(const NKikimrSchemeOp::TColumnTableSharding& proto) {
    auto conclusion = TBase::DoDeserializeFromProto(proto);
    if (conclusion.IsFail()) {
        return conclusion;
    }
    if (!proto.HasHashSharding()) {
        return TConclusionStatus::Fail("no data for modulo n sharding");
    }
    AFL_VERIFY(proto.GetHashSharding().GetFunction() == NKikimrSchemeOp::TColumnTableSharding::THashSharding::HASH_FUNCTION_MODULO_N);
    {
        TSpecificShardingInfo specialInfo;
        auto result = specialInfo.DeserializeFromProto(proto, GetClosedWritingShardIds(), GetClosedReadingShardIds());
        if (result.IsFail()) {
            return result;
        }
        if (!specialInfo.IsEmpty()) {
            SpecialShardingInfo = std::move(specialInfo);
        }
    }
    return TConclusionStatus::Success();
}

std::shared_ptr<NKikimr::NSharding::IGranuleShardingLogic> THashShardingModuloN::DoGetTabletShardingInfoOptional(const ui64 tabletId) const {
    if (SpecialShardingInfo) {
        return std::make_shared<TGranuleSharding>(GetShardingColumns(), SpecialShardingInfo->GetShardingTabletVerified(tabletId), SpecialShardingInfo->GetPartsCount());
    } else {
        TSpecificShardingInfo info(GetOrderedShardIds());
        return std::make_shared<TGranuleSharding>(GetShardingColumns(), info.GetShardingTabletVerified(tabletId), info.GetPartsCount());
    }
}

NKikimr::TConclusion<std::vector<NKikimrSchemeOp::TAlterShards>> THashShardingModuloN::DoBuildMergeShardsModifiers(const std::vector<ui64>& newTabletIds) const {
    if (newTabletIds.size() * 2 != GetOrderedShardIds().size()) {
        return TConclusionStatus::Fail("can div 2 only for reduce shards count");
    }
    if (!!SpecialShardingInfo) {
        return TConclusionStatus::Fail("not unified shards distribution for modulo");
    }
    const TSpecificShardingInfo shardingInfo = SpecialShardingInfo ? *SpecialShardingInfo : TSpecificShardingInfo(GetOrderedShardIds());
    std::vector<NKikimrSchemeOp::TAlterShards> result;
    {
        ui32 idx = 0;
        for (auto&& i : newTabletIds) {
            const ui64 from1 = GetOrderedShardIds()[idx];
            const ui64 from2 = GetOrderedShardIds()[idx + newTabletIds.size()];
            auto source1 = shardingInfo.GetShardingTabletVerified(from1);
            auto source2 = shardingInfo.GetShardingTabletVerified(from2);
            AFL_VERIFY(source1.GetAppropriateMods().size() == 1);
            AFL_VERIFY(source2.GetAppropriateMods().size() == 1);
            AFL_VERIFY(*source1.GetAppropriateMods().begin() + newTabletIds.size() == *source2.GetAppropriateMods().begin());
            {
                NKikimrSchemeOp::TAlterShards alter;
                TSpecificShardingInfo::TModuloShardingTablet newInterval(i, { *source1.GetAppropriateMods().begin() ,*source2.GetAppropriateMods().begin() });
                alter.MutableModification()->AddOpenWriteIds(i);
                *alter.MutableModification()->MutableModulo()->AddShards() = newInterval.SerializeToProto();
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

}
