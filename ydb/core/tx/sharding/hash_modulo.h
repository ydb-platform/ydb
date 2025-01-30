#pragma once
#include "hash_sharding.h"
#include <ydb/core/protos/flat_scheme_op.pb.h>

namespace NKikimr::NSharding::NModulo {

class TSpecificShardingInfo {
public:
    class TModuloShardingTablet {
    private:
        YDB_ACCESSOR(ui64, TabletId, 0);
        YDB_ACCESSOR_DEF(std::set<ui32>, AppropriateMods);
    public:
        bool operator<(const TModuloShardingTablet& item) const {
            AFL_VERIFY(AppropriateMods.size());
            AFL_VERIFY(item.AppropriateMods.size());
            return *AppropriateMods.begin() < *item.AppropriateMods.begin();
        }

        TModuloShardingTablet() = default;
        TModuloShardingTablet(const ui64 tabletId, const std::set<ui32>& mods)
            : TabletId(tabletId)
            , AppropriateMods(mods) {

        }

        NKikimrSchemeOp::TModuloShardingTablet SerializeToProto() const {
            NKikimrSchemeOp::TModuloShardingTablet result;
            result.SetTabletId(TabletId);
            for (auto&& i : AppropriateMods) {
                result.AddAppropriateMods(i);
            }
            return result;
        }

        TConclusionStatus DeserializeFromProto(const NKikimrSchemeOp::TModuloShardingTablet& proto) {
            TabletId = proto.GetTabletId();
            for (auto&& i : proto.GetAppropriateMods()) {
                AppropriateMods.emplace(i);
            }
            return TConclusionStatus::Success();
        }
    };
private:
    bool IndexConstructed = false;
    YDB_READONLY(ui32, PartsCount, 0);
    std::vector<TModuloShardingTablet> SpecialSharding;
    std::vector<TModuloShardingTablet*> ActiveWriteSpecialSharding;
    std::vector<TModuloShardingTablet*> ActiveReadSpecialSharding;

public:
    TSpecificShardingInfo() = default;
    TSpecificShardingInfo(const std::vector<ui64>& shardIds)
        : PartsCount(shardIds.size())
    {
        for (ui32 i = 0; i < shardIds.size(); ++i) {
            TModuloShardingTablet info(shardIds[i], { i });
            SpecialSharding.emplace_back(info);
        }
        BuildActivityIndex(Default<std::set<ui64>>(), Default<std::set<ui64>>()).Validate();
    }

    void SetPartsCount(const ui32 partsCount) {
        PartsCount = partsCount;
    }

    THashMap<ui64, std::vector<ui32>> MakeShardingWrite(const std::vector<ui64>& hashes) const {
        AFL_VERIFY(PartsCount);
        AFL_VERIFY(IndexConstructed);
        std::vector<std::vector<ui32>> result;
        result.resize(ActiveWriteSpecialSharding.size());
        THashMap<ui64, std::vector<ui32>> resultHash;
        std::vector<std::vector<ui32>> shardsByMod;
        shardsByMod.resize(PartsCount);
        ui32 idx = 0;
        for (auto&& i : ActiveWriteSpecialSharding) {
            for (auto&& m : i->GetAppropriateMods()) {
                shardsByMod[m].emplace_back(idx);
            }
            ++idx;
        }
        ui32 recordIdx = 0;
        for (auto&& i : hashes) {
            for (auto&& s : shardsByMod[i % PartsCount]) {
                result[s].emplace_back(recordIdx);
            }
            ++recordIdx;
        }
        for (ui32 i = 0; i < result.size(); ++i) {
            if (result[i].size()) {
                resultHash[ActiveWriteSpecialSharding[i]->GetTabletId()] = std::move(result[i]);
            }
        }
        return resultHash;
    }

    bool CheckUnifiedDistribution(const ui32 summaryShardsCount, std::vector<ui64>& orderedShardIds) {
        AFL_VERIFY(IndexConstructed);
        NKikimrSchemeOp::TColumnTableSharding proto;
        SerializeToProto(proto);
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("proto", proto.DebugString());
        if (PartsCount % summaryShardsCount) {
            return false;
        }
        const ui32 countPerShard = (PartsCount / summaryShardsCount);
        std::set<ui64> activeReadTabletIds;
        for (auto&& i : ActiveReadSpecialSharding) {
            activeReadTabletIds.emplace(i->GetTabletId());
        }
        std::set<ui64> activeWriteTabletIds;
        for (auto&& i : ActiveWriteSpecialSharding) {
            activeWriteTabletIds.emplace(i->GetTabletId());
        }
        std::set<ui64> tabletIds;
        for (auto&& i : SpecialSharding) {
            tabletIds.emplace(i.GetTabletId());
        }
        if (activeReadTabletIds != tabletIds || activeWriteTabletIds != tabletIds) {
            return false;
        }

        std::vector<ui64> orderedTabletIdsLocal;
        ui32 idx = 0;
        for (auto&& i : SpecialSharding) {
            if (i.GetAppropriateMods().size() != countPerShard) {
                return false;
            }
            for (ui32 s = 0; s < countPerShard; ++s) {
                if (!i.GetAppropriateMods().contains(idx + summaryShardsCount * s)) {
                    return false;
                }
            }
            orderedTabletIdsLocal.emplace_back(i.GetTabletId());
            ++idx;
        }
        AFL_VERIFY(orderedTabletIdsLocal.size() == summaryShardsCount)("ordered", orderedTabletIdsLocal.size())("summary", summaryShardsCount);
        std::swap(orderedTabletIdsLocal, orderedShardIds);
        return true;
    }

    bool IsEmpty() const {
        return SpecialSharding.empty();
    }

    TConclusionStatus BuildActivityIndex(const std::set<ui64>& closedWriteIds, const std::set<ui64>& closedReadIds) {
        if (SpecialSharding.empty()) {
            AFL_VERIFY(!PartsCount);
            return TConclusionStatus::Success();
        }
        ActiveReadSpecialSharding.clear();
        ActiveWriteSpecialSharding.clear();
        std::sort(SpecialSharding.begin(), SpecialSharding.end());
        std::set<ui32> modsRead;
        std::set<ui32> modsWrite;
        for (auto&& i : SpecialSharding) {
            for (auto&& n : i.GetAppropriateMods()) {
                AFL_VERIFY(n < PartsCount)("n", n)("parts_count", PartsCount);
                if (!closedWriteIds.contains(i.GetTabletId())) {
                    modsWrite.emplace(n);
                }
                if (!closedReadIds.contains(i.GetTabletId())) {
                    if (!modsRead.emplace(n).second) {
                        return TConclusionStatus::Fail("read interval twice usage impossible");
                    }
                }
            }
            if (!closedReadIds.contains(i.GetTabletId())) {
                ActiveReadSpecialSharding.emplace_back(&i);
            }
            if (!closedWriteIds.contains(i.GetTabletId())) {
                ActiveWriteSpecialSharding.emplace_back(&i);
            }
        }
        if (modsRead.size() && modsRead.size() != PartsCount) {
            return TConclusionStatus::Fail("incorrect sharding configuration read from proto (read): " + JoinSeq(", ", modsRead) + "; " + ::ToString(PartsCount));
        }
        if (modsWrite.size() && modsWrite.size() != PartsCount) {
            return TConclusionStatus::Fail("incorrect sharding configuration read from proto (write): " + JoinSeq(", ", modsWrite) + "; " + ::ToString(PartsCount));
        }
        IndexConstructed = true;
        return TConclusionStatus::Success();
    }

    TConclusionStatus DeserializeFromProto(const NKikimrSchemeOp::TColumnTableSharding& proto, const std::set<ui64>& closedWriteIds, const std::set<ui64>& closedReadIds) {
        IndexConstructed = false;
        
        PartsCount = proto.GetHashSharding().GetModuloPartsCount();
        for (auto&& i : proto.GetHashSharding().GetTabletsForModulo()) {
            TModuloShardingTablet info;
            auto conclusion = info.DeserializeFromProto(i);
            if (conclusion.IsFail()) {
                return conclusion;
            }
            SpecialSharding.emplace_back(std::move(info));
        }
        AFL_VERIFY(PartsCount || SpecialSharding.empty());
        return BuildActivityIndex(closedWriteIds, closedReadIds);
    }

    const TModuloShardingTablet& GetShardingTabletVerified(const ui64 tabletId) const {
        for (auto&& i : SpecialSharding) {
            if (i.GetTabletId() == tabletId) {
                return i;
            }
        }
        AFL_VERIFY(false);
        return Default<TModuloShardingTablet>();
    }

    void SerializeToProto(NKikimrSchemeOp::TColumnTableSharding& proto) const {
        AFL_VERIFY(PartsCount);
        proto.MutableHashSharding()->SetModuloPartsCount(PartsCount);
        for (auto&& i : SpecialSharding) {
            *proto.MutableHashSharding()->AddTabletsForModulo() = i.SerializeToProto();
        }
    }

    bool DeleteShardInfo(const ui64 tabletId) {
        const auto pred = [&](const TModuloShardingTablet& info) {
            return info.GetTabletId() == tabletId;
        };
        const ui32 sizeStart = SpecialSharding.size();
        SpecialSharding.erase(std::remove_if(SpecialSharding.begin(), SpecialSharding.end(), pred), SpecialSharding.end());

        ActiveReadSpecialSharding.clear();
        ActiveWriteSpecialSharding.clear();

        return sizeStart != SpecialSharding.size();
    }

    bool UpdateShardInfo(const TModuloShardingTablet& info) {
        for (auto&& i : SpecialSharding) {
            if (i.GetTabletId() == info.GetTabletId()) {
                i = info;
                IndexConstructed = false;
                return true;
            }
        }
        return false;
    }

    void AddShardInfo(const TModuloShardingTablet& info) {
        IndexConstructed = false;
        for (auto&& i : SpecialSharding) {
            AFL_VERIFY(i.GetTabletId() != info.GetTabletId());
        }
        SpecialSharding.emplace_back(info);

        ActiveReadSpecialSharding.clear();
        ActiveWriteSpecialSharding.clear();
    }
};

class THashShardingModuloN : public THashShardingImpl {
public:
    static TString GetClassNameStatic() {
        return "MODULO";
    }
private:
    using TBase = THashShardingImpl;
private:
    std::optional<TSpecificShardingInfo> SpecialShardingInfo;

    bool DeleteShardInfo(const ui64 tabletId) {
        AFL_VERIFY(!!SpecialShardingInfo);
        return SpecialShardingInfo->DeleteShardInfo(tabletId);
    }

    bool UpdateShardInfo(const TSpecificShardingInfo::TModuloShardingTablet& info) {
        AFL_VERIFY(SpecialShardingInfo);
        GetShardInfoVerified(info.GetTabletId()).IncrementVersion();
        if (SpecialShardingInfo->UpdateShardInfo(info)) {
            return true;
        }
        for (auto&& i : GetOrderedShardIds()) {
            if (i == info.GetTabletId()) {
                SpecialShardingInfo->AddShardInfo(info);
                return true;
            }
        }
        AFL_VERIFY(false);
        return false;
    }
    virtual std::shared_ptr<IGranuleShardingLogic> DoGetTabletShardingInfoOptional(const ui64 tabletId) const override;

protected:
    virtual TConclusion<std::vector<NKikimrSchemeOp::TAlterShards>> DoBuildSplitShardsModifiers(const std::vector<ui64>& newTabletIds) const override;

    virtual TConclusion<std::vector<NKikimrSchemeOp::TAlterShards>> DoBuildMergeShardsModifiers(const std::vector<ui64>& newTabletIds) const override;

    virtual TConclusionStatus DoOnAfterModification() override;
    virtual TConclusionStatus DoOnBeforeModification() override {
        if (!SpecialShardingInfo) {
            SpecialShardingInfo = TSpecificShardingInfo(GetOrderedShardIds());
        }
        return TConclusionStatus::Success();
    }

    virtual TConclusionStatus DoApplyModification(const NKikimrSchemeOp::TShardingModification& proto) override;
    virtual void DoSerializeToProto(NKikimrSchemeOp::TColumnTableSharding& proto) const override {
        TBase::DoSerializeToProto(proto);
        if (SpecialShardingInfo) {
            SpecialShardingInfo->SerializeToProto(proto);
        }
        proto.MutableHashSharding()->SetFunction(NKikimrSchemeOp::TColumnTableSharding::THashSharding::HASH_FUNCTION_MODULO_N);
    }


    virtual TConclusionStatus DoDeserializeFromProto(const NKikimrSchemeOp::TColumnTableSharding& proto) override;

    virtual std::set<ui64> DoGetModifiedShardIds(const NKikimrSchemeOp::TShardingModification& proto) const override {
        std::set<ui64> result;
        for (auto&& i : proto.GetModulo().GetShards()) {
            result.emplace(i.GetTabletId());
        }
        return result;
    }
public:
    using TBase::TBase;

    THashShardingModuloN() = default;

    THashShardingModuloN(const std::vector<ui64>& shardIds, const std::vector<TString>& columnNames, ui64 seed = 0)
        : TBase(shardIds, columnNames, seed)
    {}

    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }

    virtual THashMap<ui64, std::vector<ui32>> MakeSharding(const std::shared_ptr<arrow::RecordBatch>& batch) const override;
};

class TGranuleSharding: public THashGranuleSharding {
public:
    static TString GetClassNameStatic() {
        return "MODULO";
    }
private:
    using TBase = THashGranuleSharding;
    ui64 PartsCount = 0;
    TSpecificShardingInfo::TModuloShardingTablet Interval;
    static const inline TFactory::TRegistrator<TGranuleSharding> Registrator = TFactory::TRegistrator<TGranuleSharding>(GetClassNameStatic());

protected:
    virtual std::shared_ptr<NArrow::TColumnFilter> DoGetFilter(const std::shared_ptr<arrow::Table>& table) const override {
        const std::vector<ui64> hashes = CalcHashes(table);
        auto result = std::make_shared<NArrow::TColumnFilter>(NArrow::TColumnFilter::BuildAllowFilter());
        const auto getter = [&](const ui32 index) {
            return Interval.GetAppropriateMods().contains(hashes[index] % PartsCount);
        };
        result->ResetWithLambda(hashes.size(), getter);
        return result;

    }
    virtual void DoSerializeToProto(TProto& proto) const override {
        AFL_VERIFY(PartsCount);
        proto.MutableModulo()->SetModuloPartsCount(PartsCount);
        *proto.MutableModulo()->MutableHashing() = TBase::SerializeHashingToProto();
        *proto.MutableModulo()->MutableShardInfo() = Interval.SerializeToProto();
    }
    virtual TConclusionStatus DoDeserializeFromProto(const TProto& proto) override {
        PartsCount = proto.GetModulo().GetModuloPartsCount();
        if (!PartsCount) {
            return TConclusionStatus::Fail("incorrect parts count for modulo info");
        }
        {
            auto conclusion = TBase::DeserializeHashingFromProto(proto.GetModulo().GetHashing());
            if (conclusion.IsFail()) {
                return conclusion;
            }
        }
        {
            auto conclusion = Interval.DeserializeFromProto(proto.GetModulo().GetShardInfo());
            if (conclusion.IsFail()) {
                return conclusion;
            }
        }

        return TConclusionStatus::Success();
    }
public:
    TGranuleSharding() = default;

    TGranuleSharding(const std::vector<TString>& columnNames, const TSpecificShardingInfo::TModuloShardingTablet& interval, const ui64 partsCount)
        : TBase(columnNames)
        , PartsCount(partsCount)
        , Interval(interval) {
        AFL_VERIFY(PartsCount);
    }

    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }
};

}
