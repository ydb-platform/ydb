#pragma once
#include "hash_sharding.h"
#include <ydb/core/protos/flat_scheme_op.pb.h>

namespace NKikimr::NSharding::NConsistency {

class TSpecificShardingInfo {
public:
    class TConsistencyShardingTablet {
    private:
        YDB_ACCESSOR(ui64, TabletId, 0);
        YDB_READONLY(ui64, HashIntervalLeftClosed, 0);
        YDB_READONLY(ui64, HashIntervalRightOpened, Max<ui64>());
    public:
        TConsistencyShardingTablet() = default;
        TConsistencyShardingTablet(const ui64 tabletId, const ui64 hashIntervalLeftClosed, const ui64 hashIntervalRightOpened)
            : TabletId(tabletId)
            , HashIntervalLeftClosed(hashIntervalLeftClosed)
            , HashIntervalRightOpened(hashIntervalRightOpened) {
            AFL_VERIFY(HashIntervalLeftClosed < HashIntervalRightOpened);
        }

        void CutHalfIntervalFromStart() {
            const ui64 toHalf = 0.5 * HashIntervalRightOpened;
            const ui64 fromHalf = 0.5 * HashIntervalLeftClosed;
            HashIntervalRightOpened = toHalf + fromHalf;
        }

        void CutHalfIntervalToEnd() {
            const ui64 toHalf = 0.5 * HashIntervalRightOpened;
            const ui64 fromHalf = 0.5 * HashIntervalLeftClosed;
            HashIntervalLeftClosed = toHalf + fromHalf;
        }

        NKikimrSchemeOp::TConsistencyShardingTablet SerializeToProto() const {
            NKikimrSchemeOp::TConsistencyShardingTablet result;
            result.SetTabletId(TabletId);
            result.SetHashIntervalLeftClosed(HashIntervalLeftClosed);
            result.SetHashIntervalRightOpened(HashIntervalRightOpened);
            return result;
        }

        TConclusionStatus DeserializeFromProto(const NKikimrSchemeOp::TConsistencyShardingTablet& proto) {
            TabletId = proto.GetTabletId();
            HashIntervalLeftClosed = proto.GetHashIntervalLeftClosed();
            HashIntervalRightOpened = proto.GetHashIntervalRightOpened();
            AFL_VERIFY(HashIntervalLeftClosed < HashIntervalRightOpened);
            return TConclusionStatus::Success();
        }

        bool operator<(const TConsistencyShardingTablet& item) const {
            if (HashIntervalLeftClosed == item.HashIntervalLeftClosed) {
                return HashIntervalRightOpened < item.HashIntervalRightOpened;
            } else {
                return HashIntervalLeftClosed < item.HashIntervalLeftClosed;
            }
        }
    };

private:
    bool IndexConstructed = false;
    std::vector<TConsistencyShardingTablet> SpecialSharding;
    std::vector<TConsistencyShardingTablet*> ActiveWriteSpecialSharding;
    std::vector<TConsistencyShardingTablet*> ActiveReadSpecialSharding;
    ui64 GetUnifiedDistributionBorder(const ui32 idx, const ui64 shardsCount) const {
        AFL_VERIFY(idx <= shardsCount);
        if (idx == shardsCount) {
            return Max<ui64>();
        }
        return Max<ui64>() * (1.0 * idx / shardsCount);
    }

    TConclusionStatus CheckIntervalsFilling() const {
        {
            ui64 currentPos = 0;
            for (auto&& i : ActiveReadSpecialSharding) {
                if (currentPos < i->GetHashIntervalLeftClosed()) {
                    return TConclusionStatus::Fail("sharding special intervals not covered (reading) full ui64 line");
                } else if (currentPos > i->GetHashIntervalLeftClosed()) {
                    return TConclusionStatus::Fail("sharding intervals covered twice for reading full ui64 line");
                }
                currentPos = i->GetHashIntervalRightOpened();
            }
            if (currentPos != Max<ui64>()) {
                return TConclusionStatus::Fail("sharding special intervals not covered (reading) full ui64 line (final segment)");
            }
        }
        {
            ui64 currentPos = 0;
            for (auto&& i : ActiveWriteSpecialSharding) {
                if (currentPos < i->GetHashIntervalLeftClosed()) {
                    return TConclusionStatus::Fail("sharding special intervals not covered (writing) full ui64 line");
                }
                currentPos = std::max<ui64>(currentPos, i->GetHashIntervalRightOpened());
            }
            if (currentPos != Max<ui64>()) {
                return TConclusionStatus::Fail("sharding special intervals not covered (writing) full ui64 line (final segment)");
            }
        }
        return TConclusionStatus::Success();
    }

public:
    bool IsEmpty() const {
        return SpecialSharding.empty();
    }

    TSpecificShardingInfo() = default;

    TSpecificShardingInfo(const std::vector<ui64>& shardIds) {
        for (ui32 i = 0; i < shardIds.size(); ++i) {
            const ui64 start = GetUnifiedDistributionBorder(i, shardIds.size());
            const ui64 finish = GetUnifiedDistributionBorder(i + 1, shardIds.size());
            TConsistencyShardingTablet info(shardIds[i], start, finish);
            SpecialSharding.emplace_back(info);
        }
        BuildActivityIndex(Default<std::set<ui64>>(), Default<std::set<ui64>>()).Validate();
    }

    THashMap<ui64, std::vector<ui32>> MakeShardingWrite(const std::vector<ui64> hashes) const {
        AFL_VERIFY(IndexConstructed);
        std::vector<std::vector<ui32>> result;
        result.resize(ActiveWriteSpecialSharding.size());
        for (auto&& i : result) {
            i.reserve(hashes.size());
        }
        ui32 idxRecord = 0;
        for (auto&& i : hashes) {
            ui32 idxShard = 0;
            bool found = false;
            for (auto&& s : ActiveWriteSpecialSharding) {
                if (s->GetHashIntervalLeftClosed() > i || i >= s->GetHashIntervalRightOpened()) {
                    break;
                }
                result[idxShard].emplace_back(idxRecord);
                found = true;
                ++idxShard;
            }
            AFL_VERIFY(found);
            ++idxRecord;
        }
        THashMap<ui64, std::vector<ui32>> resultHash;
        for (ui32 i = 0; i < result.size(); ++i) {
            if (result[i].size()) {
                resultHash.emplace(ActiveWriteSpecialSharding[i]->GetTabletId(), std::move(result[i]));
            }
        }
        return resultHash;
    }

    void SerializeToProto(NKikimrSchemeOp::TColumnTableSharding& proto) const {
        AFL_VERIFY(IndexConstructed);
        for (auto&& i : SpecialSharding) {
            *proto.MutableHashSharding()->AddTabletsForConsistency() = i.SerializeToProto();
        }
    }

    TConclusionStatus DeserializeFromProto(const NKikimrSchemeOp::TColumnTableSharding& proto, const std::set<ui64>& closedForWrite, const std::set<ui64>& closedForRead) {
        for (auto&& i : proto.GetHashSharding().GetTabletsForConsistency()) {
            TConsistencyShardingTablet info;
            auto conclusion = info.DeserializeFromProto(i);
            if (conclusion.IsFail()) {
                return conclusion;
            }
            SpecialSharding.emplace_back(std::move(info));
        }
        IndexConstructed = false;
        return BuildActivityIndex(closedForWrite, closedForRead);
    }

    [[nodiscard]] TConclusionStatus BuildActivityIndex(const std::set<ui64>& closedForWrite, const std::set<ui64>& closedForRead) {
        std::sort(SpecialSharding.begin(), SpecialSharding.end());
        ActiveWriteSpecialSharding.clear();
        ActiveReadSpecialSharding.clear();
        if (SpecialSharding.empty()) {
            return TConclusionStatus::Success();
        }
        for (auto&& i : SpecialSharding) {
            if (!closedForWrite.contains(i.GetTabletId())) {
                ActiveWriteSpecialSharding.emplace_back(&i);
            }
            if (!closedForRead.contains(i.GetTabletId())) {
                ActiveReadSpecialSharding.emplace_back(&i);
            }
        }
        auto result = CheckIntervalsFilling();
        IndexConstructed = result.IsSuccess();
        return result;
    }

    const TConsistencyShardingTablet& GetShardingTabletVerified(const ui64 tabletId) const {
        for (auto&& i : SpecialSharding) {
            if (i.GetTabletId() == tabletId) {
                return i;
            }
        }
        AFL_VERIFY(false);
        return Default<TConsistencyShardingTablet>();
    }

    bool CheckUnifiedDistribution(const ui32 originalShardsCount, std::vector<ui64>& orderedShardIds) {
        if (originalShardsCount != SpecialSharding.size()) {
            return false;
        }
        AFL_VERIFY(IndexConstructed);
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
        ui32 idx = 0;
        std::vector<ui64> shardIdsCorrectOrder;
        for (auto&& i : SpecialSharding) {
            const ui64 start = GetUnifiedDistributionBorder(idx, SpecialSharding.size());
            const ui64 finish = GetUnifiedDistributionBorder(idx + 1, SpecialSharding.size());
            if (i.GetHashIntervalLeftClosed() != start || i.GetHashIntervalRightOpened() != finish) {
                return false;
            }
            shardIdsCorrectOrder.emplace_back(i.GetTabletId());
            ++idx;
        }
        orderedShardIds = shardIdsCorrectOrder;
        return true;
    }

    bool UpdateShardInfo(const TConsistencyShardingTablet& info) {
        for (auto&& i : SpecialSharding) {
            if (i.GetTabletId() == info.GetTabletId()) {
                i = info;
                IndexConstructed = false;
                return true;
            }
        }
        return false;
    }

    bool DeleteShardInfo(const ui64 tabletId) {
        const auto pred = [&](const TConsistencyShardingTablet& info) {
            return info.GetTabletId() == tabletId;
        };
        const ui32 sizeStart = SpecialSharding.size();
        SpecialSharding.erase(std::remove_if(SpecialSharding.begin(), SpecialSharding.end(), pred), SpecialSharding.end());
        return sizeStart != SpecialSharding.size();
    }

    void AddShardInfo(const TConsistencyShardingTablet& info) {
        for (auto&& i : SpecialSharding) {
            AFL_VERIFY(i.GetTabletId() != info.GetTabletId());
        }
        SpecialSharding.emplace_back(info);

        ActiveWriteSpecialSharding.clear();
        ActiveReadSpecialSharding.clear();

        IndexConstructed = false;
    }

};

class TConsistencySharding64: public THashShardingImpl {
public:
    static TString GetClassNameStatic() {
        return "CONSISTENCY";
    }
private:
    using TBase = THashShardingImpl;

    std::optional<TSpecificShardingInfo> SpecialShardingInfo;

    virtual TConclusion<std::vector<NKikimrSchemeOp::TAlterShards>> DoBuildSplitShardsModifiers(const std::vector<ui64>& newTabletIds) const override;
    virtual TConclusion<std::vector<NKikimrSchemeOp::TAlterShards>> DoBuildMergeShardsModifiers(const std::vector<ui64>& newTabletIds) const override;

    bool UpdateShardInfo(const TSpecificShardingInfo::TConsistencyShardingTablet& info) {
        GetShardInfoVerified(info.GetTabletId()).IncrementVersion();
        AFL_VERIFY(!!SpecialShardingInfo);
        if (SpecialShardingInfo->UpdateShardInfo(info)) {
            return true;
        }
        for (auto&& i : GetOrderedShardIds()) {
            if (i == info.GetTabletId()) {
                SpecialShardingInfo->AddShardInfo(info);
                return true;
            }
        }
        return false;
    }

    bool DeleteShardInfo(const ui64 tabletId) {
        AFL_VERIFY(!!SpecialShardingInfo);
        return SpecialShardingInfo->DeleteShardInfo(tabletId);
    }

    virtual std::shared_ptr<IGranuleShardingLogic> DoGetTabletShardingInfoOptional(const ui64 tabletId) const override;

    virtual TConclusionStatus DoOnBeforeModification() override {
        if (!SpecialShardingInfo) {
            AFL_VERIFY(!HasReadClosedShards() && !HasWriteClosedShards());
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
        proto.MutableHashSharding()->SetFunction(NKikimrSchemeOp::TColumnTableSharding::THashSharding::HASH_FUNCTION_CONSISTENCY_64);
    }
    virtual TConclusionStatus DoDeserializeFromProto(const NKikimrSchemeOp::TColumnTableSharding& proto) override;

    virtual TConclusionStatus DoOnAfterModification() override;

    virtual std::set<ui64> DoGetModifiedShardIds(const NKikimrSchemeOp::TShardingModification& proto) const override {
        std::set<ui64> result;
        for (auto&& i : proto.GetConsistency().GetShards()) {
            result.emplace(i.GetTabletId());
        }
        return result;
    }
public:
    using TBase::TBase;

    TConsistencySharding64() = default;

    TConsistencySharding64(const std::vector<ui64>& shardIds, const std::vector<TString>& columnNames, ui64 seed = 0)
        : TBase(shardIds, columnNames, seed) {
    }

    virtual THashMap<ui64, std::vector<ui32>> MakeSharding(const std::shared_ptr<arrow::RecordBatch>& batch) const override;

    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }
};

class TGranuleSharding: public THashGranuleSharding {
public:
    static TString GetClassNameStatic() {
        return "CONSISTENCY";
    }
private:
    using TBase = THashGranuleSharding;
    TSpecificShardingInfo::TConsistencyShardingTablet Interval;
    static const inline TFactory::TRegistrator<TGranuleSharding> Registrator = TFactory::TRegistrator<TGranuleSharding>(GetClassNameStatic());
protected:
    virtual std::shared_ptr<NArrow::TColumnFilter> DoGetFilter(const std::shared_ptr<arrow::Table>& table) const override {
        const std::vector<ui64> hashes = CalcHashes(table);
        auto result = std::make_shared<NArrow::TColumnFilter>(NArrow::TColumnFilter::BuildAllowFilter());
        const auto getter = [&](const ui64 index) {
            const ui64 hash = hashes[index];
            return Interval.GetHashIntervalLeftClosed() <= hash && hash < Interval.GetHashIntervalRightOpened();
        };
        result->ResetWithLambda(hashes.size(), getter);
        return result;

    }
    virtual void DoSerializeToProto(TProto& proto) const override {
        *proto.MutableConsistency()->MutableHashing() = TBase::SerializeHashingToProto();
        *proto.MutableConsistency()->MutableShardInfo() = Interval.SerializeToProto();
    }
    virtual TConclusionStatus DoDeserializeFromProto(const TProto& proto) override {
        {
            auto conclusion = TBase::DeserializeHashingFromProto(proto.GetConsistency().GetHashing());
            if (conclusion.IsFail()) {
                return conclusion;
            }
        }
        {
            auto conclusion = Interval.DeserializeFromProto(proto.GetConsistency().GetShardInfo());
            if (conclusion.IsFail()) {
                return conclusion;
            }
        }

        return TConclusionStatus::Success();
    }
public:
    TGranuleSharding() = default;

    TGranuleSharding(const std::vector<TString>& columnNames, const TSpecificShardingInfo::TConsistencyShardingTablet& interval)
        : TBase(columnNames)
        , Interval(interval) {

    }

    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }
};

}
