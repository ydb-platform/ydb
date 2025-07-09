#pragma once
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/tiering/manager.h>
#include <ydb/core/tx/columnshard/blobs_action/bs/address.h>

namespace NKikimr::NOlap {

class TWaitCompactionController: public NYDBTest::NColumnShard::TController {
private:
    using TBase = NKikimr::NYDBTest::ICSController;
    TAtomicCounter ExportsFinishedCount = 0;
    THashMap<TString, NColumnShard::NTiers::TTierConfig> OverrideTiers;
    ui32 TiersModificationsCount = 0;
    YDB_READONLY(TAtomicCounter, TieringMetadataActualizationCount, 0);
    YDB_READONLY(TAtomicCounter, StatisticsUsageCount, 0);
    YDB_READONLY(TAtomicCounter, MaxValueUsageCount, 0);
    YDB_ACCESSOR_DEF(std::optional<ui64>, SmallSizeDetector);
    YDB_ACCESSOR(bool, SkipSpecialCheckForEvict, false);

protected:
    virtual void OnTieringModified(const std::shared_ptr<NKikimr::NColumnShard::TTiersManager>& /*tiers*/) override;
    virtual void OnExportFinished() override {
        ExportsFinishedCount.Inc();
    }
    virtual bool NeedForceCompactionBacketsConstruction() const override {
        return true;
    }
    virtual ui64 DoGetSmallPortionSizeDetector(const ui64 /*def*/) const override {
        return SmallSizeDetector.value_or(0);
    }
    virtual TDuration DoGetOptimizerFreshnessCheckDuration(const TDuration /*defaultValue*/) const override {
        return TDuration::Zero();
    }
    virtual TDuration DoGetLagForCompactionBeforeTierings(const TDuration /*def*/) const override {
        return TDuration::Zero();
    }
    virtual TDuration DoGetCompactionActualizationLag(const TDuration /*def*/) const override {
        return TDuration::Zero();
    }
public:
    virtual bool CheckPortionForEvict(const TPortionInfo& portion) const override {
        if (SkipSpecialCheckForEvict) {
            return true;
        } else {
            return TBase::CheckPortionForEvict(portion);
        }
    }


    TWaitCompactionController() {
        SetOverridePeriodicWakeupActivationPeriod(TDuration::Seconds(1));
    }

    ui32 GetFinishedExportsCount() const {
        return ExportsFinishedCount.Val();
    }

    virtual void OnTieringMetadataActualized() override {
        TieringMetadataActualizationCount.Inc();
    }
    virtual void OnStatisticsUsage(const NKikimr::NOlap::NIndexes::TIndexMetaContainer& /*statOperator*/) override {
        StatisticsUsageCount.Inc();
    }
    virtual void OnMaxValueUsage() override {
        MaxValueUsageCount.Inc();
    }
    void OverrideTierConfigs(
        TTestBasicRuntime& runtime, const TActorId& tabletActorId, THashMap<TString, NColumnShard::NTiers::TTierConfig> tiers);

    THashMap<TString, NColumnShard::NTiers::TTierConfig> GetOverrideTierConfigs() const override {
        return OverrideTiers;
    }
};

class TFailingBSController: public NKikimr::NYDBTest::NColumnShard::TController {
    void DoOnCollectGarbageResult(TEvBlobStorage::TEvCollectGarbageResult::TPtr& result) override {
        NBlobOperations::NBlobStorage::TBlobAddress group(result->Cookie, result->Get()->Channel);
        if (!FailingGroup.has_value()) {
            FailingGroup = group;
        }
        if (group == FailingGroup.value() && FailsCount < 15) {
            Cerr << "Dropped EvCollectGarbageResult" << Endl;
            result->Get()->Status = NKikimrProto::ERROR;
            FailsCount++;
        }
    }

private:
    std::optional<NBlobOperations::NBlobStorage::TBlobAddress> FailingGroup = std::nullopt;
    size_t FailsCount = 0;
};

} // namespace NKikimr::NOlap
