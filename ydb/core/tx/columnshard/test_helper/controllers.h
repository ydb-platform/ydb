#pragma once
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/testlib/basics/runtime.h>

namespace NKikimr::NOlap {

class TWaitCompactionController: public NYDBTest::NColumnShard::TController {
private:
    using TBase = NKikimr::NYDBTest::ICSController;
    TAtomicCounter ExportsFinishedCount = 0;
    ui32 TiersModificationsCount = 0;
    YDB_READONLY(TAtomicCounter, StatisticsUsageCount, 0);
    YDB_READONLY(TAtomicCounter, MaxValueUsageCount, 0);
    YDB_ACCESSOR_DEF(std::optional<ui64>, SmallSizeDetector);
    YDB_ACCESSOR_DEF(std::optional<NColumnShard::NTiers::TConfigsSnapshot>, TiersSnapshot);
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
    TWaitCompactionController() {
        SetOverridePeriodicWakeupActivationPeriod(TDuration::Seconds(1));
    }

    ui32 GetFinishedExportsCount() const {
        return ExportsFinishedCount.Val();
    }

    virtual void OnStatisticsUsage(const NKikimr::NOlap::NIndexes::TIndexMetaContainer& /*statOperator*/) override {
        StatisticsUsageCount.Inc();
    }
    virtual void OnMaxValueUsage() override {
        MaxValueUsageCount.Inc();
    }
    void SetTiersSnapshot(TTestBasicRuntime& runtime, const TActorId& tabletActorId, NColumnShard::NTiers::TConfigsSnapshot snapshot);

    virtual std::optional<NColumnShard::NTiers::TConfigsSnapshot> GetTiersSnapshotOverride() const override {
        return TiersSnapshot;
    }
};

}
