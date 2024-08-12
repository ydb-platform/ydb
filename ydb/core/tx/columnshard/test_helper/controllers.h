#pragma once
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/testlib/basics/runtime.h>

namespace NKikimr::NOlap {

class TWaitCompactionController: public NYDBTest::NColumnShard::TController {
private:
    using TBase = NKikimr::NYDBTest::ICSController;
    TAtomicCounter ExportsFinishedCount = 0;
    NMetadata::NFetcher::ISnapshot::TPtr CurrentConfig;
    ui32 TiersModificationsCount = 0;
    YDB_READONLY(TAtomicCounter, StatisticsUsageCount, 0);
    YDB_READONLY(TAtomicCounter, MaxValueUsageCount, 0);
    YDB_ACCESSOR_DEF(std::optional<ui64>, SmallSizeDetector);
protected:
    virtual void OnTieringModified(const std::shared_ptr<NKikimr::NColumnShard::TTiersManager>& /*tiers*/) override;
    virtual void OnExportFinished() override {
        ExportsFinishedCount.Inc();
    }
    virtual bool NeedForceCompactionBacketsConstruction() const override {
        return true;
    }
    virtual ui64 GetSmallPortionSizeDetector(const ui64 /*def*/) const override {
        return SmallSizeDetector.value_or(0);
    }
    virtual TDuration GetOptimizerFreshnessCheckDuration(const TDuration /*defaultValue*/) const override {
        return TDuration::Zero();
    }
    virtual TDuration GetLagForCompactionBeforeTierings(const TDuration /*def*/) const override {
        return TDuration::Zero();
    }
    virtual TDuration GetCompactionActualizationLag(const TDuration /*def*/) const override {
        return TDuration::Zero();
    }
    virtual TDuration GetTTLDefaultWaitingDuration(const TDuration /*defaultValue*/) const override {
        return TDuration::Seconds(1);
    }
public:
    TWaitCompactionController() {
        SetPeriodicWakeupActivationPeriod(TDuration::Seconds(1));
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
    void SetTiersSnapshot(TTestBasicRuntime& runtime, const TActorId& tabletActorId, const NMetadata::NFetcher::ISnapshot::TPtr& snapshot);

    virtual NMetadata::NFetcher::ISnapshot::TPtr GetFallbackTiersSnapshot() const override {
        if (CurrentConfig) {
            return CurrentConfig;
        } else {
            return TBase::GetFallbackTiersSnapshot();
        }
    }
};

}
