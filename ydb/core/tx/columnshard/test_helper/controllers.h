#pragma once
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/testlib/basics/runtime.h>

namespace NKikimr::NOlap {

class TWaitCompactionController: public NYDBTest::NColumnShard::TController {
private:
    using TBase = NKikimr::NYDBTest::ICSController;
    TAtomic TTLFinishedCounter = 0;
    TAtomic TTLStartedCounter = 0;
    TAtomic InsertFinishedCounter = 0;
    TAtomic InsertStartedCounter = 0;
    TAtomicCounter ExportsFinishedCount = 0;
    NMetadata::NFetcher::ISnapshot::TPtr CurrentConfig;
    bool CompactionEnabledFlag = true;
    YDB_ACCESSOR(bool, TTLEnabled, true);
    ui32 TiersModificationsCount = 0;
    YDB_READONLY(TAtomicCounter, StatisticsUsageCount, 0);
    YDB_READONLY(TAtomicCounter, MaxValueUsageCount, 0);
protected:
    virtual void OnTieringModified(const std::shared_ptr<NKikimr::NColumnShard::TTiersManager>& /*tiers*/) override;
    virtual void OnExportFinished() override {
        ExportsFinishedCount.Inc();
    }
    virtual bool DoOnStartCompaction(std::shared_ptr<NKikimr::NOlap::TColumnEngineChanges>& changes) override;
    virtual bool DoOnWriteIndexComplete(const NKikimr::NOlap::TColumnEngineChanges& changes, const NKikimr::NColumnShard::TColumnShard& /*shard*/) override;
    virtual bool DoOnWriteIndexStart(const ui64 /*tabletId*/, const TString& changeClassName) override;
    virtual bool NeedForceCompactionBacketsConstruction() const override {
        return true;
    }
    virtual ui64 GetSmallPortionSizeDetector(const ui64 /*def*/) const override {
        return 0;
    }
    virtual TDuration GetOptimizerFreshnessCheckDuration(const TDuration /*defaultValue*/) const override {
        return TDuration::Zero();
    }
    virtual TDuration GetLagForCompactionBeforeTierings(const TDuration /*def*/) const override {
        return TDuration::Zero();
    }
    virtual TDuration GetTTLDefaultWaitingDuration(const TDuration /*defaultValue*/) const override {
        return TDuration::Seconds(1);
    }
public:
    ui32 GetFinishedExportsCount() const {
        return ExportsFinishedCount.Val();
    }

    virtual void OnStatisticsUsage(const NKikimr::NOlap::NStatistics::TOperatorContainer& /*statOperator*/) override {
        StatisticsUsageCount.Inc();
    }
    virtual void OnMaxValueUsage() override {
        MaxValueUsageCount.Inc();
    }
    void SetCompactionEnabled(const bool value) {
        CompactionEnabledFlag = value;
    }
    virtual bool IsTTLEnabled() const override {
        return TTLEnabled;
    }
    void SetTiersSnapshot(TTestBasicRuntime& runtime, const TActorId& tabletActorId, const NMetadata::NFetcher::ISnapshot::TPtr& snapshot);

    virtual NMetadata::NFetcher::ISnapshot::TPtr GetFallbackTiersSnapshot() const override {
        if (CurrentConfig) {
            return CurrentConfig;
        } else {
            return TBase::GetFallbackTiersSnapshot();
        }
    }
    i64 GetTTLFinishedCounter() const {
        return AtomicGet(TTLFinishedCounter);
    }

    i64 GetTTLStartedCounter() const {
        return AtomicGet(TTLStartedCounter);
    }

    i64 GetInsertFinishedCounter() const {
        return AtomicGet(InsertFinishedCounter);
    }

    i64 GetInsertStartedCounter() const {
        return AtomicGet(InsertStartedCounter);
    }

};

}
