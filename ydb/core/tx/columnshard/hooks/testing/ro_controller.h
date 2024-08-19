#pragma once
#include <ydb/core/tx/columnshard/blobs_action/abstract/blob_set.h>
#include <ydb/core/tx/columnshard/blob.h>
#include <ydb/core/tx/columnshard/common/tablet_id.h>
#include <ydb/core/tx/columnshard/engines/writer/write_controller.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <util/string/join.h>

namespace NKikimr::NYDBTest::NColumnShard {

class TReadOnlyController: public ICSController {
private:
    YDB_READONLY(TAtomicCounter, TTLFinishedCounter, 0);
    YDB_READONLY(TAtomicCounter, TTLStartedCounter, 0);
    YDB_READONLY(TAtomicCounter, InsertFinishedCounter, 0);
    YDB_READONLY(TAtomicCounter, InsertStartedCounter, 0);
    YDB_READONLY(TAtomicCounter, CompactionFinishedCounter, 0);
    YDB_READONLY(TAtomicCounter, CompactionStartedCounter, 0);
    YDB_READONLY(TAtomicCounter, CleaningFinishedCounter, 0);
    YDB_READONLY(TAtomicCounter, CleaningStartedCounter, 0);

    YDB_READONLY(TAtomicCounter, FilteredRecordsCount, 0);
    YDB_READONLY(TAtomicCounter, IndexesSkippingOnSelect, 0);
    YDB_READONLY(TAtomicCounter, IndexesApprovedOnSelect, 0);
    YDB_READONLY(TAtomicCounter, IndexesSkippedNoData, 0);
    YDB_READONLY(TAtomicCounter, TieringUpdates, 0);
    YDB_READONLY(TAtomicCounter, NeedActualizationCount, 0);

    YDB_READONLY(TAtomicCounter, ActualizationsCount, 0);
    YDB_READONLY(TAtomicCounter, ActualizationRefreshSchemeCount, 0);
    YDB_READONLY(TAtomicCounter, ActualizationRefreshTieringCount, 0);
    YDB_READONLY(TAtomicCounter, ShardingFiltersCount, 0);

    YDB_READONLY(TAtomicCounter, RequestTracingSnapshotsSave, 0);
    YDB_READONLY(TAtomicCounter, RequestTracingSnapshotsRemove, 0);

    YDB_ACCESSOR(TAtomicCounter, CompactionsLimit, 10000000);

protected:
    NActors::TTestBasicRuntime* const TestRuntime = nullptr;

protected:
    virtual void OnRequestTracingChanges(
        const std::set<NOlap::TSnapshot>& snapshotsToSave, const std::set<NOlap::TSnapshot>& snapshotsToRemove) override {
        RequestTracingSnapshotsSave.Add(snapshotsToSave.size());
        RequestTracingSnapshotsRemove.Add(snapshotsToRemove.size());
    }

    virtual void OnSelectShardingFilter() override {
        ShardingFiltersCount.Inc();
    }

    virtual void AddPortionForActualizer(const i32 portionsCount) override {
        NeedActualizationCount.Add(portionsCount);
    }

    virtual void OnPortionActualization(const NOlap::TPortionInfo& /*info*/) override {
        ActualizationsCount.Inc();
    }
    virtual void OnActualizationRefreshScheme() override {
        ActualizationRefreshSchemeCount.Inc();
    }
    virtual void OnActualizationRefreshTiering() override {
        ActualizationRefreshTieringCount.Inc();
    }

    virtual bool DoOnWriteIndexStart(const ui64 tabletId, NOlap::TColumnEngineChanges& change) override;
    virtual bool DoOnAfterFilterAssembling(const std::shared_ptr<arrow::RecordBatch>& batch) override;
    virtual bool DoOnWriteIndexComplete(const NOlap::TColumnEngineChanges& changes, const ::NKikimr::NColumnShard::TColumnShard& shard) override;
    virtual void OnTieringModified(const std::shared_ptr<NKikimr::NColumnShard::TTiersManager>& /*tiers*/) override {
        TieringUpdates.Inc();
    }
    virtual EOptimizerCompactionWeightControl GetCompactionControl() const override {
        return EOptimizerCompactionWeightControl::Force;
    }

protected:
    void SimulateSleep(const TDuration& duration) const {
        if (TestRuntime) {
            TestRuntime->SimulateSleep(duration);
        } else {
            Sleep(TDuration::Seconds(1));
        }
    }

public:
    TReadOnlyController() = default;
    TReadOnlyController(NActors::TTestBasicRuntime& runtime)
        : TestRuntime(&runtime)
    {}
    virtual TDuration GetOverridenGCPeriod(const TDuration /*def*/) const override {
        return TDuration::Zero();
    }

    void WaitCompactions(const TDuration d) const {
        TInstant start = TInstant::Now();
        ui32 compactionsStart = GetCompactionStartedCounter().Val();
        while (Now() - start < d) {
            if (compactionsStart != GetCompactionStartedCounter().Val()) {
                compactionsStart = GetCompactionStartedCounter().Val();
                start = TInstant::Now();
            }
            Cerr << "WAIT_COMPACTION: " << GetCompactionStartedCounter().Val() << Endl;
            Sleep(TDuration::Seconds(1));
        }
    }

    void WaitIndexation(const TDuration d) const {
        TInstant start = TInstant::Now();
        ui32 insertsStart = GetInsertStartedCounter().Val();        
        while (Now() - start < d) {
            if (insertsStart != GetInsertStartedCounter().Val()) {
                insertsStart = GetInsertStartedCounter().Val();
                start = TInstant::Now();
            }
            Cerr << "WAIT_INDEXATION: " << GetInsertStartedCounter().Val() << Endl;
            SimulateSleep(TDuration::Seconds(1));
        }
    }

    void WaitCleaning(const TDuration d) const {
        TInstant start = TInstant::Now();
        ui32 countStart = GetCleaningStartedCounter().Val();
        while (Now() - start < d) {
            if (countStart != GetCleaningStartedCounter().Val()) {
                countStart = GetCleaningStartedCounter().Val();
                start = TInstant::Now();
            }
            Cerr << "WAIT_CLEANING: " << GetCleaningStartedCounter().Val() << Endl;
            SimulateSleep(TDuration::Seconds(1));
        }
    }

    template <class TTester>
    void WaitCondition(const TDuration d, const TTester& test) const {
        const TInstant start = TInstant::Now();
        while (TInstant::Now() - start < d) {
            if (test()) {
                Cerr << "condition SUCCESS!!..." << TInstant::Now() - start << Endl;
                return;
            } else {
                Cerr << "waiting condition..." << TInstant::Now() - start << Endl;
                Sleep(TDuration::Seconds(1));
            }
        }
        AFL_VERIFY(false)("reason", "condition not reached");
    }

    void WaitActualization(const TDuration d) const {
        TInstant start = TInstant::Now();
        const i64 startVal = NeedActualizationCount.Val();
        i64 predVal = NeedActualizationCount.Val();
        while (TInstant::Now() - start < d && (!startVal || NeedActualizationCount.Val())) {
            Cerr << "waiting actualization: " << NeedActualizationCount.Val() << "/" << TInstant::Now() - start << Endl;
            if (NeedActualizationCount.Val() != predVal) {
                predVal = NeedActualizationCount.Val();
                start = TInstant::Now();
            }
            SimulateSleep(TDuration::Seconds(1));
        }
        AFL_VERIFY(!NeedActualizationCount.Val());
    }

    virtual void OnIndexSelectProcessed(const std::optional<bool> result) override {
        if (!result) {
            IndexesSkippedNoData.Inc();
        } else if (*result) {
            IndexesApprovedOnSelect.Inc();
        } else {
            IndexesSkippingOnSelect.Inc();
        }
    }
};

} // namespace NKikimr::NYDBTest::NColumnShard
