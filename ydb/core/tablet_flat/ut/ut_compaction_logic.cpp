#include "flat_comp_ut_common.h"

#include <ydb/core/tablet_flat/flat_executor_compaction_logic.h>
#include <ydb/core/tablet_flat/tablet_flat_executor.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NTabletFlatExecutor {
namespace {

    using namespace NTable;

    class TDeferredStrategy final : public ICompactionStrategy {
    public:
        void Start(TCompactionState) override { }
        void Stop() override { }
        void ReflectSchema() override { }
        void ReflectRemovedRowVersions() override { }
        void UpdateCompactions() override { }
        ui64 GetBackingSize() override { return 0; }
        ui64 GetBackingSize(ui64) override { return 0; }
        ui64 GetLastFinishedForcedCompactionId() const override { return FinishedForcedId; }
        TInstant GetLastFinishedForcedCompactionTs() const override { return {}; }
        bool AllowForcedCompaction() override { return true; }
        void PartMerged(TPartView, ui32) override { }
        void PartMerged(TIntrusiveConstPtr<TColdPart>, ui32) override { }
        TCompactionChanges PartsRemoved(TArrayRef<const TLogoBlobID>) override { return {}; }
        TCompactionState SnapshotState() override { return {}; }
        TCompactionChanges ApplyChanges() override { return {}; }
        void OutputHtml(IOutputStream&) override { }

        ui64 BeginMemCompaction(TTaskId taskId, TSnapEdge edge, ui64 forcedId) override {
            ForcedAttempts.push_back(forcedId);
            if (!Accept) {
                return 0;
            }
            Params = MakeHolder<TCompactionParams>();
            Params->Table = 1;
            Params->TaskId = taskId;
            Params->Edge = edge;
            return ScanId;
        }

        TCompactionChanges CompactionFinished(ui64 id, THolder<TCompactionParams>,
                THolder<TCompactionResult>) override {
            UNIT_ASSERT_VALUES_EQUAL(id, ScanId);
            FinishedForcedId = ForcedAttempts.back();
            return {};
        }

        static constexpr ui64 ScanId = 73;
        bool Accept = false;
        TVector<ui64> ForcedAttempts;
        THolder<TCompactionParams> Params;
        ui64 FinishedForcedId = 0;
    };

    class TConsumers final : public IMemTableMemoryConsumersCollection {
    public:
        void Register(ui32) override { }
        void Unregister(ui32) override { }
        void CompactionComplete(TIntrusivePtr<NMemory::IMemoryConsumer>) override { }
    };

    class TSnapshot final : public TTableSnapshotContext {
    public:
        TConstArrayRef<ui32> TablesToSnapshot() const override { return Tables; }
    private:
        const TVector<ui32> Tables = {1};
    };

    struct TFixture {
        NTable::NTest::TSimpleBackend Backend;
        NTable::NTest::TSimpleBroker Broker;
        NTable::NTest::TSimpleLogger Logger;
        TConsumers Consumers;
        TCompactionLogicState* State;
        TDeferredStrategy* Strategy;
        THolder<TCompactionLogic> Logic;

        TFixture() {
            TAutoPtr<TCompactionLogicState> state = new TCompactionLogicState;
            State = state.Get();
            auto& table = State->Tables[1];
            table.TableId = 1;
            table.Policy = NLocalDb::CreateDefaultTablePolicy();
            table.StrategyType = NKikimrCompaction::CompactionStrategyGenerational;
            Strategy = new TDeferredStrategy;
            table.Strategy.Reset(Strategy);
            table.InMem.Steps = 7;
            Logic = MakeHolder<TCompactionLogic>(&Consumers, &Logger, &Broker,
                &Backend, std::move(state));
        }

        ~TFixture() {
            Logic->Stop();
        }

        TCompactionLogicState::TTableInfo& Table() { return State->Tables.at(1); }

        void Wake() {
            Logic->RequestMemCompactionRetry(1);
            Logic->ApplyChanges();
        }

        TTableCompactionResult Complete() {
            Broker.FinishTask(Table().InMem.CompactionTask.TaskId, EResourceStatus::Finished);
            return Logic->CompleteCompaction(TDeferredStrategy::ScanId,
                std::move(Strategy->Params), MakeHolder<TCompactionResult>(TEpoch::Zero(), size_t(0)));
        }
    };
}

Y_UNIT_TEST_SUITE(TDeferredMemoryCompaction) {

    Y_UNIT_TEST(ConflictRetainsStepsAndForcedIdUntilExplicitWakeup) {
        TFixture f;
        const ui64 forcedId = f.Logic->PrepareForceCompaction(1);
        UNIT_ASSERT(f.Broker.RunPending());
        UNIT_ASSERT(!f.Broker.HasPending());
        UNIT_ASSERT(!f.Broker.HasRunning());
        UNIT_ASSERT(f.Table().InMem.State == ECompactionState::Compaction);
        UNIT_ASSERT_VALUES_EQUAL(f.Table().InMem.CompactionTask.TaskId, 0);
        UNIT_ASSERT_VALUES_EQUAL(f.Table().InMem.Steps, 7);
        UNIT_ASSERT_VALUES_EQUAL(f.Strategy->GetLastFinishedForcedCompactionId(), 0);

        f.Logic->UpdateInMemStatsStep(1, 3, 0);
        f.Logic->UpdateCompactions();
        UNIT_ASSERT(!f.Broker.HasPending());
        UNIT_ASSERT_VALUES_EQUAL(f.Strategy->ForcedAttempts.size(), 1);

        f.Strategy->Accept = true;
        f.Wake();
        UNIT_ASSERT(f.Table().ForcedCompactionState == EForcedCompactionState::PendingMem);
        UNIT_ASSERT(f.Broker.RunPending());
        UNIT_ASSERT_VALUES_EQUAL(f.Strategy->ForcedAttempts.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(f.Strategy->ForcedAttempts[0], forcedId);
        UNIT_ASSERT_VALUES_EQUAL(f.Strategy->ForcedAttempts[1], forcedId);
        const auto result = f.Complete();
        UNIT_ASSERT(result.MemCompacted);
        UNIT_ASSERT_VALUES_EQUAL(f.Table().InMem.Steps, 0);
        UNIT_ASSERT_VALUES_EQUAL(f.Strategy->GetLastFinishedForcedCompactionId(), forcedId);
    }

    Y_UNIT_TEST(ConflictRetainsTheSnapshotRequestAndItsOriginalEdge) {
        TFixture f;
        const TSnapEdge edge{TTxStamp(1, 1), TEpoch::FromIndex(5)};
        TIntrusivePtr<TSnapshot> context = new TSnapshot;
        f.Logic->PrepareTableSnapshot(1, edge, context.Get());
        UNIT_ASSERT(f.Broker.RunPending());
        UNIT_ASSERT(f.Table().InMem.State == ECompactionState::SnapshotCompaction);
        UNIT_ASSERT_VALUES_EQUAL(f.Table().SnapRequests.size(), 1);
        UNIT_ASSERT(f.Table().SnapRequests.front().Context.Get() == context.Get());
        UNIT_ASSERT(!f.Broker.HasPending());

        f.Strategy->Accept = true;
        f.Wake();
        UNIT_ASSERT(f.Broker.RunPending());
        UNIT_ASSERT(f.Strategy->Params->Edge == edge);
        const auto result = f.Complete();
        UNIT_ASSERT_VALUES_EQUAL(result.CompleteSnapshots.size(), 1);
        UNIT_ASSERT(result.CompleteSnapshots.front().Get() == context.Get());
        UNIT_ASSERT(f.Table().SnapRequests.empty());
        UNIT_ASSERT_VALUES_EQUAL(f.Table().InMem.Steps, 0);
    }

    Y_UNIT_TEST(WakeupDoesNotDuplicateARunningScanOrResurrectADroppedTable) {
        TFixture f;
        f.Strategy->Accept = true;
        f.Logic->PrepareForceCompaction(1);
        UNIT_ASSERT(f.Broker.RunPending());
        f.Wake();
        UNIT_ASSERT(!f.Broker.HasPending());
        UNIT_ASSERT_VALUES_EQUAL(f.Strategy->ForcedAttempts.size(), 1);
        f.Complete();
        f.Logic->Stop();
        f.Wake();
        UNIT_ASSERT(!f.Broker.HasPending());
        UNIT_ASSERT(f.State->Tables.empty());
    }
}
}
