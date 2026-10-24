#include <ydb/core/tablet_flat/flat_executor_ut_common.h>
#include <ydb/core/base/memory_controller_iface.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <atomic>

namespace NKikimr::NTabletFlatExecutor {
namespace {

    constexpr ui32 TableId = 101;
    constexpr ui32 KeyTag = 1;
    constexpr ui32 ValueTag = 2;
    constexpr ui64 MiB = 1024 * 1024;

    class TMemoryConsumer : public NMemory::IMemoryConsumer {
    public:
        void SetReport(NMemory::TConsumerReport report) override {
            Consumption = report.Used;
            Peak = Max(Peak.load(), report.Used);
            ++Reports;
        }

        std::atomic<ui64> Consumption = 0;
        std::atomic<ui64> Peak = 0;
        std::atomic<ui32> Reports = 0;
        std::atomic<ui32> Compactions = 0;
    };

    class TMemoryController : public TActorBootstrapped<TMemoryController> {
    public:
        explicit TMemoryController(TIntrusivePtr<TMemoryConsumer> consumer)
            : Consumer(std::move(consumer))
        {}

        void Bootstrap() {
            Become(&TThis::StateWork);
        }

        STFUNC(StateWork) {
            switch (ev->GetTypeRewrite()) {
                hFunc(NMemory::TEvMemTableRegister, Handle);
                hFunc(NMemory::TEvMemTableCompacted, Handle);
                hFunc(NMemory::TEvMemTableUnregister, Handle);
                hFunc(TEvents::TEvWakeup, Handle);
                hFunc(TEvents::TEvPoison, Handle);
            }
        }

    private:
        void Handle(NMemory::TEvMemTableRegister::TPtr& ev) {
            Send(ev->Sender, new NMemory::TEvMemTableRegistered(ev->Get()->Table, Consumer));
        }

        void Handle(NMemory::TEvMemTableCompacted::TPtr&) {
            ++Consumer->Compactions;
        }

        void Handle(NMemory::TEvMemTableUnregister::TPtr&) {}

        void Handle(TEvents::TEvWakeup::TPtr& ev) {
            Send(ev->Sender, new TEvents::TEvWakeup);
        }

        void Handle(TEvents::TEvPoison::TPtr& ev) {
            Send(ev->Sender, new TEvents::TEvGone);
            PassAway();
        }

        const TIntrusivePtr<TMemoryConsumer> Consumer;
    };

    void WriteString(TTransactionContext& txc, i64 id, const TString& value) {
        const auto key = NScheme::TInt64::TInstance(id);
        const auto cell = NScheme::TString::TInstance(value);
        const NTable::TUpdateOp op{ ValueTag, NTable::ECellOp::Set, cell };
        txc.DB.Update(TableId, NTable::ERowOp::Upsert, { key }, { op });
    }

    class TTxSchema : public ITransaction {
    public:
        explicit TTxSchema(ui64 threshold)
            : Threshold(threshold)
        {}

        bool Execute(TTransactionContext& txc, const TActorContext&) override {
            TCompactionPolicy policy;
            policy.InMemForceSizeToSnapshot = Threshold;
            policy.InMemSizeToSnapshot = Max<ui64>();
            policy.InMemStepsToSnapshot = Max<ui32>();
            policy.InMemForceStepsToSnapshot = Max<ui32>();
            policy.BackgroundSnapshotPolicy.Threshold = Max<ui32>();
            txc.DB.Alter()
                .AddTable("test", TableId)
                .AddColumn(TableId, "key", KeyTag, NScheme::TInt64::TypeId, false, false)
                .AddColumn(TableId, "value", ValueTag, NScheme::TString::TypeId, false, false)
                .AddColumnToKey(TableId, KeyTag)
                .SetCompactionPolicy(TableId, policy);
            WriteString(txc, 0, "seed");
            return true;
        }

        void Complete(const TActorContext& ctx) override {
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        }

    private:
        const ui64 Threshold;
    };

    struct TMemoryStats {
        ui64 Used = 0;
        ui64 Allocated = 0;
        ui64 Rows = 0;
        std::atomic<bool> Finished = false;
    };

    enum class EWriteResult {
        Commit,
        Reschedule,
        RollbackChanges,
        RepeatedRollbackChanges,
    };

    class TTxWrite : public ITransaction {
    public:
        TTxWrite(TMemoryStats& stats, ui32 rows, EWriteResult result)
            : Stats(stats)
            , Rows(rows)
            , Result(result)
        {}

        bool Execute(TTransactionContext& txc, const TActorContext&) override {
            if (++Attempt == 1) {
                const TString value(8 * MiB, 'x');
                for (ui32 row = 1; row <= Rows; ++row) {
                    WriteString(txc, row, value);
                }
                if (Result == EWriteResult::Reschedule) {
                    txc.Reschedule();
                    return false;
                }
                if (Result == EWriteResult::RollbackChanges || Result == EWriteResult::RepeatedRollbackChanges) {
                    txc.DB.RollbackChanges();
                }
                if (Result == EWriteResult::RepeatedRollbackChanges) {
                    WriteString(txc, 1, value);
                    txc.DB.RollbackChanges();
                }
            }
            Stats.Used = txc.DB.GetTableMemSize(TableId);
            Stats.Allocated = txc.DB.GetTableMemAllocatedSize(TableId);
            Stats.Rows = txc.DB.GetTableMemRowCount(TableId);
            return true;
        }

        void Complete(const TActorContext&) override {
            Stats.Finished = true;
        }

    private:
        TMemoryStats& Stats;
        const ui32 Rows;
        const EWriteResult Result;
        ui32 Attempt = 0;
    };

    struct TMemoryEnv : TMyEnvBase {
        explicit TMemoryEnv(ui64 threshold)
            : Consumer(new TMemoryConsumer)
        {
            RunOn(2, NMemory::MakeMemoryControllerId(), new TMemoryController(Consumer), NFake::EMail::Simple);
            FireDummyTablet(ui32(NFake::TDummy::EFlg::Comp));
            SendSync(new NFake::TEvExecute{ new TTxSchema(threshold) });
            Env.WaitFor("memtable consumer registration", [&] {
                return Consumer->Consumption.load() > 0;
            });
        }

        void CheckCompactionFinished() {
            WaitFor<NFake::TEvCompacted>();
            // Fence the controller mailbox so an extra rollback acknowledgement cannot remain queued
            Env.SingleSys()->Send(new IEventHandle(NMemory::MakeMemoryControllerId(), Edge, new TEvents::TEvWakeup));
            WaitForWakeUp();
            UNIT_ASSERT_VALUES_EQUAL(Consumer->Consumption.load(), 0);
            UNIT_ASSERT_VALUES_EQUAL(Consumer->Compactions.load(), 1);
        }

        const TIntrusivePtr<TMemoryConsumer> Consumer;
    };

    void CheckRollback(EWriteResult result) {
        TMemoryEnv env(2 * MiB);
        TMemoryStats stats;
        ui64 consumption = 0;
        ui32 reports = 0;
        float rejection = 0;
        env.SendSync(new NFake::TEvCall{ [&](auto* executor, const auto& ctx) {
            const ui32 before = env.Consumer->Reports;
            executor->Execute(new TTxWrite(stats, 1, result), ctx);
            reports = env.Consumer->Reports - before;
            consumption = env.Consumer->Consumption;
            rejection = executor->GetRejectProbability();
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        } });

        UNIT_ASSERT_C(consumption >= 8 * MiB, consumption);
        UNIT_ASSERT_VALUES_EQUAL(reports, 1);
        UNIT_ASSERT_VALUES_EQUAL(rejection, 1.0f);
        env.Env.WaitFor("write transaction completion", [&] {
            return stats.Finished.load();
        });
        UNIT_ASSERT_VALUES_EQUAL(stats.Rows, 1);
        UNIT_ASSERT_C(stats.Used < MiB, stats.Used);
        env.CheckCompactionFinished();
    }

} // namespace

Y_UNIT_TEST_SUITE(TFlatTableExecutor_MemTableMemory) {
    Y_UNIT_TEST(CompactionUsesAllocatedCapacity) {
        TMemoryEnv env(9 * MiB);
        TMemoryStats stats;
        env.SendSync(new NFake::TEvCall{ [&](auto* executor, const auto& ctx) {
            executor->Execute(new TTxWrite(stats, 1, EWriteResult::Commit), ctx);
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        } });
        UNIT_ASSERT_C(stats.Used < 9 * MiB, stats.Used);
        UNIT_ASSERT_C(stats.Allocated > 9 * MiB, stats.Allocated);
        UNIT_ASSERT_VALUES_EQUAL(env.Consumer->Peak.load(), stats.Allocated);
        env.Env.WaitFor("write transaction completion", [&] {
            return stats.Finished.load();
        });
        env.CheckCompactionFinished();
    }

    Y_UNIT_TEST(OverloadUsesAllocatedCapacity) {
        TMemoryEnv env(8 * MiB);
        TMemoryStats stats;
        float rejection = 0;
        env.SendSync(new NFake::TEvCall{ [&](auto* executor, const auto& ctx) {
            executor->Execute(new TTxWrite(stats, 2, EWriteResult::Commit), ctx);
            rejection = executor->GetRejectProbability();
            ctx.Send(ctx.SelfID, new NFake::TEvReturn);
        } });
        UNIT_ASSERT_C(stats.Used < 20 * MiB, stats.Used);
        UNIT_ASSERT_C(stats.Allocated > 20 * MiB, stats.Allocated);
        UNIT_ASSERT_C(rejection > 0, rejection);
        UNIT_ASSERT_VALUES_EQUAL(env.Consumer->Peak.load(), stats.Allocated);
        env.Env.WaitFor("write transaction completion", [&] {
            return stats.Finished.load();
        });
        env.CheckCompactionFinished();
    }

    Y_UNIT_TEST(RollbackRefreshesAllocatedCapacity) {
        CheckRollback(EWriteResult::Reschedule);
    }

    Y_UNIT_TEST(ExplicitRollbackRefreshesAllocatedCapacity) {
        CheckRollback(EWriteResult::RollbackChanges);
    }

    Y_UNIT_TEST(RepeatedExplicitRollbackRefreshesAllocatedCapacityOnce) {
        CheckRollback(EWriteResult::RepeatedRollbackChanges);
    }
}

} // namespace NKikimr::NTabletFlatExecutor
