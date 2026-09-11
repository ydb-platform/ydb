#include "controller_impl.h"

#include <util/string/cast.h>

namespace NKikimr::NUdfStore {

class TWasmCompileController::TTxInit : public TTxBase {
public:
    explicit TTxInit(TWasmCompileController* self)
        : TTxBase("TxInit", self)
    {}

    TTxType GetTxType() const override {
        return TXTYPE_INIT;
    }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        NIceDb::TNiceDb db(txc.DB);

        auto sysParams = db.Table<Schema::SysParams>().Range().Select();
        auto workers = db.Table<Schema::Workers>().Range().Select();
        auto assignments = db.Table<Schema::Assignments>().Range().Select();
        auto attempts = db.Table<Schema::Attempts>().Range().Select();
        if (!sysParams.IsReady() || !workers.IsReady()
            || !assignments.IsReady() || !attempts.IsReady())
        {
            return false;
        }

        Self->NextAssignmentId = 1;
        while (!sysParams.EndOfSet()) {
            const auto id = sysParams.GetValue<Schema::SysParams::Id>();
            const auto value = sysParams.GetValue<Schema::SysParams::Value>();
            if (id == (ui64)Schema::ESysParam::NextAssignmentId) {
                Self->NextAssignmentId = FromStringWithDefault<ui64>(value, 1);
            }
            if (!sysParams.Next()) {
                return false;
            }
        }

        // Restored workers are not alive: nothing has heartbeated to this
        // leader yet. They are kept so that reconcile knows which platforms
        // this tenant has before anybody reconnects.
        Self->Workers.clear();
        while (!workers.EndOfSet()) {
            TWorkerState worker;
            worker.NodeId = workers.GetValue<Schema::Workers::NodeId>();
            worker.CpuSpec = workers.GetValue<Schema::Workers::CpuSpec>();
            worker.Capacity = Max<ui32>(1, workers.GetValue<Schema::Workers::Capacity>());
            worker.Alive = false;
            Self->Workers[worker.NodeId] = std::move(worker);
            if (!workers.Next()) {
                return false;
            }
        }

        // Assignments outlive a leader change so that a worker re-confirming an
        // id in its heartbeat is matched against what was actually handed out,
        // instead of the same gap being compiled twice.
        Self->Assignments.clear();
        while (!assignments.EndOfSet()) {
            TGapKey key{
                .Name = assignments.GetValue<Schema::Assignments::Name>(),
                .Kind = assignments.GetValue<Schema::Assignments::Kind>(),
                .Uid = assignments.GetValue<Schema::Assignments::Uid>(),
                .CpuSpec = assignments.GetValue<Schema::Assignments::CpuSpec>(),
            };
            TAssignment assignment{
                .AssignmentId = assignments.GetValue<Schema::Assignments::AssignmentId>(),
                .NodeId = assignments.GetValue<Schema::Assignments::NodeId>(),
                .Deadline = TInstant::Seconds(
                    assignments.GetValue<Schema::Assignments::DeadlineSeconds>()),
                .Generation = assignments.GetValue<Schema::Assignments::Generation>(),
            };
            Self->NextAssignmentId = Max(Self->NextAssignmentId, assignment.AssignmentId + 1);
            Self->Assignments[std::move(key)] = assignment;
            if (!assignments.Next()) {
                return false;
            }
        }

        Self->Attempts.clear();
        while (!attempts.EndOfSet()) {
            TGapKey key{
                .Name = attempts.GetValue<Schema::Attempts::Name>(),
                .Kind = attempts.GetValue<Schema::Attempts::Kind>(),
                .Uid = attempts.GetValue<Schema::Attempts::Uid>(),
                .CpuSpec = attempts.GetValue<Schema::Attempts::CpuSpec>(),
            };
            TAttemptState attempt{
                .FailCount = attempts.GetValue<Schema::Attempts::FailCount>(),
                .LastError = attempts.GetValue<Schema::Attempts::LastError>(),
                .Poisoned = attempts.GetValue<Schema::Attempts::Poisoned>(),
            };
            Self->Attempts[std::move(key)] = std::move(attempt);
            if (!attempts.Next()) {
                return false;
            }
        }

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        Self->SwitchToWork(ctx);
    }
};

void TWasmCompileController::RunTxInit(const TActorContext& ctx) {
    Execute(new TTxInit(this), ctx);
}

} // namespace NKikimr::NUdfStore
