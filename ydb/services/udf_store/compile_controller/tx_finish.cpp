#include "controller_impl.h"

namespace NKikimr::NUdfStore {

//! Applies one batch of assignment and attempt changes: an assignment that
//! ended, an attempt counter that moved, or both.
class TWasmCompileController::TTxFinish : public TTxBase {
public:
    TTxFinish(TWasmCompileController* self, TStateUpdate&& update)
        : TTxBase("TxFinish", self)
        , Update(std::move(update))
    {}

    TTxType GetTxType() const override {
        return TXTYPE_FINISH;
    }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        NIceDb::TNiceDb db(txc.DB);

        for (const auto& key : Update.ErasedAssignments) {
            db.Table<Schema::Assignments>().Key(key.Name, key.Kind, key.Uid, key.CpuSpec).Delete();
        }
        for (const auto& key : Update.ErasedAttempts) {
            db.Table<Schema::Attempts>().Key(key.Name, key.Kind, key.Uid, key.CpuSpec).Delete();
        }
        for (const auto& [key, attempt] : Update.UpdatedAttempts) {
            db.Table<Schema::Attempts>()
                .Key(key.Name, key.Kind, key.Uid, key.CpuSpec)
                .Update(
                    NIceDb::TUpdate<Schema::Attempts::FailCount>(attempt.FailCount),
                    NIceDb::TUpdate<Schema::Attempts::LastError>(attempt.LastError),
                    NIceDb::TUpdate<Schema::Attempts::Poisoned>(attempt.Poisoned));
        }
        for (const ui32 nodeId : Update.ErasedWorkers) {
            db.Table<Schema::Workers>().Key(nodeId).Delete();
        }

        return true;
    }

    void Complete(const TActorContext&) override {
        for (const auto& key : Update.ReadyBroadcasts) {
            Self->BroadcastArtifactReady(key);
        }
    }

private:
    TStateUpdate Update;
};

void TWasmCompileController::RunTxFinish(TStateUpdate&& update) {
    Execute(new TTxFinish(this, std::move(update)), TActivationContext::AsActorContext());
}

} // namespace NKikimr::NUdfStore
