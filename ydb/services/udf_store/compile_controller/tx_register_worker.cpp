#include "controller_impl.h"

namespace NKikimr::NUdfStore {

class TWasmCompileController::TTxRegisterWorker : public TTxBase {
public:
    TTxRegisterWorker(TWasmCompileController* self, ui32 nodeId, const TString& cpuSpec, ui32 capacity)
        : TTxBase("TxRegisterWorker", self)
        , NodeId(nodeId)
        , CpuSpec(cpuSpec)
        , Capacity(capacity)
    {}

    TTxType GetTxType() const override {
        return TXTYPE_REGISTER_WORKER;
    }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        NIceDb::TNiceDb db(txc.DB);
        db.Table<Schema::Workers>().Key(NodeId).Update(
            NIceDb::TUpdate<Schema::Workers::CpuSpec>(CpuSpec),
            NIceDb::TUpdate<Schema::Workers::Capacity>(Capacity));
        return true;
    }

    void Complete(const TActorContext&) override {
        Self->StartReconcile(CpuSpec);
        Self->RebuildQueue();
        Self->ScheduleAssignments();
    }

private:
    const ui32 NodeId;
    const TString CpuSpec;
    const ui32 Capacity;
};

void TWasmCompileController::RunTxRegisterWorker(ui32 nodeId, const TString& cpuSpec, ui32 capacity) {
    Execute(new TTxRegisterWorker(this, nodeId, cpuSpec, capacity),
        TActivationContext::AsActorContext());
}

} // namespace NKikimr::NUdfStore
