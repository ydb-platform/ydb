#include "controller_impl.h"

#include <ydb/services/udf_store/service.h>

#include <util/string/cast.h>

namespace NKikimr::NUdfStore {

//! Persists the exclusive rights reserved by the last scheduling round and only
//! then hands them out. An assignment a restart would forget is worse than one
//! that starts a moment later.
class TWasmCompileController::TTxAssign : public TTxBase {
public:
    TTxAssign(TWasmCompileController* self, TVector<TPendingAssign>&& pending)
        : TTxBase("TxAssign", self)
        , Pending(std::move(pending))
    {}

    TTxType GetTxType() const override {
        return TXTYPE_ASSIGN;
    }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        NIceDb::TNiceDb db(txc.DB);

        for (const auto& item : Pending) {
            const auto& key = item.Key;
            db.Table<Schema::Assignments>()
                .Key(key.Name, key.Kind, key.Uid, key.CpuSpec)
                .Update(
                    NIceDb::TUpdate<Schema::Assignments::AssignmentId>(item.Assignment.AssignmentId),
                    NIceDb::TUpdate<Schema::Assignments::NodeId>(item.Assignment.NodeId),
                    NIceDb::TUpdate<Schema::Assignments::DeadlineSeconds>(
                        item.Assignment.Deadline.Seconds()),
                    NIceDb::TUpdate<Schema::Assignments::Generation>(item.Assignment.Generation));
        }

        db.Table<Schema::SysParams>()
            .Key((ui64)Schema::ESysParam::NextAssignmentId)
            .Update(NIceDb::TUpdate<Schema::SysParams::Value>(ToString(Self->NextAssignmentId)));

        return true;
    }

    void Complete(const TActorContext&) override {
        for (auto& item : Pending) {
            if (!item.Event) {
                continue;
            }
            Self->Send(MakeServiceId(item.Assignment.NodeId), item.Event.release());
        }
    }

private:
    TVector<TPendingAssign> Pending;
};

void TWasmCompileController::RunTxAssign(TVector<TPendingAssign>&& pending) {
    Execute(new TTxAssign(this, std::move(pending)), TActivationContext::AsActorContext());
}

} // namespace NKikimr::NUdfStore
