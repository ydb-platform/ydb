#include "controller_impl.h"

#include <util/generic/guid.h>

namespace NKikimr::NReplication::NController {

class TController::TTxAssignStreamName: public TTxBase {
    TEvPrivate::TEvAssignStreamName::TPtr Ev;
    TReplication::TPtr Replication;

public:
    explicit TTxAssignStreamName(TController* self, TEvPrivate::TEvAssignStreamName::TPtr& ev)
        : TTxBase("TxAssignStreamName", self)
        , Ev(ev)
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_ASSIGN_STREAM_NAME;
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        CLOG_D(ctx, "Execute: " << Ev->Get()->ToString());

        const auto rid = Ev->Get()->ReplicationId;
        const auto tid = Ev->Get()->TargetId;

        Replication = Self->Find(rid);
        if (!Replication) {
            CLOG_W(ctx, "Unknown replication"
                << ": rid# " << rid);
            return true;
        }

        auto* target = Replication->FindTarget(tid);
        if (!target) {
            CLOG_W(ctx, "Unknown target"
                << ": rid# " << rid
                << ", tid# " << tid);
            return true;
        }

        if (!target->GetStreamName().empty()) {
            CLOG_W(ctx, "Stream name already assigned"
                << ": rid# " << rid
                << ", tid# " << tid);
            return true;
        }

        target->SetStreamName(CreateGuidAsString());

        NIceDb::TNiceDb db(txc.DB);
        db.Table<Schema::SrcStreams>().Key(rid, tid).Update(
            NIceDb::TUpdate<Schema::SrcStreams::Name>(target->GetStreamName())
        );

        CLOG_N(ctx, "Stream name assigned"
            << ": rid# " << rid
            << ", tid# " << tid
            << ", name# " << target->GetStreamName());

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        CLOG_D(ctx, "Complete");

        if (Replication) {
            Replication->Progress(ctx);
        }
    }

}; // TTxAssignStreamName

void TController::RunTxAssignStreamName(TEvPrivate::TEvAssignStreamName::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxAssignStreamName(this, ev), ctx);
}

}
