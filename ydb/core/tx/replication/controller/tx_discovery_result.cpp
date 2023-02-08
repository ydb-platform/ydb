#include "controller_impl.h"
#include "util.h"

#include <util/string/join.h>

namespace NKikimr::NReplication::NController {

class TController::TTxDiscoveryResult: public TTxBase {
    TEvPrivate::TEvDiscoveryResult::TPtr Ev;
    TReplication::TPtr Replication;

public:
    explicit TTxDiscoveryResult(TController* self, TEvPrivate::TEvDiscoveryResult::TPtr& ev)
        : TTxBase("TxDiscovertResult", self)
        , Ev(ev)
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_DISCOVERY_RESULT;
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        CLOG_D(ctx, "Execute: " << Ev->Get()->ToString());

        const auto rid = Ev->Get()->ReplicationId;

        Replication = Self->Find(rid);
        if (!Replication) {
            CLOG_W(ctx, "Unknown replication"
                << ": rid# " << rid);
            return true;
        }

        NIceDb::TNiceDb db(txc.DB);

        if (Ev->Get()->IsSuccess()) {
            for (const auto& target : Ev->Get()->ToAdd) {
                const auto kind = TargetKindFromEntryType(target.first.Type);
                const auto& srcPath = target.first.Name;
                const auto& dstPath = target.second;

                const auto tid = Replication->AddTarget(kind, srcPath, dstPath);
                db.Table<Schema::Targets>().Key(rid, tid).Update(
                    NIceDb::TUpdate<Schema::Targets::Kind>(kind),
                    NIceDb::TUpdate<Schema::Targets::SrcPath>(srcPath),
                    NIceDb::TUpdate<Schema::Targets::DstPath>(dstPath)
                );

                CLOG_N(ctx, "Add target"
                    << ": rid# " << rid
                    << ", tid# " << tid
                    << ", kind# " << kind
                    << ", srcPath# " << srcPath
                    << ", dstPath# " << dstPath);
            }
        } else {
            const auto error = JoinSeq(", ", Ev->Get()->Failed);
            Replication->SetState(TReplication::EState::Error, TStringBuilder() << "Discovery error: " << error);

            CLOG_E(ctx, "Discovery error"
                << ": rid# " << rid
                << ", error# " << error);
        }

        db.Table<Schema::Replications>().Key(Replication->GetId()).Update(
            NIceDb::TUpdate<Schema::Replications::State>(Replication->GetState()),
            NIceDb::TUpdate<Schema::Replications::Issue>(Replication->GetIssue()),
            NIceDb::TUpdate<Schema::Replications::NextTargetId>(Replication->GetNextTargetId())
        );

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        CLOG_D(ctx, "Complete");

        if (Replication) {
            Replication->Progress(ctx);
        }
    }

}; // TTxDiscoveryResult

void TController::RunTxDiscoveryResult(TEvPrivate::TEvDiscoveryResult::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxDiscoveryResult(this, ev), ctx);
}

}
