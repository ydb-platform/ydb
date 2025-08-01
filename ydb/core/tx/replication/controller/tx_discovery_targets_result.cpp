#include "controller_impl.h"
#include "target_transfer.h"

#include <util/string/join.h>

namespace NKikimr::NReplication::NController {

class TController::TTxDiscoveryTargetsResult: public TTxBase {
    TEvPrivate::TEvDiscoveryTargetsResult::TPtr Ev;
    TReplication::TPtr Replication;

public:
    explicit TTxDiscoveryTargetsResult(TController* self, TEvPrivate::TEvDiscoveryTargetsResult::TPtr& ev)
        : TTxBase("TxDiscoveryTargetsResult", self)
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

        if (Replication->GetState() != TReplication::EState::Ready) {
            CLOG_W(ctx, "Replication state mismatch"
                << ": rid# " << rid
                << ", state# " << Replication->GetState());
            return true;
        }

        NIceDb::TNiceDb db(txc.DB);

        if (Ev->Get()->IsSuccess()) {
            for (const auto& target : Ev->Get()->ToAdd) {
                const auto tid = Replication->AddTarget(target.Kind, target.Config);
                
                TString transformLambda;
                TString runAsUser;
                TString directoryPath;
                if (auto p = std::dynamic_pointer_cast<const TTargetTransfer::TTransferConfig>(target.Config)) {
                    transformLambda = p->GetTransformLambda();
                    runAsUser = p->GetRunAsUser();
                    directoryPath = p->GetDirectoryPath();
                }

                db.Table<Schema::Targets>().Key(rid, tid).Update(
                    NIceDb::TUpdate<Schema::Targets::Kind>(target.Kind),
                    NIceDb::TUpdate<Schema::Targets::SrcPath>(target.Config->GetSrcPath()),
                    NIceDb::TUpdate<Schema::Targets::DstPath>(target.Config->GetDstPath()),
                    NIceDb::TUpdate<Schema::Targets::TransformLambda>(transformLambda),
                    NIceDb::TUpdate<Schema::Targets::RunAsUser>(runAsUser),
                    NIceDb::TUpdate<Schema::Targets::DirectoryPath>(directoryPath)
                );

                CLOG_N(ctx, "Add target"
                    << ": rid# " << rid
                    << ", tid# " << tid
                    << ", kind# " << target.Kind
                    << ", srcPath# " << target.Config->GetSrcPath()
                    << ", dstPath# " << target.Config->GetDstPath());
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

}; // TTxDiscoveryTargetsResult

void TController::RunTxDiscoveryTargetsResult(TEvPrivate::TEvDiscoveryTargetsResult::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxDiscoveryTargetsResult(this, ev), ctx);
}

}
