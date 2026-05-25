#include "controller_impl.h"

#include <util/generic/guid.h>
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::REPLICATION_CONTROLLER

namespace NKikimr::NReplication::NController {

extern const TString ReplicationConsumerName;

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
        YDB_LOG_CTX_DEBUG(ctx, "",
            {"LogPrefix", LogPrefix},
            {"Execute", Ev->Get()->ToString()});

        const auto rid = Ev->Get()->ReplicationId;
        const auto tid = Ev->Get()->TargetId;

        Replication = Self->Find(rid);
        if (!Replication) {
            YDB_LOG_CTX_WARN(ctx, "Unknown replication",
                {"LogPrefix", LogPrefix},
                {"rid", rid});
            return true;
        }

        auto* target = Replication->FindTarget(tid);
        if (!target) {
            YDB_LOG_CTX_WARN(ctx, "Unknown target",
                {"LogPrefix", LogPrefix},
                {"rid", rid},
                {"tid", tid});
            return true;
        }

        if (!target->GetStreamName().empty()) {
            YDB_LOG_CTX_WARN(ctx, "Stream name already assigned",
                {"LogPrefix", LogPrefix},
                {"rid", rid},
                {"tid", tid});
            return true;
        }

        target->SetStreamName(CreateGuidAsString());

        TString consumerName;
        if (Replication->GetConfig().HasTransferSpecific()) {
            auto& target = Replication->GetConfig().GetTransferSpecific().GetTarget();
            if (target.HasConsumerName()) {
                consumerName = target.GetConsumerName();
            } else {
                consumerName = CreateGuidAsString();
            }
        } else {
            consumerName = ReplicationConsumerName;
        }
        target->SetStreamConsumerName(consumerName);

        NIceDb::TNiceDb db(txc.DB);
        db.Table<Schema::SrcStreams>().Key(rid, tid).Update(
            NIceDb::TUpdate<Schema::SrcStreams::Name>(target->GetStreamName()),
            NIceDb::TUpdate<Schema::SrcStreams::ConsumerName>(target->GetStreamConsumerName())
        );

        YDB_LOG_CTX_NOTICE(ctx, "Stream name assigned",
            {"LogPrefix", LogPrefix},
            {"rid", rid},
            {"tid", tid},
            {"name", target->GetStreamName()});

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        YDB_LOG_CTX_DEBUG(ctx, "Complete",
            {"LogPrefix", LogPrefix});

        if (Replication) {
            Replication->Progress(ctx);
        }
    }

}; // TTxAssignStreamName

void TController::RunTxAssignStreamName(TEvPrivate::TEvAssignStreamName::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxAssignStreamName(this, ev), ctx);
}

}
