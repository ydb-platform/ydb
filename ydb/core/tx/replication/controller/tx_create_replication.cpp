#include "controller_impl.h"
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::REPLICATION_CONTROLLER

namespace NKikimr::NReplication::NController {

class TController::TTxCreateReplication: public TTxBase {
    TEvController::TEvCreateReplication::TPtr Ev;
    THolder<TEvController::TEvCreateReplicationResult> Result;
    TReplication::TPtr Replication;

public:
    explicit TTxCreateReplication(TController* self, TEvController::TEvCreateReplication::TPtr& ev)
        : TTxBase("TxCreateReplication", self)
        , Ev(ev)
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_CREATE_REPLICATION;
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        YDB_LOG_CTX_DEBUG(ctx, "",
            {"LogPrefix", LogPrefix},
            {"Execute", Ev->Get()->ToString()});

        auto& record = Ev->Get()->Record;
        Result = MakeHolder<TEvController::TEvCreateReplicationResult>();
        Result->Record.MutableOperationId()->CopyFrom(record.GetOperationId());
        Result->Record.SetOrigin(Self->TabletID());

        const auto pathId = TPathId::FromProto(record.GetPathId());
        if (Self->Find(pathId)) {
            YDB_LOG_CTX_WARN(ctx, "Replication already exists",
                {"LogPrefix", LogPrefix},
                {"pathId", pathId});

            Result->Record.SetStatus(NKikimrReplication::TEvCreateReplicationResult::ALREADY_EXISTS);
            return true;
        }

        NIceDb::TNiceDb db(txc.DB);

        const auto rid = Self->SysParams.AllocateReplicationId(db);
        YDB_LOG_CTX_NOTICE(ctx, "Add replication",
            {"LogPrefix", LogPrefix},
            {"rid", rid},
            {"pathId", pathId});

        db.Table<Schema::Replications>().Key(rid).Update(
            NIceDb::TUpdate<Schema::Replications::PathOwnerId>(pathId.OwnerId),
            NIceDb::TUpdate<Schema::Replications::PathLocalId>(pathId.LocalPathId),
            NIceDb::TUpdate<Schema::Replications::Config>(record.GetConfig().SerializeAsString()),
            NIceDb::TUpdate<Schema::Replications::Database>(record.GetDatabase())
        );
        if (record.HasLocation()) {
            record.MutableConfig()->MutableLocation()->CopyFrom(record.GetLocation());
        }
        Replication = Self->Add(rid, pathId, std::move(*record.MutableConfig()), std::move(record.GetDatabase()));

        Result->Record.SetStatus(NKikimrReplication::TEvCreateReplicationResult::SUCCESS);
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        YDB_LOG_CTX_DEBUG(ctx, "Complete",
            {"LogPrefix", LogPrefix});

        if (Result) {
            ctx.Send(Ev->Sender, Result.Release(), 0, Ev->Cookie);
        }

        if (Replication) {
            const auto& tenant = Replication->GetDatabase();
            Y_ABORT_UNLESS(tenant);
            if (!Self->NodesManager.HasTenant(tenant)) {
                YDB_LOG_CTX_INFO(ctx, "Discover tenant nodes:",
                    {"LogPrefix", LogPrefix},
                    {"tenant", tenant});
                Self->NodesManager.DiscoverNodes(tenant, Self->DiscoveryCache, ctx);
            }

            Replication->Progress(ctx);
        }
    }

}; // TTxCreateReplication

void TController::RunTxCreateReplication(TEvController::TEvCreateReplication::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxCreateReplication(this, ev), ctx);
}

}
