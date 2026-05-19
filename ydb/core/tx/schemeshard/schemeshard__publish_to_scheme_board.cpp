#include "schemeshard_impl.h"
#include "schemeshard_path_describer.h"

#include <ydb/core/tx/scheme_board/events_schemeshard.h>
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::FLAT_TX_SCHEMESHARD

namespace NKikimr {
namespace NSchemeShard {

using namespace NTabletFlatExecutor;

struct TSchemeShard::TTxPublishToSchemeBoard: public TSchemeShard::TRwTxBase {
    THashMap<TTxId, TDeque<TPathId>> Paths;
    THashMap<TTxId, TVector<THolder<TEvSchemeShard::TEvDescribeSchemeResultBuilder>>> Descriptions;

    TTxPublishToSchemeBoard(TSelf *self, THashMap<TTxId, TDeque<TPathId>>&& paths)
        : TRwTxBase(self)
        , Paths(std::move(paths))
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_DEFERRED_UPDATE_ON_SCHEME_BOARD;
    }

    void DoExecute(TTransactionContext&, const TActorContext& ctx) override {
        YDB_LOG_CTX_INFO(ctx, "TTxPublishToSchemeBoard DoExecute",
            {"at_schemeshard", Self->TabletID()});

        ui32 size = 0;
        while (!Paths.empty() && size++ < Self->PublishChunkSize) {
            auto it = Paths.begin();
            const auto txId = it->first;
            auto& paths = it->second;

            YDB_LOG_CTX_DEBUG(ctx, "TTxPublishToSchemeBoard DescribePath, path",
                {"at_schemeshard", Self->TabletID()},
                {"txId", txId},
                {"id", paths.front()});

            Descriptions[txId].emplace_back(DescribePath(Self, ctx, paths.front()));
            paths.pop_front();

            if (paths.empty()) {
                Paths.erase(it);
            }
        }
    }

    void DoComplete(const TActorContext& ctx) override {
        YDB_LOG_CTX_INFO(ctx, "TTxPublishToSchemeBoard DoComplete",
            {"at_schemeshard", Self->TabletID()});

        for (auto& kv : Descriptions) {
            const auto txId = kv.first;
            auto& descriptions = kv.second;

            for (auto& desc : descriptions) {
                YDB_LOG_CTX_DEBUG(ctx, "TTxPublishToSchemeBoard Send, path",
                    {"to_populator", Self->SchemeBoardPopulator},
                    {"at_schemeshard", Self->TabletID()},
                    {"txId", txId},
                    {"id", desc->Record.GetPathId()});

                ctx.Send(Self->SchemeBoardPopulator, std::move(desc), 0, ui64(txId));
            }
        }

        if (Paths) {
            Self->PublishToSchemeBoard(std::move(Paths), ctx);
        }
    }

}; // TTxPublishToSchemeBoard

struct TSchemeShard::TTxAckPublishToSchemeBoard: public TTransactionBase<TSchemeShard> {
    NSchemeBoard::NSchemeshardEvents::TEvUpdateAck::TPtr Ev;
    TSideEffects SideEffects;

    TTxAckPublishToSchemeBoard(TSelf *self, NSchemeBoard::NSchemeshardEvents::TEvUpdateAck::TPtr& ev)
        : TBase(self)
        , Ev(ev)
    {
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        const auto& record = Ev->Get()->Record;

        YDB_LOG_CTX_DEBUG(ctx, "TTxAckPublishToSchemeBoard Execute",
            {"at_schemeshard", Self->TabletID()},
            {"msg", record.ShortDebugString()},
            {"cookie", Ev->Cookie});

        const auto txId = TTxId(Ev->Cookie);
        const auto pathId = TPathId(record.GetPathOwnerId(), record.GetLocalPathId());
        const ui64 version = record.GetVersion();

        NIceDb::TNiceDb db(txc.DB);

        if (Self->Operations.contains(txId)) {
            YDB_LOG_CTX_INFO(ctx, "Operation in-flight",
                {"at_schemeshard", Self->TabletID()},
                {"txId", txId});

            TOperation::TPtr operation = Self->Operations.at(txId);
            if (AckPublish(db, txId, pathId, version, operation->Publications, ctx)
                    && operation->IsReadyToNotify(ctx)) {
                YDB_LOG_CTX_NOTICE(ctx, "TTxAckPublishToSchemeBoard, operation is ready to notify",
                    {"at_schemeshard", Self->TabletID()},
                    {"txId", txId});

                operation->DoNotify(Self, SideEffects, ctx);
            }

            auto toActivateWaitPublication = operation->ActivatePartsWaitPublication(pathId, version);
            for (auto opId: toActivateWaitPublication) {
                YDB_LOG_CTX_NOTICE(ctx, "TTxAckPublishToSchemeBoard, operation is ready to ack that some awaited paths are published, left await",
                    {"opId", opId},
                    {"publications", operation->CountWaitPublication(opId)},
                    {"at_schemeshard", Self->TabletID()},
                    {"txId", txId});

                THolder<TEvPrivate::TEvCompletePublication> msg = MakeHolder<TEvPrivate::TEvCompletePublication>(opId, pathId, version);
                TEvPrivate::TEvCompletePublication::TPtr personalEv = (TEventHandle<TEvPrivate::TEvCompletePublication>*) new IEventHandle(
                    Self->SelfId(), Self->SelfId(), msg.Release());

                TMemoryChanges memChanges;
                TStorageChanges dbChanges;
                TOperationContext context{Self, txc, ctx, SideEffects, memChanges, dbChanges};

                operation->Parts[opId.GetSubTxId()]->HandleReply(personalEv, context);
            }
        } else if (Self->Publications.contains(txId)) {
            YDB_LOG_CTX_INFO(ctx, "Publication in-flight",
                {"count", Self->Publications.at(txId).Paths.size()},
                {"at_schemeshard", Self->TabletID()},
                {"txId", txId});

            auto& publication = Self->Publications.at(txId);
            if (AckPublish(db, txId, pathId, version, publication.Paths, ctx)) {
                YDB_LOG_CTX_NOTICE(ctx, "Publication complete, notify & remove",
                    {"at_schemeshard", Self->TabletID()},
                    {"txId", txId},
                    {"subscribers", publication.Subscribers.size()});

                Notify(txId, publication.Subscribers, ctx);
                Self->Publications.erase(txId);
            }
        } else {
            YDB_LOG_CTX_WARN(ctx, "Unknown operation & publication",
                {"at_schemeshard", Self->TabletID()},
                {"txId", txId});
        }

        SideEffects.ApplyOnExecute(Self, txc, ctx);
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        YDB_LOG_CTX_DEBUG(ctx, "TTxAckPublishToSchemeBoard Complete",
            {"at_schemeshard", Self->TabletID()},
            {"cookie", Ev->Cookie});

        SideEffects.ApplyOnComplete(Self, ctx);
    }

private:
    bool AckPublish(
        NIceDb::TNiceDb& db,
        TTxId txId, TPathId pathId, ui64 version,
        TSet<std::pair<TPathId, ui64>>& paths,
        const TActorContext& ctx
    ) {
        auto it = paths.lower_bound({pathId, 0});
        while (it != paths.end() && it->first == pathId && it->second <= version) {
            YDB_LOG_CTX_INFO(ctx, "AckPublish",
                {"at_schemeshard", Self->TabletID()},
                {"txId", txId},
                {"pathId", pathId},
                {"version", it->second});

            Self->PersistRemovePublishingPath(db, txId, pathId, it->second);

            auto eraseIt = it;
            ++it;
            paths.erase(eraseIt);
        }

        return paths.empty();
    }

    void Notify(TTxId txId, const THashSet<TActorId>& subscribers, const TActorContext& ctx) {
        for (const auto& subscriber : subscribers) {
            YDB_LOG_CTX_DEBUG(ctx, "TTxAckPublishToSchemeBoard Notify send TEvNotifyTxCompletionResult",
                {"at_schemeshard", Self->TabletID()},
                {"to_actorId", subscriber});

            SideEffects.Send(subscriber, new TEvSchemeShard::TEvNotifyTxCompletionResult(ui64(txId)), ui64(txId));
        }
    }

}; // TTxAckPublishToSchemeBoard

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxPublishToSchemeBoard(THashMap<TTxId, TDeque<TPathId>>&& paths) {
    return new TTxPublishToSchemeBoard(this, std::move(paths));
}

void TSchemeShard::PublishToSchemeBoard(THashMap<TTxId, TDeque<TPathId>>&& paths, const TActorContext& ctx) {
    Execute(CreateTxPublishToSchemeBoard(std::move(paths)), ctx);
}

void TSchemeShard::PublishToSchemeBoard(TTxId txId, TDeque<TPathId>&& paths, const TActorContext& ctx) {
    PublishToSchemeBoard({{txId, std::move(paths)}}, ctx);
}

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxAckPublishToSchemeBoard(NSchemeBoard::NSchemeshardEvents::TEvUpdateAck::TPtr& ev) {
    return new TTxAckPublishToSchemeBoard(this, ev);
}

} // NSchemeShard
} // NKikimr
