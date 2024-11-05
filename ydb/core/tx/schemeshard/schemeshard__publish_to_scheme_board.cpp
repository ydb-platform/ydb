#include <ydb/core/tx/scheme_board/events_schemeshard.h>

#include "schemeshard_impl.h"
#include "schemeshard_path_describer.h"

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
        LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "TTxPublishToSchemeBoard DoExecute"
                       << ", at schemeshard: " << Self->TabletID());

        ui32 size = 0;
        while (!Paths.empty() && size++ < Self->PublishChunkSize) {
            auto it = Paths.begin();
            const auto txId = it->first;
            auto& paths = it->second;

            LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        "TTxPublishToSchemeBoard DescribePath"
                            << ", at schemeshard: " << Self->TabletID()
                            << ", txId: " << txId
                            << ", path id: " << paths.front());

            Descriptions[txId].emplace_back(DescribePath(Self, ctx, paths.front()));
            paths.pop_front();

            if (paths.empty()) {
                Paths.erase(it);
            }
        }
    }

    void DoComplete(const TActorContext& ctx) override {
        LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "TTxPublishToSchemeBoard DoComplete"
                       << ", at schemeshard: " << Self->TabletID());

        for (auto& kv : Descriptions) {
            const auto txId = kv.first;
            auto& descriptions = kv.second;

            for (auto& desc : descriptions) {
                LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                            "TTxPublishToSchemeBoard Send"
                                << ", to populator: " << Self->SchemeBoardPopulator
                                << ", at schemeshard: " << Self->TabletID()
                                << ", txId: " << txId
                                << ", path id: " << desc->Record.GetPathId());

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

        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "TTxAckPublishToSchemeBoard Execute"
                       << ", at schemeshard: " << Self->TabletID()
                       << ", msg: " << record.ShortDebugString()
                       << ", cookie: " << Ev->Cookie);

        const auto txId = TTxId(Ev->Cookie);
        const auto pathId = TPathId(record.GetPathOwnerId(), record.GetLocalPathId());
        const ui64 version = record.GetVersion();

        NIceDb::TNiceDb db(txc.DB);

        if (Self->Operations.contains(txId)) {
            LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                       "Operation in-flight"
                           << ", at schemeshard: " << Self->TabletID()
                           << ", txId: " << txId);

            TOperation::TPtr operation = Self->Operations.at(txId);
            if (AckPublish(db, txId, pathId, version, operation->Publications, ctx)
                    && operation->IsReadyToNotify(ctx)) {
                LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                           "TTxAckPublishToSchemeBoard"
                               << ", operation is ready to notify"
                               << ", at schemeshard: " << Self->TabletID()
                               << ", txId: " << txId);

                operation->DoNotify(Self, SideEffects, ctx);
            }

            auto toActivateWaitPublication = operation->ActivatePartsWaitPublication(pathId, version);
            for (auto opId: toActivateWaitPublication) {
                LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                           "TTxAckPublishToSchemeBoard"
                               << ", operation is ready to ack that some awaited paths are published"
                               << ", opId: " << opId
                               << ", left await publications: " << operation->CountWaitPublication(opId)
                               << ", at schemeshard: " << Self->TabletID()
                               << ", txId: " << txId);

                THolder<TEvPrivate::TEvCompletePublication> msg = MakeHolder<TEvPrivate::TEvCompletePublication>(opId, pathId, version);
                TEvPrivate::TEvCompletePublication::TPtr personalEv = (TEventHandle<TEvPrivate::TEvCompletePublication>*) new IEventHandle(
                    Self->SelfId(), Self->SelfId(), msg.Release());

                TMemoryChanges memChanges;
                TStorageChanges dbChanges;
                TOperationContext context{Self, txc, ctx, SideEffects, memChanges, dbChanges};

                operation->Parts[opId.GetSubTxId()]->HandleReply(personalEv, context);
            }
        } else if (Self->Publications.contains(txId)) {
            LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                       "Publication in-flight"
                           << ", count: " << Self->Publications.at(txId).Paths.size()
                           << ", at schemeshard: " << Self->TabletID()
                           << ", txId: " << txId);

            auto& publication = Self->Publications.at(txId);
            if (AckPublish(db, txId, pathId, version, publication.Paths, ctx)) {
                LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                           "Publication complete, notify & remove"
                               << ", at schemeshard: " << Self->TabletID()
                               << ", txId: " << txId
                               << ", subscribers: " << publication.Subscribers.size());

                Notify(txId, publication.Subscribers, ctx);
                Self->Publications.erase(txId);
            }
        } else {
            LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                       "Unknown operation & publication"
                           << ", at schemeshard: " << Self->TabletID()
                           << ", txId: " << txId);
        }

        SideEffects.ApplyOnExecute(Self, txc, ctx);
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "TTxAckPublishToSchemeBoard Complete"
                       << ", at schemeshard: " << Self->TabletID()
                       << ", cookie: " << Ev->Cookie);

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
            LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                       "AckPublish"
                           << ", at schemeshard: " << Self->TabletID()
                           << ", txId: " << txId
                           << ", pathId: " << pathId
                           << ", version: " << it->second);

            Self->PersistRemovePublishingPath(db, txId, pathId, it->second);

            auto eraseIt = it;
            ++it;
            paths.erase(eraseIt);
        }

        return paths.empty();
    }

    void Notify(TTxId txId, const THashSet<TActorId>& subscribers, const TActorContext& ctx) {
        for (const auto& subscriber : subscribers) {
            LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                       "TTxAckPublishToSchemeBoard Notify"
                           << " send TEvNotifyTxCompletionResult"
                           << ", at schemeshard: " << Self->TabletID()
                           << ", to actorId: " << subscriber);

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
