#pragma once

#include "schemeshard_impl.h"

#include <ydb/core/backup/query/table_checksum_snapshot.h>
#include <ydb/library/actors/core/log.h>

#include <util/generic/ptr.h>
#include <util/generic/vector.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::FLAT_TX_SCHEMESHARD

namespace NKikimr {
namespace NSchemeShard {

class TSchemeShard::TXxport::TTxBase: public NTabletFlatExecutor::TTransactionBase<TSchemeShard> {
    TVector<THolder<IEventHandle>> SendOnComplete;
    TVector<TActorId> ChecksumActorsToFinish;
    TVector<THolder<NActors::IActor>> ChecksumCleanupActors;

protected:
    explicit TTxBase(TSelf* self)
        : TBase(self)
    {
    }

    virtual ~TTxBase() = default;

    void Send(const TActorId& recipient, IEventBase* ev, ui32 flags = 0, ui64 cookie = 0) {
        SendOnComplete.emplace_back(new IEventHandle(recipient, Self->SelfId(), ev, flags, cookie));
    }

    template <typename TEvent>
    void Send(const TActorId& recipient, THolder<TEvent> ev, ui32 flags = 0, ui64 cookie = 0) {
        return Send(recipient, static_cast<IEventBase*>(ev.Release()), flags, cookie);
    }

    void FinishTableChecksum(TImportInfo& info, ui32 itemIdx) {
        auto& item = info.Items.at(itemIdx);
        auto snapshot = std::exchange(item.ChecksumSnapshot, TRowVersion::Min());
        if (auto actor = std::exchange(item.ChecksumActor, {})) {
            ChecksumActorsToFinish.push_back(actor);
            return;
        }
        // Recovery may skip validation, leaving no checksum actor to receive completion
        if (!snapshot.TxId && item.State == TImportInfo::EState::Transferring) {
            snapshot = TRowVersion(0, ui64(item.WaitTxId));
        }
        if (snapshot.TxId && item.DstPathId) {
            ChecksumCleanupActors.emplace_back(
                NBackup::CreateTableChecksumSnapshotReleaser(TTableId(item.DstPathId), Self->TabletID(), snapshot));
        }
    }

    TPathId DomainPathId(const TString& dbName) const {
        const TPath domainPath = TPath::Resolve(dbName, Self);
        if (domainPath.IsResolved()) {
            return domainPath.Base()->PathId;
        }

        return TPathId();
    }

    template <typename TInfoPtr>
    static bool IsSameDomain(const TInfoPtr info, const TPathId& domainPathId) {
        if (!domainPathId) {
            return true;
        }

        return info->DomainPathId == domainPathId;
    }

    template <typename TInfoPtr>
    bool IsSameDomain(const TInfoPtr info, const TString& dbName) const {
        return IsSameDomain(info, DomainPathId(dbName));
    }

    template <typename TInfoPtr>
    void SendNotificationsIfFinished(TInfoPtr info, bool force = false) {
        if (!info->IsFinished() && !force) {
            return;
        }

        YDB_LOG_TRACE("SendNotifications",
            {"id", info->Id},
            {"subscriberCount", info->Subscribers.size()},
        );

        TSet<TActorId> toAnswer;
        toAnswer.swap(info->Subscribers);
        for (auto& actorId: toAnswer) {
            Send(actorId, new TEvSchemeShard::TEvNotifyTxCompletionResult(info->Id));
        }
    }

public:
    virtual bool DoExecute(TTransactionContext& txc, const TActorContext& ctx) = 0;
    virtual void DoComplete(const TActorContext& ctx) = 0;

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        return DoExecute(txc, ctx);
    }

    void Complete(const TActorContext& ctx) override {
        DoComplete(ctx);

        // Keep actors tracked until commit so tablet death also releases their handles
        for (const auto& actor : ChecksumActorsToFinish) {
            Self->RunningTableChecksumQueries.erase(actor);
            ctx.Send(actor, new NBackup::TEvTableChecksum::TEvFinish());
        }
        for (auto& actor : ChecksumCleanupActors) {
            ctx.Register(actor.Release());
        }

        for (auto& ev : SendOnComplete) {
            ctx.Send(std::move(ev));
        }
    }

}; // TTxBase

} // NSchemeShard
} // NKikimr

#undef YDB_LOG_THIS_FILE_COMPONENT
