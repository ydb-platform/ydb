#pragma once

#include "schemeshard_impl.h"

#include <util/generic/ptr.h>
#include <util/generic/vector.h>

namespace NKikimr {
namespace NSchemeShard {

class TSchemeShard::TXxport::TTxBase: public NTabletFlatExecutor::TTransactionBase<TSchemeShard> {
    TVector<THolder<IEventHandle>> SendOnComplete;

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

        LOG_TRACE_S(TlsActivationContext->AsActorContext(), NKikimrServices::FLAT_TX_SCHEMESHARD,
            "SendNotifications: "
                << ": id# " << info->Id
                << ", subscribers count# " << info->Subscribers.size());

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

        for (auto& ev : SendOnComplete) {
            ctx.Send(std::move(ev));
        }
    }

}; // TTxBase

} // NSchemeShard
} // NKikimr
