#pragma once

#include "logging.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>

namespace NKikimr::NPQ {

void DoLogUnhandledException(NKikimrServices::EServiceKikimr service, const TStructuredLogPrefix& prefix, const std::exception& exc);
void DoLogUnhandledException(NKikimrServices::EServiceKikimr service, TStringBuf prefix, const std::exception& exc);

namespace NPrivate {
    class ILogPrefixBase {
    public:
        virtual const TStructuredLogPrefix& GetLogPrefix() const = 0;
    protected:
        ~ILogPrefixBase() = default;
    };

    void IncrementUnhandledExceptionCounter(const NActors::TActorContext& ctx);
};

template<typename TDerived>
class TBaseActor : public NActors::TActorBootstrapped<TDerived>
                 , public NActors::IActorExceptionHandler
                 , virtual public NPrivate::ILogPrefixBase
                 , public TLogPrefix {
public:
    using TBase = NActors::TActorBootstrapped<TDerived>;
    using TThis = TDerived;

    TBaseActor(NKikimrServices::EServiceKikimr service)
        : TLogPrefix(service)
    {
    }

    bool OnUnhandledException(const std::exception& exc) override {
        if (AppData()->FeatureFlags.GetEnableTabletRestartOnUnhandledExceptions()) {
            DoLogUnhandledException(Service, NPQ_LOG_PREFIX, exc);

            OnException(exc);

            NPrivate::IncrementUnhandledExceptionCounter(this->ActorContext());
            this->PassAway();

            return true;
        }

        return false;
    }

    virtual void OnException(const std::exception& exc) {
        Y_UNUSED(exc);
    }

    TStructuredLogPrefix LogBuilder() const {
        return YDB_LOG_CREATE_MESSAGE(
            {"selfId", TBase::SelfId()});
    }

    TStructuredLogPrefix LogPrefix() const override {
        return MakeNpqLogPrefix(LogBuilder(), GetLogPrefix());
    }

    void PassAway() override {
        TBase::PassAway();
    }

protected:
    template <typename TEv>
    TString EventStr(const char * func, const TEv& ev) {
        return TStringBuilder() << func << " event# " << ev->GetTypeRewrite() << " (" << ev->GetTypeName() << ") "
            << ", Sender " << ev->Sender.ToString() << ", Recipient " << ev->Recipient.ToString()
            << ", Cookie: " << ev->Cookie;
    }
};


template<typename TDerived>
class TBaseTabletActor : public TBaseActor<TDerived> {
public:
    using TBase = TBaseActor<TDerived>;
    using TThis = TDerived;

    TBaseTabletActor(ui64 tabletId, NActors::TActorId tabletActorId, NKikimrServices::EServiceKikimr service)
        : TBaseActor<TDerived>(service)
        , TabletId(tabletId)
        , TabletActorId(tabletActorId)
    {
    }

    void OnException(const std::exception&) override {
        RestartTablet();
    }

    void RestartTablet() {
        TDerived& self = static_cast<TDerived&>(*this);
        self.Send(TabletActorId, new NActors::TEvents::TEvPoison());
    }

    TStructuredLogPrefix LogBuilder() const {
        return YDB_LOG_CREATE_MESSAGE(
            {"tabletId", TabletId});
    }

    TStructuredLogPrefix LogPrefix() const override {
        return MakeNpqLogPrefix(LogBuilder(), this->GetLogPrefix());
    }

protected:
    const ui64 TabletId;
    const NActors::TActorId TabletActorId;
};


class TConstantLogPrefix: virtual public NPrivate::ILogPrefixBase {
public:
    const TStructuredLogPrefix& GetLogPrefix() const final;
    virtual TStructuredLogPrefix BuildLogPrefix() const {
        return {};
    }

private:
    mutable TMaybe<TStructuredLogPrefix> LogPrefix_;
};

class TPipeCacheClient {
public:
    explicit TPipeCacheClient(const NActors::IActorOps* actorOps)
        : ActorOps(actorOps)
    {
    }

    void SendToTablet(ui64 tabletId, IEventBase *ev, ui64 cookie = 0) {
        auto& pipe = Pipes[tabletId];
        auto forward = std::make_unique<TEvPipeCache::TEvForward>(ev, tabletId, !pipe.Subscribed, pipe.GetCookie());
        ActorOps->Send(MakePipePerNodeCacheID(false), forward.release(), 0, cookie);
        pipe.Subscribed = true;
    }

    bool OnUndelivered(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
        auto it = Pipes.find(ev->Get()->TabletId);
        if (it == Pipes.end()) {
            return false;
        }
        if (ev->Cookie == it->second.Cookie) {
            it->second.Subscribed = false;
            return true;
        }
        return false;
    }

    void Close() {
        if (!Pipes.empty()) {
            ActorOps->Send(MakePipePerNodeCacheID(false), new TEvPipeCache::TEvUnlink(0));
            Pipes.clear();
        }
    }

private:
    const NActors::IActorOps* ActorOps;

    struct TPipeInfo {
        ui64 Cookie = 0;
        bool Subscribed = false;

        ui64 GetCookie() {
            if (Subscribed) {
                return Cookie;
            }
            return ++Cookie;
        }
    };
    absl::flat_hash_map<ui64, TPipeInfo> Pipes;
};

}
