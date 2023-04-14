#pragma once
#include "accessor_refresh.h"

namespace NKikimr::NMetadata::NProvider {

class TDSAccessorNotifier;

class TEvAsk: public NActors::TEventLocal<TEvAsk, EEvents::EvAskLocal> {
private:
    YDB_READONLY_DEF(TActorId, RequesterId);
public:
    TEvAsk(const TActorId& requesterId)
        : RequesterId(requesterId) {

    }
};

class TEvSubscribe: public NActors::TEventLocal<TEvSubscribe, EEvents::EvSubscribeLocal> {
private:
    YDB_READONLY_DEF(TActorId, SubscriberId);
public:
    TEvSubscribe(const TActorId& subscriberId)
        : SubscriberId(subscriberId) {

    }
};

class TEvUnsubscribe: public NActors::TEventLocal<TEvUnsubscribe, EEvents::EvUnsubscribeLocal> {
private:
    YDB_READONLY_DEF(TActorId, SubscriberId);
public:
    TEvUnsubscribe(const TActorId& subscriberId)
        : SubscriberId(subscriberId) {

    }
};

class TDSAccessorNotifier: public TDSAccessorRefresher {
private:
    using TBase = TDSAccessorRefresher;
    std::set<NActors::TActorId> Subscribed;
    std::map<TInstant, std::set<NActors::TActorId>> Asked;
protected:
    virtual void OnBootstrap() override;
    virtual void OnSnapshotModified() override;
    virtual void OnSnapshotRefresh() override;
public:
    TDSAccessorNotifier(const TConfig& config, NFetcher::ISnapshotsFetcher::TPtr sParser)
        : TBase(config, sParser) {
    }

    STFUNC(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvSubscribe, Handle);
            hFunc(TEvAsk, Handle);
            hFunc(TEvUnsubscribe, Handle);
            default:
                TBase::StateMain(ev);
        }
    }


    void Handle(TEvAsk::TPtr& context);
    void Handle(TEvSubscribe::TPtr& context);
    void Handle(TEvUnsubscribe::TPtr& context);
};

class TExternalData: public TDSAccessorNotifier {
private:
    using TBase = TDSAccessorNotifier;
public:
    TExternalData(const TConfig& config, NFetcher::ISnapshotsFetcher::TPtr sParser)
        : TBase(config, sParser) {

    }
};

}
