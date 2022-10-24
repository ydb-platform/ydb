#pragma once
#include "accessor_refresh.h"

namespace NKikimr::NMetadataProvider {

class TDSAccessorNotifier;

class TEvSubscribe: public NActors::TEventLocal<TEvSubscribe, EEvSubscribe::EvSubscribeLocal> {
private:
    YDB_READONLY_DEF(TActorId, SubscriberId);
public:
    TEvSubscribe(const TActorId& subscriberId)
        : SubscriberId(subscriberId)
    {

    }
};

class TEvUnsubscribe: public NActors::TEventLocal<TEvUnsubscribe, EEvSubscribe::EvUnsubscribeLocal> {
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
protected:
    virtual void RegisterState() override {
        Become(&TDSAccessorNotifier::StateMain);
    }
    virtual void OnSnapshotModified() override;
public:
    using TBase::Handle;

    TDSAccessorNotifier(const TConfig& config, ISnapshotParser::TPtr sParser)
        : TBase(config, sParser) {
    }

    STFUNC(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvSubscribe, Handle);
            hFunc(TEvUnsubscribe, Handle);
            default:
                TBase::StateMain(ev, ctx);
        }
    }


    void Handle(TEvSubscribe::TPtr& context);
    void Handle(TEvUnsubscribe::TPtr& context);
};

class TExternalData: public TDSAccessorNotifier {
private:
    using TBase = TDSAccessorNotifier;
public:
    TExternalData(const TConfig& config, ISnapshotParser::TPtr sParser)
        : TBase(config, sParser) {

    }
};

}
