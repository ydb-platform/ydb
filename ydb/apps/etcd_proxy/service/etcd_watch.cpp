#include "etcd_watch.h"
#include "etcd_shared.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NEtcd {

using namespace NActors;

namespace {

class TWatchman : public TActorBootstrapped<TWatchman> {
public:
    TWatchman() {}

    ~TWatchman() {}

    void Bootstrap(const TActorContext&) {
        TSharedStuff::Get()->Watchman = SelfId();
        Become(&TThis::StateFunc);
    }

private:

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NKikimr::NGRpcService::TEvWatchRequest, Handle);
        }
    }

    void Handle(NKikimr::NGRpcService::TEvWatchRequest::TPtr& ev, const TActorContext& ctx) {
        auto ip = ev->Get()->GetStreamCtx()->GetPeerName();
        Cerr << __func__ << ':' << ip << Endl;
        if (true) {
                etcdserverpb::WatchResponse resp;
            ev->Get()->GetStreamCtx()->Attach(ctx.SelfID);
            ev->Get()->GetStreamCtx()->WriteAndFinish(std::move(resp), grpc::Status::OK);
            return;
        }
    }

};

}

NActors::IActor* CreateEtcdWatchman() {
    return new TWatchman;

}

}

