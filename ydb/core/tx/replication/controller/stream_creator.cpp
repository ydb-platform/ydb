#include "stream_creator.h"
#include "private_events.h"

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/hfunc.h>

#include <ydb/core/tx/replication/ydb_proxy/ydb_proxy.h>

namespace NKikimr::NReplication::NController {

class TStreamCreator: public TActorBootstrapped<TStreamCreator> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::REPLICATION_CONTROLLER_STREAM_CREATOR;
    }

    explicit TStreamCreator(const TActorId& parent, ui64 rid, ui64 tid, const TActorId& proxy)
        : Parent(parent)
        , ReplicationId(rid)
        , TargetId(tid)
        , YdbProxy(proxy)
    {
        // TODO: remove it
        Y_UNUSED(Parent);
        Y_UNUSED(ReplicationId);
        Y_UNUSED(TargetId);
        Y_UNUSED(YdbProxy);
    }

    void Bootstrap() {
        // TODO: send request
        Become(&TThis::StateWork);
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            // TODO: handle response
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

private:
    const TActorId Parent;
    const ui64 ReplicationId;
    const ui64 TargetId;
    const TActorId YdbProxy;

}; // TStreamCreator

IActor* CreateStreamCreator(const TActorId& parent, ui64 rid, ui64 tid, const TActorId& proxy) {
    return new TStreamCreator(parent, rid, tid, proxy);
}

}
