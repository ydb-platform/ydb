#pragma once

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/core/events.h>

#include <ydb/core/tx/columnshard/overload/events.h>
#include <ydb/core/tx/columnshard/overload/overload_subscribers.h>

namespace NKikimr::NColumnShard::NOverload {

class TOverloadManager: public NActors::TActor<TOverloadManager> {
    TOverloadSubscribers OverloadSubscribers;

    STATEFN(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvOverloadSubscribe, Handle);
            hFunc(TEvOverloadUnsubscribe, Handle);
            hFunc(TEvOverloadPipeServerDisconnected, Handle);
            hFunc(TEvOverloadResourcesReleased, Handle);
            hFunc(NActors::TEvents::TEvPoison, Handle);
            default:
                AFL_VERIFY(false)("unexpected_event", ev->GetTypeName());
        }
    }

    void Handle(const TEvOverloadSubscribe::TPtr& ev);
    void Handle(const TEvOverloadUnsubscribe::TPtr& ev);
    void Handle(const TEvOverloadPipeServerDisconnected::TPtr& ev);
    void Handle(const TEvOverloadResourcesReleased::TPtr& ev);

    void Handle(const NActors::TEvents::TEvPoison::TPtr&) {
        // TODO: maybe send events to subscribers
        PassAway();
    }

public:
    TOverloadManager();
};

} // namespace NKikimr::NColumnShard::NOverload
