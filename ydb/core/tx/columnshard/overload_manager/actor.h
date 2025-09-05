#pragma once

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/core/events.h>

#include <ydb/core/tx/columnshard/overload_manager/events.h>
#include <ydb/core/tx/columnshard/overload_manager/overload_subscribers.h>

namespace NKikimr::NColumnShard::NOverload {

class TOverloadManager: public NActors::TActor<TOverloadManager> {
    TOverloadSubscribers OverloadSubscribers;

    STATEFN(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NOverload::TEvOverloadSubscribe, Handle);
            hFunc(NOverload::TEvOverloadUnsubscribe, Handle);
            hFunc(NOverload::TEvOverloadPipeServerDisconnected, Handle);
            hFunc(NOverload::TEvOverloadResourcesReleased, Handle);
            hFunc(NOverload::TEvOverloadColumnShardDied, Handle);
            hFunc(NActors::TEvents::TEvPoison, Handle);
            default:
                AFL_VERIFY(false)("unexpected_event", ev->GetTypeName());
        }
    }

    void Handle(const NOverload::TEvOverloadSubscribe::TPtr& ev);
    void Handle(const NOverload::TEvOverloadUnsubscribe::TPtr& ev);
    void Handle(const NOverload::TEvOverloadPipeServerDisconnected::TPtr& ev);
    void Handle(const NOverload::TEvOverloadResourcesReleased::TPtr& ev);
    void Handle(const NOverload::TEvOverloadColumnShardDied::TPtr& ev);
    void Handle(const NActors::TEvents::TEvPoison::TPtr& ev);

public:
    TOverloadManager();
};

} // namespace NKikimr::NColumnShard::NOverload
