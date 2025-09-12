#pragma once

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/core/events.h>

#include <ydb/core/tx/columnshard/overload_manager/overload_manager_counters.h>
#include <ydb/core/tx/columnshard/overload_manager/overload_manager_events.h>
#include <ydb/core/tx/columnshard/overload_manager/overload_subscribers.h>

namespace NKikimr::NColumnShard::NOverload {

class TOverloadManager: public NActors::TActor<TOverloadManager> {
    TCSOverloadManagerCounters Counters;
    TOverloadSubscribers OverloadSubscribers;

    STRICT_STFUNC(
        StateMain,
        hFunc(NOverload::TEvOverloadSubscribe, Handle)
        hFunc(NOverload::TEvOverloadUnsubscribe, Handle)
        hFunc(NOverload::TEvOverloadPipeServerDisconnected, Handle)
        hFunc(NOverload::TEvOverloadResourcesReleased, Handle)
        hFunc(NOverload::TEvOverloadColumnShardDied, Handle)
    )

    void Handle(const NOverload::TEvOverloadSubscribe::TPtr& ev);
    void Handle(const NOverload::TEvOverloadUnsubscribe::TPtr& ev);
    void Handle(const NOverload::TEvOverloadPipeServerDisconnected::TPtr& ev);
    void Handle(const NOverload::TEvOverloadResourcesReleased::TPtr& ev);
    void Handle(const NOverload::TEvOverloadColumnShardDied::TPtr& ev);

public:
    TOverloadManager(TIntrusivePtr<::NMonitoring::TDynamicCounters> countersGroup);
};

} // namespace NKikimr::NColumnShard::NOverload
