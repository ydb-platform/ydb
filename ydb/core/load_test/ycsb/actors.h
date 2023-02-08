#pragma once

#include "defs.h"
#include "test_load_actor.h"

#include <ydb/core/tx/datashard/datashard.h>

namespace NKikimr::NDataShardLoad {

IActor *CreateUpsertBulkActor(
    const NKikimrDataShardLoad::TEvYCSBTestLoadRequest::TUpdateStart& cmd,
    const NKikimrDataShardLoad::TEvYCSBTestLoadRequest::TTargetShard& target,
    const TActorId& parent,
    TIntrusivePtr<::NMonitoring::TDynamicCounters> counters,
    const TSubLoadId& id);

IActor *CreateLocalMkqlUpsertActor(
    const NKikimrDataShardLoad::TEvYCSBTestLoadRequest::TUpdateStart& cmd,
    const NKikimrDataShardLoad::TEvYCSBTestLoadRequest::TTargetShard& target,
    const TActorId& parent,
    TIntrusivePtr<::NMonitoring::TDynamicCounters> counters,
    const TSubLoadId& id);

IActor *CreateKqpUpsertActor(
    const NKikimrDataShardLoad::TEvYCSBTestLoadRequest::TUpdateStart& cmd,
    const NKikimrDataShardLoad::TEvYCSBTestLoadRequest::TTargetShard& target,
    const TActorId& parent,
    TIntrusivePtr<::NMonitoring::TDynamicCounters> counters,
    const TSubLoadId& id);

IActor *CreateProposeUpsertActor(
    const NKikimrDataShardLoad::TEvYCSBTestLoadRequest::TUpdateStart& cmd,
    const NKikimrDataShardLoad::TEvYCSBTestLoadRequest::TTargetShard& target,
    const TActorId& parent,
    TIntrusivePtr<::NMonitoring::TDynamicCounters> counters,
    const TSubLoadId& id);

IActor *CreateReadIteratorActor(
    const NKikimrDataShardLoad::TEvYCSBTestLoadRequest::TReadStart& cmd,
    const NKikimrDataShardLoad::TEvYCSBTestLoadRequest::TTargetShard& target,
    const TActorId& parent,
    TIntrusivePtr<::NMonitoring::TDynamicCounters> counters,
    const TSubLoadId& id);

IActor *CreateKqpSelectActor(
    const NKikimrDataShardLoad::TEvYCSBTestLoadRequest::TReadStart& cmd,
    const NKikimrDataShardLoad::TEvYCSBTestLoadRequest::TTargetShard& target,
    const TActorId& parent,
    TIntrusivePtr<::NMonitoring::TDynamicCounters> counters,
    const TSubLoadId& id);

class TLoadManagerException : public yexception {
};

#define VERIFY_PARAM2(FIELD, NAME) \
    do { \
        if (!(FIELD).Has##NAME()) { \
            ythrow TLoadManagerException() << "missing " << #NAME << " parameter"; \
        } \
    } while (false)

} // NKikimr::NDataShardLoad
