#pragma once

#include "defs.h"
#include "test_load_actor.h"

#include <ydb/core/tx/datashard/datashard.h>

namespace NKikimr::NDataShardLoad {

IActor *CreateUpsertBulkActor(
    const NKikimrDataShardLoad::TEvTestLoadRequest::TUpdateStart& cmd,
    const NKikimrDataShardLoad::TEvTestLoadRequest::TTargetShard& target,
    const TActorId& parent,
    TIntrusivePtr<::NMonitoring::TDynamicCounters> counters,
    ui64 tag);

IActor *CreateLocalMkqlUpsertActor(
    const NKikimrDataShardLoad::TEvTestLoadRequest::TUpdateStart& cmd,
    const NKikimrDataShardLoad::TEvTestLoadRequest::TTargetShard& target,
    const TActorId& parent,
    TIntrusivePtr<::NMonitoring::TDynamicCounters> counters,
    ui64 tag);

IActor *CreateKqpUpsertActor(
    const NKikimrDataShardLoad::TEvTestLoadRequest::TUpdateStart& cmd,
    const NKikimrDataShardLoad::TEvTestLoadRequest::TTargetShard& target,
    const TActorId& parent,
    TIntrusivePtr<::NMonitoring::TDynamicCounters> counters,
    ui64 tag);

IActor *CreateProposeUpsertActor(
    const NKikimrDataShardLoad::TEvTestLoadRequest::TUpdateStart& cmd,
    const NKikimrDataShardLoad::TEvTestLoadRequest::TTargetShard& target,
    const TActorId& parent,
    TIntrusivePtr<::NMonitoring::TDynamicCounters> counters,
    ui64 tag);

IActor *CreateReadIteratorActor(
    const NKikimrDataShardLoad::TEvTestLoadRequest::TReadStart& cmd,
    const NKikimrDataShardLoad::TEvTestLoadRequest::TTargetShard& target,
    const TActorId& parent,
    TIntrusivePtr<::NMonitoring::TDynamicCounters> counters,
    ui64 tag);

class TLoadManagerException : public yexception {
};

#define VERIFY_PARAM2(FIELD, NAME) \
    do { \
        if (!(FIELD).Has##NAME()) { \
            ythrow TLoadManagerException() << "missing " << #NAME << " parameter"; \
        } \
    } while (false)

} // NKikimr::NDataShardLoad
