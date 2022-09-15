#pragma once

#include "defs.h"
#include "test_load_actor.h"

#include <ydb/core/tx/datashard/datashard.h>

namespace NKikimr::NDataShardLoad {

NActors::IActor *CreateUpsertBulkActor(const NKikimrDataShardLoad::TEvTestLoadRequest::TUpdateStart& cmd,
        const NActors::TActorId& parent, TIntrusivePtr<::NMonitoring::TDynamicCounters> counters, ui64 tag);

NActors::IActor *CreateLocalMkqlUpsertActor(const NKikimrDataShardLoad::TEvTestLoadRequest::TUpdateStart& cmd,
        const NActors::TActorId& parent, TIntrusivePtr<::NMonitoring::TDynamicCounters> counters, ui64 tag);

NActors::IActor *CreateKqpUpsertActor(const NKikimrDataShardLoad::TEvTestLoadRequest::TUpdateStart& cmd,
        const NActors::TActorId& parent, TIntrusivePtr<::NMonitoring::TDynamicCounters> counters, ui64 tag);

NActors::IActor *CreateProposeUpsertActor(const NKikimrDataShardLoad::TEvTestLoadRequest::TUpdateStart& cmd,
        const NActors::TActorId& parent, TIntrusivePtr<::NMonitoring::TDynamicCounters> counters, ui64 tag);

NActors::IActor *CreateReadIteratorActor(const NKikimrDataShardLoad::TEvTestLoadRequest::TReadStart& cmd,
        const NActors::TActorId& parent, TIntrusivePtr<::NMonitoring::TDynamicCounters> counters, ui64 tag);

class TLoadActorException : public yexception {
};

#define VERIFY_PARAM2(FIELD, NAME) \
    do { \
        if (!(FIELD).Has##NAME()) { \
            ythrow TLoadActorException() << "missing " << #NAME << " parameter"; \
        } \
    } while (false)

#define VERIFY_PARAM(NAME) VERIFY_PARAM2(cmd, NAME)

} // NKikimr::NDataShardLoad
