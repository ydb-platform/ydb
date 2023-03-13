#pragma once
#include <library/cpp/actors/core/actor.h>
#include <library/cpp/actors/core/actorid.h>
#include <library/cpp/actors/core/events.h>
#include <library/cpp/actors/core/actor_virtual.h>
#include <library/cpp/actors/core/actorsystem.h>
#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <ydb/library/accessor/accessor.h>
#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/core/base/events.h>

namespace NKikimr::NMetadata::NRequest {

enum EEvents {
    EvCreateTableRequest = EventSpaceBegin(TKikimrEvents::ES_INTERNAL_REQUEST),
    EvCreateTableInternalResponse,
    EvCreateTableResponse,

    EvDropTableRequest,
    EvDropTableInternalResponse,
    EvDropTableResponse,

    EvSelectRequest,
    EvSelectInternalResponse,
    EvSelectResponse,

    EvYQLRequest,
    EvYQLInternalResponse,
    EvGeneralYQLResponse,

    EvCreateSessionRequest,
    EvCreateSessionInternalResponse,
    EvCreateSessionResponse,

    EvModifyPermissionsRequest,
    EvModifyPermissionsInternalResponse,
    EvModifyPermissionsResponse,
    
    EvRequestFinished,
    EvRequestFailed,
    EvRequestStart,
    EvEnd
};

static_assert(EEvents::EvEnd < EventSpaceEnd(TKikimrEvents::ES_INTERNAL_REQUEST), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_INTERNAL_REQUESTS)");

}
