#pragma once

#include <ydb/public/api/protos/ydb_monitoring.pb.h>
#include <ydb/core/base/events.h>

namespace NKikimr {
namespace NHealthCheck {

enum EEv {
    // requests
    EvSelfCheckRequest = EventSpaceBegin(TKikimrEvents::ES_HEALTH_CHECK),
    EvNodeCheckRequest,

    // replies
    EvSelfCheckResult = EvSelfCheckRequest + 512,

    EvEnd
};

static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_HEALTH_CHECK), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_HEALTH_CHECK)");

struct TEvSelfCheckRequest : TEventLocal<TEvSelfCheckRequest, EvSelfCheckRequest> {
    Ydb::Monitoring::SelfCheckRequest Request;
    TString Database;
};

struct TEvNodeCheckRequest : TEventLocal<TEvNodeCheckRequest, EvNodeCheckRequest> {
    Ydb::Monitoring::NodeCheckRequest Request;
};

struct TEvSelfCheckResult : TEventLocal<TEvSelfCheckResult, EvSelfCheckResult> {
    Ydb::Monitoring::SelfCheckResult Result;
};

inline NActors::TActorId MakeHealthCheckID() { return NActors::TActorId(0, "healthcheck"); }
IActor* CreateHealthCheckService();

}
}
