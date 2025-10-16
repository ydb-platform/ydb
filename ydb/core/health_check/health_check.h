#pragma once

#include <ydb/public/api/protos/ydb_monitoring.pb.h>
#include <ydb/core/base/events.h>

namespace NKikimr {
namespace NHealthCheck {

enum EEv {
    // requests
    EvSelfCheckRequest = EventSpaceBegin(TKikimrEvents::ES_HEALTH_CHECK),
    EvNodeCheckRequest,
    EvSelfCheckRequestProto,
    EvClusterStateRequest,
    EvClusterStateRequestProto,

    // replies
    EvSelfCheckResult = EvSelfCheckRequest + 512,
    EvSelfCheckResultProto,
    EvClusterStateResult,
    EvClusterStateResultProto,

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

struct TEvClusterStateRequest : TEventLocal<TEvClusterStateRequest, EvClusterStateRequest> {
    Ydb::Monitoring::ClusterStateRequest Request;
};

struct TEvClusterStateResult : TEventLocal<TEvClusterStateResult, EvClusterStateResult> {
    Ydb::Monitoring::ClusterStateResult Result;
};

struct TEvSelfCheckRequestProto : TEventPB<TEvSelfCheckRequestProto, Ydb::Monitoring::SelfCheckRequest, EvSelfCheckRequestProto> {};

struct TEvSelfCheckResultProto : TEventPB<TEvSelfCheckResultProto, Ydb::Monitoring::SelfCheckResult, EvSelfCheckResultProto> {};

struct TEvClusterStateRequestProto : TEventPB<TEvClusterStateRequestProto, Ydb::Monitoring::ClusterStateRequest, EvClusterStateRequestProto> {};

struct TEvClusterStateResultProto : TEventPB<TEvClusterStateResultProto, Ydb::Monitoring::ClusterStateResult, EvClusterStateResultProto> {};

void RemoveUnrequestedEntries(Ydb::Monitoring::SelfCheckResult& result, const Ydb::Monitoring::SelfCheckRequest& request);

inline NActors::TActorId MakeHealthCheckID() { return NActors::TActorId(0, "healthcheck"); }
IActor* CreateHealthCheckService();

}
}
