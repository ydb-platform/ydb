#pragma once

#include <ydb/public/api/protos/ydb_monitoring.pb.h>
#include <ydb/core/base/events.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>

namespace NKikimr {
namespace NHealthCheck {

enum EEv {
    // requests
    EvSelfCheckRequest = EventSpaceBegin(TKikimrEvents::ES_HEALTH_CHECK),
    EvNodeCheckRequest,
    EvSelfCheckRequestProto,
    EvClusterStateRequest,

    // replies
    EvSelfCheckResult = EvSelfCheckRequest + 512,
    EvSelfCheckResultProto,
    EvClusterStateResult,

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

void RemoveUnrequestedEntries(Ydb::Monitoring::SelfCheckResult& result, const Ydb::Monitoring::SelfCheckRequest& request);

inline NActors::TActorId MakeHealthCheckID() { return NActors::TActorId(0, "healthcheck"); }
IActor* CreateHealthCheckService();

struct TEvPrivate {
    enum EEv {
        EvRetryNodeWhiteboard = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE)");

    struct TEvRetryNodeWhiteboard : NActors::TEventLocal<TEvRetryNodeWhiteboard, EvRetryNodeWhiteboard> {
        NNodeWhiteboard::TNodeId NodeId;
        int EventId;

        TEvRetryNodeWhiteboard(NNodeWhiteboard::TNodeId nodeId, int eventId)
            : NodeId(nodeId)
            , EventId(eventId)
        {}
    };
};

extern const TString NONE;
extern const TString BLOCK_4_2;
extern const TString MIRROR_3_DC;

}
}
