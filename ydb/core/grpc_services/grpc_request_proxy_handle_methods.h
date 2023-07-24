#pragma once

#include "rpc_calls.h"

namespace NKikimr {
namespace NGRpcService {

class TGRpcRequestProxyHandleMethods {
protected:
    static void Handle(TEvBiStreamPingRequest::TPtr& ev, const TActorContext& ctx);
    static void Handle(TEvStreamPQWriteRequest::TPtr& ev, const TActorContext& ctx);
    static void Handle(TEvStreamPQMigrationReadRequest::TPtr& ev, const TActorContext& ctx);
    static void Handle(TEvStreamTopicWriteRequest::TPtr& ev, const TActorContext& ctx);
    static void Handle(TEvStreamTopicReadRequest::TPtr& ev, const TActorContext& ctx);
    static void Handle(TEvCommitOffsetRequest::TPtr& ev, const TActorContext& ctx);
    static void Handle(TEvPQReadInfoRequest::TPtr& ev, const TActorContext& ctx);
    static void Handle(TEvPQDropTopicRequest::TPtr& ev, const TActorContext& ctx);
    static void Handle(TEvPQCreateTopicRequest::TPtr& ev, const TActorContext& ctx);
    static void Handle(TEvPQAlterTopicRequest::TPtr& ev, const TActorContext& ctx);
    static void Handle(TEvPQAddReadRuleRequest::TPtr& ev, const TActorContext& ctx);
    static void Handle(TEvPQRemoveReadRuleRequest::TPtr& ev, const TActorContext& ctx);
    static void Handle(TEvPQDescribeTopicRequest::TPtr& ev, const TActorContext& ctx);
    static void Handle(TEvDiscoverPQClustersRequest::TPtr& ev, const TActorContext& ctx);
    static void Handle(TEvLoginRequest::TPtr& ev, const TActorContext& ctx);
    static void Handle(TEvNodeCheckRequest::TPtr& ev, const TActorContext& ctx);
    static void Handle(TEvCoordinationSessionRequest::TPtr& ev, const TActorContext& ctx);
    static void Handle(TEvDropTopicRequest::TPtr& ev, const TActorContext& ctx);
    static void Handle(TEvCreateTopicRequest::TPtr& ev, const TActorContext& ctx);
    static void Handle(TEvAlterTopicRequest::TPtr& ev, const TActorContext& ctx);
    static void Handle(TEvDescribeTopicRequest::TPtr& ev, const TActorContext& ctx);
    static void Handle(TEvDescribeConsumerRequest::TPtr& ev, const TActorContext& ctx);
    static void Handle(TEvDescribePartitionRequest::TPtr& ev, const TActorContext& ctx);
};

}
}
