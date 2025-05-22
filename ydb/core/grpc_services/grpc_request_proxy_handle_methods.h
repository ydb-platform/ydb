#pragma once

#include "rpc_calls.h"

namespace NKikimr {
namespace NGRpcService {

class TGRpcRequestProxyHandleMethods {
protected:
    template<typename TCtx>
    static bool ValidateAndReplyOnError(TCtx* ctx);
    static void Handle(TEvBiStreamPingRequest::TPtr& ev, const TActorContext& ctx);
    static void Handle(TEvStreamPQWriteRequest::TPtr& ev, const TActorContext& ctx);
    static void Handle(TEvStreamPQMigrationReadRequest::TPtr& ev, const TActorContext& ctx);
    static void Handle(TEvStreamTopicWriteRequest::TPtr& ev, const TActorContext& ctx);
    static void Handle(TEvStreamTopicReadRequest::TPtr& ev, const TActorContext& ctx);
    static void Handle(TEvStreamTopicDirectReadRequest::TPtr& ev, const TActorContext& ctx);
    static void Handle(TEvCommitOffsetRequest::TPtr& ev, const TActorContext& ctx);
    static void Handle(TEvPQReadInfoRequest::TPtr& ev, const TActorContext& ctx);
    static void Handle(TEvDiscoverPQClustersRequest::TPtr& ev, const TActorContext& ctx);
    static void Handle(TEvNodeCheckRequest::TPtr& ev, const TActorContext& ctx);
    static void Handle(TEvCoordinationSessionRequest::TPtr& ev, const TActorContext& ctx);
};

}
}
