#pragma once

#include <ydb/core/grpc_services/base/base.h>
#include <ydb/public/api/protos/ydb_topic.pb.h>
#include <ydb/services/persqueue_v1/actors/schema_actors.h>

#include <ydb/core/grpc_services/rpc_calls.h>
#include <ydb/core/grpc_services/rpc_scheme_base.h>

namespace NKikimr::NGRpcService {

using TEvRpcCreateTopicRequest = TGrpcRequestOperationCall<Ydb::Topic::CreateTopicRequest,
        Ydb::Topic::CreateTopicResponse>;

using TEvRpcAlterTopicRequest = TGrpcRequestOperationCall<Ydb::Topic::AlterTopicRequest,
        Ydb::Topic::AlterTopicResponse>;

using TEvRpcDropTopicRequest = TGrpcRequestOperationCall<Ydb::Topic::DropTopicRequest,
        Ydb::Topic::DropTopicResponse>;

template<>
IActor* TEvRpcCreateTopicRequest::CreateRpcActor(NKikimr::NGRpcService::IRequestOpCtx* msg) {
    return new NGRpcProxy::V1::TCreateTopicActor(msg);
}

template<>
IActor* TEvRpcAlterTopicRequest::CreateRpcActor(NKikimr::NGRpcService::IRequestOpCtx* msg) {
    return new NGRpcProxy::V1::TAlterTopicActor(msg);
}

template<>
IActor* TEvRpcDropTopicRequest::CreateRpcActor(NKikimr::NGRpcService::IRequestOpCtx* msg) {
    return new NGRpcProxy::V1::TDropTopicActor(msg);
}

} // namespace NKikimr::NGRpcService
