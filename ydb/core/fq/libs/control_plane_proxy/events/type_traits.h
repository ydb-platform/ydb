#pragma once

#include <ydb/public/api/protos/draft/fq.pb.h>

namespace NFq {
namespace NEvControlPlaneProxy {

template<class RequestProtoMessage>
struct TResponseProtoMessage;

template<>
struct TResponseProtoMessage<FederatedQuery::CreateQueryRequest> {
    using type = FederatedQuery::CreateQueryResult;
};
template<>
struct TResponseProtoMessage<FederatedQuery::ListQueriesRequest> {
    using type = FederatedQuery::ListQueriesResult;
};
template<>
struct TResponseProtoMessage<FederatedQuery::DescribeQueryRequest> {
    using type = FederatedQuery::DescribeQueryResult;
};
template<>
struct TResponseProtoMessage<FederatedQuery::GetQueryStatusRequest> {
    using type = FederatedQuery::GetQueryStatusResult;
};
template<>
struct TResponseProtoMessage<FederatedQuery::ModifyQueryRequest> {
    using type = FederatedQuery::ModifyQueryResult;
};
template<>
struct TResponseProtoMessage<FederatedQuery::DeleteQueryRequest> {
    using type = FederatedQuery::DeleteQueryResult;
};
template<>
struct TResponseProtoMessage<FederatedQuery::ControlQueryRequest> {
    using type = FederatedQuery::ControlQueryResult;
};
template<>
struct TResponseProtoMessage<FederatedQuery::GetResultDataRequest> {
    using type = FederatedQuery::GetResultDataResult;
};
template<>
struct TResponseProtoMessage<FederatedQuery::ListJobsRequest> {
    using type = FederatedQuery::ListJobsResult;
};
template<>
struct TResponseProtoMessage<FederatedQuery::DescribeJobRequest> {
    using type = FederatedQuery::DescribeJobResult;
};
template<>
struct TResponseProtoMessage<FederatedQuery::CreateConnectionRequest> {
    using type = FederatedQuery::CreateConnectionResult;
};
template<>
struct TResponseProtoMessage<FederatedQuery::ListConnectionsRequest> {
    using type = FederatedQuery::ListConnectionsResult;
};
template<>
struct TResponseProtoMessage<FederatedQuery::DescribeConnectionRequest> {
    using type = FederatedQuery::DescribeConnectionResult;
};
template<>
struct TResponseProtoMessage<FederatedQuery::ModifyConnectionRequest> {
    using type = FederatedQuery::ModifyConnectionResult;
};
template<>
struct TResponseProtoMessage<FederatedQuery::DeleteConnectionRequest> {
    using type = FederatedQuery::DeleteConnectionResult;
};
template<>
struct TResponseProtoMessage<FederatedQuery::TestConnectionRequest> {
    using type = FederatedQuery::TestConnectionResult;
};
template<>
struct TResponseProtoMessage<FederatedQuery::CreateBindingRequest> {
    using type = FederatedQuery::CreateBindingResult;
};
template<>
struct TResponseProtoMessage<FederatedQuery::ListBindingsRequest> {
    using type = FederatedQuery::ListBindingsResult;
};
template<>
struct TResponseProtoMessage<FederatedQuery::DescribeBindingRequest> {
    using type = FederatedQuery::DescribeBindingResult;
};
template<>
struct TResponseProtoMessage<FederatedQuery::ModifyBindingRequest> {
    using type = FederatedQuery::ModifyBindingResult;
};
template<>
struct TResponseProtoMessage<FederatedQuery::DeleteBindingRequest> {
    using type = FederatedQuery::DeleteBindingResult;
};

template<class Request>
struct ResponseSelector;

} // namespace NEvControlPlaneProxy
} // namespace NFq
