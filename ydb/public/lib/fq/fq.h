#pragma once

#include <ydb/public/sdk/cpp/client/ydb_table/table.h>

#include <ydb/public/api/grpc/draft/fq_v1.grpc.pb.h>

namespace NYdb::NFq {

template<class TProtoResult>
class TProtoResultWrapper : public NYdb::TStatus {
    friend class TClient;

private:
    TProtoResultWrapper(
            NYdb::TStatus&& status,
            std::unique_ptr<TProtoResult> result)
        : TStatus(std::move(status))
        , Result(std::move(result))
    { }

public:
    const TProtoResult& GetResult() const {
        if (!Result) {
            ythrow yexception() << "Uninitialized result: " << GetIssues().ToString();
        }
        return *Result;
    }

    bool HasResult() const {
        return Result;
    }

private:
    std::unique_ptr<TProtoResult> Result;
};

using TCreateQueryResult = TProtoResultWrapper<FederatedQuery::CreateQueryResult>;
using TAsyncCreateQueryResult = NThreading::TFuture<TCreateQueryResult>;
struct TCreateQuerySettings : public NYdb::TOperationRequestSettings<TCreateQuerySettings> {};

using TListQueriesResult = TProtoResultWrapper<FederatedQuery::ListQueriesResult>;
using TAsyncListQueriesResult = NThreading::TFuture<TListQueriesResult>;
struct TListQueriesSettings : public NYdb::TOperationRequestSettings<TListQueriesSettings> {};

using TDescribeQueryResult = TProtoResultWrapper<FederatedQuery::DescribeQueryResult>;
using TAsyncDescribeQueryResult = NThreading::TFuture<TDescribeQueryResult>;
struct TDescribeQuerySettings : public NYdb::TOperationRequestSettings<TDescribeQuerySettings> {};

using TGetQueryStatusResult = TProtoResultWrapper<FederatedQuery::GetQueryStatusResult>;
using TAsyncGetQueryStatusResult = NThreading::TFuture<TGetQueryStatusResult>;
struct TGetQueryStatusSettings : public NYdb::TOperationRequestSettings<TGetQueryStatusSettings> {};

using TModifyQueryResult = TProtoResultWrapper<FederatedQuery::ModifyQueryResult>;
using TAsyncModifyQueryResult = NThreading::TFuture<TModifyQueryResult>;
struct TModifyQuerySettings : public NYdb::TOperationRequestSettings<TModifyQuerySettings> {};

using TDeleteQueryResult = TProtoResultWrapper<FederatedQuery::DeleteQueryResult>;
using TAsyncDeleteQueryResult = NThreading::TFuture<TDeleteQueryResult>;
struct TDeleteQuerySettings : public NYdb::TOperationRequestSettings<TDeleteQuerySettings> {};

using TControlQueryResult = TProtoResultWrapper<FederatedQuery::ControlQueryResult>;
using TAsyncControlQueryResult = NThreading::TFuture<TControlQueryResult>;
struct TControlQuerySettings : public NYdb::TOperationRequestSettings<TControlQuerySettings> {};

using TGetResultDataResult = TProtoResultWrapper<FederatedQuery::GetResultDataResult>;
using TAsyncGetResultDataResult = NThreading::TFuture<TGetResultDataResult>;
struct TGetResultDataSettings : public NYdb::TOperationRequestSettings<TGetResultDataSettings> {};

using TListJobsResult = TProtoResultWrapper<FederatedQuery::ListJobsResult>;
using TAsyncListJobsResult = NThreading::TFuture<TListJobsResult>;
struct TListJobsSettings : public NYdb::TOperationRequestSettings<TListJobsSettings> {};

using TDescribeJobResult = TProtoResultWrapper<FederatedQuery::DescribeJobResult>;
using TAsyncDescribeJobResult = NThreading::TFuture<TDescribeJobResult>;
struct TDescribeJobSettings : public NYdb::TOperationRequestSettings<TDescribeJobSettings> {};

using TCreateConnectionResult = TProtoResultWrapper<FederatedQuery::CreateConnectionResult>;
using TAsyncCreateConnectionResult = NThreading::TFuture<TCreateConnectionResult>;
struct TCreateConnectionSettings : public NYdb::TOperationRequestSettings<TCreateConnectionSettings> {};

using TListConnectionsResult = TProtoResultWrapper<FederatedQuery::ListConnectionsResult>;
using TAsyncListConnectionsResult = NThreading::TFuture<TListConnectionsResult>;
struct TListConnectionsSettings : public NYdb::TOperationRequestSettings<TListConnectionsSettings> {};

using TDescribeConnectionResult = TProtoResultWrapper<FederatedQuery::DescribeConnectionResult>;
using TAsyncDescribeConnectionResult = NThreading::TFuture<TDescribeConnectionResult>;
struct TDescribeConnectionSettings : public NYdb::TOperationRequestSettings<TDescribeConnectionSettings> {};

using TModifyConnectionResult = TProtoResultWrapper<FederatedQuery::ModifyConnectionResult>;
using TAsyncModifyConnectionResult = NThreading::TFuture<TModifyConnectionResult>;
struct TModifyConnectionSettings : public NYdb::TOperationRequestSettings<TModifyConnectionSettings> {};

using TDeleteConnectionResult = TProtoResultWrapper<FederatedQuery::DeleteConnectionResult>;
using TAsyncDeleteConnectionResult = NThreading::TFuture<TDeleteConnectionResult>;
struct TDeleteConnectionSettings : public NYdb::TOperationRequestSettings<TDeleteConnectionSettings> {};

using TTestConnectionResult = TProtoResultWrapper<FederatedQuery::TestConnectionResult>;
using TAsyncTestConnectionResult = NThreading::TFuture<TTestConnectionResult>;
struct TTestConnectionSettings : public NYdb::TOperationRequestSettings<TTestConnectionSettings> {};

using TCreateBindingResult = TProtoResultWrapper<FederatedQuery::CreateBindingResult>;
using TAsyncCreateBindingResult = NThreading::TFuture<TCreateBindingResult>;
struct TCreateBindingSettings : public NYdb::TOperationRequestSettings<TCreateBindingSettings> {};

using TListBindingsResult = TProtoResultWrapper<FederatedQuery::ListBindingsResult>;
using TAsyncListBindingsResult = NThreading::TFuture<TListBindingsResult>;
struct TListBindingsSettings : public NYdb::TOperationRequestSettings<TListBindingsSettings> {};

using TDescribeBindingResult = TProtoResultWrapper<FederatedQuery::DescribeBindingResult>;
using TAsyncDescribeBindingResult = NThreading::TFuture<TDescribeBindingResult>;
struct TDescribeBindingSettings : public NYdb::TOperationRequestSettings<TDescribeBindingSettings> {};

using TModifyBindingResult = TProtoResultWrapper<FederatedQuery::ModifyBindingResult>;
using TAsyncModifyBindingResult = NThreading::TFuture<TModifyBindingResult>;
struct TModifyBindingSettings : public NYdb::TOperationRequestSettings<TModifyBindingSettings> {};

using TDeleteBindingResult = TProtoResultWrapper<FederatedQuery::DeleteBindingResult>;
using TAsyncDeleteBindingResult = NThreading::TFuture<TDeleteBindingResult>;
struct TDeleteBindingSettings : public NYdb::TOperationRequestSettings<TDeleteBindingSettings> {};

class TClient {
    class TImpl;

public:
    TClient(const NYdb::TDriver& driver, const NYdb::TCommonClientSettings& settings = NYdb::TCommonClientSettings());

    TAsyncCreateQueryResult CreateQuery(
        const FederatedQuery::CreateQueryRequest& request,
        const TCreateQuerySettings& settings = TCreateQuerySettings());

    TAsyncListQueriesResult ListQueries(
        const FederatedQuery::ListQueriesRequest& request,
        const TListQueriesSettings& settings = TListQueriesSettings());

    TAsyncDescribeQueryResult DescribeQuery(
        const FederatedQuery::DescribeQueryRequest& request,
        const TDescribeQuerySettings& settings = TDescribeQuerySettings());

    TAsyncGetQueryStatusResult GetQueryStatus(
        const FederatedQuery::GetQueryStatusRequest& request,
        const TGetQueryStatusSettings& settings = TGetQueryStatusSettings());

    TAsyncModifyQueryResult ModifyQuery(
        const FederatedQuery::ModifyQueryRequest& request,
        const TModifyQuerySettings& settings = TModifyQuerySettings());

    TAsyncDeleteQueryResult DeleteQuery(
        const FederatedQuery::DeleteQueryRequest& request,
        const TDeleteQuerySettings& settings = TDeleteQuerySettings());

    TAsyncControlQueryResult ControlQuery(
        const FederatedQuery::ControlQueryRequest& request,
        const TControlQuerySettings& settings = TControlQuerySettings());

    TAsyncGetResultDataResult GetResultData(
        const FederatedQuery::GetResultDataRequest& request,
        const TGetResultDataSettings& settings = TGetResultDataSettings());

    TAsyncListJobsResult ListJobs(
        const FederatedQuery::ListJobsRequest& request,
        const TListJobsSettings& settings = TListJobsSettings());

    TAsyncDescribeJobResult DescribeJob(
        const FederatedQuery::DescribeJobRequest& request,
        const TDescribeJobSettings& settings = TDescribeJobSettings());

    TAsyncCreateConnectionResult CreateConnection(
        const FederatedQuery::CreateConnectionRequest& request,
        const TCreateConnectionSettings& settings = TCreateConnectionSettings());

    TAsyncListConnectionsResult ListConnections(
        const FederatedQuery::ListConnectionsRequest& request,
        const TListConnectionsSettings& settings = TListConnectionsSettings());

    TAsyncDescribeConnectionResult DescribeConnection(
        const FederatedQuery::DescribeConnectionRequest& request,
        const TDescribeConnectionSettings& settings = TDescribeConnectionSettings());

    TAsyncModifyConnectionResult ModifyConnection(
        const FederatedQuery::ModifyConnectionRequest& request,
        const TModifyConnectionSettings& settings = TModifyConnectionSettings());

    TAsyncDeleteConnectionResult DeleteConnection(
        const FederatedQuery::DeleteConnectionRequest& request,
        const TDeleteConnectionSettings& settings = TDeleteConnectionSettings());

    TAsyncTestConnectionResult TestConnection(
        const FederatedQuery::TestConnectionRequest& request,
        const TTestConnectionSettings& settings = TTestConnectionSettings());

    TAsyncCreateBindingResult CreateBinding(
        const FederatedQuery::CreateBindingRequest& request,
        const TCreateBindingSettings& settings = TCreateBindingSettings());

    TAsyncListBindingsResult ListBindings(
        const FederatedQuery::ListBindingsRequest& request,
        const TListBindingsSettings& settings = TListBindingsSettings());

    TAsyncDescribeBindingResult DescribeBinding(
        const FederatedQuery::DescribeBindingRequest& request,
        const TDescribeBindingSettings& settings = TDescribeBindingSettings());

    TAsyncModifyBindingResult ModifyBinding(
        const FederatedQuery::ModifyBindingRequest& request,
        const TModifyBindingSettings& settings = TModifyBindingSettings());

    TAsyncDeleteBindingResult DeleteBinding(
        const FederatedQuery::DeleteBindingRequest& request,
        const TDeleteBindingSettings& settings = TDeleteBindingSettings());

private:
    std::shared_ptr<TImpl> Impl_;
};

} // namespace NYdb::NFq
