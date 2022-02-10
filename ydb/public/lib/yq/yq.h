#pragma once

#include <ydb/public/sdk/cpp/client/ydb_table/table.h>

#include <ydb/public/api/grpc/yq_v1.grpc.pb.h>

namespace NYdb::NYq {

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

using TCreateQueryResult = TProtoResultWrapper<YandexQuery::CreateQueryResult>;
using TAsyncCreateQueryResult = NThreading::TFuture<TCreateQueryResult>;
struct TCreateQuerySettings : public NYdb::TOperationRequestSettings<TCreateQuerySettings> {};

using TListQueriesResult = TProtoResultWrapper<YandexQuery::ListQueriesResult>;
using TAsyncListQueriesResult = NThreading::TFuture<TListQueriesResult>;
struct TListQueriesSettings : public NYdb::TOperationRequestSettings<TListQueriesSettings> {};

using TDescribeQueryResult = TProtoResultWrapper<YandexQuery::DescribeQueryResult>;
using TAsyncDescribeQueryResult = NThreading::TFuture<TDescribeQueryResult>;
struct TDescribeQuerySettings : public NYdb::TOperationRequestSettings<TDescribeQuerySettings> {};

using TGetQueryStatusResult = TProtoResultWrapper<YandexQuery::GetQueryStatusResult>; 
using TAsyncGetQueryStatusResult = NThreading::TFuture<TGetQueryStatusResult>; 
struct TGetQueryStatusSettings : public NYdb::TOperationRequestSettings<TGetQueryStatusSettings> {}; 
 
using TModifyQueryResult = TProtoResultWrapper<YandexQuery::ModifyQueryResult>;
using TAsyncModifyQueryResult = NThreading::TFuture<TModifyQueryResult>;
struct TModifyQuerySettings : public NYdb::TOperationRequestSettings<TModifyQuerySettings> {};

using TDeleteQueryResult = TProtoResultWrapper<YandexQuery::DeleteQueryResult>;
using TAsyncDeleteQueryResult = NThreading::TFuture<TDeleteQueryResult>;
struct TDeleteQuerySettings : public NYdb::TOperationRequestSettings<TDeleteQuerySettings> {};

using TControlQueryResult = TProtoResultWrapper<YandexQuery::ControlQueryResult>;
using TAsyncControlQueryResult = NThreading::TFuture<TControlQueryResult>;
struct TControlQuerySettings : public NYdb::TOperationRequestSettings<TControlQuerySettings> {};

using TGetResultDataResult = TProtoResultWrapper<YandexQuery::GetResultDataResult>;
using TAsyncGetResultDataResult = NThreading::TFuture<TGetResultDataResult>;
struct TGetResultDataSettings : public NYdb::TOperationRequestSettings<TGetResultDataSettings> {};

using TListJobsResult = TProtoResultWrapper<YandexQuery::ListJobsResult>;
using TAsyncListJobsResult = NThreading::TFuture<TListJobsResult>;
struct TListJobsSettings : public NYdb::TOperationRequestSettings<TListJobsSettings> {};

using TDescribeJobResult = TProtoResultWrapper<YandexQuery::DescribeJobResult>; 
using TAsyncDescribeJobResult = NThreading::TFuture<TDescribeJobResult>; 
struct TDescribeJobSettings : public NYdb::TOperationRequestSettings<TDescribeJobSettings> {}; 
 
using TCreateConnectionResult = TProtoResultWrapper<YandexQuery::CreateConnectionResult>;
using TAsyncCreateConnectionResult = NThreading::TFuture<TCreateConnectionResult>;
struct TCreateConnectionSettings : public NYdb::TOperationRequestSettings<TCreateConnectionSettings> {};

using TListConnectionsResult = TProtoResultWrapper<YandexQuery::ListConnectionsResult>;
using TAsyncListConnectionsResult = NThreading::TFuture<TListConnectionsResult>;
struct TListConnectionsSettings : public NYdb::TOperationRequestSettings<TListConnectionsSettings> {};

using TDescribeConnectionResult = TProtoResultWrapper<YandexQuery::DescribeConnectionResult>;
using TAsyncDescribeConnectionResult = NThreading::TFuture<TDescribeConnectionResult>;
struct TDescribeConnectionSettings : public NYdb::TOperationRequestSettings<TDescribeConnectionSettings> {};

using TModifyConnectionResult = TProtoResultWrapper<YandexQuery::ModifyConnectionResult>;
using TAsyncModifyConnectionResult = NThreading::TFuture<TModifyConnectionResult>;
struct TModifyConnectionSettings : public NYdb::TOperationRequestSettings<TModifyConnectionSettings> {};

using TDeleteConnectionResult = TProtoResultWrapper<YandexQuery::DeleteConnectionResult>;
using TAsyncDeleteConnectionResult = NThreading::TFuture<TDeleteConnectionResult>;
struct TDeleteConnectionSettings : public NYdb::TOperationRequestSettings<TDeleteConnectionSettings> {};

using TTestConnectionResult = TProtoResultWrapper<YandexQuery::TestConnectionResult>;
using TAsyncTestConnectionResult = NThreading::TFuture<TTestConnectionResult>;
struct TTestConnectionSettings : public NYdb::TOperationRequestSettings<TTestConnectionSettings> {};

using TCreateBindingResult = TProtoResultWrapper<YandexQuery::CreateBindingResult>;
using TAsyncCreateBindingResult = NThreading::TFuture<TCreateBindingResult>;
struct TCreateBindingSettings : public NYdb::TOperationRequestSettings<TCreateBindingSettings> {};

using TListBindingsResult = TProtoResultWrapper<YandexQuery::ListBindingsResult>;
using TAsyncListBindingsResult = NThreading::TFuture<TListBindingsResult>;
struct TListBindingsSettings : public NYdb::TOperationRequestSettings<TListBindingsSettings> {};

using TDescribeBindingResult = TProtoResultWrapper<YandexQuery::DescribeBindingResult>;
using TAsyncDescribeBindingResult = NThreading::TFuture<TDescribeBindingResult>;
struct TDescribeBindingSettings : public NYdb::TOperationRequestSettings<TDescribeBindingSettings> {};

using TModifyBindingResult = TProtoResultWrapper<YandexQuery::ModifyBindingResult>;
using TAsyncModifyBindingResult = NThreading::TFuture<TModifyBindingResult>;
struct TModifyBindingSettings : public NYdb::TOperationRequestSettings<TModifyBindingSettings> {};

using TDeleteBindingResult = TProtoResultWrapper<YandexQuery::DeleteBindingResult>;
using TAsyncDeleteBindingResult = NThreading::TFuture<TDeleteBindingResult>;
struct TDeleteBindingSettings : public NYdb::TOperationRequestSettings<TDeleteBindingSettings> {};

class TClient {
    class TImpl;

public:
    TClient(const NYdb::TDriver& driver, const NYdb::TCommonClientSettings& settings = NYdb::TCommonClientSettings());

    TAsyncCreateQueryResult CreateQuery(
        const YandexQuery::CreateQueryRequest& request,
        const TCreateQuerySettings& settings = TCreateQuerySettings());

    TAsyncListQueriesResult ListQueries(
        const YandexQuery::ListQueriesRequest& request,
        const TListQueriesSettings& settings = TListQueriesSettings());

    TAsyncDescribeQueryResult DescribeQuery(
        const YandexQuery::DescribeQueryRequest& request,
        const TDescribeQuerySettings& settings = TDescribeQuerySettings());

    TAsyncGetQueryStatusResult GetQueryStatus( 
        const YandexQuery::GetQueryStatusRequest& request, 
        const TGetQueryStatusSettings& settings = TGetQueryStatusSettings()); 
 
    TAsyncModifyQueryResult ModifyQuery(
        const YandexQuery::ModifyQueryRequest& request,
        const TModifyQuerySettings& settings = TModifyQuerySettings());

    TAsyncDeleteQueryResult DeleteQuery(
        const YandexQuery::DeleteQueryRequest& request,
        const TDeleteQuerySettings& settings = TDeleteQuerySettings());

    TAsyncControlQueryResult ControlQuery(
        const YandexQuery::ControlQueryRequest& request,
        const TControlQuerySettings& settings = TControlQuerySettings());

    TAsyncGetResultDataResult GetResultData(
        const YandexQuery::GetResultDataRequest& request,
        const TGetResultDataSettings& settings = TGetResultDataSettings());

    TAsyncListJobsResult ListJobs(
        const YandexQuery::ListJobsRequest& request,
        const TListJobsSettings& settings = TListJobsSettings());

    TAsyncDescribeJobResult DescribeJob( 
        const YandexQuery::DescribeJobRequest& request, 
        const TDescribeJobSettings& settings = TDescribeJobSettings()); 
 
    TAsyncCreateConnectionResult CreateConnection(
        const YandexQuery::CreateConnectionRequest& request,
        const TCreateConnectionSettings& settings = TCreateConnectionSettings());

    TAsyncListConnectionsResult ListConnections(
        const YandexQuery::ListConnectionsRequest& request,
        const TListConnectionsSettings& settings = TListConnectionsSettings());

    TAsyncDescribeConnectionResult DescribeConnection(
        const YandexQuery::DescribeConnectionRequest& request,
        const TDescribeConnectionSettings& settings = TDescribeConnectionSettings());

    TAsyncModifyConnectionResult ModifyConnection(
        const YandexQuery::ModifyConnectionRequest& request,
        const TModifyConnectionSettings& settings = TModifyConnectionSettings());

    TAsyncDeleteConnectionResult DeleteConnection(
        const YandexQuery::DeleteConnectionRequest& request,
        const TDeleteConnectionSettings& settings = TDeleteConnectionSettings());

    TAsyncTestConnectionResult TestConnection(
        const YandexQuery::TestConnectionRequest& request,
        const TTestConnectionSettings& settings = TTestConnectionSettings());

    TAsyncCreateBindingResult CreateBinding(
        const YandexQuery::CreateBindingRequest& request,
        const TCreateBindingSettings& settings = TCreateBindingSettings());

    TAsyncListBindingsResult ListBindings(
        const YandexQuery::ListBindingsRequest& request,
        const TListBindingsSettings& settings = TListBindingsSettings());

    TAsyncDescribeBindingResult DescribeBinding(
        const YandexQuery::DescribeBindingRequest& request,
        const TDescribeBindingSettings& settings = TDescribeBindingSettings());

    TAsyncModifyBindingResult ModifyBinding(
        const YandexQuery::ModifyBindingRequest& request,
        const TModifyBindingSettings& settings = TModifyBindingSettings());

    TAsyncDeleteBindingResult DeleteBinding(
        const YandexQuery::DeleteBindingRequest& request,
        const TDeleteBindingSettings& settings = TDeleteBindingSettings());

private:
    std::shared_ptr<TImpl> Impl_;
};

} // namespace NYdb::NYq
