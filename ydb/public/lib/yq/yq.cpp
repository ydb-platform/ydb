#include "yq.h"

#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/make_request/make.h>
#undef INCLUDE_YDB_INTERNAL_H

#include <ydb/public/sdk/cpp/client/ydb_common_client/impl/client.h>

namespace NYdb::NYq {

using namespace NYdb;

class TClient::TImpl : public TClientImplCommon<TClient::TImpl> {
public:
    TImpl(std::shared_ptr<TGRpcConnectionsImpl>&& connections, const TCommonClientSettings& settings)
        : TClientImplCommon(std::move(connections), settings) {}

    template<class TProtoResult, class TResultWrapper>
    auto MakeResultExtractor(NThreading::TPromise<TResultWrapper> promise) {
        return [promise = std::move(promise)]
            (google::protobuf::Any* any, TPlainStatus status) mutable {
                std::unique_ptr<TProtoResult> result;
                if (any) {
                    result.reset(new TProtoResult);
                    any->UnpackTo(result.get());
                }

                promise.SetValue(
                    TResultWrapper(
                        TStatus(std::move(status)),
                        std::move(result)));
            };
    }

    TAsyncCreateQueryResult CreateQuery(
        const YandexQuery::CreateQueryRequest& protoRequest,
        const TCreateQuerySettings& settings) {
        auto request = MakeOperationRequest<YandexQuery::CreateQueryRequest>(settings);
        request = protoRequest;

        auto promise = NThreading::NewPromise<TCreateQueryResult>();
        auto future = promise.GetFuture();

        auto extractor = MakeResultExtractor<
            YandexQuery::CreateQueryResult,
            TCreateQueryResult>(std::move(promise));

        Connections_->RunDeferred<
            YandexQuery::V1::YandexQueryService,
            YandexQuery::CreateQueryRequest,
            YandexQuery::CreateQueryResponse>(
                std::move(request),
                std::move(extractor),
                &YandexQuery::V1::YandexQueryService::Stub::AsyncCreateQuery,
                DbDriverState_,
                INITIAL_DEFERRED_CALL_DELAY,
                TRpcRequestSettings::Make(settings),
                settings.ClientTimeout_);

        return future;
    }

    TAsyncListQueriesResult ListQueries(
        const YandexQuery::ListQueriesRequest& protoRequest,
        const TListQueriesSettings& settings) {
        auto request = MakeOperationRequest<YandexQuery::ListQueriesRequest>(settings);
        request = protoRequest;

        auto promise = NThreading::NewPromise<TListQueriesResult>();
        auto future = promise.GetFuture();

        auto extractor = MakeResultExtractor<
            YandexQuery::ListQueriesResult,
            TListQueriesResult>(std::move(promise));

        Connections_->RunDeferred<
            YandexQuery::V1::YandexQueryService,
            YandexQuery::ListQueriesRequest,
            YandexQuery::ListQueriesResponse>(
                std::move(request),
                std::move(extractor),
                &YandexQuery::V1::YandexQueryService::Stub::AsyncListQueries,
                DbDriverState_,
                INITIAL_DEFERRED_CALL_DELAY,
                TRpcRequestSettings::Make(settings),
                settings.ClientTimeout_);

        return future;
    }

    TAsyncDescribeQueryResult DescribeQuery(
        const YandexQuery::DescribeQueryRequest& protoRequest,
        const TDescribeQuerySettings& settings) {
        auto request = MakeOperationRequest<YandexQuery::DescribeQueryRequest>(settings);
        request = protoRequest;

        auto promise = NThreading::NewPromise<TDescribeQueryResult>();
        auto future = promise.GetFuture();

        auto extractor = MakeResultExtractor<
            YandexQuery::DescribeQueryResult,
            TDescribeQueryResult>(std::move(promise));

        Connections_->RunDeferred<
            YandexQuery::V1::YandexQueryService,
            YandexQuery::DescribeQueryRequest,
            YandexQuery::DescribeQueryResponse>(
                std::move(request),
                std::move(extractor),
                &YandexQuery::V1::YandexQueryService::Stub::AsyncDescribeQuery,
                DbDriverState_,
                INITIAL_DEFERRED_CALL_DELAY,
                TRpcRequestSettings::Make(settings),
                settings.ClientTimeout_);

        return future;
    }

    TAsyncGetQueryStatusResult GetQueryStatus(
        const YandexQuery::GetQueryStatusRequest& protoRequest,
        const TGetQueryStatusSettings& settings) {
        auto request = MakeOperationRequest<YandexQuery::GetQueryStatusRequest>(settings);
        request = protoRequest;

        auto promise = NThreading::NewPromise<TGetQueryStatusResult>();
        auto future = promise.GetFuture();

        auto extractor = MakeResultExtractor<
            YandexQuery::GetQueryStatusResult,
            TGetQueryStatusResult>(std::move(promise));

        Connections_->RunDeferred<
            YandexQuery::V1::YandexQueryService,
            YandexQuery::GetQueryStatusRequest,
            YandexQuery::GetQueryStatusResponse>(
                std::move(request),
                std::move(extractor),
                &YandexQuery::V1::YandexQueryService::Stub::AsyncGetQueryStatus,
                DbDriverState_,
                INITIAL_DEFERRED_CALL_DELAY,
                TRpcRequestSettings::Make(settings),
                settings.ClientTimeout_);

        return future;
    }

    TAsyncModifyQueryResult ModifyQuery(
        const YandexQuery::ModifyQueryRequest& protoRequest,
        const TModifyQuerySettings& settings) {
        auto request = MakeOperationRequest<YandexQuery::ModifyQueryRequest>(settings);
        request = protoRequest;

        auto promise = NThreading::NewPromise<TModifyQueryResult>();
        auto future = promise.GetFuture();

        auto extractor = MakeResultExtractor<
            YandexQuery::ModifyQueryResult,
            TModifyQueryResult>(std::move(promise));

        Connections_->RunDeferred<
            YandexQuery::V1::YandexQueryService,
            YandexQuery::ModifyQueryRequest,
            YandexQuery::ModifyQueryResponse>(
                std::move(request),
                std::move(extractor),
                &YandexQuery::V1::YandexQueryService::Stub::AsyncModifyQuery,
                DbDriverState_,
                INITIAL_DEFERRED_CALL_DELAY,
                TRpcRequestSettings::Make(settings),
                settings.ClientTimeout_);

        return future;
    }

    TAsyncDeleteQueryResult DeleteQuery(
        const YandexQuery::DeleteQueryRequest& protoRequest,
        const TDeleteQuerySettings& settings) {
        auto request = MakeOperationRequest<YandexQuery::DeleteQueryRequest>(settings);
        request = protoRequest;

        auto promise = NThreading::NewPromise<TDeleteQueryResult>();
        auto future = promise.GetFuture();

        auto extractor = MakeResultExtractor<
            YandexQuery::DeleteQueryResult,
            TDeleteQueryResult>(std::move(promise));

        Connections_->RunDeferred<
            YandexQuery::V1::YandexQueryService,
            YandexQuery::DeleteQueryRequest,
            YandexQuery::DeleteQueryResponse>(
                std::move(request),
                std::move(extractor),
                &YandexQuery::V1::YandexQueryService::Stub::AsyncDeleteQuery,
                DbDriverState_,
                INITIAL_DEFERRED_CALL_DELAY,
                TRpcRequestSettings::Make(settings),
                settings.ClientTimeout_);

        return future;
    }

    TAsyncControlQueryResult ControlQuery(
        const YandexQuery::ControlQueryRequest& protoRequest,
        const TControlQuerySettings& settings) {
        auto request = MakeOperationRequest<YandexQuery::ControlQueryRequest>(settings);
        request = protoRequest;

        auto promise = NThreading::NewPromise<TControlQueryResult>();
        auto future = promise.GetFuture();

        auto extractor = MakeResultExtractor<
            YandexQuery::ControlQueryResult,
            TControlQueryResult>(std::move(promise));

        Connections_->RunDeferred<
            YandexQuery::V1::YandexQueryService,
            YandexQuery::ControlQueryRequest,
            YandexQuery::ControlQueryResponse>(
                std::move(request),
                std::move(extractor),
                &YandexQuery::V1::YandexQueryService::Stub::AsyncControlQuery,
                DbDriverState_,
                INITIAL_DEFERRED_CALL_DELAY,
                TRpcRequestSettings::Make(settings),
                settings.ClientTimeout_);

        return future;
    }

    TAsyncGetResultDataResult GetResultData(
        const YandexQuery::GetResultDataRequest& protoRequest,
        const TGetResultDataSettings& settings) {
        auto request = MakeOperationRequest<YandexQuery::GetResultDataRequest>(settings);
        request = protoRequest;

        auto promise = NThreading::NewPromise<TGetResultDataResult>();
        auto future = promise.GetFuture();

        auto extractor = MakeResultExtractor<
            YandexQuery::GetResultDataResult,
            TGetResultDataResult>(std::move(promise));

        Connections_->RunDeferred<
            YandexQuery::V1::YandexQueryService,
            YandexQuery::GetResultDataRequest,
            YandexQuery::GetResultDataResponse>(
                std::move(request),
                std::move(extractor),
                &YandexQuery::V1::YandexQueryService::Stub::AsyncGetResultData,
                DbDriverState_,
                INITIAL_DEFERRED_CALL_DELAY,
                TRpcRequestSettings::Make(settings),
                settings.ClientTimeout_);

        return future;
    }

    TAsyncListJobsResult ListJobs(
        const YandexQuery::ListJobsRequest& protoRequest,
        const TListJobsSettings& settings) {
        auto request = MakeOperationRequest<YandexQuery::ListJobsRequest>(settings);
        request = protoRequest;

        auto promise = NThreading::NewPromise<TListJobsResult>();
        auto future = promise.GetFuture();

        auto extractor = MakeResultExtractor<
            YandexQuery::ListJobsResult,
            TListJobsResult>(std::move(promise));

        Connections_->RunDeferred<
            YandexQuery::V1::YandexQueryService,
            YandexQuery::ListJobsRequest,
            YandexQuery::ListJobsResponse>(
                std::move(request),
                std::move(extractor),
                &YandexQuery::V1::YandexQueryService::Stub::AsyncListJobs,
                DbDriverState_,
                INITIAL_DEFERRED_CALL_DELAY,
                TRpcRequestSettings::Make(settings),
                settings.ClientTimeout_);

        return future;
    }

    TAsyncDescribeJobResult DescribeJob(
        const YandexQuery::DescribeJobRequest& protoRequest,
        const TDescribeJobSettings& settings) {
        auto request = MakeOperationRequest<YandexQuery::DescribeJobRequest>(settings);
        request = protoRequest;

        auto promise = NThreading::NewPromise<TDescribeJobResult>();
        auto future = promise.GetFuture();

        auto extractor = MakeResultExtractor<
            YandexQuery::DescribeJobResult,
            TDescribeJobResult>(std::move(promise));

        Connections_->RunDeferred<
            YandexQuery::V1::YandexQueryService,
            YandexQuery::DescribeJobRequest,
            YandexQuery::DescribeJobResponse>(
                std::move(request),
                std::move(extractor),
                &YandexQuery::V1::YandexQueryService::Stub::AsyncDescribeJob,
                DbDriverState_,
                INITIAL_DEFERRED_CALL_DELAY,
                TRpcRequestSettings::Make(settings),
                settings.ClientTimeout_);

        return future;
    }

    TAsyncCreateConnectionResult CreateConnection(
        const YandexQuery::CreateConnectionRequest& protoRequest,
        const TCreateConnectionSettings& settings) {
        auto request = MakeOperationRequest<YandexQuery::CreateConnectionRequest>(settings);
        request = protoRequest;

        auto promise = NThreading::NewPromise<TCreateConnectionResult>();
        auto future = promise.GetFuture();

        auto extractor = MakeResultExtractor<
            YandexQuery::CreateConnectionResult,
            TCreateConnectionResult>(std::move(promise));

        Connections_->RunDeferred<
            YandexQuery::V1::YandexQueryService,
            YandexQuery::CreateConnectionRequest,
            YandexQuery::CreateConnectionResponse>(
                std::move(request),
                std::move(extractor),
                &YandexQuery::V1::YandexQueryService::Stub::AsyncCreateConnection,
                DbDriverState_,
                INITIAL_DEFERRED_CALL_DELAY,
                TRpcRequestSettings::Make(settings),
                settings.ClientTimeout_);

        return future;
    }

    TAsyncListConnectionsResult ListConnections(
        const YandexQuery::ListConnectionsRequest& protoRequest,
        const TListConnectionsSettings& settings) {
        auto request = MakeOperationRequest<YandexQuery::ListConnectionsRequest>(settings);
        request = protoRequest;

        auto promise = NThreading::NewPromise<TListConnectionsResult>();
        auto future = promise.GetFuture();

        auto extractor = MakeResultExtractor<
            YandexQuery::ListConnectionsResult,
            TListConnectionsResult>(std::move(promise));

        Connections_->RunDeferred<
            YandexQuery::V1::YandexQueryService,
            YandexQuery::ListConnectionsRequest,
            YandexQuery::ListConnectionsResponse>(
                std::move(request),
                std::move(extractor),
                &YandexQuery::V1::YandexQueryService::Stub::AsyncListConnections,
                DbDriverState_,
                INITIAL_DEFERRED_CALL_DELAY,
                TRpcRequestSettings::Make(settings),
                settings.ClientTimeout_);

        return future;
    }

    TAsyncDescribeConnectionResult DescribeConnection(
        const YandexQuery::DescribeConnectionRequest& protoRequest,
        const TDescribeConnectionSettings& settings) {
        auto request = MakeOperationRequest<YandexQuery::DescribeConnectionRequest>(settings);
        request = protoRequest;

        auto promise = NThreading::NewPromise<TDescribeConnectionResult>();
        auto future = promise.GetFuture();

        auto extractor = MakeResultExtractor<
            YandexQuery::DescribeConnectionResult,
            TDescribeConnectionResult>(std::move(promise));

        Connections_->RunDeferred<
            YandexQuery::V1::YandexQueryService,
            YandexQuery::DescribeConnectionRequest,
            YandexQuery::DescribeConnectionResponse>(
                std::move(request),
                std::move(extractor),
                &YandexQuery::V1::YandexQueryService::Stub::AsyncDescribeConnection,
                DbDriverState_,
                INITIAL_DEFERRED_CALL_DELAY,
                TRpcRequestSettings::Make(settings),
                settings.ClientTimeout_);

        return future;
    }

    TAsyncModifyConnectionResult ModifyConnection(
        const YandexQuery::ModifyConnectionRequest& protoRequest,
        const TModifyConnectionSettings& settings) {
        auto request = MakeOperationRequest<YandexQuery::ModifyConnectionRequest>(settings);
        request = protoRequest;

        auto promise = NThreading::NewPromise<TModifyConnectionResult>();
        auto future = promise.GetFuture();

        auto extractor = MakeResultExtractor<
            YandexQuery::ModifyConnectionResult,
            TModifyConnectionResult>(std::move(promise));

        Connections_->RunDeferred<
            YandexQuery::V1::YandexQueryService,
            YandexQuery::ModifyConnectionRequest,
            YandexQuery::ModifyConnectionResponse>(
                std::move(request),
                std::move(extractor),
                &YandexQuery::V1::YandexQueryService::Stub::AsyncModifyConnection,
                DbDriverState_,
                INITIAL_DEFERRED_CALL_DELAY,
                TRpcRequestSettings::Make(settings),
                settings.ClientTimeout_);

        return future;
    }

    TAsyncDeleteConnectionResult DeleteConnection(
        const YandexQuery::DeleteConnectionRequest& protoRequest,
        const TDeleteConnectionSettings& settings) {
        auto request = MakeOperationRequest<YandexQuery::DeleteConnectionRequest>(settings);
        request = protoRequest;

        auto promise = NThreading::NewPromise<TDeleteConnectionResult>();
        auto future = promise.GetFuture();

        auto extractor = MakeResultExtractor<
            YandexQuery::DeleteConnectionResult,
            TDeleteConnectionResult>(std::move(promise));

        Connections_->RunDeferred<
            YandexQuery::V1::YandexQueryService,
            YandexQuery::DeleteConnectionRequest,
            YandexQuery::DeleteConnectionResponse>(
                std::move(request),
                std::move(extractor),
                &YandexQuery::V1::YandexQueryService::Stub::AsyncDeleteConnection,
                DbDriverState_,
                INITIAL_DEFERRED_CALL_DELAY,
                TRpcRequestSettings::Make(settings),
                settings.ClientTimeout_);

        return future;
    }

    TAsyncTestConnectionResult TestConnection(
        const YandexQuery::TestConnectionRequest& protoRequest,
        const TTestConnectionSettings& settings) {
        auto request = MakeOperationRequest<YandexQuery::TestConnectionRequest>(settings);
        request = protoRequest;

        auto promise = NThreading::NewPromise<TTestConnectionResult>();
        auto future = promise.GetFuture();

        auto extractor = MakeResultExtractor<
            YandexQuery::TestConnectionResult,
            TTestConnectionResult>(std::move(promise));

        Connections_->RunDeferred<
            YandexQuery::V1::YandexQueryService,
            YandexQuery::TestConnectionRequest,
            YandexQuery::TestConnectionResponse>(
                    std::move(request),
                    std::move(extractor),
                    &YandexQuery::V1::YandexQueryService::Stub::AsyncTestConnection,
                    DbDriverState_,
                    INITIAL_DEFERRED_CALL_DELAY,
                    TRpcRequestSettings::Make(settings),
                    settings.ClientTimeout_);

        return future;
    }

    TAsyncCreateBindingResult CreateBinding(
        const YandexQuery::CreateBindingRequest& protoRequest,
        const TCreateBindingSettings& settings) {
        auto request = MakeOperationRequest<YandexQuery::CreateBindingRequest>(settings);
        request = protoRequest;

        auto promise = NThreading::NewPromise<TCreateBindingResult>();
        auto future = promise.GetFuture();

        auto extractor = MakeResultExtractor<
            YandexQuery::CreateBindingResult,
            TCreateBindingResult>(std::move(promise));

        Connections_->RunDeferred<
            YandexQuery::V1::YandexQueryService,
            YandexQuery::CreateBindingRequest,
            YandexQuery::CreateBindingResponse>(
                std::move(request),
                std::move(extractor),
                &YandexQuery::V1::YandexQueryService::Stub::AsyncCreateBinding,
                DbDriverState_,
                INITIAL_DEFERRED_CALL_DELAY,
                TRpcRequestSettings::Make(settings),
                settings.ClientTimeout_);

        return future;
    }

    TAsyncListBindingsResult ListBindings(
        const YandexQuery::ListBindingsRequest& protoRequest,
        const TListBindingsSettings& settings) {
        auto request = MakeOperationRequest<YandexQuery::ListBindingsRequest>(settings);
        request = protoRequest;

        auto promise = NThreading::NewPromise<TListBindingsResult>();
        auto future = promise.GetFuture();

        auto extractor = MakeResultExtractor<
            YandexQuery::ListBindingsResult,
            TListBindingsResult>(std::move(promise));

        Connections_->RunDeferred<
            YandexQuery::V1::YandexQueryService,
            YandexQuery::ListBindingsRequest,
            YandexQuery::ListBindingsResponse>(
                std::move(request),
                std::move(extractor),
                &YandexQuery::V1::YandexQueryService::Stub::AsyncListBindings,
                DbDriverState_,
                INITIAL_DEFERRED_CALL_DELAY,
                TRpcRequestSettings::Make(settings),
                settings.ClientTimeout_);

        return future;
    }

    TAsyncDescribeBindingResult DescribeBinding(
        const YandexQuery::DescribeBindingRequest& protoRequest,
        const TDescribeBindingSettings& settings) {
        auto request = MakeOperationRequest<YandexQuery::DescribeBindingRequest>(settings);
        request = protoRequest;

        auto promise = NThreading::NewPromise<TDescribeBindingResult>();
        auto future = promise.GetFuture();

        auto extractor = MakeResultExtractor<
            YandexQuery::DescribeBindingResult,
            TDescribeBindingResult>(std::move(promise));

        Connections_->RunDeferred<
            YandexQuery::V1::YandexQueryService,
            YandexQuery::DescribeBindingRequest,
            YandexQuery::DescribeBindingResponse>(
                std::move(request),
                std::move(extractor),
                &YandexQuery::V1::YandexQueryService::Stub::AsyncDescribeBinding,
                DbDriverState_,
                INITIAL_DEFERRED_CALL_DELAY,
                TRpcRequestSettings::Make(settings),
                settings.ClientTimeout_);

        return future;
    }

    TAsyncModifyBindingResult ModifyBinding(
        const YandexQuery::ModifyBindingRequest& protoRequest,
        const TModifyBindingSettings& settings) {
        auto request = MakeOperationRequest<YandexQuery::ModifyBindingRequest>(settings);
        request = protoRequest;

        auto promise = NThreading::NewPromise<TModifyBindingResult>();
        auto future = promise.GetFuture();

        auto extractor = MakeResultExtractor<
            YandexQuery::ModifyBindingResult,
            TModifyBindingResult>(std::move(promise));

        Connections_->RunDeferred<
            YandexQuery::V1::YandexQueryService,
            YandexQuery::ModifyBindingRequest,
            YandexQuery::ModifyBindingResponse>(
                std::move(request),
                std::move(extractor),
                &YandexQuery::V1::YandexQueryService::Stub::AsyncModifyBinding,
                DbDriverState_,
                INITIAL_DEFERRED_CALL_DELAY,
                TRpcRequestSettings::Make(settings),
                settings.ClientTimeout_);

        return future;
    }

    TAsyncDeleteBindingResult DeleteBinding(
        const YandexQuery::DeleteBindingRequest& protoRequest,
        const TDeleteBindingSettings& settings) {
        auto request = MakeOperationRequest<YandexQuery::DeleteBindingRequest>(settings);
        request = protoRequest;

        auto promise = NThreading::NewPromise<TDeleteBindingResult>();
        auto future = promise.GetFuture();

        auto extractor = MakeResultExtractor<
            YandexQuery::DeleteBindingResult,
            TDeleteBindingResult>(std::move(promise));

        Connections_->RunDeferred<
            YandexQuery::V1::YandexQueryService,
            YandexQuery::DeleteBindingRequest,
            YandexQuery::DeleteBindingResponse>(
                std::move(request),
                std::move(extractor),
                &YandexQuery::V1::YandexQueryService::Stub::AsyncDeleteBinding,
                DbDriverState_,
                INITIAL_DEFERRED_CALL_DELAY,
                TRpcRequestSettings::Make(settings),
                settings.ClientTimeout_);

        return future;
    }
};

TClient::TClient(const TDriver& driver, const TCommonClientSettings& settings)
    : Impl_(new TImpl(CreateInternalInterface(driver), settings))
{}

TAsyncCreateQueryResult TClient::CreateQuery(
    const YandexQuery::CreateQueryRequest& request,
    const TCreateQuerySettings& settings) {
    return Impl_->CreateQuery(request, settings);
}

TAsyncListQueriesResult TClient::ListQueries(
    const YandexQuery::ListQueriesRequest& request,
    const TListQueriesSettings& settings) {
    return Impl_->ListQueries(request, settings);
}

TAsyncDescribeQueryResult TClient::DescribeQuery(
    const YandexQuery::DescribeQueryRequest& request,
    const TDescribeQuerySettings& settings) {
    return Impl_->DescribeQuery(request, settings);
}

TAsyncGetQueryStatusResult TClient::GetQueryStatus(
    const YandexQuery::GetQueryStatusRequest& request,
    const TGetQueryStatusSettings& settings) {
    return Impl_->GetQueryStatus(request, settings);
}

TAsyncModifyQueryResult TClient::ModifyQuery(
    const YandexQuery::ModifyQueryRequest& request,
    const TModifyQuerySettings& settings) {
    return Impl_->ModifyQuery(request, settings);
}

TAsyncDeleteQueryResult TClient::DeleteQuery(
    const YandexQuery::DeleteQueryRequest& request,
    const TDeleteQuerySettings& settings) {
    return Impl_->DeleteQuery(request, settings);
}

TAsyncControlQueryResult TClient::ControlQuery(
    const YandexQuery::ControlQueryRequest& request,
    const TControlQuerySettings& settings) {
    return Impl_->ControlQuery(request, settings);
}

TAsyncGetResultDataResult TClient::GetResultData(
    const YandexQuery::GetResultDataRequest& request,
    const TGetResultDataSettings& settings) {
    return Impl_->GetResultData(request, settings);
}

TAsyncListJobsResult TClient::ListJobs(
    const YandexQuery::ListJobsRequest& request,
    const TListJobsSettings& settings) {
    return Impl_->ListJobs(request, settings);
}

TAsyncDescribeJobResult TClient::DescribeJob(
    const YandexQuery::DescribeJobRequest& request,
    const TDescribeJobSettings& settings) {
    return Impl_->DescribeJob(request, settings);
}

TAsyncCreateConnectionResult TClient::CreateConnection(
    const YandexQuery::CreateConnectionRequest& request,
    const TCreateConnectionSettings& settings) {
    return Impl_->CreateConnection(request, settings);
}

TAsyncListConnectionsResult TClient::ListConnections(
    const YandexQuery::ListConnectionsRequest& request,
    const TListConnectionsSettings& settings) {
    return Impl_->ListConnections(request, settings);
}

TAsyncDescribeConnectionResult TClient::DescribeConnection(
    const YandexQuery::DescribeConnectionRequest& request,
    const TDescribeConnectionSettings& settings) {
    return Impl_->DescribeConnection(request, settings);
}

TAsyncModifyConnectionResult TClient::ModifyConnection(
    const YandexQuery::ModifyConnectionRequest& request,
    const TModifyConnectionSettings& settings) {
    return Impl_->ModifyConnection(request, settings);
}

TAsyncDeleteConnectionResult TClient::DeleteConnection(
    const YandexQuery::DeleteConnectionRequest& request,
    const TDeleteConnectionSettings& settings) {
    return Impl_->DeleteConnection(request, settings);
}

TAsyncTestConnectionResult TClient::TestConnection(
    const YandexQuery::TestConnectionRequest& request,
    const TTestConnectionSettings& settings) {
    return Impl_->TestConnection(request, settings);
}

TAsyncCreateBindingResult TClient::CreateBinding(
    const YandexQuery::CreateBindingRequest& request,
    const TCreateBindingSettings& settings) {
    return Impl_->CreateBinding(request, settings);
}

TAsyncListBindingsResult TClient::ListBindings(
    const YandexQuery::ListBindingsRequest& request,
    const TListBindingsSettings& settings) {
    return Impl_->ListBindings(request, settings);
}

TAsyncDescribeBindingResult TClient::DescribeBinding(
    const YandexQuery::DescribeBindingRequest& request,
    const TDescribeBindingSettings& settings) {
    return Impl_->DescribeBinding(request, settings);
}

TAsyncModifyBindingResult TClient::ModifyBinding(
    const YandexQuery::ModifyBindingRequest& request,
    const TModifyBindingSettings& settings) {
    return Impl_->ModifyBinding(request, settings);
}

TAsyncDeleteBindingResult TClient::DeleteBinding(
    const YandexQuery::DeleteBindingRequest& request,
    const TDeleteBindingSettings& settings) {
    return Impl_->DeleteBinding(request, settings);
}

}

