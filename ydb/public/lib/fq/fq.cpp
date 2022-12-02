#include "fq.h"

#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/make_request/make.h>
#undef INCLUDE_YDB_INTERNAL_H

#include <ydb/public/sdk/cpp/client/ydb_common_client/impl/client.h>

namespace NYdb::NFq {

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
        const FederatedQuery::CreateQueryRequest& protoRequest,
        const TCreateQuerySettings& settings) {
        auto request = MakeOperationRequest<FederatedQuery::CreateQueryRequest>(settings);
        request = protoRequest;

        auto promise = NThreading::NewPromise<TCreateQueryResult>();
        auto future = promise.GetFuture();

        auto extractor = MakeResultExtractor<
            FederatedQuery::CreateQueryResult,
            TCreateQueryResult>(std::move(promise));

        Connections_->RunDeferred<
            FederatedQuery::V1::FederatedQueryService,
            FederatedQuery::CreateQueryRequest,
            FederatedQuery::CreateQueryResponse>(
                std::move(request),
                std::move(extractor),
                &FederatedQuery::V1::FederatedQueryService::Stub::AsyncCreateQuery,
                DbDriverState_,
                INITIAL_DEFERRED_CALL_DELAY,
                TRpcRequestSettings::Make(settings));

        return future;
    }

    TAsyncListQueriesResult ListQueries(
        const FederatedQuery::ListQueriesRequest& protoRequest,
        const TListQueriesSettings& settings) {
        auto request = MakeOperationRequest<FederatedQuery::ListQueriesRequest>(settings);
        request = protoRequest;

        auto promise = NThreading::NewPromise<TListQueriesResult>();
        auto future = promise.GetFuture();

        auto extractor = MakeResultExtractor<
            FederatedQuery::ListQueriesResult,
            TListQueriesResult>(std::move(promise));

        Connections_->RunDeferred<
            FederatedQuery::V1::FederatedQueryService,
            FederatedQuery::ListQueriesRequest,
            FederatedQuery::ListQueriesResponse>(
                std::move(request),
                std::move(extractor),
                &FederatedQuery::V1::FederatedQueryService::Stub::AsyncListQueries,
                DbDriverState_,
                INITIAL_DEFERRED_CALL_DELAY,
                TRpcRequestSettings::Make(settings));

        return future;
    }

    TAsyncDescribeQueryResult DescribeQuery(
        const FederatedQuery::DescribeQueryRequest& protoRequest,
        const TDescribeQuerySettings& settings) {
        auto request = MakeOperationRequest<FederatedQuery::DescribeQueryRequest>(settings);
        request = protoRequest;

        auto promise = NThreading::NewPromise<TDescribeQueryResult>();
        auto future = promise.GetFuture();

        auto extractor = MakeResultExtractor<
            FederatedQuery::DescribeQueryResult,
            TDescribeQueryResult>(std::move(promise));

        Connections_->RunDeferred<
            FederatedQuery::V1::FederatedQueryService,
            FederatedQuery::DescribeQueryRequest,
            FederatedQuery::DescribeQueryResponse>(
                std::move(request),
                std::move(extractor),
                &FederatedQuery::V1::FederatedQueryService::Stub::AsyncDescribeQuery,
                DbDriverState_,
                INITIAL_DEFERRED_CALL_DELAY,
                TRpcRequestSettings::Make(settings));

        return future;
    }

    TAsyncGetQueryStatusResult GetQueryStatus(
        const FederatedQuery::GetQueryStatusRequest& protoRequest,
        const TGetQueryStatusSettings& settings) {
        auto request = MakeOperationRequest<FederatedQuery::GetQueryStatusRequest>(settings);
        request = protoRequest;

        auto promise = NThreading::NewPromise<TGetQueryStatusResult>();
        auto future = promise.GetFuture();

        auto extractor = MakeResultExtractor<
            FederatedQuery::GetQueryStatusResult,
            TGetQueryStatusResult>(std::move(promise));

        Connections_->RunDeferred<
            FederatedQuery::V1::FederatedQueryService,
            FederatedQuery::GetQueryStatusRequest,
            FederatedQuery::GetQueryStatusResponse>(
                std::move(request),
                std::move(extractor),
                &FederatedQuery::V1::FederatedQueryService::Stub::AsyncGetQueryStatus,
                DbDriverState_,
                INITIAL_DEFERRED_CALL_DELAY,
                TRpcRequestSettings::Make(settings));

        return future;
    }

    TAsyncModifyQueryResult ModifyQuery(
        const FederatedQuery::ModifyQueryRequest& protoRequest,
        const TModifyQuerySettings& settings) {
        auto request = MakeOperationRequest<FederatedQuery::ModifyQueryRequest>(settings);
        request = protoRequest;

        auto promise = NThreading::NewPromise<TModifyQueryResult>();
        auto future = promise.GetFuture();

        auto extractor = MakeResultExtractor<
            FederatedQuery::ModifyQueryResult,
            TModifyQueryResult>(std::move(promise));

        Connections_->RunDeferred<
            FederatedQuery::V1::FederatedQueryService,
            FederatedQuery::ModifyQueryRequest,
            FederatedQuery::ModifyQueryResponse>(
                std::move(request),
                std::move(extractor),
                &FederatedQuery::V1::FederatedQueryService::Stub::AsyncModifyQuery,
                DbDriverState_,
                INITIAL_DEFERRED_CALL_DELAY,
                TRpcRequestSettings::Make(settings));

        return future;
    }

    TAsyncDeleteQueryResult DeleteQuery(
        const FederatedQuery::DeleteQueryRequest& protoRequest,
        const TDeleteQuerySettings& settings) {
        auto request = MakeOperationRequest<FederatedQuery::DeleteQueryRequest>(settings);
        request = protoRequest;

        auto promise = NThreading::NewPromise<TDeleteQueryResult>();
        auto future = promise.GetFuture();

        auto extractor = MakeResultExtractor<
            FederatedQuery::DeleteQueryResult,
            TDeleteQueryResult>(std::move(promise));

        Connections_->RunDeferred<
            FederatedQuery::V1::FederatedQueryService,
            FederatedQuery::DeleteQueryRequest,
            FederatedQuery::DeleteQueryResponse>(
                std::move(request),
                std::move(extractor),
                &FederatedQuery::V1::FederatedQueryService::Stub::AsyncDeleteQuery,
                DbDriverState_,
                INITIAL_DEFERRED_CALL_DELAY,
                TRpcRequestSettings::Make(settings));

        return future;
    }

    TAsyncControlQueryResult ControlQuery(
        const FederatedQuery::ControlQueryRequest& protoRequest,
        const TControlQuerySettings& settings) {
        auto request = MakeOperationRequest<FederatedQuery::ControlQueryRequest>(settings);
        request = protoRequest;

        auto promise = NThreading::NewPromise<TControlQueryResult>();
        auto future = promise.GetFuture();

        auto extractor = MakeResultExtractor<
            FederatedQuery::ControlQueryResult,
            TControlQueryResult>(std::move(promise));

        Connections_->RunDeferred<
            FederatedQuery::V1::FederatedQueryService,
            FederatedQuery::ControlQueryRequest,
            FederatedQuery::ControlQueryResponse>(
                std::move(request),
                std::move(extractor),
                &FederatedQuery::V1::FederatedQueryService::Stub::AsyncControlQuery,
                DbDriverState_,
                INITIAL_DEFERRED_CALL_DELAY,
                TRpcRequestSettings::Make(settings));

        return future;
    }

    TAsyncGetResultDataResult GetResultData(
        const FederatedQuery::GetResultDataRequest& protoRequest,
        const TGetResultDataSettings& settings) {
        auto request = MakeOperationRequest<FederatedQuery::GetResultDataRequest>(settings);
        request = protoRequest;

        auto promise = NThreading::NewPromise<TGetResultDataResult>();
        auto future = promise.GetFuture();

        auto extractor = MakeResultExtractor<
            FederatedQuery::GetResultDataResult,
            TGetResultDataResult>(std::move(promise));

        Connections_->RunDeferred<
            FederatedQuery::V1::FederatedQueryService,
            FederatedQuery::GetResultDataRequest,
            FederatedQuery::GetResultDataResponse>(
                std::move(request),
                std::move(extractor),
                &FederatedQuery::V1::FederatedQueryService::Stub::AsyncGetResultData,
                DbDriverState_,
                INITIAL_DEFERRED_CALL_DELAY,
                TRpcRequestSettings::Make(settings));

        return future;
    }

    TAsyncListJobsResult ListJobs(
        const FederatedQuery::ListJobsRequest& protoRequest,
        const TListJobsSettings& settings) {
        auto request = MakeOperationRequest<FederatedQuery::ListJobsRequest>(settings);
        request = protoRequest;

        auto promise = NThreading::NewPromise<TListJobsResult>();
        auto future = promise.GetFuture();

        auto extractor = MakeResultExtractor<
            FederatedQuery::ListJobsResult,
            TListJobsResult>(std::move(promise));

        Connections_->RunDeferred<
            FederatedQuery::V1::FederatedQueryService,
            FederatedQuery::ListJobsRequest,
            FederatedQuery::ListJobsResponse>(
                std::move(request),
                std::move(extractor),
                &FederatedQuery::V1::FederatedQueryService::Stub::AsyncListJobs,
                DbDriverState_,
                INITIAL_DEFERRED_CALL_DELAY,
                TRpcRequestSettings::Make(settings));

        return future;
    }

    TAsyncDescribeJobResult DescribeJob(
        const FederatedQuery::DescribeJobRequest& protoRequest,
        const TDescribeJobSettings& settings) {
        auto request = MakeOperationRequest<FederatedQuery::DescribeJobRequest>(settings);
        request = protoRequest;

        auto promise = NThreading::NewPromise<TDescribeJobResult>();
        auto future = promise.GetFuture();

        auto extractor = MakeResultExtractor<
            FederatedQuery::DescribeJobResult,
            TDescribeJobResult>(std::move(promise));

        Connections_->RunDeferred<
            FederatedQuery::V1::FederatedQueryService,
            FederatedQuery::DescribeJobRequest,
            FederatedQuery::DescribeJobResponse>(
                std::move(request),
                std::move(extractor),
                &FederatedQuery::V1::FederatedQueryService::Stub::AsyncDescribeJob,
                DbDriverState_,
                INITIAL_DEFERRED_CALL_DELAY,
                TRpcRequestSettings::Make(settings));

        return future;
    }

    TAsyncCreateConnectionResult CreateConnection(
        const FederatedQuery::CreateConnectionRequest& protoRequest,
        const TCreateConnectionSettings& settings) {
        auto request = MakeOperationRequest<FederatedQuery::CreateConnectionRequest>(settings);
        request = protoRequest;

        auto promise = NThreading::NewPromise<TCreateConnectionResult>();
        auto future = promise.GetFuture();

        auto extractor = MakeResultExtractor<
            FederatedQuery::CreateConnectionResult,
            TCreateConnectionResult>(std::move(promise));

        Connections_->RunDeferred<
            FederatedQuery::V1::FederatedQueryService,
            FederatedQuery::CreateConnectionRequest,
            FederatedQuery::CreateConnectionResponse>(
                std::move(request),
                std::move(extractor),
                &FederatedQuery::V1::FederatedQueryService::Stub::AsyncCreateConnection,
                DbDriverState_,
                INITIAL_DEFERRED_CALL_DELAY,
                TRpcRequestSettings::Make(settings));

        return future;
    }

    TAsyncListConnectionsResult ListConnections(
        const FederatedQuery::ListConnectionsRequest& protoRequest,
        const TListConnectionsSettings& settings) {
        auto request = MakeOperationRequest<FederatedQuery::ListConnectionsRequest>(settings);
        request = protoRequest;

        auto promise = NThreading::NewPromise<TListConnectionsResult>();
        auto future = promise.GetFuture();

        auto extractor = MakeResultExtractor<
            FederatedQuery::ListConnectionsResult,
            TListConnectionsResult>(std::move(promise));

        Connections_->RunDeferred<
            FederatedQuery::V1::FederatedQueryService,
            FederatedQuery::ListConnectionsRequest,
            FederatedQuery::ListConnectionsResponse>(
                std::move(request),
                std::move(extractor),
                &FederatedQuery::V1::FederatedQueryService::Stub::AsyncListConnections,
                DbDriverState_,
                INITIAL_DEFERRED_CALL_DELAY,
                TRpcRequestSettings::Make(settings));

        return future;
    }

    TAsyncDescribeConnectionResult DescribeConnection(
        const FederatedQuery::DescribeConnectionRequest& protoRequest,
        const TDescribeConnectionSettings& settings) {
        auto request = MakeOperationRequest<FederatedQuery::DescribeConnectionRequest>(settings);
        request = protoRequest;

        auto promise = NThreading::NewPromise<TDescribeConnectionResult>();
        auto future = promise.GetFuture();

        auto extractor = MakeResultExtractor<
            FederatedQuery::DescribeConnectionResult,
            TDescribeConnectionResult>(std::move(promise));

        Connections_->RunDeferred<
            FederatedQuery::V1::FederatedQueryService,
            FederatedQuery::DescribeConnectionRequest,
            FederatedQuery::DescribeConnectionResponse>(
                std::move(request),
                std::move(extractor),
                &FederatedQuery::V1::FederatedQueryService::Stub::AsyncDescribeConnection,
                DbDriverState_,
                INITIAL_DEFERRED_CALL_DELAY,
                TRpcRequestSettings::Make(settings));

        return future;
    }

    TAsyncModifyConnectionResult ModifyConnection(
        const FederatedQuery::ModifyConnectionRequest& protoRequest,
        const TModifyConnectionSettings& settings) {
        auto request = MakeOperationRequest<FederatedQuery::ModifyConnectionRequest>(settings);
        request = protoRequest;

        auto promise = NThreading::NewPromise<TModifyConnectionResult>();
        auto future = promise.GetFuture();

        auto extractor = MakeResultExtractor<
            FederatedQuery::ModifyConnectionResult,
            TModifyConnectionResult>(std::move(promise));

        Connections_->RunDeferred<
            FederatedQuery::V1::FederatedQueryService,
            FederatedQuery::ModifyConnectionRequest,
            FederatedQuery::ModifyConnectionResponse>(
                std::move(request),
                std::move(extractor),
                &FederatedQuery::V1::FederatedQueryService::Stub::AsyncModifyConnection,
                DbDriverState_,
                INITIAL_DEFERRED_CALL_DELAY,
                TRpcRequestSettings::Make(settings));

        return future;
    }

    TAsyncDeleteConnectionResult DeleteConnection(
        const FederatedQuery::DeleteConnectionRequest& protoRequest,
        const TDeleteConnectionSettings& settings) {
        auto request = MakeOperationRequest<FederatedQuery::DeleteConnectionRequest>(settings);
        request = protoRequest;

        auto promise = NThreading::NewPromise<TDeleteConnectionResult>();
        auto future = promise.GetFuture();

        auto extractor = MakeResultExtractor<
            FederatedQuery::DeleteConnectionResult,
            TDeleteConnectionResult>(std::move(promise));

        Connections_->RunDeferred<
            FederatedQuery::V1::FederatedQueryService,
            FederatedQuery::DeleteConnectionRequest,
            FederatedQuery::DeleteConnectionResponse>(
                std::move(request),
                std::move(extractor),
                &FederatedQuery::V1::FederatedQueryService::Stub::AsyncDeleteConnection,
                DbDriverState_,
                INITIAL_DEFERRED_CALL_DELAY,
                TRpcRequestSettings::Make(settings));

        return future;
    }

    TAsyncTestConnectionResult TestConnection(
        const FederatedQuery::TestConnectionRequest& protoRequest,
        const TTestConnectionSettings& settings) {
        auto request = MakeOperationRequest<FederatedQuery::TestConnectionRequest>(settings);
        request = protoRequest;

        auto promise = NThreading::NewPromise<TTestConnectionResult>();
        auto future = promise.GetFuture();

        auto extractor = MakeResultExtractor<
            FederatedQuery::TestConnectionResult,
            TTestConnectionResult>(std::move(promise));

        Connections_->RunDeferred<
            FederatedQuery::V1::FederatedQueryService,
            FederatedQuery::TestConnectionRequest,
            FederatedQuery::TestConnectionResponse>(
                    std::move(request),
                    std::move(extractor),
                    &FederatedQuery::V1::FederatedQueryService::Stub::AsyncTestConnection,
                    DbDriverState_,
                    INITIAL_DEFERRED_CALL_DELAY,
                    TRpcRequestSettings::Make(settings));

        return future;
    }

    TAsyncCreateBindingResult CreateBinding(
        const FederatedQuery::CreateBindingRequest& protoRequest,
        const TCreateBindingSettings& settings) {
        auto request = MakeOperationRequest<FederatedQuery::CreateBindingRequest>(settings);
        request = protoRequest;

        auto promise = NThreading::NewPromise<TCreateBindingResult>();
        auto future = promise.GetFuture();

        auto extractor = MakeResultExtractor<
            FederatedQuery::CreateBindingResult,
            TCreateBindingResult>(std::move(promise));

        Connections_->RunDeferred<
            FederatedQuery::V1::FederatedQueryService,
            FederatedQuery::CreateBindingRequest,
            FederatedQuery::CreateBindingResponse>(
                std::move(request),
                std::move(extractor),
                &FederatedQuery::V1::FederatedQueryService::Stub::AsyncCreateBinding,
                DbDriverState_,
                INITIAL_DEFERRED_CALL_DELAY,
                TRpcRequestSettings::Make(settings));

        return future;
    }

    TAsyncListBindingsResult ListBindings(
        const FederatedQuery::ListBindingsRequest& protoRequest,
        const TListBindingsSettings& settings) {
        auto request = MakeOperationRequest<FederatedQuery::ListBindingsRequest>(settings);
        request = protoRequest;

        auto promise = NThreading::NewPromise<TListBindingsResult>();
        auto future = promise.GetFuture();

        auto extractor = MakeResultExtractor<
            FederatedQuery::ListBindingsResult,
            TListBindingsResult>(std::move(promise));

        Connections_->RunDeferred<
            FederatedQuery::V1::FederatedQueryService,
            FederatedQuery::ListBindingsRequest,
            FederatedQuery::ListBindingsResponse>(
                std::move(request),
                std::move(extractor),
                &FederatedQuery::V1::FederatedQueryService::Stub::AsyncListBindings,
                DbDriverState_,
                INITIAL_DEFERRED_CALL_DELAY,
                TRpcRequestSettings::Make(settings));

        return future;
    }

    TAsyncDescribeBindingResult DescribeBinding(
        const FederatedQuery::DescribeBindingRequest& protoRequest,
        const TDescribeBindingSettings& settings) {
        auto request = MakeOperationRequest<FederatedQuery::DescribeBindingRequest>(settings);
        request = protoRequest;

        auto promise = NThreading::NewPromise<TDescribeBindingResult>();
        auto future = promise.GetFuture();

        auto extractor = MakeResultExtractor<
            FederatedQuery::DescribeBindingResult,
            TDescribeBindingResult>(std::move(promise));

        Connections_->RunDeferred<
            FederatedQuery::V1::FederatedQueryService,
            FederatedQuery::DescribeBindingRequest,
            FederatedQuery::DescribeBindingResponse>(
                std::move(request),
                std::move(extractor),
                &FederatedQuery::V1::FederatedQueryService::Stub::AsyncDescribeBinding,
                DbDriverState_,
                INITIAL_DEFERRED_CALL_DELAY,
                TRpcRequestSettings::Make(settings));

        return future;
    }

    TAsyncModifyBindingResult ModifyBinding(
        const FederatedQuery::ModifyBindingRequest& protoRequest,
        const TModifyBindingSettings& settings) {
        auto request = MakeOperationRequest<FederatedQuery::ModifyBindingRequest>(settings);
        request = protoRequest;

        auto promise = NThreading::NewPromise<TModifyBindingResult>();
        auto future = promise.GetFuture();

        auto extractor = MakeResultExtractor<
            FederatedQuery::ModifyBindingResult,
            TModifyBindingResult>(std::move(promise));

        Connections_->RunDeferred<
            FederatedQuery::V1::FederatedQueryService,
            FederatedQuery::ModifyBindingRequest,
            FederatedQuery::ModifyBindingResponse>(
                std::move(request),
                std::move(extractor),
                &FederatedQuery::V1::FederatedQueryService::Stub::AsyncModifyBinding,
                DbDriverState_,
                INITIAL_DEFERRED_CALL_DELAY,
                TRpcRequestSettings::Make(settings));

        return future;
    }

    TAsyncDeleteBindingResult DeleteBinding(
        const FederatedQuery::DeleteBindingRequest& protoRequest,
        const TDeleteBindingSettings& settings) {
        auto request = MakeOperationRequest<FederatedQuery::DeleteBindingRequest>(settings);
        request = protoRequest;

        auto promise = NThreading::NewPromise<TDeleteBindingResult>();
        auto future = promise.GetFuture();

        auto extractor = MakeResultExtractor<
            FederatedQuery::DeleteBindingResult,
            TDeleteBindingResult>(std::move(promise));

        Connections_->RunDeferred<
            FederatedQuery::V1::FederatedQueryService,
            FederatedQuery::DeleteBindingRequest,
            FederatedQuery::DeleteBindingResponse>(
                std::move(request),
                std::move(extractor),
                &FederatedQuery::V1::FederatedQueryService::Stub::AsyncDeleteBinding,
                DbDriverState_,
                INITIAL_DEFERRED_CALL_DELAY,
                TRpcRequestSettings::Make(settings));

        return future;
    }
};

TClient::TClient(const TDriver& driver, const TCommonClientSettings& settings)
    : Impl_(new TImpl(CreateInternalInterface(driver), settings))
{}

TAsyncCreateQueryResult TClient::CreateQuery(
    const FederatedQuery::CreateQueryRequest& request,
    const TCreateQuerySettings& settings) {
    return Impl_->CreateQuery(request, settings);
}

TAsyncListQueriesResult TClient::ListQueries(
    const FederatedQuery::ListQueriesRequest& request,
    const TListQueriesSettings& settings) {
    return Impl_->ListQueries(request, settings);
}

TAsyncDescribeQueryResult TClient::DescribeQuery(
    const FederatedQuery::DescribeQueryRequest& request,
    const TDescribeQuerySettings& settings) {
    return Impl_->DescribeQuery(request, settings);
}

TAsyncGetQueryStatusResult TClient::GetQueryStatus(
    const FederatedQuery::GetQueryStatusRequest& request,
    const TGetQueryStatusSettings& settings) {
    return Impl_->GetQueryStatus(request, settings);
}

TAsyncModifyQueryResult TClient::ModifyQuery(
    const FederatedQuery::ModifyQueryRequest& request,
    const TModifyQuerySettings& settings) {
    return Impl_->ModifyQuery(request, settings);
}

TAsyncDeleteQueryResult TClient::DeleteQuery(
    const FederatedQuery::DeleteQueryRequest& request,
    const TDeleteQuerySettings& settings) {
    return Impl_->DeleteQuery(request, settings);
}

TAsyncControlQueryResult TClient::ControlQuery(
    const FederatedQuery::ControlQueryRequest& request,
    const TControlQuerySettings& settings) {
    return Impl_->ControlQuery(request, settings);
}

TAsyncGetResultDataResult TClient::GetResultData(
    const FederatedQuery::GetResultDataRequest& request,
    const TGetResultDataSettings& settings) {
    return Impl_->GetResultData(request, settings);
}

TAsyncListJobsResult TClient::ListJobs(
    const FederatedQuery::ListJobsRequest& request,
    const TListJobsSettings& settings) {
    return Impl_->ListJobs(request, settings);
}

TAsyncDescribeJobResult TClient::DescribeJob(
    const FederatedQuery::DescribeJobRequest& request,
    const TDescribeJobSettings& settings) {
    return Impl_->DescribeJob(request, settings);
}

TAsyncCreateConnectionResult TClient::CreateConnection(
    const FederatedQuery::CreateConnectionRequest& request,
    const TCreateConnectionSettings& settings) {
    return Impl_->CreateConnection(request, settings);
}

TAsyncListConnectionsResult TClient::ListConnections(
    const FederatedQuery::ListConnectionsRequest& request,
    const TListConnectionsSettings& settings) {
    return Impl_->ListConnections(request, settings);
}

TAsyncDescribeConnectionResult TClient::DescribeConnection(
    const FederatedQuery::DescribeConnectionRequest& request,
    const TDescribeConnectionSettings& settings) {
    return Impl_->DescribeConnection(request, settings);
}

TAsyncModifyConnectionResult TClient::ModifyConnection(
    const FederatedQuery::ModifyConnectionRequest& request,
    const TModifyConnectionSettings& settings) {
    return Impl_->ModifyConnection(request, settings);
}

TAsyncDeleteConnectionResult TClient::DeleteConnection(
    const FederatedQuery::DeleteConnectionRequest& request,
    const TDeleteConnectionSettings& settings) {
    return Impl_->DeleteConnection(request, settings);
}

TAsyncTestConnectionResult TClient::TestConnection(
    const FederatedQuery::TestConnectionRequest& request,
    const TTestConnectionSettings& settings) {
    return Impl_->TestConnection(request, settings);
}

TAsyncCreateBindingResult TClient::CreateBinding(
    const FederatedQuery::CreateBindingRequest& request,
    const TCreateBindingSettings& settings) {
    return Impl_->CreateBinding(request, settings);
}

TAsyncListBindingsResult TClient::ListBindings(
    const FederatedQuery::ListBindingsRequest& request,
    const TListBindingsSettings& settings) {
    return Impl_->ListBindings(request, settings);
}

TAsyncDescribeBindingResult TClient::DescribeBinding(
    const FederatedQuery::DescribeBindingRequest& request,
    const TDescribeBindingSettings& settings) {
    return Impl_->DescribeBinding(request, settings);
}

TAsyncModifyBindingResult TClient::ModifyBinding(
    const FederatedQuery::ModifyBindingRequest& request,
    const TModifyBindingSettings& settings) {
    return Impl_->ModifyBinding(request, settings);
}

TAsyncDeleteBindingResult TClient::DeleteBinding(
    const FederatedQuery::DeleteBindingRequest& request,
    const TDeleteBindingSettings& settings) {
    return Impl_->DeleteBinding(request, settings);
}

}
