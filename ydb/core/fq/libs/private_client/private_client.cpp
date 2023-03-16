#include "private_client.h"
#include <ydb/library/yql/public/issue/yql_issue.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>
#include <ydb/public/sdk/cpp/client/ydb_common_client/impl/client.h>

namespace NFq {

using namespace NYdb;

class TPrivateClient::TImpl : public TClientImplCommon<TPrivateClient::TImpl> {
public:
    TImpl(
        std::shared_ptr<TGRpcConnectionsImpl>&& connections,
        const TCommonClientSettings& settings,
        const NMonitoring::TDynamicCounterPtr& counters)
        : TClientImplCommon(std::move(connections), settings)
        , Counters(counters->GetSubgroup("subsystem", "private_api")->GetSubgroup("subcomponent", "ClientMetrics"))
        , PingTaskTime(Counters->GetHistogram("PingTaskMs", NMonitoring::ExponentialHistogram(10, 2, 50)))
        , GetTaskTime(Counters->GetHistogram("GetTaskMs", NMonitoring::ExponentialHistogram(10, 2, 50)))
        , WriteTaskResultTime(Counters->GetHistogram("WriteTaskResultMs", NMonitoring::ExponentialHistogram(10, 2, 50)))
        , NodesHealthCheckTime(Counters->GetHistogram("NodesHealthCheckMs", NMonitoring::ExponentialHistogram(10, 2, 50)))
        , CreateRateLimiterResourceTime(Counters->GetHistogram("CreateRateLimiterResourceMs", NMonitoring::ExponentialHistogram(10, 2, 50)))
        , DeleteRateLimiterResourceTime(Counters->GetHistogram("DeleteRateLimiterResourceMs", NMonitoring::ExponentialHistogram(10, 2, 50)))
    {}

    template<class TProtoResult, class TResultWrapper>
    auto MakeResultExtractor(NThreading::TPromise<TResultWrapper> promise, const NMonitoring::THistogramPtr& hist, TInstant startedAt) {
        return [=, promise = std::move(promise)]
            (google::protobuf::Any* any, TPlainStatus status) mutable {
                std::unique_ptr<TProtoResult> result;
                if (any) {
                    result.reset(new TProtoResult);
                    any->UnpackTo(result.get());
                }

                hist->Collect((TInstant::Now() - startedAt).MilliSeconds());

                promise.SetValue(
                    TResultWrapper(
                        TStatus(std::move(status)),
                        std::move(result)));
            };
    }

    TAsyncPingTaskResult PingTask(
        Fq::Private::PingTaskRequest&& request,
        const TPingTaskSettings& settings) {
        const auto startedAt = TInstant::Now();
        auto promise = NThreading::NewPromise<TPingTaskResult>();
        auto future = promise.GetFuture();

        auto extractor = MakeResultExtractor<
            Fq::Private::PingTaskResult,
            TPingTaskResult>(std::move(promise), PingTaskTime, startedAt);

        Connections_->RunDeferred<
            Fq::Private::V1::FqPrivateTaskService,
            Fq::Private::PingTaskRequest,
            Fq::Private::PingTaskResponse>(
                std::move(request),
                std::move(extractor),
                &Fq::Private::V1::FqPrivateTaskService::Stub::AsyncPingTask,
                DbDriverState_,
                INITIAL_DEFERRED_CALL_DELAY,
                TRpcRequestSettings::Make(settings));

        return future;
    }

    TAsyncGetTaskResult GetTask(
        Fq::Private::GetTaskRequest&& request,
        const TGetTaskSettings& settings) {
        const auto startedAt = TInstant::Now();
        auto promise = NThreading::NewPromise<TGetTaskResult>();
        auto future = promise.GetFuture();
        auto extractor = MakeResultExtractor<
            Fq::Private::GetTaskResult,
            TGetTaskResult>(std::move(promise), GetTaskTime, startedAt);

        Connections_->RunDeferred<
            Fq::Private::V1::FqPrivateTaskService,
            Fq::Private::GetTaskRequest,
            Fq::Private::GetTaskResponse>(
                std::move(request),
                std::move(extractor),
                &Fq::Private::V1::FqPrivateTaskService::Stub::AsyncGetTask,
                DbDriverState_,
                INITIAL_DEFERRED_CALL_DELAY,
                TRpcRequestSettings::Make(settings));

        return future;
    }

    TAsyncWriteTaskResult WriteTaskResult(
        Fq::Private::WriteTaskResultRequest&& request,
        const TWriteTaskResultSettings& settings) {
        const auto startedAt = TInstant::Now();
        auto promise = NThreading::NewPromise<TWriteTaskResult>();
        auto future = promise.GetFuture();
        auto extractor = MakeResultExtractor<
            Fq::Private::WriteTaskResultResult,
            TWriteTaskResult>(std::move(promise), WriteTaskResultTime, startedAt);

        Connections_->RunDeferred<
            Fq::Private::V1::FqPrivateTaskService,
            Fq::Private::WriteTaskResultRequest,
            Fq::Private::WriteTaskResultResponse>(
                std::move(request),
                std::move(extractor),
                &Fq::Private::V1::FqPrivateTaskService::Stub::AsyncWriteTaskResult,
                DbDriverState_,
                INITIAL_DEFERRED_CALL_DELAY,
                TRpcRequestSettings::Make(settings));

        return future;
    }

    TAsyncNodesHealthCheckResult NodesHealthCheck(
        Fq::Private::NodesHealthCheckRequest&& request,
        const TNodesHealthCheckSettings& settings) {
        const auto startedAt = TInstant::Now();
        auto promise = NThreading::NewPromise<TNodesHealthCheckResult>();
        auto future = promise.GetFuture();
        auto extractor = MakeResultExtractor<
            Fq::Private::NodesHealthCheckResult,
            TNodesHealthCheckResult>(std::move(promise), NodesHealthCheckTime, startedAt);

        Connections_->RunDeferred<
            Fq::Private::V1::FqPrivateTaskService,
            Fq::Private::NodesHealthCheckRequest,
            Fq::Private::NodesHealthCheckResponse>(
                std::move(request),
                std::move(extractor),
                &Fq::Private::V1::FqPrivateTaskService::Stub::AsyncNodesHealthCheck,
                DbDriverState_,
                INITIAL_DEFERRED_CALL_DELAY,
                TRpcRequestSettings::Make(settings));

        return future;
    }

    TAsyncCreateRateLimiterResourceResult CreateRateLimiterResource(
        Fq::Private::CreateRateLimiterResourceRequest&& request,
        const TCreateRateLimiterResourceSettings& settings) {
        const auto startedAt = TInstant::Now();
        auto promise = NThreading::NewPromise<TCreateRateLimiterResourceResult>();
        auto future = promise.GetFuture();

        auto extractor = MakeResultExtractor<
            Fq::Private::CreateRateLimiterResourceResult,
            TCreateRateLimiterResourceResult>(std::move(promise), CreateRateLimiterResourceTime, startedAt);

        Connections_->RunDeferred<
            Fq::Private::V1::FqPrivateTaskService,
            Fq::Private::CreateRateLimiterResourceRequest,
            Fq::Private::CreateRateLimiterResourceResponse>(
                std::move(request),
                std::move(extractor),
                &Fq::Private::V1::FqPrivateTaskService::Stub::AsyncCreateRateLimiterResource,
                DbDriverState_,
                INITIAL_DEFERRED_CALL_DELAY,
                TRpcRequestSettings::Make(settings));

        return future;
    }

    TAsyncDeleteRateLimiterResourceResult DeleteRateLimiterResource(
        Fq::Private::DeleteRateLimiterResourceRequest&& request,
        const TDeleteRateLimiterResourceSettings& settings) {
        const auto startedAt = TInstant::Now();
        auto promise = NThreading::NewPromise<TDeleteRateLimiterResourceResult>();
        auto future = promise.GetFuture();

        auto extractor = MakeResultExtractor<
            Fq::Private::DeleteRateLimiterResourceResult,
            TDeleteRateLimiterResourceResult>(std::move(promise), DeleteRateLimiterResourceTime, startedAt);

        Connections_->RunDeferred<
            Fq::Private::V1::FqPrivateTaskService,
            Fq::Private::DeleteRateLimiterResourceRequest,
            Fq::Private::DeleteRateLimiterResourceResponse>(
                std::move(request),
                std::move(extractor),
                &Fq::Private::V1::FqPrivateTaskService::Stub::AsyncDeleteRateLimiterResource,
                DbDriverState_,
                INITIAL_DEFERRED_CALL_DELAY,
                TRpcRequestSettings::Make(settings));

        return future;
    }
private:
    const NMonitoring::TDynamicCounterPtr Counters;
    const NMonitoring::THistogramPtr PingTaskTime;
    const NMonitoring::THistogramPtr GetTaskTime;
    const NMonitoring::THistogramPtr WriteTaskResultTime;
    const NMonitoring::THistogramPtr NodesHealthCheckTime;
    const NMonitoring::THistogramPtr CreateRateLimiterResourceTime;
    const NMonitoring::THistogramPtr DeleteRateLimiterResourceTime;
};

TPrivateClient::TPrivateClient(
    const TDriver& driver,
    const TCommonClientSettings& settings,
    const NMonitoring::TDynamicCounterPtr& counters)
    : Impl(new TImpl(CreateInternalInterface(driver), settings, counters))
{}

TAsyncPingTaskResult TPrivateClient::PingTask(
    Fq::Private::PingTaskRequest&& request,
    const TPingTaskSettings& settings) {
    return Impl->PingTask(std::move(request), settings);
}

TAsyncGetTaskResult TPrivateClient::GetTask(
    Fq::Private::GetTaskRequest&& request,
    const TGetTaskSettings& settings) {
    return Impl->GetTask(std::move(request), settings);
}

TAsyncWriteTaskResult TPrivateClient::WriteTaskResult(
    Fq::Private::WriteTaskResultRequest&& request,
    const TWriteTaskResultSettings& settings) {
    return Impl->WriteTaskResult(std::move(request), settings);
}

TAsyncNodesHealthCheckResult TPrivateClient::NodesHealthCheck(
    Fq::Private::NodesHealthCheckRequest&& request,
    const TNodesHealthCheckSettings& settings) {
    return Impl->NodesHealthCheck(std::move(request), settings);
}

TAsyncCreateRateLimiterResourceResult TPrivateClient::CreateRateLimiterResource(
    Fq::Private::CreateRateLimiterResourceRequest&& request,
    const TCreateRateLimiterResourceSettings& settings) {
    return Impl->CreateRateLimiterResource(std::move(request), settings);
}

TAsyncDeleteRateLimiterResourceResult TPrivateClient::DeleteRateLimiterResource(
    Fq::Private::DeleteRateLimiterResourceRequest&& request,
    const TDeleteRateLimiterResourceSettings& settings) {
    return Impl->DeleteRateLimiterResource(std::move(request), settings);
}

} //NFq
