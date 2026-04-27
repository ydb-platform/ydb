#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_actor_tracing.h>

#include <ydb/public/sdk/cpp/src/client/common_client/impl/client.h>
#include <ydb/public/sdk/cpp/src/client/impl/internal/make_request/make.h>

#include <ydb/public/api/grpc/ydb_actor_tracing_v1.grpc.pb.h>
#include <ydb/public/api/protos/ydb_actor_tracing.pb.h>

namespace NYdb::inline Dev::NActorTracing {

class TActorTracingClient::TImpl : public TClientImplCommon<TActorTracingClient::TImpl> {
public:
    TImpl(std::shared_ptr<TGRpcConnectionsImpl> connections, const TCommonClientSettings& settings)
        : TClientImplCommon(std::move(connections), settings)
    {}

    TAsyncStatus TraceStart(const TTraceStartSettings& settings) {
        auto request = MakeOperationRequest<Ydb::ActorTracing::TraceStartRequest>(settings);
        for (uint32_t id : settings.NodeIds_) {
            request.add_node_ids(id);
        }
        return RunSimple<
            Ydb::ActorTracing::V1::ActorTracingService,
            Ydb::ActorTracing::TraceStartRequest,
            Ydb::ActorTracing::TraceStartResponse>(
            std::move(request),
            &Ydb::ActorTracing::V1::ActorTracingService::Stub::AsyncTraceStart,
            TRpcRequestSettings::Make(settings));
    }

    TAsyncStatus TraceStop(const TTraceStopSettings& settings) {
        auto request = MakeOperationRequest<Ydb::ActorTracing::TraceStopRequest>(settings);
        for (uint32_t id : settings.NodeIds_) {
            request.add_node_ids(id);
        }
        return RunSimple<
            Ydb::ActorTracing::V1::ActorTracingService,
            Ydb::ActorTracing::TraceStopRequest,
            Ydb::ActorTracing::TraceStopResponse>(
            std::move(request),
            &Ydb::ActorTracing::V1::ActorTracingService::Stub::AsyncTraceStop,
            TRpcRequestSettings::Make(settings));
    }

    TAsyncTraceFetchResult TraceFetch(const TTraceFetchSettings& settings) {
        auto request = MakeOperationRequest<Ydb::ActorTracing::TraceFetchRequest>(settings);
        for (uint32_t id : settings.NodeIds_) {
            request.add_node_ids(id);
        }
        auto promise = NThreading::NewPromise<TTraceFetchResult>();

        auto extractor = [promise](google::protobuf::Any* any, TPlainStatus status) mutable {
            std::string traceData;
            if (any) {
                Ydb::ActorTracing::TraceFetchResult result;
                if (any->UnpackTo(&result)) {
                    traceData = result.trace_data();
                }
            }
            promise.SetValue(TTraceFetchResult(TStatus(std::move(status)), std::move(traceData)));
        };

        Connections_->RunDeferred<
            Ydb::ActorTracing::V1::ActorTracingService,
            Ydb::ActorTracing::TraceFetchRequest,
            Ydb::ActorTracing::TraceFetchResponse>(
            std::move(request),
            extractor,
            &Ydb::ActorTracing::V1::ActorTracingService::Stub::AsyncTraceFetch,
            DbDriverState_,
            INITIAL_DEFERRED_CALL_DELAY,
            TRpcRequestSettings::Make(settings));

        return promise.GetFuture();
    }
};

TActorTracingClient::TActorTracingClient(const TDriver& driver, const TCommonClientSettings& settings)
    : Impl_(new TImpl(CreateInternalInterface(driver), settings))
{}

TActorTracingClient::~TActorTracingClient() = default;

TAsyncStatus TActorTracingClient::TraceStart(const TTraceStartSettings& settings) {
    return Impl_->TraceStart(settings);
}

TAsyncStatus TActorTracingClient::TraceStop(const TTraceStopSettings& settings) {
    return Impl_->TraceStop(settings);
}

TAsyncTraceFetchResult TActorTracingClient::TraceFetch(const TTraceFetchSettings& settings) {
    return Impl_->TraceFetch(settings);
}

} // namespace NYdb::NActorTracing
