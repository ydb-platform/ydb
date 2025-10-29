#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/monitoring/monitoring.h>

#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/src/client/impl/internal/make_request/make.h>
#undef INCLUDE_YDB_INTERNAL_H

#include <ydb/public/api/grpc/ydb_monitoring_v1.grpc.pb.h>
#include <ydb/public/api/protos/ydb_monitoring.pb.h>
#include <ydb/public/sdk/cpp/src/client/common_client/impl/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>

namespace NYdb::inline Dev {
namespace NMonitoring {

using namespace NThreading;

class TSelfCheckResult::TImpl {
public:
    TImpl(Ydb::Monitoring::SelfCheckResult&& result)
        : Result(std::move(result))
    {}
    Ydb::Monitoring::SelfCheckResult Result;
};

class TClusterStateResult::TImpl {
public:
    TImpl(Ydb::Monitoring::ClusterStateResult&& result)
        : Result(std::move(result))
    {}
    Ydb::Monitoring::ClusterStateResult Result;
};

TSelfCheckResult::TSelfCheckResult(TStatus&& status, Ydb::Monitoring::SelfCheckResult&& result)
    : TStatus(std::move(status))
    , Impl_(std::make_shared<TSelfCheckResult::TImpl>(std::move(result)))
{}

TClusterStateResult::TClusterStateResult(TStatus&& status, Ydb::Monitoring::ClusterStateResult&& result)
    : TStatus(std::move(status))
    , Impl_(std::make_shared<TClusterStateResult::TImpl>(std::move(result)))
{}

class TMonitoringClient::TImpl : public TClientImplCommon<TMonitoringClient::TImpl> {
public:
    TImpl(std::shared_ptr<TGRpcConnectionsImpl>&& connections, const TCommonClientSettings& settings)
            : TClientImplCommon(std::move(connections), settings)
    {}

    TAsyncSelfCheckResult SelfCheck(const TSelfCheckSettings& settings) {
        auto request = MakeOperationRequest<Ydb::Monitoring::SelfCheckRequest>(settings);

        if (settings.ReturnVerboseStatus_) {
            request.set_return_verbose_status(settings.ReturnVerboseStatus_.value());
        }

        if (settings.NoMerge_) {
            request.set_merge_records(!settings.NoMerge_.value());
        }

        if (settings.NoCache_) {
            request.set_do_not_cache(settings.NoCache_.value());
        }

        if (settings.MinimumStatus_) {
            request.set_minimum_status((::Ydb::Monitoring::StatusFlag_Status)settings.MinimumStatus_.value());
        }

        if (settings.MaximumLevel_) {
            request.set_maximum_level(settings.MaximumLevel_.value());
        }

        auto promise = NThreading::NewPromise<TSelfCheckResult>();

        auto extractor = [promise]
            (google::protobuf::Any* any, TPlainStatus status) mutable {
                Ydb::Monitoring::SelfCheckResult result;
                if (any) {
                    any->UnpackTo(&result);
                }
                TSelfCheckResult val(
                    TStatus(std::move(status)),
                    std::move(result));

                promise.SetValue(std::move(val));
        };

        using Ydb::Monitoring::SelfCheckRequest;
        using Ydb::Monitoring::SelfCheckResponse;

        auto requestSettings = TRpcRequestSettings::Make(settings);
        requestSettings.EndpointPolicy = TRpcRequestSettings::TEndpointPolicy::UseDiscoveryEndpoint;

        Connections_->RunDeferred<Ydb::Monitoring::V1::MonitoringService, SelfCheckRequest, SelfCheckResponse>(
            std::move(request),
            extractor,
            &Ydb::Monitoring::V1::MonitoringService::Stub::AsyncSelfCheck,
            DbDriverState_,
            INITIAL_DEFERRED_CALL_DELAY,
            requestSettings);

        return promise.GetFuture();
    }

    TAsyncClusterStateResult ClusterState(const TClusterStateSettings& settings) {
        auto request = MakeOperationRequest<Ydb::Monitoring::ClusterStateRequest>(settings);

        if (settings.DurationSeconds_) {
            request.set_duration_seconds(settings.DurationSeconds_.value());
        }

        if (settings.PeriodSeconds_) {
            request.set_period_seconds(settings.PeriodSeconds_.value());
        }
        auto promise = NThreading::NewPromise<TClusterStateResult>();

        auto extractor = [promise]
            (google::protobuf::Any* any, TPlainStatus status) mutable {
                Ydb::Monitoring::ClusterStateResult result;
                if (any) {
                    any->UnpackTo(&result);
                }
                TClusterStateResult val(
                    TStatus(std::move(status)),
                    std::move(result));

                promise.SetValue(std::move(val));
        };

        using Ydb::Monitoring::ClusterStateRequest;
        using Ydb::Monitoring::ClusterStateResponse;

        Connections_->RunDeferred<Ydb::Monitoring::V1::MonitoringService, ClusterStateRequest, ClusterStateResponse>(
            std::move(request),
            extractor,
            &Ydb::Monitoring::V1::MonitoringService::Stub::AsyncClusterState,
            DbDriverState_,
            INITIAL_DEFERRED_CALL_DELAY,
            TRpcRequestSettings::Make(settings));

        return promise.GetFuture();
    }
};

TMonitoringClient::TMonitoringClient(const TDriver& driver, const TCommonClientSettings& settings)
    : Impl_(new TImpl(CreateInternalInterface(driver), settings))
{}

TAsyncSelfCheckResult TMonitoringClient::SelfCheck(const TSelfCheckSettings& settings) {
    return Impl_->SelfCheck(settings);
}

TAsyncClusterStateResult TMonitoringClient::ClusterState(const TClusterStateSettings& settings) {
    return Impl_->ClusterState(settings);
}

}

const Ydb::Monitoring::SelfCheckResult& TProtoAccessor::GetProto(const NYdb::NMonitoring::TSelfCheckResult& selfCheckResult) {
    return selfCheckResult.Impl_->Result;
}

const Ydb::Monitoring::ClusterStateResult& TProtoAccessor::GetProto(const NYdb::NMonitoring::TClusterStateResult& clusterStateResult) {
    return clusterStateResult.Impl_->Result;
}
}
