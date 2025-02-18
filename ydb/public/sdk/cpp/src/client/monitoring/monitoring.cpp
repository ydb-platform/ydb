#include <ydb-cpp-sdk/client/monitoring/monitoring.h>

#define INCLUDE_YDB_INTERNAL_H
#include <src/client/impl/ydb_internal/make_request/make.h>
#undef INCLUDE_YDB_INTERNAL_H

#include <ydb/public/api/grpc/ydb_monitoring_v1.grpc.pb.h>
#include <ydb/public/api/protos/ydb_monitoring.pb.h>
#include <src/client/common_client/impl/client.h>
#include <ydb-cpp-sdk/client/proto/accessor.h>

namespace NYdb::inline V3 {
namespace NMonitoring {

using namespace NThreading;

class TSelfCheckResult::TImpl {
public:
    TImpl(NYdbProtos::Monitoring::SelfCheckResult&& result)
        : Result(std::move(result))
    {}
    NYdbProtos::Monitoring::SelfCheckResult Result;
};

TSelfCheckResult::TSelfCheckResult(TStatus&& status, NYdbProtos::Monitoring::SelfCheckResult&& result)
    : TStatus(std::move(status))
    , Impl_(std::make_shared<TSelfCheckResult::TImpl>(std::move(result)))
{}

class TMonitoringClient::TImpl : public TClientImplCommon<TMonitoringClient::TImpl> {
public:
    TImpl(std::shared_ptr<TGRpcConnectionsImpl>&& connections, const TCommonClientSettings& settings)
            : TClientImplCommon(std::move(connections), settings)
    {}

    TAsyncSelfCheckResult SelfCheck(const TSelfCheckSettings& settings) {
        auto request = MakeOperationRequest<NYdbProtos::Monitoring::SelfCheckRequest>(settings);

        if (settings.ReturnVerboseStatus_) {
            request.set_return_verbose_status(settings.ReturnVerboseStatus_.value());
        }

        if (settings.MinimumStatus_) {
            request.set_minimum_status((::NYdbProtos::Monitoring::StatusFlag_Status)settings.MinimumStatus_.value());
        }

        if (settings.MaximumLevel_) {
            request.set_maximum_level(settings.MaximumLevel_.value());
        }

        auto promise = NThreading::NewPromise<TSelfCheckResult>();

        auto extractor = [promise]
            (google::protobuf::Any* any, TPlainStatus status) mutable {
                NYdbProtos::Monitoring::SelfCheckResult result;
                if (any) {
                    any->UnpackTo(&result);
                }
                TSelfCheckResult val(
                    TStatus(std::move(status)),
                    std::move(result));

                promise.SetValue(std::move(val));
        };

        using NYdbProtos::Monitoring::SelfCheckRequest;
        using NYdbProtos::Monitoring::SelfCheckResponse;

        auto requestSettings = TRpcRequestSettings::Make(settings);
        requestSettings.EndpointPolicy = TRpcRequestSettings::TEndpointPolicy::UseDiscoveryEndpoint;

        Connections_->RunDeferred<NYdbProtos::Monitoring::V1::MonitoringService, SelfCheckRequest, SelfCheckResponse>(
            std::move(request),
            extractor,
            &NYdbProtos::Monitoring::V1::MonitoringService::Stub::AsyncSelfCheck,
            DbDriverState_,
            INITIAL_DEFERRED_CALL_DELAY,
            requestSettings);

        return promise.GetFuture();
    }
};

TMonitoringClient::TMonitoringClient(const TDriver& driver, const TCommonClientSettings& settings)
    : Impl_(new TImpl(CreateInternalInterface(driver), settings))
{}

TAsyncSelfCheckResult TMonitoringClient::SelfCheck(const TSelfCheckSettings& settings) {
    return Impl_->SelfCheck(settings);
}

}

const NYdbProtos::Monitoring::SelfCheckResult& TProtoAccessor::GetProto(const NYdb::NMonitoring::TSelfCheckResult& selfCheckResult) {
    return selfCheckResult.Impl_->Result;
}

}
