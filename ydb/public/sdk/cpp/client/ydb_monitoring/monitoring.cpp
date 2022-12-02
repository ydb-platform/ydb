#include "monitoring.h"

#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/make_request/make.h>
#undef INCLUDE_YDB_INTERNAL_H

#include <ydb/public/api/grpc/ydb_monitoring_v1.grpc.pb.h>
#include <ydb/public/api/protos/ydb_monitoring.pb.h>
#include <ydb/public/sdk/cpp/client/ydb_common_client/impl/client.h>
#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>

namespace NYdb {
namespace NMonitoring {

using namespace NThreading;

class TSelfCheckResult::TImpl {
public:
    TImpl(Ydb::Monitoring::SelfCheckResult&& result)
        : Result(std::move(result))
    {}
    Ydb::Monitoring::SelfCheckResult Result;
};

TSelfCheckResult::TSelfCheckResult(TStatus&& status, Ydb::Monitoring::SelfCheckResult&& result)
    : TStatus(std::move(status))
    , Impl_(std::make_shared<TSelfCheckResult::TImpl>(std::move(result)))
{}

class TMonitoringClient::TImpl : public TClientImplCommon<TMonitoringClient::TImpl> {
public:
    TImpl(std::shared_ptr<TGRpcConnectionsImpl>&& connections, const TCommonClientSettings& settings)
            : TClientImplCommon(std::move(connections), settings)
    {}

    TAsyncSelfCheckResult SelfCheck(const TSelfCheckSettings& settings) {
        auto request = MakeOperationRequest<Ydb::Monitoring::SelfCheckRequest>(settings);

        if (settings.ReturnVerboseStatus_) {
            request.set_return_verbose_status(settings.ReturnVerboseStatus_.GetRef());
        }

        if (settings.MinimumStatus_) {
            request.set_minimum_status((::Ydb::Monitoring::StatusFlag_Status)settings.MinimumStatus_.GetRef());
        }

        if (settings.MaximumLevel_) {
            request.set_maximum_level(settings.MaximumLevel_.GetRef());
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
};

TMonitoringClient::TMonitoringClient(const TDriver& driver, const TCommonClientSettings& settings)
    : Impl_(new TImpl(CreateInternalInterface(driver), settings))
{}

TAsyncSelfCheckResult TMonitoringClient::SelfCheck(const TSelfCheckSettings& settings) {
    return Impl_->SelfCheck(settings);
}

}

const Ydb::Monitoring::SelfCheckResult& TProtoAccessor::GetProto(const NYdb::NMonitoring::TSelfCheckResult& selfCheckResult) {
    return selfCheckResult.Impl_->Result;
}

}
