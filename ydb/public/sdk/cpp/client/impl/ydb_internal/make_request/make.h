#pragma once

#include <ydb/public/sdk/cpp/client/impl/ydb_internal/internal_header.h>
#include <ydb/public/api/protos/ydb_common.pb.h>

#include <util/datetime/base.h>

#include <google/protobuf/duration.pb.h>

namespace NYdb {

void SetDuration(const TDuration& duration, google::protobuf::Duration& protoValue);

template <typename TRequestSettings, typename TProtoRequest>
void FillOperationParams(const TRequestSettings& settings, TProtoRequest& params) {
    auto& operationParams = *params.mutable_operation_params();

    if (settings.CancelAfter_) {
        SetDuration(settings.CancelAfter_, *operationParams.mutable_cancel_after());
    }

    if (settings.ForgetAfter_) {
        SetDuration(settings.ForgetAfter_, *operationParams.mutable_forget_after());
    }

    if (settings.OperationTimeout_) {
        SetDuration(settings.OperationTimeout_, *operationParams.mutable_operation_timeout());
    } else if (settings.ClientTimeout_ && settings.UseClientTimeoutForOperation_) {
        SetDuration(settings.ClientTimeout_, *operationParams.mutable_operation_timeout());
    }

    if (settings.ReportCostInfo_) {
        operationParams.set_report_cost_info(Ydb::FeatureFlag::ENABLED);
    }
}

template <typename TProtoRequest>
TProtoRequest MakeRequest() {
    return TProtoRequest();
}

template <typename TProtoRequest, typename TRequestSettings>
TProtoRequest MakeOperationRequest(const TRequestSettings& settings) {
    auto request = MakeRequest<TProtoRequest>();
    FillOperationParams(settings, request);
    return request;
}

} // namespace NYdb
