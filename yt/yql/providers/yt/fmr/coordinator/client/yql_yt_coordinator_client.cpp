#include <library/cpp/http/simple/http_client.h>

#include <library/cpp/retry/retry.h>
#include <yt/yql/providers/yt/fmr/proto/coordinator.pb.h>
#include <yt/yql/providers/yt/fmr/coordinator/interface/proto_helpers/yql_yt_coordinator_proto_helpers.h>

#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/utils/log/log_component.h>
#include <yql/essentials/utils/yql_panic.h>

#include "yql_yt_coordinator_client.h"

namespace NYql::NFmr {

namespace {

class TFmrCoordinatorClient: public IFmrCoordinator {
public:
    TFmrCoordinatorClient(const TFmrCoordinatorClientSettings& settings): Host_(settings.Host), Port_(settings.Port)
    {
        Headers_["Content-Type"] = "application/x-protobuf";
    }

    NThreading::TFuture<TStartOperationResponse> StartOperation(const TStartOperationRequest& startOperationRequest) override {
        NProto::TStartOperationRequest protoStartOperationRequest = StartOperationRequestToProto(startOperationRequest);
        TString startOperationRequestUrl = "/operation";
        auto httpClient = TKeepAliveHttpClient(Host_, Port_);
        TStringStream outputStream;

        auto startOperationFunc = [&]() {
            httpClient.DoPost(startOperationRequestUrl, protoStartOperationRequest.SerializeAsString(), &outputStream, Headers_);
            TString serializedResponse = outputStream.ReadAll();
            NProto::TStartOperationResponse protoStartOperationResponse;
            YQL_ENSURE(protoStartOperationResponse.ParseFromString(serializedResponse));
            return NThreading::MakeFuture(StartOperationResponseFromProto(protoStartOperationResponse));
        };
        return *DoWithRetry<NThreading::TFuture<TStartOperationResponse>, yexception>(startOperationFunc, RetryPolicy_, true, OnFail_);
    }

    NThreading::TFuture<TGetOperationResponse> GetOperation(const TGetOperationRequest& getOperationRequest) override {
        TString getOperationRequestUrl = "/operation/" + getOperationRequest.OperationId;
        auto httpClient = TKeepAliveHttpClient(Host_, Port_);
        TStringStream outputStream;

        auto getOperationFunc = [&]() {
            httpClient.DoGet(getOperationRequestUrl, &outputStream, Headers_);
            TString serializedResponse = outputStream.ReadAll();
            NProto::TGetOperationResponse protoGetOperationResponse;
            YQL_ENSURE(protoGetOperationResponse.ParseFromString(serializedResponse));
            return NThreading::MakeFuture(GetOperationResponseFromProto(protoGetOperationResponse));
        };
        return *DoWithRetry<NThreading::TFuture<TGetOperationResponse>, yexception>(getOperationFunc, RetryPolicy_, true, OnFail_);
    }

    NThreading::TFuture<TDeleteOperationResponse> DeleteOperation(const TDeleteOperationRequest& deleteOperationRequest) override {
        TString deleteOperationRequestUrl = "/operation/" + deleteOperationRequest.OperationId;
        auto httpClient = TKeepAliveHttpClient(Host_, Port_);
        TStringStream outputStream;

        auto deleteOperationFunc = [&]() {
            httpClient.DoRequest("DELETE", deleteOperationRequestUrl, "", &outputStream, Headers_);
            TString serializedResponse = outputStream.ReadAll();
            NProto::TDeleteOperationResponse protoDeleteOperationResponse;
            YQL_ENSURE(protoDeleteOperationResponse.ParseFromString(serializedResponse));
            return NThreading::MakeFuture(DeleteOperationResponseFromProto(protoDeleteOperationResponse));
        };
        return *DoWithRetry<NThreading::TFuture<TDeleteOperationResponse>, yexception>(deleteOperationFunc, RetryPolicy_, true, OnFail_);
    }

    NThreading::TFuture<THeartbeatResponse> SendHeartbeatResponse(const THeartbeatRequest& heartbeatRequest) override {
        NProto::THeartbeatRequest protoSendHeartbeatRequest = HeartbeatRequestToProto(heartbeatRequest);
        TString sendHearbeatRequestUrl = "/worker_heartbeat";
        auto httpClient = TKeepAliveHttpClient(Host_, Port_);
        TStringStream outputStream;

        auto sendHeartbeatRequestFunc = [&]() {
            httpClient.DoPost(sendHearbeatRequestUrl, protoSendHeartbeatRequest.SerializeAsString(), &outputStream, Headers_);
            TString serializedResponse = outputStream.ReadAll();
            NProto::THeartbeatResponse protoHeartbeatResponse;
            YQL_ENSURE(protoHeartbeatResponse.ParseFromString(serializedResponse));
            return NThreading::MakeFuture(HeartbeatResponseFromProto(protoHeartbeatResponse));
        };

        return *DoWithRetry<NThreading::TFuture<THeartbeatResponse>, yexception>(sendHeartbeatRequestFunc, RetryPolicy_, true, OnFail_);
    }

    NThreading::TFuture<TGetFmrTableInfoResponse> GetFmrTableInfo(const TGetFmrTableInfoRequest& getFmrTableInfoRequest) override {
        NProto::TGetFmrTableInfoRequest protoGetFmrTableInfoRequest = GetFmrTableInfoRequestToProto(getFmrTableInfoRequest);
        TString sendHearbeatRequestUrl = "/fmr_table_info";
        auto httpClient = TKeepAliveHttpClient(Host_, Port_);
        TStringStream outputStream;

        auto getFmrTableInfoRequestFunc = [&]() {
            httpClient.DoGet(sendHearbeatRequestUrl, &outputStream, Headers_);
            TString serializedResponse = outputStream.ReadAll();
            NProto::TGetFmrTableInfoResponse protoGetFmrTableInfoResponse;
            YQL_ENSURE(protoGetFmrTableInfoResponse.ParseFromString(serializedResponse));
            return NThreading::MakeFuture(GetFmrTableInfoResponseFromProto(protoGetFmrTableInfoResponse));
        };
        return *DoWithRetry<NThreading::TFuture<TGetFmrTableInfoResponse>, yexception>(getFmrTableInfoRequestFunc, RetryPolicy_, true, OnFail_);
    }

private:
    TString Host_;
    ui16 Port_;
    TSimpleHttpClient::THeaders Headers_;
    std::shared_ptr<IRetryPolicy<const yexception&>> RetryPolicy_ = IRetryPolicy<const yexception&>::GetExponentialBackoffPolicy(
        /*retryClassFunction*/ [] (const yexception&) {
            return ERetryErrorClass::LongRetry;
        },
        /*minDelay*/ TDuration::MilliSeconds(10),
        /*minLongRetryDelay*/ TDuration::Seconds(1),
        /* maxDelay */ TDuration::Seconds(30),
        /*maxRetries*/ 3
    );

    std::function<void(const yexception&)> OnFail_ = [](const yexception& exc) {
        YQL_CLOG(DEBUG, FastMapReduce) << "Got exception, retrying: " << exc.what();
    };
};

} // namespace

IFmrCoordinator::TPtr MakeFmrCoordinatorClient(const TFmrCoordinatorClientSettings& settings) {
    return MakeIntrusive<TFmrCoordinatorClient>(settings);
}

} // namespace NYql::NFmr
