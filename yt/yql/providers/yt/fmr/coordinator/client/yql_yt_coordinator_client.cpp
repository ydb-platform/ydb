#include <library/cpp/http/simple/http_client.h>

#include <yt/yql/providers/yt/fmr/proto/coordinator.pb.h>
#include <yt/yql/providers/yt/fmr/coordinator/interface/proto_helpers/yql_yt_coordinator_proto_helpers.h>

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

        httpClient.DoPost(startOperationRequestUrl, protoStartOperationRequest.SerializeAsString(), &outputStream, Headers_);
        TString serializedResponse = outputStream.ReadAll();
        NProto::TStartOperationResponse protoStartOperationResponse;
        YQL_ENSURE(protoStartOperationResponse.ParseFromString(serializedResponse));
        return NThreading::MakeFuture(StartOperationResponseFromProto(protoStartOperationResponse));
    }

    NThreading::TFuture<TGetOperationResponse> GetOperation(const TGetOperationRequest& getOperationRequest) override {
        TString getOperationRequestUrl = "/operation/" + getOperationRequest.OperationId;
        auto httpClient = TKeepAliveHttpClient(Host_, Port_);
        TStringStream outputStream;

        httpClient.DoGet(getOperationRequestUrl, &outputStream, Headers_);
        TString serializedResponse = outputStream.ReadAll();
        NProto::TGetOperationResponse protoGetOperationResponse;
        YQL_ENSURE(protoGetOperationResponse.ParseFromString(serializedResponse));
        return NThreading::MakeFuture(GetOperationResponseFromProto(protoGetOperationResponse));
    }

    NThreading::TFuture<TDeleteOperationResponse> DeleteOperation(const TDeleteOperationRequest& deleteOperationRequest) override {
        TString deleteOperationRequestUrl = "/operation/" + deleteOperationRequest.OperationId;
        auto httpClient = TKeepAliveHttpClient(Host_, Port_);
        TStringStream outputStream;

        httpClient.DoRequest("DELETE", deleteOperationRequestUrl, "", &outputStream, Headers_);
        TString serializedResponse = outputStream.ReadAll();
        NProto::TDeleteOperationResponse protoDeleteOperationResponse;
        YQL_ENSURE(protoDeleteOperationResponse.ParseFromString(serializedResponse));
        return NThreading::MakeFuture(DeleteOperationResponseFromProto(protoDeleteOperationResponse));
    }

    NThreading::TFuture<THeartbeatResponse> SendHeartbeatResponse(const THeartbeatRequest& heartbeatRequest) override {
        NProto::THeartbeatRequest protoSendHeartbeatRequest = HeartbeatRequestToProto(heartbeatRequest);
        TString sendHearbeatRequestUrl = "/worker_heartbeat";
        auto httpClient = TKeepAliveHttpClient(Host_, Port_);
        TStringStream outputStream;

        httpClient.DoPost(sendHearbeatRequestUrl, protoSendHeartbeatRequest.SerializeAsString(), &outputStream, Headers_);
        TString serializedResponse = outputStream.ReadAll();
        NProto::THeartbeatResponse protoHeartbeatResponse;
        YQL_ENSURE(protoHeartbeatResponse.ParseFromString(serializedResponse));
        return NThreading::MakeFuture(HeartbeatResponseFromProto(protoHeartbeatResponse));
    }

    NThreading::TFuture<TGetFmrTableInfoResponse> GetFmrTableInfo(const TGetFmrTableInfoRequest& getFmrTableInfoRequest) override {
        NProto::TGetFmrTableInfoRequest protoGetFmrTableInfoRequest = GetFmrTableInfoRequestToProto(getFmrTableInfoRequest);
        TString sendHearbeatRequestUrl = "/fmr_table_info";
        auto httpClient = TKeepAliveHttpClient(Host_, Port_);
        TStringStream outputStream;

        httpClient.DoGet(sendHearbeatRequestUrl, &outputStream, Headers_);
        TString serializedResponse = outputStream.ReadAll();
        NProto::TGetFmrTableInfoResponse protoGetFmrTableInfoResponse;
        YQL_ENSURE(protoGetFmrTableInfoResponse.ParseFromString(serializedResponse));
        return NThreading::MakeFuture(GetFmrTableInfoResponseFromProto(protoGetFmrTableInfoResponse));
    }

private:
    TString Host_;
    ui16 Port_;
    TSimpleHttpClient::THeaders Headers_;
};

} // namespace

IFmrCoordinator::TPtr MakeFmrCoordinatorClient(const TFmrCoordinatorClientSettings& settings) {
    return MakeIntrusive<TFmrCoordinatorClient>(settings);
}

} // namespace NYql::NFmr
