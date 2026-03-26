#include <library/cpp/http/simple/http_client.h>

#include <library/cpp/retry/retry.h>
#include <util/string/vector.h>
#include <yt/yql/providers/yt/fmr/proto/coordinator.pb.h>
#include <yt/yql/providers/yt/fmr/coordinator/interface/proto_helpers/yql_yt_coordinator_proto_helpers.h>

#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/utils/log/log_component.h>
#include <yql/essentials/utils/yql_panic.h>
#include <yt/yql/providers/yt/fmr/utils/yql_yt_log_context.h>

#include "yql_yt_coordinator_client.h"

namespace NYql::NFmr {

namespace {

class TFmrCoordinatorClient: public IFmrCoordinator {
public:
    TFmrCoordinatorClient(const TFmrCoordinatorClientSettings& settings, IFmrTvmClient::TPtr tvmClient)
        : Host_(settings.Host)
        , Port_(settings.Port)
        , DestinationTvmId_(settings.DestinationTvmId)
        , TvmClient_(tvmClient)
    {
        Headers_["Content-Type"] = "application/x-protobuf";
    }

    NThreading::TFuture<TStartOperationResponse> StartOperation(const TStartOperationRequest& startOperationRequest) override {
        NProto::TStartOperationRequest protoStartOperationRequest = StartOperationRequestToProto(startOperationRequest);
        TString startOperationRequestUrl = "/operation";
        auto httpClient = TKeepAliveHttpClient(Host_, Port_);
        TStringStream outputStream;

        auto startOperationFunc = [&]() {
            auto statusCode = httpClient.DoPost(
                startOperationRequestUrl,
                protoStartOperationRequest.SerializeAsString(),
                &outputStream,
                GetFullHttpHeaders(Headers_, TvmClient_, DestinationTvmId_, false));
            TString serializedResponse = outputStream.ReadAll();
            HandleHttpError(statusCode, serializedResponse);
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
            auto statusCode = httpClient.DoGet(
                getOperationRequestUrl,
                &outputStream,
                GetFullHttpHeaders(Headers_, TvmClient_, DestinationTvmId_, false)
            );
            TString serializedResponse = outputStream.ReadAll();
            HandleHttpError(statusCode, serializedResponse);
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
            auto statusCode= httpClient.DoRequest(
                "DELETE",
                deleteOperationRequestUrl,
                "",
                &outputStream,
                GetFullHttpHeaders(Headers_, TvmClient_, DestinationTvmId_, false)
            );
            TString serializedResponse = outputStream.ReadAll();
            HandleHttpError(statusCode, serializedResponse);
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
            auto statusCode = httpClient.DoPost(
                sendHearbeatRequestUrl,
                protoSendHeartbeatRequest.SerializeAsString(),
                &outputStream,
                GetFullHttpHeaders(Headers_, TvmClient_, DestinationTvmId_, false)
            );
            TString serializedResponse = outputStream.ReadAll();
            HandleHttpError(statusCode, serializedResponse);
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
            auto statusCode = httpClient.DoGet(
                sendHearbeatRequestUrl,
                &outputStream,
                GetFullHttpHeaders(Headers_, TvmClient_, DestinationTvmId_, false)
            );
            TString serializedResponse = outputStream.ReadAll();
            HandleHttpError(statusCode, serializedResponse);
            NProto::TGetFmrTableInfoResponse protoGetFmrTableInfoResponse;
            YQL_ENSURE(protoGetFmrTableInfoResponse.ParseFromString(serializedResponse));
            return NThreading::MakeFuture(GetFmrTableInfoResponseFromProto(protoGetFmrTableInfoResponse));
        };
        return *DoWithRetry<NThreading::TFuture<TGetFmrTableInfoResponse>, yexception>(getFmrTableInfoRequestFunc, RetryPolicy_, true, OnFail_);
    }

    NThreading::TFuture<void> ClearSession(const TClearSessionRequest& request) override {
        NProto::TClearSessionRequest protoClearSessionRequest = ClearSessionRequestToProto(request);
        TString clearSessionUrl = "/clear_session";
        auto httpClient = TKeepAliveHttpClient(Host_, Port_);
        TStringStream outputStream;
        auto clearSessionFunc = [&]() {
            auto statusCode = httpClient.DoPost(
                clearSessionUrl,
                protoClearSessionRequest.SerializeAsStringOrThrow(),
                &outputStream,
                GetFullHttpHeaders(Headers_, TvmClient_, DestinationTvmId_, false)
            );
            HandleHttpError(statusCode, outputStream.ReadAll());
            return NThreading::MakeFuture();
        };
        return *DoWithRetry<NThreading::TFuture<void>, yexception>(clearSessionFunc, RetryPolicy_, true, OnFail_);
    };

    NThreading::TFuture<TDropTablesResponse> DropTables(const TDropTablesRequest& request) override {
        NProto::TDropTablesRequest protoRequest = DropTablesRequestToProto(request);
        TString dropTablesUrl = "/drop_tables";
        auto httpClient = TKeepAliveHttpClient(Host_, Port_);
        TStringStream outputStream;

        auto dropTablesFunc = [&]() {
            auto statusCode = httpClient.DoPost(
                dropTablesUrl,
                protoRequest.SerializeAsString(),
                &outputStream,
                GetFullHttpHeaders(Headers_, TvmClient_, DestinationTvmId_, false)
            );
            TString serializedResponse = outputStream.ReadAll();
            HandleHttpError(statusCode, serializedResponse);
            NProto::TDropTablesResponse protoResponse;
            YQL_ENSURE(protoResponse.ParseFromString(serializedResponse));
            return NThreading::MakeFuture(DropTablesResponseFromProto(protoResponse));
        };

        return *DoWithRetry<NThreading::TFuture<TDropTablesResponse>, yexception>(dropTablesFunc, RetryPolicy_, true, OnFail_);
    }

    NThreading::TFuture<TOpenSessionResponse> OpenSession(const TOpenSessionRequest& request) override {
        NProto::TOpenSessionRequest protoRequest = OpenSessionRequestToProto(request);
        TString url = "/open_session";
        auto httpClient = TKeepAliveHttpClient(Host_, Port_);
        TStringStream outputStream;

        auto func = [&]() {
            auto statusCode = httpClient.DoPost(
                url,
                protoRequest.SerializeAsString(),
                &outputStream,
                GetFullHttpHeaders(Headers_, TvmClient_, DestinationTvmId_, false)
            );
            TString serializedResponse = outputStream.ReadAll();
            HandleHttpError(statusCode, serializedResponse);
            NProto::TOpenSessionResponse protoResponse;
            YQL_ENSURE(protoResponse.ParseFromString(serializedResponse));
            return NThreading::MakeFuture(OpenSessionResponseFromProto(protoResponse));
        };
        return *DoWithRetry<NThreading::TFuture<TOpenSessionResponse>, yexception>(func, RetryPolicy_, true, OnFail_);
    }

    NThreading::TFuture<TPingSessionResponse> PingSession(const TPingSessionRequest& request) override {
        NProto::TPingSessionRequest protoRequest = PingSessionRequestToProto(request);
        TString url = "/ping_session";
        auto httpClient = TKeepAliveHttpClient(Host_, Port_);
        TStringStream outputStream;

        auto func = [&]() {
            auto statusCode = httpClient.DoPost(
                url,
                protoRequest.SerializeAsString(),
                &outputStream,
                GetFullHttpHeaders(Headers_, TvmClient_, DestinationTvmId_, false)
            );
            TString serializedResponse = outputStream.ReadAll();
            HandleHttpError(statusCode, serializedResponse);
            NProto::TPingSessionResponse protoResponse;
            YQL_ENSURE(protoResponse.ParseFromString(serializedResponse));
            return NThreading::MakeFuture(PingSessionResponseFromProto(protoResponse));
        };
        return *DoWithRetry<NThreading::TFuture<TPingSessionResponse>, yexception>(func, RetryPolicy_, true, OnFail_);
    }

    NThreading::TFuture<TListSessionsResponse> ListSessions(const TListSessionsRequest& request) override {
        NProto::TListSessionsRequest protoRequest = ListSessionsRequestToProto(request);
        TString url = "/list_sessions";
        auto httpClient = TKeepAliveHttpClient(Host_, Port_);
        TStringStream outputStream;

        auto func = [&]() {
            auto statusCode = httpClient.DoGet(
                url,
                &outputStream,
                GetFullHttpHeaders(Headers_, TvmClient_, DestinationTvmId_, false)
            );
            TString serializedResponse = outputStream.ReadAll();
            HandleHttpError(statusCode, serializedResponse);
            NProto::TListSessionsResponse protoResponse;
            YQL_ENSURE(protoResponse.ParseFromString(serializedResponse));
            return NThreading::MakeFuture(ListSessionsResponseFromProto(protoResponse));
        };
        return *DoWithRetry<NThreading::TFuture<TListSessionsResponse>, yexception>(func, RetryPolicy_, true, OnFail_);
    }

    NThreading::TFuture<TPrepareOperationResponse> PrepareOperation(const TPrepareOperationRequest& request) override {
        NProto::TPrepareOperationRequest protoRequest = PrepareOperationRequestToProto(request);
        TString url = "/prepare_operation";
        auto httpClient = TKeepAliveHttpClient(Host_, Port_);
        TStringStream outputStream;

        auto func = [&]() {
            auto statusCode = httpClient.DoPost(
                url,
                protoRequest.SerializeAsString(),
                &outputStream,
                GetFullHttpHeaders(Headers_, TvmClient_, DestinationTvmId_, false)
            );
            TString serializedResponse = outputStream.ReadAll();
            HandleHttpError(statusCode, serializedResponse);
            NProto::TPrepareOperationResponse protoResponse;
            YQL_ENSURE(protoResponse.ParseFromString(serializedResponse));
            return NThreading::MakeFuture(PrepareOperationResponseFromProto(protoResponse));
        };
        return *DoWithRetry<NThreading::TFuture<TPrepareOperationResponse>, yexception>(func, RetryPolicy_, true, OnFail_);
    }

private:
    TString Host_;
    const ui16 Port_;
    const TTvmId DestinationTvmId_ = 0;
    IFmrTvmClient::TPtr TvmClient_;
    TKeepAliveHttpClient::THeaders Headers_;
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

IFmrCoordinator::TPtr MakeFmrCoordinatorClient(const TFmrCoordinatorClientSettings& settings, IFmrTvmClient::TPtr tvmClient) {
    return MakeIntrusive<TFmrCoordinatorClient>(settings, tvmClient);
}

} // namespace NYql::NFmr
