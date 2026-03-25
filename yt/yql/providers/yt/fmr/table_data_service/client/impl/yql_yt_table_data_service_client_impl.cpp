#include "yql_yt_table_data_service_client_impl.h"

#include <library/cpp/threading/future/future.h>
#include <library/cpp/http/simple/http_client.h>
#include <library/cpp/retry/retry.h>
#include <library/cpp/yson/node/node_io.h>
#include <yt/yql/providers/yt/fmr/table_data_service/client/proto_helpers/yql_yt_table_data_service_proto_helpers.h>
#include <yt/yql/providers/yt/fmr/table_data_service/discovery/file/yql_yt_file_service_discovery.h>
#include <yt/yql/providers/yt/fmr/table_data_service/discovery/interface/yql_yt_service_discovery.h>
#include <yt/yql/providers/yt/fmr/utils/yql_yt_log_context.h>
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/utils/log/log_component.h>
#include <yql/essentials/utils/yql_panic.h>

namespace NYql::NFmr {

namespace {

class TFmrTableDataServiceClient: public ITableDataService {
public:
    TFmrTableDataServiceClient(
        ITableDataServiceDiscovery::TPtr discovery,
        IFmrTvmClient::TPtr tvmClient = nullptr,
        TTvmId destinationTvmId = 0
    )
        : TableDataServiceDiscovery_(discovery)
        , TvmClient_(tvmClient)
        , DestinationTvmId_(destinationTvmId)
    {
    }

    NThreading::TFuture<bool> Put(const TString& group, const TString& chunkId, const TString& value) override {
        TString putRequestUrl = "/put_data?group=" + group + "&chunkId=" + chunkId;
        ui64 workersNum = TableDataServiceDiscovery_->GetHostCount();
        auto tableDataServiceWorkerNum = std::hash<TString>()(group + chunkId) % workersNum;
        auto workerConnection = TableDataServiceDiscovery_->GetHosts()[tableDataServiceWorkerNum];
        auto httpClient = TKeepAliveHttpClient(workerConnection.Host, workerConnection.Port);
        TStringStream outputStream;
        YQL_CLOG(TRACE, FastMapReduce) << "Sending put request with url: " << putRequestUrl <<
            " To table data service worker with host: " << workerConnection.Host << " and port: " << ToString(workerConnection.Port);

        auto putTableDataServiceFunc = [&]() {
            try {
                auto statusCode = httpClient.DoPost(putRequestUrl, value, &outputStream, GetFullHttpHeaders(Headers_, TvmClient_, DestinationTvmId_, false));
                TString serializedResponse = outputStream.ReadAll();
                HandleHttpError(statusCode, serializedResponse);
                NProto::TTableDataServicePutResponse protoPutResponse;
                YQL_ENSURE(protoPutResponse.ParseFromString(serializedResponse));
                bool putSuccess = TableDataServicePutResponseFromProto(protoPutResponse);
                if (!putSuccess) {
                    throw yexception() << "Failed to put chunkId " << chunkId << " to table data service - memory limit exceeded";
                    // Throw basic retryable exception in case other keys in table service are cleared soon.
                }
                return NThreading::MakeFuture<bool>(true);
            } catch (...) {
                return NThreading::MakeErrorFuture<bool>(std::current_exception());
            }
        };
        return *DoWithRetry<NThreading::TFuture<bool>, yexception>(putTableDataServiceFunc, RetryPolicy_, true, OnFail_);
    }

    NThreading::TFuture<TMaybe<TString>> Get(const TString& group, const TString& chunkId) const override {
        TString getRequestUrl = "/get_data?group=" + group + "&chunkId=" + chunkId;
        ui64 workersNum = TableDataServiceDiscovery_->GetHostCount();
        auto tableDataServiceWorkerNum = std::hash<TString>()(group + chunkId) % workersNum;
        auto workerConnection = TableDataServiceDiscovery_->GetHosts()[tableDataServiceWorkerNum];
        auto httpClient = TKeepAliveHttpClient(workerConnection.Host, workerConnection.Port);
        TStringStream outputStream;
        YQL_CLOG(TRACE, FastMapReduce) << "Sending get request with url: " << getRequestUrl <<
            " To table data service worker with host: " << workerConnection.Host << " and port: " << ToString(workerConnection.Port);

        auto getTableDataServiceFunc = [&]() {
            try {
                auto statusCode = httpClient.DoGet(getRequestUrl,&outputStream, GetFullHttpHeaders(Headers_, TvmClient_, DestinationTvmId_, false));
                TString serializedResponse = outputStream.ReadAll();
                HandleHttpError(statusCode, serializedResponse);
                NProto::TTableDataServiceGetResponse protoGetResponse;
                YQL_ENSURE(protoGetResponse.ParseFromString(serializedResponse));
                TMaybe<TString> getResponse = TableDataServiceGetResponseFromProto(protoGetResponse);
                if (!getResponse.Defined()) {
                    throw TFmrNonRetryableJobException() << "Failed to get group " << group << " and chunkId " << chunkId << " from table data service";
                }
                return NThreading::MakeFuture(getResponse);
            } catch (...) {
                return NThreading::MakeErrorFuture<TMaybe<TString>>(std::current_exception());
            }
        };
        return *DoWithRetry<NThreading::TFuture<TMaybe<TString>>, yexception>(getTableDataServiceFunc, RetryPolicy_, true, OnFail_);
    }


    NThreading::TFuture<void> Delete(const TString& group, const TString& chunkId) override {
        TString deleteRequestUrl = "/delete_data?group=" + group + "&chunkId=" + chunkId;
        ui64 workersNum = TableDataServiceDiscovery_->GetHostCount();
        auto tableDataServiceWorkerNum = std::hash<TString>()(group + chunkId) % workersNum;
        auto workerConnection = TableDataServiceDiscovery_->GetHosts()[tableDataServiceWorkerNum];
        auto httpClient = TKeepAliveHttpClient(workerConnection.Host, workerConnection.Port);
        TStringStream outputStream;
        YQL_CLOG(TRACE, FastMapReduce) << "Sending delete request with url: " << deleteRequestUrl <<
            " To table data service worker with host: " << workerConnection.Host << " and port: " << ToString(workerConnection.Port);

        auto deleteTableDataServiceFunc = [&]() {
            try {
                auto statusCode = httpClient.DoRequest("DELETE", deleteRequestUrl, "", &outputStream, GetFullHttpHeaders(Headers_, TvmClient_, DestinationTvmId_, false));
                HandleHttpError(statusCode, outputStream.ReadAll());
                return NThreading::MakeFuture();
            } catch (...) {
                return NThreading::MakeErrorFuture<void>(std::current_exception());
            }
        };
        return *DoWithRetry<NThreading::TFuture<void>, yexception>(deleteTableDataServiceFunc, RetryPolicy_, true, OnFail_);
    }

    NThreading::TFuture<void> RegisterDeletion(const std::vector<TString>& groups) override {
        NProto::TTableDataServiceGroupDeletionRequest protoDeletionRequest = TableDataServiceGroupDeletionRequestToProto(groups);
        TString serializedProtoDeletionRequest = protoDeletionRequest.SerializeAsStringOrThrow();

        TString deleteGroupsRequestUrl = "/delete_groups";
        ui64 totalWorkersNum = TableDataServiceDiscovery_->GetHostCount();
        std::vector<NThreading::TFuture<void>> allNodesDeletions;
        for (ui64 workerNum = 0; workerNum < totalWorkersNum; ++workerNum) {
            auto workerConnection = TableDataServiceDiscovery_->GetHosts()[workerNum];
            auto httpClient = TKeepAliveHttpClient(workerConnection.Host, workerConnection.Port);
            TStringStream outputStream;
            YQL_CLOG(TRACE, FastMapReduce) << "Sending delete groups request with url: " << deleteGroupsRequestUrl <<
                " To table data service worker with host: " << workerConnection.Host << " and port: " << ToString(workerConnection.Port);
            auto deletionRequestFunc = [&]() {
                try {
                    auto protobufHeaders = TKeepAliveHttpClient::THeaders{{"Content-Type", "application/x-protobuf"}};
                    auto statusCode = httpClient.DoPost(deleteGroupsRequestUrl, serializedProtoDeletionRequest, &outputStream, GetFullHttpHeaders(Headers_, TvmClient_, DestinationTvmId_, false));
                    HandleHttpError(statusCode, outputStream.ReadAll());
                    return NThreading::MakeFuture();
                } catch (...) {
                    return NThreading::MakeErrorFuture<void>(std::current_exception());
                }
            };
            allNodesDeletions.emplace_back(*DoWithRetry<NThreading::TFuture<void>, yexception>(deletionRequestFunc, RetryPolicy_, true, OnFail_));
        }
        return WaitExceptionOrAll(allNodesDeletions);
    }

    NThreading::TFuture<void> Clear() override {
        TString clearRequestUrl = "/clear";
        ui64 totalWorkersNum = TableDataServiceDiscovery_->GetHostCount();
        for (ui64 workerNum = 0; workerNum < totalWorkersNum; ++workerNum) {
            auto workerConnection = TableDataServiceDiscovery_->GetHosts()[workerNum];
            auto httpClient = TKeepAliveHttpClient(workerConnection.Host, workerConnection.Port);
            TStringStream outputStream;
            YQL_CLOG(TRACE, FastMapReduce) << "Sending clear request with url: " << clearRequestUrl <<
                " To table data service worker with host: " << workerConnection.Host << " and port: " << ToString(workerConnection.Port);
            try {
                auto statusCode = httpClient.DoPost(clearRequestUrl, TString(), &outputStream, GetFullHttpHeaders(Headers_, TvmClient_, DestinationTvmId_, false));
                HandleHttpError(statusCode, outputStream.ReadAll());
            } catch (...) {
                YQL_CLOG(ERROR, FastMapReduce) << "Failed to clear table data service host: " << workerConnection.Host << " and port: " << ToString(workerConnection.Port)
                    << "with error message: " << CurrentExceptionMessage();
            }
        }
        return NThreading::MakeFuture(); // For now just log errors and ignore request failiures.
    }

private:
    ITableDataServiceDiscovery::TPtr TableDataServiceDiscovery_;
    TKeepAliveHttpClient::THeaders Headers_{};
    IFmrTvmClient::TPtr TvmClient_;
    ui32 DestinationTvmId_ = 0;

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

ITableDataService::TPtr MakeTableDataServiceClient(
    ITableDataServiceDiscovery::TPtr discovery, IFmrTvmClient::TPtr tvmClient, TTvmId destinationTvmId
) {
    return MakeIntrusive<TFmrTableDataServiceClient>(discovery, tvmClient, destinationTvmId);
}

} // namespace NYql::NFmr
