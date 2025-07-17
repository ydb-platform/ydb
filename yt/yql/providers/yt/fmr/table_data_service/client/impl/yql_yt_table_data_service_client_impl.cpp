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
    TFmrTableDataServiceClient(ITableDataServiceDiscovery::TPtr discovery): TableDataServiceDiscovery_(discovery) {}

    NThreading::TFuture<void> Put(const TString& key, const TString& value) override {
        TString putRequestUrl = "/put_data?key=" + key;
        ui64 workersNum = TableDataServiceDiscovery_->GetHostCount();
        auto tableDataServiceWorkerNum = std::hash<TString>()(key) % workersNum;
        auto workerConnection = TableDataServiceDiscovery_->GetHosts()[tableDataServiceWorkerNum];
        auto httpClient = TKeepAliveHttpClient(workerConnection.Host, workerConnection.Port);
        YQL_CLOG(TRACE, FastMapReduce) << "Sending put request with url: " << putRequestUrl <<
            " To table data service worker with host: " << workerConnection.Host << " and port: " << ToString(workerConnection.Port);

        auto putTableDataServiceFunc = [&]() {
            try {
                httpClient.DoPost(putRequestUrl, value, nullptr, GetHeadersWithLogContext(Headers_));
                return NThreading::MakeFuture();
            } catch (...) {
                return NThreading::MakeErrorFuture<void>(std::current_exception());
            }
        };
        return *DoWithRetry<NThreading::TFuture<void>, yexception>(putTableDataServiceFunc, RetryPolicy_, true, OnFail_);
    }

    NThreading::TFuture<TMaybe<TString>> Get(const TString& key) const override {
        TString getRequestUrl = "/get_data?key=" + key;
        ui64 workersNum = TableDataServiceDiscovery_->GetHostCount();
        auto tableDataServiceWorkerNum = std::hash<TString>()(key) % workersNum;
        auto workerConnection = TableDataServiceDiscovery_->GetHosts()[tableDataServiceWorkerNum];
        auto httpClient = TKeepAliveHttpClient(workerConnection.Host, workerConnection.Port);
        TStringStream outputStream;
        YQL_CLOG(TRACE, FastMapReduce) << "Sending get request with url: " << getRequestUrl <<
            " To table data service worker with host: " << workerConnection.Host << " and port: " << ToString(workerConnection.Port);

        auto getTableDataServiceFunc = [&]() {
            try {
                httpClient.DoGet(getRequestUrl,&outputStream, GetHeadersWithLogContext(Headers_));
                TString value = outputStream.ReadAll();
                TMaybe<TString> result;
                if (value) {
                    result = value;
                }
                return NThreading::MakeFuture(result);
            } catch (...) {
                return NThreading::MakeErrorFuture<TMaybe<TString>>(std::current_exception());
            }
        };
        return *DoWithRetry<NThreading::TFuture<TMaybe<TString>>, yexception>(getTableDataServiceFunc, RetryPolicy_, true, OnFail_);
    }


    NThreading::TFuture<void> Delete(const TString& key) override {
        TString deleteRequestUrl = "/delete_data?key=" + key;
        ui64 workersNum = TableDataServiceDiscovery_->GetHostCount();
        auto tableDataServiceWorkerNum = std::hash<TString>()(key) % workersNum;
        auto workerConnection = TableDataServiceDiscovery_->GetHosts()[tableDataServiceWorkerNum];
        auto httpClient = TKeepAliveHttpClient(workerConnection.Host, workerConnection.Port);
        YQL_CLOG(TRACE, FastMapReduce) << "Sending delete request with url: " << deleteRequestUrl <<
            " To table data service worker with host: " << workerConnection.Host << " and port: " << ToString(workerConnection.Port);

        auto deleteTableDataServiceFunc = [&]() {
            try {
                httpClient.DoRequest("DELETE", deleteRequestUrl, "", nullptr, GetHeadersWithLogContext(Headers_));
                return NThreading::MakeFuture();
            } catch (...) {
                return NThreading::MakeErrorFuture<void>(std::current_exception());
            }
        };
        return *DoWithRetry<NThreading::TFuture<void>, yexception>(deleteTableDataServiceFunc, RetryPolicy_, true, OnFail_);
    }

    NThreading::TFuture<void> RegisterDeletion(const std::vector<TString>& groups) override {
        NProto::TTableDataServiceGroupDeletionRequest protoDeletionRequest = TTableDataServiceGroupDeletionRequestToProto(groups);
        TString serializedProtoDeletionRequest = protoDeletionRequest.SerializeAsStringOrThrow();

        TString deleteGroupsRequestUrl = "/delete_groups";
        ui64 totalWorkersNum = TableDataServiceDiscovery_->GetHostCount();
        std::vector<NThreading::TFuture<void>> allNodesDeletions;
        for (ui64 workerNum = 0; workerNum < totalWorkersNum; ++workerNum) {
            auto workerConnection = TableDataServiceDiscovery_->GetHosts()[workerNum];
            auto httpClient = TKeepAliveHttpClient(workerConnection.Host, workerConnection.Port);
            YQL_CLOG(TRACE, FastMapReduce) << "Sending delete groups request with url: " << deleteGroupsRequestUrl <<
                " To table data service worker with host: " << workerConnection.Host << " and port: " << ToString(workerConnection.Port);
            auto deletionRequestFunc = [&]() {
                try {
                    auto protobufHeaders = TKeepAliveHttpClient::THeaders{{"Content-Type", "application/x-protobuf"}};
                    httpClient.DoPost(deleteGroupsRequestUrl, serializedProtoDeletionRequest, nullptr, GetHeadersWithLogContext(protobufHeaders));
                    return NThreading::MakeFuture();
                } catch (...) {
                    return NThreading::MakeErrorFuture<void>(std::current_exception());
                }
            };
            allNodesDeletions.emplace_back(*DoWithRetry<NThreading::TFuture<void>, yexception>(deletionRequestFunc, RetryPolicy_, true, OnFail_));
        }
        return WaitExceptionOrAll(allNodesDeletions);
    }

private:
    ITableDataServiceDiscovery::TPtr TableDataServiceDiscovery_;
    TKeepAliveHttpClient::THeaders Headers_{};

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

ITableDataService::TPtr MakeTableDataServiceClient(ITableDataServiceDiscovery::TPtr discovery) {
    return MakeIntrusive<TFmrTableDataServiceClient>(discovery);
}

} // namespace NYql::NFmr
