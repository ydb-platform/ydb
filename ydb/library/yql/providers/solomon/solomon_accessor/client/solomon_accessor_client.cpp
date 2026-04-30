#include "solomon_accessor_client.h"

#include <library/cpp/json/writer/json.h>
#include <library/cpp/protobuf/interop/cast.h>
#include <library/cpp/threading/future/wait/wait.h>
#include <util/string/join.h>
#include <ydb/library/yql/providers/common/http_gateway/yql_http_gateway.h>
#include <ydb/library/yql/providers/solomon/common/constants.h>
#include <ydb/public/sdk/cpp/src/library/grpc/client/grpc_client_low.h>
#include <yql/essentials/utils/url_builder.h>
#include <yql/essentials/utils/yql_panic.h>

#include <ydb/library/yql/providers/solomon/solomon_accessor/grpc/data_service.pb.h>
#include <ydb/library/yql/providers/solomon/solomon_accessor/grpc/data_service.grpc.pb.h>

#include <util/generic/size_literals.h>
#include <util/string/join.h>

#include <algorithm>

namespace NYql::NSo {

using namespace yandex::cloud::priv::monitoring::v3;

namespace {

Downsampling::GapFilling ParseGapFilling(const TString& fill) {
    if (fill == "NULL"sv) {
        return Downsampling::GAP_FILLING_NULL;
    }
    if (fill == "NONE"sv) {
        return Downsampling::GAP_FILLING_NONE;
    }
    if (fill == "PREVIOUS"sv) {
        return Downsampling::GAP_FILLING_PREVIOUS;
    }
    return Downsampling::GAP_FILLING_UNSPECIFIED;
}

Downsampling::GridAggregation ParseGridAggregation(const TString& aggregation) {
    if (aggregation == "MAX"sv) {
        return Downsampling::GRID_AGGREGATION_MAX;
    }
    if (aggregation == "MIN"sv) {
        return Downsampling::GRID_AGGREGATION_MIN;
    }
    if (aggregation == "SUM"sv) {
        return Downsampling::GRID_AGGREGATION_SUM;
    }
    if (aggregation == "AVG"sv) {
        return Downsampling::GRID_AGGREGATION_AVG;
    }
    if (aggregation == "LAST"sv) {
        return Downsampling::GRID_AGGREGATION_LAST;
    }
    if (aggregation == "COUNT"sv) {
        return Downsampling::GRID_AGGREGATION_COUNT;
    }
    return Downsampling::GRID_AGGREGATION_UNSPECIFIED;
}

TString MetricTypeToString(MetricType type) {
    switch (type) {
        case MetricType::DGAUGE:
            return "DGAUGE";
        case MetricType::IGAUGE:
            return "IGAUGE";
        case MetricType::COUNTER:
            return "COUNTER";
        case MetricType::RATE:
            return "RATE";
        default:
            return "UNSPECIFIED";
    }
}

TGetLabelsResponse ProcessGetLabelsResponse(NYql::IHTTPGateway::TResult&& response, const TSelectors& knownSelectors) {
    TGetLabelsResult result;

    if (response.CurlResponseCode != CURLE_OK) {
        return TGetLabelsResponse(TStringBuilder() << "Monitoring api get labels response: " << response.Issues.ToOneLineString() <<
            ", internal code: " << static_cast<int>(response.CurlResponseCode));
    }

    if (response.Content.HttpResponseCode < 200 || response.Content.HttpResponseCode >= 300) {
        // Do not log the raw response body — it may contain sensitive information.
        return TGetLabelsResponse(TStringBuilder() <<
            "Monitoring api get labels request failed with HTTP " << response.Content.HttpResponseCode);
    }

    NJson::TJsonValue json;
    try {
        NJson::ReadJsonTree(response.Content.data(), &json, /*throwOnError*/ true);
    } catch (const std::exception& e) {
        return TGetLabelsResponse("Monitoring api get labels response is not a valid json");
    }

    if (!json.IsMap() || !json.Has("names") || !json["names"].IsArray()) {
        return TGetLabelsResponse("Monitoring api get labels response doesn't contain requested info");
    }

    const auto names = json["names"].GetArray();

    for (const auto& name : names) {
        if (!name.IsString()) {
            return TGetLabelsResponse("Monitoring api get labels response contains invalid label names");
        }
        result.Labels.push_back(name.GetString());
    }
    for (const auto& [key, selector] : knownSelectors) {
        result.Labels.push_back(key);
    }

    return TGetLabelsResponse(std::move(result), response.Content.size() + response.Content.Headers.size());
}

TListMetricsResponse ProcessListMetricsResponse(NYql::IHTTPGateway::TResult&& response) {
    TListMetricsResult result;

    if (response.CurlResponseCode != CURLE_OK) {
        return TListMetricsResponse(TStringBuilder() << "Monitoring api list metrics response: " << response.Issues.ToOneLineString() <<
            ", internal code: " << static_cast<int>(response.CurlResponseCode));
    }

    if (response.Content.HttpResponseCode < 200 || response.Content.HttpResponseCode >= 300) {
        // Do not log the raw response body — it may contain sensitive metric data.
        return TListMetricsResponse(TStringBuilder() <<
            "Monitoring api list metrics request failed with HTTP " << response.Content.HttpResponseCode);
    }

    NJson::TJsonValue json;
    try {
        NJson::ReadJsonTree(response.Content.data(), &json, /*throwOnError*/ true);
    } catch (const std::exception& e) {
        return TListMetricsResponse("Monitoring api list metrics response is not a valid json" );
    }

    if (!json.IsMap() || !json.Has("result") || !json.Has("page")) {
        return TListMetricsResponse("Monitoring api list metrics response doesn't contain requested info");
    }

    if (!json["result"].IsArray()) {
        return TListMetricsResponse("Monitoring api list metrics response: 'result' field is not an array");
    }

    const auto pagesInfo = json["page"];
    if (!pagesInfo.IsMap() ||
        !pagesInfo.Has("pagesCount") || !pagesInfo["pagesCount"].IsInteger() ||
        !pagesInfo.Has("totalCount") || !pagesInfo["totalCount"].IsInteger()) {
        return TListMetricsResponse("Monitoring api list metrics response doesn't contain paging info");
    }

    result.PagesCount = pagesInfo["pagesCount"].GetInteger();
    result.TotalCount = pagesInfo["totalCount"].GetInteger();

    for (const auto& metricObj : json["result"].GetArray()) {
        if (!metricObj.IsMap() || !metricObj.Has("labels") || !metricObj["labels"].IsMap() || !metricObj.Has("type") || !metricObj["type"].IsString()) {
            return TListMetricsResponse("Monitoring api list metrics response contains invalid metrics");
        }

        TSelectors selectors;
        for (const auto& [key, value] : metricObj["labels"].GetMap()) {
            selectors[key] = {"==", value.GetString()};
        }

        result.Metrics.emplace_back(std::move(selectors), metricObj["type"].GetString());
    }

    return TListMetricsResponse(std::move(result), response.Content.size() + response.Content.Headers.size());
}

TListMetricsLabelsResponse ProcessListMetricsLabelsResponse(NYql::IHTTPGateway::TResult&& response) {
    TListMetricsLabelsResult result;

    if (response.CurlResponseCode != CURLE_OK) {
        return TListMetricsLabelsResponse(TStringBuilder() << "Monitoring api list metrics labels response: " << response.Issues.ToOneLineString() <<
            ", internal code: " << static_cast<int>(response.CurlResponseCode));
    }

    if (response.Content.HttpResponseCode < 200 || response.Content.HttpResponseCode >= 300) {
        // Do not log the raw response body — it may contain sensitive label values.
        return TListMetricsLabelsResponse(TStringBuilder() <<
            "Monitoring api list metrics labels request failed with HTTP " << response.Content.HttpResponseCode);
    }

    NJson::TJsonValue json;
    try {
        NJson::ReadJsonTree(response.Content.data(), &json, /*throwOnError*/ true);
    } catch (const std::exception& e) {
        return TListMetricsLabelsResponse(TStringBuilder() << "Monitoring api list metrics labels response is not a valid json: " << e.what());
    }

    if (!json.IsMap() || !json.Has("labels") || !json.Has("totalCount")) {
        return TListMetricsLabelsResponse("Monitoring api list metrics labels response doesn't contain requested info");
    }

    if (!json["totalCount"].IsInteger() || !json["labels"].IsArray()) {
        return TListMetricsLabelsResponse("Monitoring api list metrics labels response contains invalid data");
    }

    result.TotalCount = json["totalCount"].GetInteger();

    for (const auto& label : json["labels"].GetArray()) {
        try {
            TString name = label["name"].GetStringSafe();
            bool absent = label["absent"].GetBooleanSafe();
            bool truncated = label["truncated"].GetBooleanSafe();
            const auto& jsonValues = label["values"].GetArraySafe();
            std::vector<TString> values;
    
            values.reserve(jsonValues.size());
            for (const auto& labelValue : jsonValues) {
                if (!labelValue.IsString()) {
                    return TListMetricsLabelsResponse("Monitoring api list metrics labels response contains invalid label values");
                }
                values.push_back(labelValue.GetString());
            }
    
            result.Labels.emplace_back(name, absent, truncated, std::move(values));
        } catch (const NJson::TJsonException& e) {
            return TListMetricsLabelsResponse(TStringBuilder() << "Monitoring api list metrics labels response contains invalid labels: " << e.what());
        }

    }

    return TListMetricsLabelsResponse(std::move(result), response.Content.size() + response.Content.Headers.size());
}

TGetPointsCountResponse ProcessGetPointsCountResponse(NYql::IHTTPGateway::TResult&& response, ui64 downsampledPointsCount) {
    TGetPointsCountResult result;

    if (response.CurlResponseCode != CURLE_OK) {
        return TGetPointsCountResponse(TStringBuilder() << "Monitoring api points count response: " << response.Issues.ToOneLineString() <<
            ", internal code: " << static_cast<int>(response.CurlResponseCode));
    }

    if (response.Content.HttpResponseCode < 200 || response.Content.HttpResponseCode >= 300) {
        // Parse the error body as JSON and look for a structured "message" field.
        // Falling back to a substring search on raw response bodies is fragile and
        // risks leaking sensitive data into error messages.
        NJson::TJsonValue errJson;
        bool parsed = false;
        try {
            NJson::ReadJsonTree(response.Content.data(), &errJson, /*throwOnError*/ true);
            parsed = true;
        } catch (...) {}

        if (parsed && errJson.IsMap() && errJson.Has("message") && errJson["message"].IsString()) {
            const TString& message = errJson["message"].GetString();
            // Known benign condition: the metric exists but has no data points in
            // the requested time range.  Treat it as zero points rather than an error.
            if (message.Contains("Not able to apply function count on vector with size 0")) {
                result.PointsCount = downsampledPointsCount;
                return TGetPointsCountResponse(std::move(result), response.Content.size() + response.Content.Headers.size());
            }
            return TGetPointsCountResponse(TStringBuilder() << "Monitoring api points count response: " << message <<
                ", internal code: " << response.Content.HttpResponseCode);
        }

        // Non-JSON or no "message" field — report the HTTP status code only to
        // avoid logging potentially large / sensitive response bodies.
        return TGetPointsCountResponse(TStringBuilder() <<
            "Monitoring api points count request failed with HTTP " << response.Content.HttpResponseCode);
    }

    NJson::TJsonValue json;
    try {
        NJson::ReadJsonTree(response.Content.data(), &json, /*throwOnError*/ true);
    } catch (const std::exception& e) {
        return TGetPointsCountResponse("Monitoring api points count response is not a valid json");
    }

    if (!json.IsMap() || !json.Has("scalar") || !json["scalar"].IsInteger()) {
        return TGetPointsCountResponse("Monitoring api points count response doesn't contain requested info");
    }

    result.PointsCount = json["scalar"].GetInteger() + downsampledPointsCount;

    return TGetPointsCountResponse(std::move(result), response.Content.size() + response.Content.Headers.size());
}

TGetDataResponse ProcessGetDataResponse(NYdbGrpc::TGrpcStatus&& status, ReadResponse&& response) {
    TGetDataResult result;

    if (!status.Ok()) {
        TString error = TStringBuilder() << "Monitoring api get data response: " << status.Msg;
        if (status.GRpcStatusCode == grpc::StatusCode::RESOURCE_EXHAUSTED ||
            status.GRpcStatusCode == grpc::StatusCode::UNAVAILABLE ||
            status.GRpcStatusCode == grpc::StatusCode::DEADLINE_EXCEEDED ||
            status.GRpcStatusCode == grpc::StatusCode::INTERNAL ||
            status.GRpcStatusCode == grpc::StatusCode::ABORTED) {
            return TGetDataResponse(error, EStatus::STATUS_RETRIABLE_ERROR);
        }
        return TGetDataResponse(error);
    }

    if (response.response_per_query_size() != 1) {
        return TGetDataResponse("Monitoring api get data response is invalid");
    }

    const auto& responseValue = response.response_per_query()[0];
    if (!responseValue.has_timeseries_vector()) {
        return TGetDataResponse("Monitoring api get data response: missing timeseries_vector in response");
    }
    for (const auto& queryResponse : responseValue.timeseries_vector().values()) {
        auto type = MetricTypeToString(queryResponse.type());

        TSelectors selectors;
        for (const auto& [key, value] : queryResponse.labels()) {
            selectors[key] = {"==", value};
        }
        std::vector<int64_t> timestamps(queryResponse.timestamp_values().values().begin(), queryResponse.timestamp_values().values().end());
        std::vector<double> values(queryResponse.double_values().values().begin(), queryResponse.double_values().values().end());

        if (TString name = queryResponse.name()) {
            selectors["name"] = {"==", name};
        }

        TMetric metric {
            .Selectors = std::move(selectors),
            .Type = std::move(type),
        };

        result.Timeseries.emplace_back(std::move(metric), std::move(timestamps), std::move(values));
    }

    return TGetDataResponse(std::move(result), response.ByteSize());
}

class TSolomonAccessorClient : public ISolomonAccessorClient, public std::enable_shared_from_this<TSolomonAccessorClient> {
public:
    TSolomonAccessorClient(
        NYql::NSo::NProto::TDqSolomonSource&& settings,
        std::shared_ptr<NYdb::ICredentialsProvider> credentialsProvider,
        const TSolomonReadActorConfig& cfg)
        : EnableSolomonClientPostApi(cfg.EnablePostApi)
        , MaxListingPageSize(cfg.MaxListingPageSize)
        , LabelsListingLimit(cfg.LabelsListingLimit)
        , Settings(std::move(settings))
        , CredentialsProvider(credentialsProvider) {

        HttpConfig.SetMaxInFlightCount(cfg.MaxApiInflight);
        HttpGateway = IHTTPGateway::Make(&HttpConfig);

        HttpRetryPolicy = IHTTPGateway::TRetryPolicy::GetExponentialBackoffPolicy(
            [](CURLcode curlCode, long httpCode) {
                if (curlCode != CURLE_OK) {
                    return ERetryErrorClass::ShortRetry;
                }
                const auto& codes = NConstants::RetriableHttpCodes;
                if (std::find(codes.begin(), codes.end(), httpCode) != codes.end()) {
                    return ERetryErrorClass::ShortRetry;
                }
                return ERetryErrorClass::NoRetry;
            },
            cfg.RetryConfig.MinDelay,
            cfg.RetryConfig.MinLongRetryDelay,
            cfg.RetryConfig.MaxDelay,
            cfg.RetryConfig.MaxRetries,
            cfg.RetryConfig.MaxTime
        );

        GrpcConfig.Locator = GetGrpcSolomonEndpoint();
        GrpcConfig.EnableSsl = Settings.GetUseSsl();
        GrpcClient = std::make_shared<NYdbGrpc::TGRpcClientLow>();
        GrpcConnection = GrpcClient->CreateGRpcServiceConnection<DataService>(GrpcConfig);
    }

    ~TSolomonAccessorClient() override {
        GrpcClient->Stop();
    }

public:
    NThreading::TFuture<TGetLabelsResponse> GetLabelNames(const TSelectors& selectors, TInstant from, TInstant to) const override final {
        auto [url, body] = BuildGetLabelsHttpParams(selectors, from, to);

        auto resultPromise = NThreading::NewPromise<TGetLabelsResponse>();
        
        auto cb = [resultPromise, selectors](NYql::IHTTPGateway::TResult&& result) mutable {
            resultPromise.SetValue(ProcessGetLabelsResponse(std::move(result), selectors));
        };

        auto error = DoHttpRequest(
            std::move(cb),
            std::move(url),
            std::move(body)
        );

        if (error) {
            return NThreading::MakeFuture(TGetLabelsResponse(*error));
        }

        return resultPromise.GetFuture();
    }

    NThreading::TFuture<TListMetricsResponse> ListMetrics(const TSelectors& selectors, TInstant from, TInstant to) const override final {
        auto [url, body] = BuildListMetricsHttpParams(selectors, from, to);

        auto resultPromise = NThreading::NewPromise<TListMetricsResponse>();
        
        auto cb = [resultPromise](NYql::IHTTPGateway::TResult&& result) mutable {
            resultPromise.SetValue(ProcessListMetricsResponse(std::move(result)));
        };

        auto error = DoHttpRequest(
            std::move(cb),
            std::move(url),
            std::move(body)
        );

        if (error) {
            return NThreading::MakeFuture(TListMetricsResponse(*error));
        }

        return resultPromise.GetFuture();
    }

    NThreading::TFuture<TListMetricsLabelsResponse> ListMetricsLabels(const TSelectors& selectors, TInstant from, TInstant to) const override final {
        auto [url, body] = BuildListMetricsLabelsHttpParams(selectors, from, to);

        auto resultPromise = NThreading::NewPromise<TListMetricsLabelsResponse>();
        
        auto cb = [resultPromise](NYql::IHTTPGateway::TResult&& result) mutable {
            resultPromise.SetValue(ProcessListMetricsLabelsResponse(std::move(result)));
        };

        auto error = DoHttpRequest(
            std::move(cb),
            std::move(url),
            std::move(body)
        );

        if (error) {
            return NThreading::MakeFuture(TListMetricsLabelsResponse(*error));
        }

        return resultPromise.GetFuture();
    }

    NThreading::TFuture<TGetPointsCountResponse> GetPointsCount(const TSelectors& selectors, TInstant from, TInstant to) const override final {        
        auto resultPromise = NThreading::NewPromise<TGetPointsCountResponse>();

        TInstant sevenDaysAgo = TInstant::Now() - NConstants::DownsamplingCutoff;

        TInstant downsamplingFrom = from;
        TInstant downsamplingTo = Settings.GetDownsampling().GetDisabled() ? std::max(std::min(sevenDaysAgo, to), from) : to;
        ui64 gridMs = Settings.GetDownsampling().GetDisabled() ? NConstants::DefaultGridInterval.MilliSeconds() : Settings.GetDownsampling().GetGridMs();

        ui64 downsampledPointsCount = ceil((downsamplingTo - downsamplingFrom).Seconds() * 1000.0 / gridMs) + 1;

        if (downsamplingTo < to) {
            auto fullSelectors = AddRequiredLabels(selectors);
            TString program = TStringBuilder() << "count(" << BuildSelectorsProgram(fullSelectors) << ")";
            
            auto [url, body] = BuildGetPointsCountHttpParams(program, downsamplingTo, to);
            
            auto cb = [resultPromise, downsampledPointsCount](NYql::IHTTPGateway::TResult&& response) mutable {
                resultPromise.SetValue(ProcessGetPointsCountResponse(std::move(response), downsampledPointsCount));
            };
    
            auto error = DoHttpRequest(
                std::move(cb),
                std::move(url),
                std::move(body)
            );

            if (error) {
                return NThreading::MakeFuture(TGetPointsCountResponse(*error));
            }

        } else {
            TGetPointsCountResult result;
            result.PointsCount = downsampledPointsCount;

            resultPromise.SetValue(TGetPointsCountResponse(std::move(result), 0));
        }

        return resultPromise.GetFuture();
    }

    NThreading::TFuture<TGetDataResponse> GetData(const TSelectors& selectors, TInstant from, TInstant to) const override final {
        auto fullSelectors = AddRequiredLabels(selectors);
        bool isMonitoring = Settings.GetClusterType() == NProto::CT_MONITORING;

        if (isMonitoring) {
            fullSelectors["folderId"] = fullSelectors["cluster"];
            fullSelectors.erase("cluster");
            fullSelectors.erase("project");
        }

        TString program = BuildSelectorsProgram(fullSelectors, isMonitoring);

        return GetData(program, from, to);
    }

    NThreading::TFuture<TGetDataResponse> GetData(const TString& program, TInstant from, TInstant to) const override final {
        const auto request = BuildGetDataRequest(program, from, to);

        NYdbGrpc::TCallMeta callMeta;
        TString authInfo;
        if (auto error = GetAuthInfo(authInfo)) {
            return NThreading::MakeFuture(TGetDataResponse(*error));
        }
        if (!authInfo.empty()) {
            callMeta.Aux.emplace_back("authorization", authInfo);
        }
        callMeta.Aux.emplace_back("x-client-id", TString(NConstants::ClientId));

        auto resultPromise = NThreading::NewPromise<TGetDataResponse>();

        auto context = GrpcClient->CreateContext();
        if (!context) {
            resultPromise.SetValue(TGetDataResponse("Client is being shutted down"));
            return resultPromise.GetFuture();
        }
        
        // hold context until reply
        auto cb = [resultPromise, context](NYdbGrpc::TGrpcStatus&& status, ReadResponse&& result) mutable {
            resultPromise.SetValue(ProcessGetDataResponse(std::move(status), std::move(result)));
        };

        GrpcConnection->DoRequest<ReadRequest, ReadResponse>(
            std::move(request),
            std::move(cb),
            &DataService::Stub::AsyncRead,
            callMeta,
            context.get()
        );

        return resultPromise.GetFuture();
    }

private:
    std::optional<TString> GetAuthInfo(TString& auth) const {
        auth.clear();

        if (!Settings.GetUseSsl()) {
            return {};
        }

        TString authToken;
        try {
            authToken = CredentialsProvider->GetAuthInfo();
        } catch (const std::exception& ex) {
            return TStringBuilder() << "Couldn't get auth info: " << ex.what();
        }

        switch (Settings.GetClusterType()) {
            case NSo::NProto::ESolomonClusterType::CT_SOLOMON:
                auth = TStringBuilder() << "OAuth " << authToken;
                break;
            case NSo::NProto::ESolomonClusterType::CT_MONITORING:
                auth = TStringBuilder() << "Bearer " << authToken;
                break;
            default:
                return TStringBuilder() << "Can't provide auth info for unknown cluster type: " << static_cast<int>(Settings.GetClusterType());
        }

        return {};
    }

    TString GetHttpSolomonEndpoint() const {
        return TStringBuilder() << (Settings.GetUseSsl() ? "https://" : "http://") << Settings.GetHttpEndpoint();
    }

    TString GetGrpcSolomonEndpoint() const {
        return TStringBuilder() << Settings.GetGrpcEndpoint();
    }

    template <typename TCallback>
    std::optional<TString> DoHttpRequest(TCallback&& callback, TString&& url, TString&& body = "") const {
        IHTTPGateway::THeaders headers;
        TString authInfo;
        if (auto error = GetAuthInfo(authInfo)) {
            return *error;
        }
        if (!authInfo.empty()) {
            headers.Fields.emplace_back(TStringBuilder() << "Authorization: " << authInfo);
        }
        headers.Fields.emplace_back(TStringBuilder() << "x-client-id: " << NConstants::ClientId);
        headers.Fields.emplace_back("accept: application/json;charset=UTF-8");
        headers.Fields.emplace_back("Content-Type: application/json;charset=UTF-8");

        if (!body.empty()) {
            HttpGateway->Upload(
                std::move(url),
                std::move(headers),
                std::move(body),
                std::move(callback),
                false,
                HttpRetryPolicy
            );
        } else {
            HttpGateway->Download(
                std::move(url),
                std::move(headers),
                0,
                NConstants::MaxHttpGetResponseSize,
                std::move(callback),
                {},
                HttpRetryPolicy
            );
        }

        return {};
    }

    std::tuple<TString, TString> BuildGetLabelsHttpParams(const TSelectors& selectors, TInstant from, TInstant to) const {
        TUrlBuilder builder(GetHttpSolomonEndpoint());

        builder.AddPathComponent("api");
        builder.AddPathComponent("v2");
        builder.AddPathComponent("projects");
        builder.AddPathComponent(Settings.GetProject());
        builder.AddPathComponent("sensors");
        builder.AddPathComponent("names");

        NJsonWriter::TBuf w;

        if (EnableSolomonClientPostApi) {
            w.BeginObject()
                .UnsafeWriteKey("selectors").WriteString(BuildSelectorsProgram(selectors))
                .UnsafeWriteKey("from").WriteString(from.ToString())
                .UnsafeWriteKey("to").WriteString(to.ToString())
            .EndObject();
        } else {
            builder.AddUrlParam("selectors", BuildSelectorsProgram(selectors));
            builder.AddUrlParam("from", from.ToString());
            builder.AddUrlParam("to", to.ToString());
        }

        return { builder.Build(), w.Str() };
    }

    std::tuple<TString, TString> BuildListMetricsHttpParams(const TSelectors& selectors, TInstant from, TInstant to) const {
        TUrlBuilder builder(GetHttpSolomonEndpoint());

        builder.AddPathComponent("api");
        builder.AddPathComponent("v2");
        builder.AddPathComponent("projects");
        builder.AddPathComponent(Settings.GetProject());
        builder.AddPathComponent("sensors");

        builder.AddUrlParam("pageSize", ToString(MaxListingPageSize));

        NJsonWriter::TBuf w;

        if (EnableSolomonClientPostApi) {
            w.BeginObject()
                .UnsafeWriteKey("selectors").WriteString(BuildSelectorsProgram(selectors))
                .UnsafeWriteKey("from").WriteString(from.ToString())
                .UnsafeWriteKey("to").WriteString(to.ToString())
            .EndObject();
        } else {
            builder.AddUrlParam("selectors", BuildSelectorsProgram(selectors));
            builder.AddUrlParam("from", from.ToString());
            builder.AddUrlParam("to", to.ToString());
        }

        return { builder.Build(), w.Str() };
    }

    std::tuple<TString, TString> BuildListMetricsLabelsHttpParams(const TSelectors& selectors, TInstant from, TInstant to) const {
        TUrlBuilder builder(GetHttpSolomonEndpoint());

        builder.AddPathComponent("api");
        builder.AddPathComponent("v2");
        builder.AddPathComponent("projects");
        builder.AddPathComponent(Settings.GetProject());
        builder.AddPathComponent("sensors");
        builder.AddPathComponent("labels");

        NJsonWriter::TBuf w;

        if (EnableSolomonClientPostApi) {
            w.BeginObject()
                .UnsafeWriteKey("selectors").WriteString(BuildSelectorsProgram(selectors))
                .UnsafeWriteKey("from").WriteString(from.ToString())
                .UnsafeWriteKey("to").WriteString(to.ToString())
                .UnsafeWriteKey("limit").WriteLongLong(LabelsListingLimit)
            .EndObject();
        } else {
            builder.AddUrlParam("selectors", BuildSelectorsProgram(selectors));
            builder.AddUrlParam("from", from.ToString());
            builder.AddUrlParam("to", to.ToString());
            builder.AddUrlParam("limit", ToString(LabelsListingLimit));
        }

        return { builder.Build(), w.Str() };
    }

    std::tuple<TString, TString> BuildGetPointsCountHttpParams(const TString& program, TInstant from, TInstant to) const {
        TUrlBuilder builder(GetHttpSolomonEndpoint());

        builder.AddPathComponent("api");
        builder.AddPathComponent("v2");
        builder.AddPathComponent("projects");
        builder.AddPathComponent(Settings.GetProject());
        builder.AddPathComponent("sensors");
        builder.AddPathComponent("data");

        const auto& ds = Settings.GetDownsampling();
        NJsonWriter::TBuf w;
        w.BeginObject()
            .UnsafeWriteKey("from").WriteString(from.ToString())
            .UnsafeWriteKey("to").WriteString(to.ToString())
            .UnsafeWriteKey("program").WriteString(program)
            .UnsafeWriteKey("downsampling")
                .BeginObject()
                    .UnsafeWriteKey("disabled").WriteBool(ds.GetDisabled());

        if (!ds.GetDisabled()) {
            w
                .UnsafeWriteKey("aggregation").WriteString(ds.GetAggregation())
                .UnsafeWriteKey("fill").WriteString(ds.GetFill())
                .UnsafeWriteKey("gridMillis").WriteLongLong(ds.GetGridMs());
        }
        w.EndObject().EndObject();

        return { builder.Build(), w.Str() };
    }

    ReadRequest BuildGetDataRequest(const TString& program, TInstant from, TInstant to) const {
        ReadRequest request;

        if (Settings.GetClusterType() == NProto::CT_SOLOMON) {
            request.mutable_container()->set_project_id(Settings.GetProject());
        } else {
            request.mutable_container()->set_folder_id(Settings.GetCluster());
        }
        *request.mutable_from_time() = NProtoInterop::CastToProto(from);
        *request.mutable_to_time() = NProtoInterop::CastToProto(to);

        if (Settings.GetDownsampling().GetDisabled()) {
            request.mutable_downsampling()->set_disabled(true);
        } else {
            const auto downsampling = Settings.GetDownsampling();
            request.mutable_downsampling()->set_grid_interval(downsampling.GetGridMs());
            request.mutable_downsampling()->set_grid_aggregation(ParseGridAggregation(downsampling.GetAggregation()));
            request.mutable_downsampling()->set_gap_filling(ParseGapFilling(downsampling.GetFill()));
        }

        auto query = request.mutable_queries()->Add();
        *query->mutable_value() = program;
        *query->mutable_name() = "query";
        query->set_hidden(false);

        return request;
    }

    TSelectors AddRequiredLabels(const TSelectors& selectors) const {
        TSelectors fullSelectors;
        for (const auto& labelName : Settings.GetRequiredLabelNames()) {
            if (auto it = selectors.find(labelName); it != selectors.end()) {
                fullSelectors[labelName] = it->second;
            } else {
                fullSelectors[labelName] = {"=", "-"};
            }
        }
        return fullSelectors;
    }

    static TString EscapeSelectorValue(const TString& value) {
        TString escaped;
        escaped.reserve(value.size());
        for (char c : value) {
            if (c == '"' || c == '\\') {
                escaped += '\\';
            }
            escaped += c;
        }
        return escaped;
    }

    TString BuildSelectorsProgram(const TSelectors& selectors, bool useNewFormat = false) const {
        std::vector<TString> mappedValues;
        for (const auto& [key, selector] : selectors) {
            if (useNewFormat && key == "name"sv) {
                continue;
            }
            mappedValues.push_back(TStringBuilder() << key << selector.Op << "\"" << EscapeSelectorValue(selector.Value) << "\"");
        }

        TStringBuilder result;
        if (auto it = selectors.find("name"); useNewFormat && it != selectors.end()) {
            result << "\"" << EscapeSelectorValue(it->second.Value) << "\"";
        }

        return result << "{" << JoinSeq(",", mappedValues) << "}";
    }

private:
    const bool EnableSolomonClientPostApi;
    const ui64 MaxListingPageSize;
    const ui64 LabelsListingLimit;
    const NYql::NSo::NProto::TDqSolomonSource Settings;
    const std::shared_ptr<NYdb::ICredentialsProvider> CredentialsProvider;

    THttpGatewayConfig HttpConfig;
    IHTTPGateway::TPtr HttpGateway;
    IHTTPGateway::TRetryPolicy::TPtr HttpRetryPolicy;
    NYdbGrpc::TGRpcClientConfig GrpcConfig;
    std::unique_ptr<NYdbGrpc::TServiceConnection<DataService>> GrpcConnection;
    std::shared_ptr<NYdbGrpc::TGRpcClientLow> GrpcClient;
};

} // namespace

ISolomonAccessorClient::TPtr
ISolomonAccessorClient::Make(
    NYql::NSo::NProto::TDqSolomonSource source,
    std::shared_ptr<NYdb::ICredentialsProvider> credentialsProvider,
    const TSolomonReadActorConfig& cfg) {
    return std::make_shared<TSolomonAccessorClient>(std::move(source), credentialsProvider, cfg);
}

} // namespace NYql::NSo
