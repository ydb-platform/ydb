#include "solomon_accessor_client.h"

#include <library/cpp/json/writer/json.h>
#include <library/cpp/protobuf/interop/cast.h>
#include <library/cpp/threading/future/wait/wait.h>
#include <util/string/join.h>
#include <ydb/library/yql/providers/common/http_gateway/yql_http_gateway.h>
#include <ydb/public/sdk/cpp/src/library/grpc/client/grpc_client_low.h>
#include <yql/essentials/utils/url_builder.h>
#include <yql/essentials/utils/yql_panic.h>

#include <ydb/library/yql/providers/solomon/solomon_accessor/grpc/data_service.pb.h>
#include <ydb/library/yql/providers/solomon/solomon_accessor/grpc/data_service.grpc.pb.h>

#include <util/string/join.h>
#include <util/string/strip.h>
#include <util/string/split.h>

namespace NYql::NSo {

using namespace yandex::monitoring::api::v3;

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

std::vector<TString> ParseKnownLabelNames(const TString& selectors) {
    auto selectorValues = StringSplitter(selectors.substr(1, selectors.size() - 2)).Split(',').SkipEmpty().ToList<TString>();
    std::vector<TString> result;
    result.reserve(selectorValues.size());
    for (const auto& value : selectorValues) {
        result.push_back(StripString(value.substr(0, value.find('='))));
    }
    return result;
}

TGetLabelsResponse ProcessGetLabelsResponse(NYql::IHTTPGateway::TResult&& response, std::vector<TString>&& knownLabelNames) {
    TGetLabelsResult result;

    if (response.CurlResponseCode != CURLE_OK) {
        return TGetLabelsResponse(TStringBuilder{} << "Error while sending list metric names request to monitoring api: " << response.Issues.ToOneLineString());
    }

    if (response.Content.HttpResponseCode < 200 || response.Content.HttpResponseCode >= 300) {
        return TGetLabelsResponse(TStringBuilder{} << "Error while sending list metric names request to monitoring api: " << response.Content.data());
    }

    NJson::TJsonValue json;
    try {
        NJson::ReadJsonTree(response.Content.data(), &json, /*throwOnError*/ true);
    } catch (const std::exception& e) {
        return TGetLabelsResponse(TStringBuilder{} << "Failed to parse response from monitoring api: " << e.what());
    }

    if (!json.IsMap() || !json.Has("names") || !json["names"].IsArray()) {
        return TGetLabelsResponse("Invalid result from monitoring api");
    }

    const auto names = json["names"].GetArray();

    for (const auto& name : names) {
        if (!name.IsString()) {
            return TGetLabelsResponse("Invalid label names from monitoring api");
        } else {
            result.Labels.push_back(name.GetString());
        }
    }
    for (const auto& name : knownLabelNames) {
        result.Labels.push_back(name);
    }

    return TGetLabelsResponse(std::move(result));
}

TListMetricsResponse ProcessListMetricsResponse(NYql::IHTTPGateway::TResult&& response) {
    TListMetricsResult result;

    if (response.CurlResponseCode != CURLE_OK) {
        return TListMetricsResponse(TStringBuilder{} << "Error while sending list metrics request to monitoring api: " << response.Issues.ToOneLineString());
    }

    if (response.Content.HttpResponseCode < 200 || response.Content.HttpResponseCode >= 300) {
        return TListMetricsResponse(TStringBuilder{} << "Error while sending list metrics request to monitoring api: " << response.Content.data());
    }

    NJson::TJsonValue json;
    try {
        NJson::ReadJsonTree(response.Content.data(), &json, /*throwOnError*/ true);
    } catch (const std::exception& e) {
        return TListMetricsResponse(TStringBuilder{} << "Failed to parse response from monitoring api: " << e.what());
    }

    if (!json.IsMap() || !json.Has("result") || !json.Has("page")) {
        return TListMetricsResponse("Invalid list metrics result from monitoring api");
    }

    const auto pagesInfo = json["page"];
    if (!pagesInfo.IsMap() || !pagesInfo.Has("pagesCount") || !pagesInfo["pagesCount"].IsInteger()) {
        return TListMetricsResponse("Invalid paging info from monitoring api");
    }

    result.PagesCount = pagesInfo["pagesCount"].GetInteger();

    for (const auto& metricObj : json["result"].GetArray()) {
        if (!metricObj.IsMap() || !metricObj.Has("labels") || !metricObj["labels"].IsMap() || !metricObj.Has("type") || !metricObj["type"].IsString()) {
            return TListMetricsResponse("Invalid list metrics result from monitoring api");
        }

        std::map<TString, TString> metricLabels;
        for (const auto& [key, value] : metricObj["labels"].GetMap()) {
            metricLabels[key] = value.GetString();
        }

        result.Metrics.emplace_back(std::move(metricLabels), metricObj["type"].GetString());
    }

    return TListMetricsResponse(std::move(result));
}

TGetPointsCountResponse ProcessGetPointsCountResponse(NYql::IHTTPGateway::TResult&& response, size_t expectedSize) {
    TGetPointsCountResult result;

    if (response.CurlResponseCode != CURLE_OK) {
        return TGetPointsCountResponse(TStringBuilder() << "Error while sending points count request to monitoring api: " << response.Issues.ToOneLineString());
    }

    if (response.Content.HttpResponseCode < 200 || response.Content.HttpResponseCode >= 300) {
        return TGetPointsCountResponse(TStringBuilder{} << "Error while sending points count request to monitoring api: " << response.Content.data());
    }

    NJson::TJsonValue json;
    try {
        NJson::ReadJsonTree(response.Content.data(), &json, /*throwOnError*/ true);
    } catch (const std::exception& e) {
        return TGetPointsCountResponse(TStringBuilder{} << "Failed to parse points count response from monitoring api: " << e.what());
    }

    if (!json.Has("vector") || !json["vector"].IsArray()) {
        return TGetPointsCountResponse("Invalid points count result from monitoring api");
    }

    const auto counts = json["vector"].GetArray();
    if (counts.size() != expectedSize) {
        return TGetPointsCountResponse("Invalid points count response size from monitoring api");
    }

    for (size_t i = 0; i < counts.size(); ++i) {
        if (!counts[i].IsMap() || !counts[i].Has("scalar") || !counts[i].GetMap().at("scalar").IsInteger()) {
            return TGetPointsCountResponse("Invalid points count response format from monitoring api");
        }
        result.PointsCount.push_back(counts[i].GetMap().at("scalar").GetInteger());
    }

    return TGetPointsCountResponse(std::move(result));
}

TGetDataResponse ProcessGetDataResponse(NYdbGrpc::TGrpcStatus&& status, ReadResponse&& response) {
    TGetDataResult result;

    if (!status.Ok()) {
        if (status.GRpcStatusCode == grpc::StatusCode::RESOURCE_EXHAUSTED) {
            return TGetDataResponse();
        }
        return TGetDataResponse(TStringBuilder{} << "Error while sending data request to monitoring api: " << status.Msg);
    }

    if (response.response_per_query_size() != 1) {
        return TGetDataResponse("Invalid get data repsonse size from monitoring api");
    }

    const auto& responseValue = response.response_per_query()[0];
    YQL_ENSURE(responseValue.has_timeseries_vector());
    for (const auto& queryResponse : responseValue.timeseries_vector().values()) {
        auto type = MetricTypeToString(queryResponse.type());

        std::map<TString, TString> labels(queryResponse.labels().begin(), queryResponse.labels().end());
        std::vector<int64_t> timestamps(queryResponse.timestamp_values().values().begin(), queryResponse.timestamp_values().values().end());
        std::vector<double> values(queryResponse.double_values().values().begin(), queryResponse.double_values().values().end());

        TMetric metric {
            .Labels = labels,
            .Type = type,
        };

        result.Timeseries.emplace_back(std::move(metric), std::move(timestamps), std::move(values));
    }

    return TGetDataResponse(std::move(result));
}

class TSolomonAccessorClient : public ISolomonAccessorClient, public std::enable_shared_from_this<TSolomonAccessorClient> {
public:
    TSolomonAccessorClient(
        const TString& defaultReplica,
        const ui64 defaultGrpcPort,
        NYql::NSo::NProto::TDqSolomonSource&& settings,
        std::shared_ptr<NYdb::ICredentialsProvider> credentialsProvider)
        : DefaultReplica(defaultReplica)
        , DefaultGrpcPort(defaultGrpcPort)
        , Settings(std::move(settings))
        , CredentialsProvider(credentialsProvider) {

        HttpConfig.SetMaxInFlightCount(HttpMaxInflight);
        HttpGateway = IHTTPGateway::Make(&HttpConfig);

        GrpcConfig.Locator = GetGrpcSolomonEndpoint();
        GrpcConfig.EnableSsl = Settings.GetUseSsl();
        GrpcConfig.MaxInFlight = GrpcMaxInflight;
        GrpcClient = std::make_shared<NYdbGrpc::TGRpcClientLow>();
        GrpcConnection = GrpcClient->CreateGRpcServiceConnection<DataService>(GrpcConfig);
    }

    ~TSolomonAccessorClient() override {
        GrpcClient->Stop();
    }

public:
    NThreading::TFuture<TGetLabelsResponse> GetLabelNames(const TString& selectors) const override final {
        auto resultPromise = NThreading::NewPromise<TGetLabelsResponse>();
        
        auto cb = [resultPromise, selectors](NYql::IHTTPGateway::TResult&& result) mutable {
            resultPromise.SetValue(ProcessGetLabelsResponse(std::move(result), ParseKnownLabelNames(selectors)));
        };

        DoHttpRequest(
            std::move(cb),
            BuildGetLabelsUrl(selectors)
        );

        return resultPromise.GetFuture();
    }

    NThreading::TFuture<TListMetricsResponse> ListMetrics(const TString& selectors, int pageSize, int page) const override final {
        auto resultPromise = NThreading::NewPromise<TListMetricsResponse>();
        
        auto cb = [resultPromise](NYql::IHTTPGateway::TResult&& result) mutable {
            resultPromise.SetValue(ProcessListMetricsResponse(std::move(result)));
        };

        DoHttpRequest(
            std::move(cb),
            BuildListMetricsUrl(selectors, pageSize, page)
        );

        return resultPromise.GetFuture();
    }

    NThreading::TFuture<TGetPointsCountResponse> GetPointsCount(const std::vector<TMetric>& metrics) const override final {
        auto requestUrl = BuildGetPointsCountUrl();
        auto requestBody = BuildGetPointsCountBody(metrics);

        auto resultPromise = NThreading::NewPromise<TGetPointsCountResponse>();

        auto cb = [resultPromise, expectedSize = metrics.size()](NYql::IHTTPGateway::TResult&& response) mutable {
            resultPromise.SetValue(ProcessGetPointsCountResponse(std::move(response), expectedSize));
        };

        DoHttpRequest(
            std::move(cb),
            std::move(requestUrl),
            std::move(requestBody)
        );

        return resultPromise.GetFuture();
    }

    NThreading::TFuture<TGetDataResponse> GetData(TMetric metric, TInstant from, TInstant to) const override final {
        return GetData(BuildSelectorsProgram(metric), from, to);
    }

    NThreading::TFuture<TGetDataResponse> GetData(TString selectors, TInstant from, TInstant to) const override final {
        const auto request = BuildGetDataRequest(selectors, from, to);

        NYdbGrpc::TCallMeta callMeta;
        if (auto authInfo = GetAuthInfo()) {
            callMeta.Aux.emplace_back("authorization", *authInfo);
        }
        callMeta.Aux.emplace_back("x-client-id", "yandex-query");

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
    std::optional<TString> GetAuthInfo() const {
        if (!Settings.GetUseSsl()) {
            return {};
        }

        const TString authToken = CredentialsProvider->GetAuthInfo();

        switch (Settings.GetClusterType()) {
            case NSo::NProto::ESolomonClusterType::CT_SOLOMON:
                return "OAuth " + authToken;
            case NSo::NProto::ESolomonClusterType::CT_MONITORING:
                return "Bearer " + authToken;
            default:
                Y_ENSURE(false, "Invalid cluster type " << ToString<ui32>(Settings.GetClusterType()));
        }
    }

    TString GetHttpSolomonEndpoint() const {
        return (Settings.GetUseSsl() ? "https://" : "http://") + Settings.GetEndpoint();
    }

    TString GetGrpcSolomonEndpoint() const {
        return TStringBuilder() << Settings.GetEndpoint() << ":" << DefaultGrpcPort;
    }

    template <typename TCallback>
    void DoHttpRequest(TCallback&& callback, TString&& url, TString&& body = "") const {
        IHTTPGateway::THeaders headers;
        if (auto authInfo = GetAuthInfo()) {
            headers.Fields.emplace_back(TStringBuilder{} << "Authorization: " << *authInfo);
        }
        headers.Fields.emplace_back("x-client-id: yandex-query");
        headers.Fields.emplace_back("accept: application/json;charset=UTF-8");
        headers.Fields.emplace_back("Content-Type: application/json;charset=UTF-8");

        auto retryPolicy = IHTTPGateway::TRetryPolicy::GetExponentialBackoffPolicy(
            [](CURLcode, long httpCode) {
                if (httpCode == 429 /*RESOURCE_EXHAUSTED*/) {
                    return ERetryErrorClass::ShortRetry;
                }
                return ERetryErrorClass::NoRetry;
            },
            TDuration::MilliSeconds(10),
            TDuration::MilliSeconds(200),
            TDuration::Seconds(1)
        );

        if (!body.empty()) {
            HttpGateway->Upload(
                std::move(url),
                std::move(headers),
                std::move(body),
                std::move(callback),
                false,
                retryPolicy
            );
        } else {
            HttpGateway->Download(
                std::move(url),
                std::move(headers),
                0,
                ListSizeLimit,
                std::move(callback),
                {},
                retryPolicy
            );
        }
    }

    TString BuildGetLabelsUrl(const TString& selectors) const {
        TUrlBuilder builder(GetHttpSolomonEndpoint());

        builder.AddPathComponent("api");
        builder.AddPathComponent("v2");
        builder.AddPathComponent("projects");
        builder.AddPathComponent(Settings.GetProject());
        builder.AddPathComponent("sensors");
        builder.AddPathComponent("names");

        builder.AddUrlParam("selectors", selectors);
        builder.AddUrlParam("forceCluster", DefaultReplica);

        return builder.Build();
    }

    TString BuildListMetricsUrl(const TString& selectors, int pageSize, int page) const {
        TUrlBuilder builder(GetHttpSolomonEndpoint());

        builder.AddPathComponent("api");
        builder.AddPathComponent("v2");
        builder.AddPathComponent("projects");
        builder.AddPathComponent(Settings.GetProject());
        builder.AddPathComponent("sensors");

        builder.AddUrlParam("selectors", selectors);
        builder.AddUrlParam("forceCluster", DefaultReplica);
        builder.AddUrlParam("pageSize", std::to_string(pageSize));
        builder.AddUrlParam("page", std::to_string(page));
        builder.AddUrlParam("from", TInstant::Seconds(Settings.GetFrom()).ToIsoStringLocalUpToSeconds());
        builder.AddUrlParam("to", TInstant::Seconds(Settings.GetTo()).ToIsoStringLocalUpToSeconds());

        return builder.Build();
    }

    TString BuildGetPointsCountUrl() const {
        TUrlBuilder builder(GetHttpSolomonEndpoint());

        builder.AddPathComponent("api");
        builder.AddPathComponent("v2");
        builder.AddPathComponent("projects");
        builder.AddPathComponent(Settings.GetProject());
        builder.AddPathComponent("sensors");
        builder.AddPathComponent("data");

        return builder.Build();
    }

    TString BuildGetPointsCountBody(const std::vector<TMetric>& metrics) const {
        std::vector<TString> selectors;
        selectors.reserve(metrics.size());
        std::transform(
            metrics.begin(),
            metrics.end(),
            std::back_inserter(selectors),
            [this](const TMetric& metric) -> TString {
                return TStringBuilder() << "count(" << BuildSelectorsProgram(metric) << ")";
            }
        );

        TString program = TStringBuilder() << "[" << JoinSeq(",", selectors) << "]";

        const auto& ds = Settings.GetDownsampling();
        NJsonWriter::TBuf w;
        w.BeginObject()
            .UnsafeWriteKey("from").WriteString(TInstant::Seconds(Settings.GetFrom()).ToString())
            .UnsafeWriteKey("to").WriteString(TInstant::Seconds(Settings.GetTo()).ToString())
            .UnsafeWriteKey("program").WriteString(program)
            .UnsafeWriteKey("downsampling")
                .BeginObject()
                    .UnsafeWriteKey("disabled").WriteBool(ds.GetDisabled());

        if (!ds.GetDisabled()) {
            w.UnsafeWriteKey("aggregation").WriteString(ds.GetAggregation())
                .UnsafeWriteKey("fill").WriteString(ds.GetFill())
                .UnsafeWriteKey("gridMillis").WriteLongLong(ds.GetGridMs());
        }
        w.EndObject()
            .UnsafeWriteKey("forceCluster").WriteString(DefaultReplica)
        .EndObject();

        return w.Str();
    }

    ReadRequest BuildGetDataRequest(const TString& selectors, TInstant from, TInstant to) const {
        ReadRequest request;

        request.mutable_container()->set_project_id(Settings.GetProject());
        *request.mutable_from_time() = NProtoInterop::CastToProto(from);
        *request.mutable_to_time() = NProtoInterop::CastToProto(to);
        *request.mutable_force_replica() = DefaultReplica;

        if (Settings.GetDownsampling().GetDisabled()) {
            request.mutable_downsampling()->set_disabled(true);
        } else {
            const auto downsampling = Settings.GetDownsampling();
            request.mutable_downsampling()->set_grid_interval(downsampling.GetGridMs());
            request.mutable_downsampling()->set_grid_aggregation(ParseGridAggregation(downsampling.GetAggregation()));
            request.mutable_downsampling()->set_gap_filling(ParseGapFilling(downsampling.GetFill()));
        }

        auto query = request.mutable_queries()->Add();
        *query->mutable_value() = selectors;
        *query->mutable_name() = "query";
        query->set_hidden(false);

        return request;
    }

    TString BuildSelectorsProgram(const TMetric& metric) const {
        std::vector<TString> mappedValues;
        mappedValues.reserve(Settings.GetRequiredLabelNames().size());
        std::transform(
            Settings.GetRequiredLabelNames().begin(),
            Settings.GetRequiredLabelNames().end(),
            std::back_inserter(mappedValues),
            [&labels = metric.Labels](const TString& labelName) -> TString {
                if (labels.find(labelName) == labels.end()) {
                    return TStringBuilder() << labelName << "=\"-\"";
                }
                return TStringBuilder() << labelName << "=\"" << labels.at(labelName) << "\"";
            }
        );

        return TStringBuilder() << "{" << JoinSeq(",", mappedValues) << "}";
    }

private:
    const TString DefaultReplica;
    const ui64 DefaultGrpcPort;
    const ui64 ListSizeLimit = 1ull << 20;
    const ui64 HttpMaxInflight = 200;
    const ui64 GrpcMaxInflight = 2000;
    const NYql::NSo::NProto::TDqSolomonSource Settings;
    const std::shared_ptr<NYdb::ICredentialsProvider> CredentialsProvider;

    THttpGatewayConfig HttpConfig;
    IHTTPGateway::TPtr HttpGateway;
    NYdbGrpc::TGRpcClientConfig GrpcConfig;
    std::unique_ptr<NYdbGrpc::TServiceConnection<DataService>> GrpcConnection;
    std::shared_ptr<NYdbGrpc::TGRpcClientLow> GrpcClient;
};

} // namespace

ISolomonAccessorClient::TPtr
ISolomonAccessorClient::Make(
    NYql::NSo::NProto::TDqSolomonSource source,
    std::shared_ptr<NYdb::ICredentialsProvider> credentialsProvider) {
    const auto& settings = source.settings();

    TString defaultReplica = "sas";
    if (auto it = settings.find("solomonClientDefaultReplica"); it != settings.end()) {
        defaultReplica = it->second;
    }

    ui64 defaultGrpcPort = 443;
    if (auto it = settings.find("grpcPort"); it != settings.end()) {
        defaultGrpcPort = FromString<ui64>(it->second);
    }

    return std::make_shared<TSolomonAccessorClient>(defaultReplica, defaultGrpcPort, std::move(source), credentialsProvider);
}

} // namespace NYql::NSo
