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

TGetLabelsResponse ProcessGetLabelsResponse(NYql::IHTTPGateway::TResult&& response, const std::map<TString, TString>& knownSelectors) {
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
    for (const auto& [key, value] : knownSelectors) {
        result.Labels.push_back(key);
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
    if (!pagesInfo.IsMap() || 
        !pagesInfo.Has("pagesCount") || !pagesInfo["pagesCount"].IsInteger() || 
        !pagesInfo.Has("totalCount") || !pagesInfo["totalCount"].IsInteger()) {
        return TListMetricsResponse("Invalid paging info from monitoring api");
    }

    result.PagesCount = pagesInfo["pagesCount"].GetInteger();
    result.TotalCount = pagesInfo["totalCount"].GetInteger();

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

TGetPointsCountResponse ProcessGetPointsCountResponse(NYql::IHTTPGateway::TResult&& response, ui64 downsampledPointsCount) {
    static std::set<TString> whitelistIssues = {
        "Not able to apply function count on vector with size 0"
    };

    TGetPointsCountResult result;

    if (response.CurlResponseCode != CURLE_OK) {
        TString issues = response.Issues.ToOneLineString();

        for (const auto& whitelistIssue : whitelistIssues) {
            if (issues.find(whitelistIssue) != issues.npos) {
                result.PointsCount = 0;
                return TGetPointsCountResponse(std::move(result));
            }
        }

        return TGetPointsCountResponse(TStringBuilder() << "Error while sending points count request to monitoring api: " << issues);
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

    if (!json.IsMap() || !json.Has("scalar") || !json["scalar"].IsInteger()) {
        return TGetPointsCountResponse("Invalid points count result from monitoring api");
    }

    result.PointsCount = json["scalar"].GetInteger() + downsampledPointsCount;

    return TGetPointsCountResponse(std::move(result));
}

TGetDataResponse ProcessGetDataResponse(NYdbGrpc::TGrpcStatus&& status, ReadResponse&& response) {
    TGetDataResult result;

    if (!status.Ok()) {
        TString error = TStringBuilder{} << "Error while sending data request to monitoring api: " << status.Msg;
        if (status.GRpcStatusCode == grpc::StatusCode::RESOURCE_EXHAUSTED || status.GRpcStatusCode == grpc::StatusCode::UNAVAILABLE) {
            return TGetDataResponse(error, EStatus::STATUS_RETRIABLE_ERROR);
        }
        return TGetDataResponse(error);
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

        if (TString name = queryResponse.name()) {
            labels["name"] = name;
        }

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
        ui64 maxApiInflight,
        NYql::NSo::NProto::TDqSolomonSource&& settings,
        std::shared_ptr<NYdb::ICredentialsProvider> credentialsProvider)
        : DefaultReplica(defaultReplica)
        , Settings(std::move(settings))
        , CredentialsProvider(credentialsProvider) {

        HttpConfig.SetMaxInFlightCount(maxApiInflight);
        HttpGateway = IHTTPGateway::Make(&HttpConfig);

        GrpcConfig.Locator = GetGrpcSolomonEndpoint();
        GrpcConfig.EnableSsl = Settings.GetUseSsl();
        GrpcClient = std::make_shared<NYdbGrpc::TGRpcClientLow>();
        GrpcConnection = GrpcClient->CreateGRpcServiceConnection<DataService>(GrpcConfig);
    }

    ~TSolomonAccessorClient() override {
        GrpcClient->Stop();
    }

public:
    NThreading::TFuture<TGetLabelsResponse> GetLabelNames(const std::map<TString, TString>& selectors, TInstant from, TInstant to) const override final {
        auto requestUrl = BuildGetLabelsUrl(selectors, from, to);

        auto resultPromise = NThreading::NewPromise<TGetLabelsResponse>();
        
        auto cb = [resultPromise, selectors](NYql::IHTTPGateway::TResult&& result) mutable {
            resultPromise.SetValue(ProcessGetLabelsResponse(std::move(result), selectors));
        };

        DoHttpRequest(
            std::move(cb),
            std::move(requestUrl)
        );

        return resultPromise.GetFuture();
    }

    NThreading::TFuture<TListMetricsResponse> ListMetrics(const std::map<TString, TString>& selectors, TInstant from, TInstant to, int pageSize, int page) const override final {
        auto requestUrl = BuildListMetricsUrl(selectors, from, to, pageSize, page);

        auto resultPromise = NThreading::NewPromise<TListMetricsResponse>();
        
        auto cb = [resultPromise](NYql::IHTTPGateway::TResult&& result) mutable {
            resultPromise.SetValue(ProcessListMetricsResponse(std::move(result)));
        };

        DoHttpRequest(
            std::move(cb),
            std::move(requestUrl)
        );

        return resultPromise.GetFuture();
    }

    NThreading::TFuture<TGetPointsCountResponse> GetPointsCount(const std::map<TString, TString>& selectors, TInstant from, TInstant to) const override final {        
        auto resultPromise = NThreading::NewPromise<TGetPointsCountResponse>();

        TInstant sevenDaysAgo = TInstant::Now() - TDuration::Days(7); // points older then a week ago are automatically downsampled by solomon backend

        TInstant downsamplingFrom = from;
        TInstant downsamplingTo = Settings.GetDownsampling().GetDisabled() ? std::max(std::min(sevenDaysAgo, to), from) : to;

        ui64 downsampledPointsCount = floor((downsamplingTo - downsamplingFrom).Seconds() * 1000.0 / Settings.GetDownsampling().GetGridMs()) + 1;

        if (downsamplingTo < to) {
            auto fullSelectors = AddRequiredLabels(selectors);
            TString program = TStringBuilder() << "count(" << BuildSelectorsProgram(fullSelectors) << ")";
            
            auto requestUrl = BuildGetPointsCountUrl();
            auto requestBody = BuildGetPointsCountBody(program, downsamplingTo, to);
            
            auto cb = [resultPromise, downsampledPointsCount](NYql::IHTTPGateway::TResult&& response) mutable {
                resultPromise.SetValue(ProcessGetPointsCountResponse(std::move(response), downsampledPointsCount));
            };
    
            DoHttpRequest(
                std::move(cb),
                std::move(requestUrl),
                std::move(requestBody)
            );

        } else {
            TGetPointsCountResult result;
            result.PointsCount = downsampledPointsCount;

            resultPromise.SetValue(TGetPointsCountResponse(std::move(result)));
        }

        return resultPromise.GetFuture();
    }

    NThreading::TFuture<TGetDataResponse> GetData(const std::map<TString, TString>& selectors, TInstant from, TInstant to) const override final {
        auto fullSelectors = AddRequiredLabels(selectors);

        if (Settings.GetClusterType() == NProto::CT_MONITORING) {
            fullSelectors["folderId"] = fullSelectors["cluster"];
            fullSelectors.erase("cluster");
            fullSelectors.erase("project");
        }

        TString program = BuildSelectorsProgram(fullSelectors, true);

        return GetData(program, from, to);
    }

    NThreading::TFuture<TGetDataResponse> GetData(TString program, TInstant from, TInstant to) const override final {
        const auto request = BuildGetDataRequest(program, from, to);

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
        return TStringBuilder() << (Settings.GetUseSsl() ? "https://" : "http://") << Settings.GetHttpEndpoint();
    }

    TString GetGrpcSolomonEndpoint() const {
        return TStringBuilder() << Settings.GetGrpcEndpoint();
    }

    TString GetProjectId() const {
        switch (Settings.GetClusterType()) {
            case NSo::NProto::ESolomonClusterType::CT_SOLOMON:
                return Settings.GetProject();
            case NSo::NProto::ESolomonClusterType::CT_MONITORING:
                return Settings.GetCluster();
            default:
                Y_ENSURE(false, "Invalid cluster type " << ToString<ui32>(Settings.GetClusterType()));
        }
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
                if (httpCode == 429 /* RESOURCE_EXHAUSTED */ || httpCode == 503 /* shard in not ready yet */) {
                    return ERetryErrorClass::ShortRetry;
                }
                return ERetryErrorClass::NoRetry;
            },
            TDuration::MilliSeconds(50),
            TDuration::MilliSeconds(200),
            TDuration::MilliSeconds(1000),
            10
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

    TString BuildGetLabelsUrl(const std::map<TString, TString>& selectors, TInstant from, TInstant to) const {
        TUrlBuilder builder(GetHttpSolomonEndpoint());

        builder.AddPathComponent("api");
        builder.AddPathComponent("v2");
        builder.AddPathComponent("projects");
        builder.AddPathComponent(Settings.GetProject());
        builder.AddPathComponent("sensors");
        builder.AddPathComponent("names");

        builder.AddUrlParam("projectId", GetProjectId());
        builder.AddUrlParam("selectors", BuildSelectorsProgram(selectors));
        builder.AddUrlParam("forceCluster", DefaultReplica);
        builder.AddUrlParam("from", from.ToString());
        builder.AddUrlParam("to", to.ToString());

        return builder.Build();
    }

    TString BuildListMetricsUrl(const std::map<TString, TString>& selectors, TInstant from, TInstant to, int pageSize, int page) const {
        TUrlBuilder builder(GetHttpSolomonEndpoint());

        builder.AddPathComponent("api");
        builder.AddPathComponent("v2");
        builder.AddPathComponent("projects");
        builder.AddPathComponent(Settings.GetProject());
        builder.AddPathComponent("sensors");

        builder.AddUrlParam("projectId", GetProjectId());
        builder.AddUrlParam("selectors", BuildSelectorsProgram(selectors));
        builder.AddUrlParam("forceCluster", DefaultReplica);
        builder.AddUrlParam("from", from.ToString());
        builder.AddUrlParam("to", to.ToString());
        builder.AddUrlParam("pageSize", std::to_string(pageSize));
        builder.AddUrlParam("page", std::to_string(page));

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

        builder.AddUrlParam("projectId", GetProjectId());

        return builder.Build();
    }

    TString BuildGetPointsCountBody(const TString& program, TInstant from, TInstant to) const {
        const auto& ds = Settings.GetDownsampling();
        NJsonWriter::TBuf w;
        w.BeginObject()
            .UnsafeWriteKey("from").WriteString(from.ToString())
            .UnsafeWriteKey("to").WriteString(to.ToString())
            .UnsafeWriteKey("program").WriteString(program)
            .UnsafeWriteKey("forceCluster").WriteString(DefaultReplica)
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

        return w.Str();
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

    std::map<TString, TString> AddRequiredLabels(const std::map<TString, TString>& labels) const {
        std::map<TString, TString> fullSelectors;
        for (const auto& labelName : Settings.GetRequiredLabelNames()) {
            if (auto it = labels.find(labelName); it != labels.end()) {
                fullSelectors[labelName] = it->second;
            } else {
                fullSelectors[labelName] = "-";
            }
        }
        return fullSelectors;
    }

    TString BuildSelectorsProgram(const std::map<TString, TString>& labels, bool useNewFormat = false) const {
        std::vector<TString> mappedValues;
        for (const auto& [key, value] : labels) {
            if (useNewFormat && key == "name"sv) {
                continue;
            }
            mappedValues.push_back(TStringBuilder() << key << "=\"" << value << "\"");
        }

        TStringBuilder result;
        if (auto it = labels.find("name"); useNewFormat && it != labels.end()) {
            result << "\"" << it->second << "\"";
        }

        return result << "{" << JoinSeq(",", mappedValues) << "}";
    }

private:
    const TString DefaultReplica;
    const ui64 ListSizeLimit = 1ull << 20;
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

    TString defaultReplica;
    if (auto it = settings.find("solomonClientDefaultReplica"); it != settings.end()) {
        defaultReplica = it->second;
    }

    ui64 maxApiInflight = 40;
    if (auto it = settings.find("maxApiInflight"); it != settings.end()) {
        maxApiInflight = FromString<ui64>(it->second);
    }

    return std::make_shared<TSolomonAccessorClient>(defaultReplica, maxApiInflight, std::move(source), credentialsProvider);
}

} // namespace NYql::NSo
