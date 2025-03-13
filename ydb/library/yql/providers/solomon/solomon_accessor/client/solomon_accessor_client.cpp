#include "solomon_accessor_client.h"

#include <library/cpp/protobuf/interop/cast.h>
#include <ydb/library/yql/providers/common/http_gateway/yql_http_gateway.h>
#include <ydb/public/sdk/cpp/src/library/grpc/client/grpc_client_low.h>
#include <yql/essentials/utils/url_builder.h>
#include <yql/essentials/utils/yql_panic.h>

#include <ydb/library/yql/providers/solomon/solomon_accessor/grpc/solomon_accessor_pb.pb.h>
#include <ydb/library/yql/providers/solomon/solomon_accessor/grpc/solomon_accessor_pb.grpc.pb.h>

namespace NYql::NSo {

using namespace yandex::monitoring::api::v3;

namespace {

Downsampling::GapFilling ParseGapFilling(const TString& fill)
{
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

Downsampling::GridAggregation ParseGridAggregation(const TString& aggregation)
{
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

TString MetricTypeToString(MetricType type)
{
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

class TSolomonAccessorClient : public ISolomonAccessorClient, public std::enable_shared_from_this<TSolomonAccessorClient>
{
public:
    TSolomonAccessorClient(
        const TString& defaultReplica,
        const ui64 defaultGrpcPort,
        const NYql::NSo::NProto::TDqSolomonSource& settings,
        std::shared_ptr<NYdb::ICredentialsProvider> credentialsProvider
        )
        : DefaultReplica(defaultReplica)
        , DefaultGrpcPort(defaultGrpcPort)
        , Settings(settings)
        , CredentialsProvider(credentialsProvider)
        , HttpGateway(IHTTPGateway::Make())
        , GrpcClient(std::make_shared<NYdbGrpc::TGRpcClientLow>())
    {
        GrpcConfig.Locator = GetGrpcSolomonEndpoint();
        GrpcConfig.EnableSsl = Settings.GetUseSsl();
    }

public:
    NThreading::TFuture<TListMetricsResult> ListMetrics(const TString& selectors, int pageSize, int page) const override final
    {
        const auto request = BuildListMetricsRequest(selectors, pageSize, page);

        IHTTPGateway::THeaders headers;
        if (auto authInfo = GetAuthInfo()) {
            headers.Fields.emplace_back(TStringBuilder{} << "Authorization: " << *authInfo);
        }

        auto resultPromise = NThreading::NewPromise<TListMetricsResult>();
        
        std::weak_ptr<const TSolomonAccessorClient> weakSelf = shared_from_this();
        // hold context until reply
        auto cb = [weakSelf, resultPromise](NYql::IHTTPGateway::TResult&& result) mutable
        {
            if (auto self = weakSelf.lock()) {
                resultPromise.SetValue(self->ProcessHttpResponse(std::move(result)));
            } else {
                resultPromise.SetValue(TListMetricsResult("Client has been shut down"));
            }
        };

        HttpGateway->Download(
            request,
            headers,
            0,
            ListSizeLimit,
            std::move(cb)
        );

        return resultPromise.GetFuture();
    }

    NThreading::TFuture<TGetDataResult> GetData(const std::vector<TString>& selectors) const override final
    {
        const auto request = BuildGetDataRequest(selectors);

        NYdbGrpc::TCallMeta callMeta;
        if (auto authInfo = GetAuthInfo()) {
            callMeta.Aux.emplace_back("authorization", *authInfo);
        }

        auto resultPromise = NThreading::NewPromise<TGetDataResult>();

        const auto connection = GrpcClient->CreateGRpcServiceConnection<DataService>(GrpcConfig);

        auto context = GrpcClient->CreateContext();
        if (!context) {
            resultPromise.SetValue(TGetDataResult("Client is being shutted down"));
            return resultPromise.GetFuture();
        }
        
        std::weak_ptr<const TSolomonAccessorClient> weakSelf = shared_from_this();
        // hold context until reply
        auto cb = [weakSelf, resultPromise, context](
            NYdbGrpc::TGrpcStatus&& status,
            ReadResponse&& result) mutable
        {
            if (auto self = weakSelf.lock()) {
                resultPromise.SetValue(self->ProcessGrpcResponse(std::move(status), std::move(result)));
            } else {
                resultPromise.SetValue(TGetDataResult("Client has been shut down"));
            }
        };

        connection->DoRequest<ReadRequest, ReadResponse>(
                    std::move(request),
                    std::move(cb),
                    &DataService::Stub::AsyncRead,
                    callMeta,
                    context.get()
                );

        return resultPromise.GetFuture();
    }

private:
    TMaybe<TString> GetAuthInfo() const
    {
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

    TString GetHttpSolomonEndpoint() const
    {
        return (Settings.GetUseSsl() ? "https://" : "http://") + Settings.GetEndpoint();
    }

    TString GetGrpcSolomonEndpoint() const
    {
        return TStringBuilder() << Settings.GetEndpoint() << ":" << DefaultGrpcPort;
    }

    TString BuildListMetricsRequest(const TString& selectors, int pageSize, int page) const
    {
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

        return builder.Build();
    }

    ReadRequest BuildGetDataRequest(const std::vector<TString>& selectors) const
    {
        ReadRequest request;

        request.mutable_container()->set_project_id(Settings.GetProject());
        *request.mutable_from_time() = NProtoInterop::CastToProto(TInstant::Seconds(Settings.GetFrom()));
        *request.mutable_to_time() = NProtoInterop::CastToProto(TInstant::Seconds(Settings.GetTo()));
        *request.mutable_force_replica() = DefaultReplica;

        if (Settings.GetDownsampling().GetDisabled()) {
            request.mutable_downsampling()->set_disabled(true);
        } else {
            const auto downsampling = Settings.GetDownsampling();
            request.mutable_downsampling()->set_grid_interval(downsampling.GetGridMs());
            request.mutable_downsampling()->set_grid_aggregation(ParseGridAggregation(downsampling.GetAggregation()));
            request.mutable_downsampling()->set_gap_filling(ParseGapFilling(downsampling.GetFill()));
        }

        ui64 cnt = 0;
        for (const auto& metric : selectors) {
            auto query = request.mutable_queries()->Add();
            *query->mutable_value() = metric;
            *query->mutable_name() = TStringBuilder() << "query" << cnt++;
            query->set_hidden(false);
        }

        return request;
    }

    TListMetricsResult ProcessHttpResponse(NYql::IHTTPGateway::TResult&& response) const
    {
        std::vector<TMetric> result;

        if (response.Content.HttpResponseCode < 200 || response.Content.HttpResponseCode >= 300) {
            return TListMetricsResult(TStringBuilder{} << "Error while sending list metrics request to monitoring api: " << response.Content.data());
        }

        NJson::TJsonValue json;
        try {
            NJson::ReadJsonTree(response.Content.data(), &json, /*throwOnError*/ true);
        } catch (const std::exception& e) {
            return TListMetricsResult(TStringBuilder{} << "Failed to parse response from monitoring api: " << e.what());
        }

        if (!json.IsMap() || !json.Has("result") || !json.Has("page")) {
            return TListMetricsResult(TStringBuilder{} << "Invalid result from monitoring api");
        }

        const auto pagesInfo = json["page"];
        if (!pagesInfo.IsMap() || !pagesInfo.Has("pagesCount") || !pagesInfo["pagesCount"].IsInteger()) {
            return TListMetricsResult(TStringBuilder{} << "Invalid paging info from monitoring api");
        }

        size_t pagesCount = pagesInfo["pagesCount"].GetInteger();

        for (const auto& metricObj : json["result"].GetArray()) {
            try {
                result.emplace_back(metricObj);
            } catch (const std::exception& e) {
                return TListMetricsResult(TStringBuilder{} << "Failed to parse result response from monitoring: " << e.what());
            }
        }

        return TListMetricsResult(pagesCount, std::move(result));
    }

    TGetDataResult ProcessGrpcResponse(NYdbGrpc::TGrpcStatus&& status, ReadResponse&& response) const
    {
        std::vector<TTimeseries> result;

        if (!status.Ok()) {
            return TGetDataResult(TStringBuilder{} << "Error while sending data request to monitoring api: " << status.Msg);
        }

        for (const auto& responseValue : response.response_per_query()) {
            YQL_ENSURE(responseValue.has_timeseries_vector());
            for (const auto& queryResponse : responseValue.timeseries_vector().values()) {
                auto type = MetricTypeToString(queryResponse.type());
    
                std::map<TString, TString> labels(queryResponse.labels().begin(), queryResponse.labels().end());                
                std::vector<int64_t> timestamps(queryResponse.timestamp_values().values().begin(), queryResponse.timestamp_values().values().end());
                std::vector<double> values(queryResponse.double_values().values().begin(), queryResponse.double_values().values().end());
    
                result.emplace_back(queryResponse.name(), std::move(labels), type, std::move(timestamps), std::move(values));
            }

        }

        return TGetDataResult(std::move(result));
    }

private:
    const TString DefaultReplica;
    const ui64 DefaultGrpcPort;
    const size_t ListSizeLimit = 1ull << 20;
    const NYql::NSo::NProto::TDqSolomonSource& Settings;
    const std::shared_ptr<NYdb::ICredentialsProvider> CredentialsProvider;

    IHTTPGateway::TPtr HttpGateway;
    NYdbGrpc::TGRpcClientConfig GrpcConfig;
    std::shared_ptr<NYdbGrpc::TGRpcClientLow> GrpcClient;
};

} // namespace

TMetric::TMetric(const NJson::TJsonValue& value)
{
    YQL_ENSURE(value.IsMap());

    if (value.Has("labels")) {
        auto labels = value["labels"];
        YQL_ENSURE(labels.IsMap());

        for (const auto& [key, value] : labels.GetMapSafe()) {
            YQL_ENSURE(value.IsString());
            Labels[key] = value.GetString();
        }
    }

    if (value.Has("type")) {
        YQL_ENSURE(value["type"].IsString());
        Type = value["type"].GetString();
    }

    if (value.Has("createdAt")) {
        YQL_ENSURE(value["createdAt"].IsString());
        CreatedAt = value["createdAt"].GetString();
    }
}

ISolomonAccessorClient::TListMetricsResult::TListMetricsResult()
    : Success(false)
{}

ISolomonAccessorClient::TListMetricsResult::TListMetricsResult(const TString& error)
    : Success(false)
    , ErrorMsg(error)
{}

ISolomonAccessorClient::TListMetricsResult::TListMetricsResult(size_t pagesCount, std::vector<TMetric>&& result)
    : Success(true)
    , PagesCount(pagesCount)
    , Result(std::move(result))
{}

ISolomonAccessorClient::TGetDataResult::TGetDataResult()
    : Success(false)
{}

ISolomonAccessorClient::TGetDataResult::TGetDataResult(const TString& error)
    : Success(false)
    , ErrorMsg(error)
{}

ISolomonAccessorClient::TGetDataResult::TGetDataResult(std::vector<TTimeseries>&& result)
    : Success(true)
    , Result(std::move(result))
{}

ISolomonAccessorClient::TPtr
ISolomonAccessorClient::Make(
    const NYql::NSo::NProto::TDqSolomonSource& source,
    std::shared_ptr<NYdb::ICredentialsProvider> credentialsProvider)
{
    const auto& settings = source.settings();

    TString defaultReplica = "sas";
    if (auto it = settings.find("solomonClientDefaultReplica"); it != settings.end()) {
        defaultReplica = it->second;
    }

    ui64 defaultGrpcPort = 443;
    if (auto it = settings.find("grpcPort"); it != settings.end()) {
        defaultGrpcPort = FromString<ui64>(it->second);
    }

    return std::make_shared<TSolomonAccessorClient>(defaultReplica, defaultGrpcPort, source, credentialsProvider);
}

} // namespace NYql::NSo
