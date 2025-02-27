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

MetricType ParseMetricType(const TString& type)
{
    if (type == "DGAUGE"sv) {
        return MetricType::DGAUGE;
    }
    if (type == "IGAUGE"sv) {
        return MetricType::IGAUGE;
    }
    if (type == "COUNTER"sv) {
        return MetricType::COUNTER;
    }
    if (type == "RATE"sv) {
        return MetricType::RATE;
    }
    return MetricType::METRIC_TYPE_UNSPECIFIED;
}

class TSolomonAccessorClient : public ISolomonAccessorClient, public std::enable_shared_from_this<TSolomonAccessorClient>
{
public:
    TSolomonAccessorClient(
        NYql::NSo::NProto::TDqSolomonSource&& settings,
        std::shared_ptr<NYdb::ICredentialsProvider> credentialsProvider
        )
        : DefaultReplica("sas")
        , Settings(std::move(settings))
        , CredentialsProvider(credentialsProvider)
        , GrpcClient(std::make_shared<NYdbGrpc::TGRpcClientLow>())
        , HttpGateway(IHTTPGateway::Make())
    {}

public:
    NThreading::TFuture<TListMetricsResult> ListMetrics(const TString& selectors, int pageSize, int page) override final
    {
        const auto request = BuildListMetricsRequest(selectors, pageSize, page);

        IHTTPGateway::THeaders headers;
        headers.Fields.emplace_back(TStringBuilder{} << "Authorization: " << GetAuthInfo());

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

    NThreading::TFuture<TGetDataResult> GetData(const std::vector<TString>& selectors) override final
    {
        const auto request = BuildGetDataRequest(selectors);

        NYdbGrpc::TCallMeta callMeta;
        callMeta.Aux.emplace_back("authorization", GetAuthInfo());

        auto resultPromise = NThreading::NewPromise<TGetDataResult>();

        NYdbGrpc::TGRpcClientConfig grpcConf;
        grpcConf.Locator = GetGrpcSolomonEndpoint();
        grpcConf.EnableSsl = Settings.GetUseSsl();
        const auto connection = GrpcClient->CreateGRpcServiceConnection<DataService>(grpcConf);

        auto context = GrpcClient->CreateContext();
        if (!context) {
            throw yexception() << "Client is being shutted down";
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
    TString GetAuthInfo() const
    {
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
        return Settings.GetEndpoint() + ":443";
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
        *request.mutable_from_time() = NProtoInterop::CastToProto(TInstant::FromValue(Settings.GetFrom()));
        *request.mutable_to_time() = NProtoInterop::CastToProto(TInstant::FromValue(Settings.GetTo()));
        *request.mutable_force_replica() = DefaultReplica;

        if (Settings.GetDownsampling().GetDisabled()) {
            request.mutable_downsampling()->set_disabled(true);
        } else {
            const auto downsampling = Settings.GetDownsampling();
            request.mutable_downsampling()->set_grid_interval(downsampling.GetGridMs());
            request.mutable_downsampling()->set_grid_aggregation(ParseGridAggregation(downsampling.GetAggregation()));
            request.mutable_downsampling()->set_gap_filling(ParseGapFilling(downsampling.GetFill()));
        }

        for (const auto& metric : selectors) {
            auto query = request.mutable_queries()->Add();
            *query->mutable_value() = TStringBuilder{} << "{" << metric << "}";
            query->set_hidden(false);
        }

        return request;
    }

    TListMetricsResult ProcessHttpResponse(NYql::IHTTPGateway::TResult&& response) const
    {
        std::vector<TMetric> result;

        if (response.Content.HttpResponseCode < 200 || response.Content.HttpResponseCode >= 300) {
            return TListMetricsResult(TStringBuilder{} << "Error while sending request to monitoring api: " << response.Content.data());
        }

        NJson::TJsonValue json;
        try {
            NJson::ReadJsonTree(response.Content.data(), &json, /*throwOnError*/ true);
        } catch (const std::exception& e) {
            return TStringBuilder{} << "Failed to parse response from monitoring api: " << e.what();
        }

        if (!json.IsMap() || !json.Has("result")) {
            return TListMetricsResult{"Invalid result from monitoring api"};
        }

        for (const auto& metricObj : json["result"].GetArray()) {
            try {
                result.emplace_back(metricObj);
            } catch (const std::exception& e) {
                return TStringBuilder{} << "Failed to parse result response from monitoring: " << e.what();
            }
        }

        return std::move(result);
    }

    TGetDataResult ProcessGrpcResponse(NYdbGrpc::TGrpcStatus&& status, ReadResponse&& response) const
    {
        std::vector<TTimeseries> result;

        if (!status.Ok()) {
            return TStringBuilder{} << "Error while sending request to monitoring api: " << status.Msg;
        }

        for (const auto& responseValue : response.response_per_query()) {
            YQL_ENSURE(responseValue.has_timeseries_vector());
            YQL_ENSURE(responseValue.timeseries_vector().values_size() == 1); // one response per one set of selectors

            const auto& queryResponse = responseValue.timeseries_vector().values()[0];
            
            std::vector<int64_t> timestamps;
            std::vector<double> values;

            timestamps.reserve(queryResponse.timestamp_values().values_size());
            values.reserve(queryResponse.double_values().values_size());

            for (int64_t value : queryResponse.timestamp_values().values()) {
                timestamps.push_back(value);
            }
            for (double value : queryResponse.double_values().values()) {
                values.push_back(value);
            }

            result.emplace_back(queryResponse.name(), queryResponse.type(), std::move(timestamps), std::move(values));
        }

        return std::move(result);
    }

private:
    const TString DefaultReplica;
    const size_t ListSizeLimit = 1ull << 30;

    const NYql::NSo::NProto::TDqSolomonSource Settings;
    const std::shared_ptr<NYdb::ICredentialsProvider> CredentialsProvider;

    const std::shared_ptr<NYdbGrpc::TGRpcClientLow> GrpcClient;
    const std::shared_ptr<IHTTPGateway> HttpGateway;
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
        Type = ParseMetricType(value["type"].GetString());
    }

    if (value.Has("createdAt")) {
        YQL_ENSURE(value["createdAt"].IsString());
        CreatedAt = value["createdAt"].GetString();
    }
}

ISolomonAccessorClient::TListMetricsResult::TListMetricsResult(const TString& error)
    : Success(false)
    , ErrorMsg(error)
{}

ISolomonAccessorClient::TListMetricsResult::TListMetricsResult(std::vector<TMetric>&& result)
    : Success(true)
    , Result(std::move(result))
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
    NYql::NSo::NProto::TDqSolomonSource&& settings,
    std::shared_ptr<NYdb::ICredentialsProvider> credentialsProvider)
{
    return std::make_shared<TSolomonAccessorClient>(std::move(settings), credentialsProvider);
}

} // namespace NYql::NSo
