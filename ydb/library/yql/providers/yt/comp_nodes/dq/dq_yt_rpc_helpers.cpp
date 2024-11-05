#include "dq_yt_rpc_helpers.h"

#include <ydb/library/yql/providers/yt/lib/yt_rpc_helpers/yt_convert_helpers.h>

namespace NYql::NDqs {

NYT::NYPath::TRichYPath ConvertYPathFromOld(const NYT::TRichYPath& richYPath) {
    TStringStream ss;
    NYT::PathToNode(richYPath).Save(&ss);
    NYT::NYPath::TRichYPath path;
    NYT::NYPath::Deserialize(path, NYT::NYTree::ConvertToNode(NYT::NYson::TYsonString(ss.Str())));
    return path;
}

std::unique_ptr<TSettingsHolder> CreateInputStreams(bool isArrow, const TString& token, const TString& clusterName, const ui64 timeout, bool unordered, const TVector<std::pair<NYT::TRichYPath, NYT::TFormat>>& tables, NYT::TNode samplingSpec) {
    auto connectionConfig = NYT::New<NYT::NApi::NRpcProxy::TConnectionConfig>();
    connectionConfig->ClusterUrl = clusterName;
    connectionConfig->EnableRetries = true;
    connectionConfig->DefaultPingPeriod = TDuration::MilliSeconds(5000);

    auto connection = CreateConnection(connectionConfig);
    auto clientOptions = NYT::NApi::TClientOptions();

    if (token) {
        clientOptions.Token = token;
    }

    auto client = DynamicPointerCast<NYT::NApi::NRpcProxy::TClient>(connection->CreateClient(clientOptions));
    Y_ABORT_UNLESS(client);
    auto apiServiceProxy = client->CreateApiServiceProxy();

    TVector<NYT::NApi::NRpcProxy::TApiServiceProxy::TReqReadTablePtr> requests;

    size_t inputIdx = 0;
    TVector<size_t> originalIndexes;

    for (auto [richYPath, format]: tables) {
        if (richYPath.GetRanges() && richYPath.GetRanges()->empty()) {
            ++inputIdx;
            continue;
        }
        originalIndexes.emplace_back(inputIdx++);

        auto request = apiServiceProxy.ReadTable();
        client->InitStreamingRequest(*request);
        request->ServerAttachmentsStreamingParameters().WriteTimeout = TDuration::MilliSeconds(timeout);
        request->ClientAttachmentsStreamingParameters().ReadTimeout = TDuration::MilliSeconds(timeout);

        TString ppath;
        auto tableYPath = ConvertYPathFromOld(richYPath);

        NYT::NYPath::ToProto(&ppath, tableYPath);
        request->set_path(ppath);
        request->set_desired_rowset_format(isArrow ? NYT::NApi::NRpcProxy::NProto::ERowsetFormat::RF_ARROW : NYT::NApi::NRpcProxy::NProto::ERowsetFormat::RF_FORMAT);
        if (isArrow) {
            request->set_arrow_fallback_rowset_format(NYT::NApi::NRpcProxy::NProto::ERowsetFormat::RF_FORMAT);
        }

        // TODO() Enable row indexes
        request->set_enable_row_index(!isArrow);
        request->set_enable_table_index(true);
        // TODO() Enable range indexes
        request->set_enable_range_index(!isArrow);

        request->set_unordered(unordered);

        // https://a.yandex-team.ru/arcadia/yt/yt_proto/yt/client/api/rpc_proxy/proto/api_service.proto?rev=r11519304#L2338
        if (!samplingSpec.IsUndefined()) {
            TStringStream ss;
            samplingSpec.Save(&ss);
            request->set_config(ss.Str());
        }

        ConfigureTransaction(request, richYPath);
        // Get skiff format yson string
        TStringStream fmt;
        format.Config.Save(&fmt);
        request->set_format(fmt.Str());

        requests.emplace_back(std::move(request));
    }
    return std::make_unique<TSettingsHolder>(std::move(connection), std::move(client), std::move(requests), std::move(originalIndexes));
}

NYT::TFuture<NYT::NConcurrency::IAsyncZeroCopyInputStreamPtr> CreateInputStream(NYT::NApi::NRpcProxy::TApiServiceProxy::TReqReadTablePtr request) {
    return CreateRpcClientInputStream(std::move(request)).ApplyUnique(BIND([](NYT::NConcurrency::IAsyncZeroCopyInputStreamPtr&& stream) {
            // first packet contains meta, skip it
            return stream->Read().ApplyUnique(BIND([stream = std::move(stream)](NYT::TSharedRef&&) {
                return std::move(stream);
            }));
        }));
}


}
