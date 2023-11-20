#include "dq_yt_rpc_helpers.h"

#include <ydb/library/yql/providers/yt/lib/yt_rpc_helpers/yt_convert_helpers.h>

namespace NYql::NDqs {

NYT::NYPath::TRichYPath ConvertYPathFromOld(const NYT::TRichYPath& richYPath) {
    NYT::NYPath::TRichYPath tableYPath(richYPath.Path_);
    const auto& rngs = richYPath.GetRanges();
    if (rngs) {
        TVector<NYT::NChunkClient::TReadRange> ranges;
        for (const auto& rng: *rngs) {
            auto& range = ranges.emplace_back();
            if (rng.LowerLimit_.Offset_) {
                range.LowerLimit().SetOffset(*rng.LowerLimit_.Offset_);
            }

            if (rng.LowerLimit_.TabletIndex_) {
                range.LowerLimit().SetTabletIndex(*rng.LowerLimit_.TabletIndex_);
            }

            if (rng.LowerLimit_.RowIndex_) {
                range.LowerLimit().SetRowIndex(*rng.LowerLimit_.RowIndex_);
            }

            if (rng.UpperLimit_.Offset_) {
                range.UpperLimit().SetOffset(*rng.UpperLimit_.Offset_);
            }

            if (rng.UpperLimit_.TabletIndex_) {
                range.UpperLimit().SetTabletIndex(*rng.UpperLimit_.TabletIndex_);
            }

            if (rng.UpperLimit_.RowIndex_) {
                range.UpperLimit().SetRowIndex(*rng.UpperLimit_.RowIndex_);
            }
        }
        tableYPath.SetRanges(std::move(ranges));
    }

    if (richYPath.Columns_) {
        tableYPath.SetColumns(richYPath.Columns_->Parts_);
    }

    return tableYPath;
}


std::unique_ptr<TSettingsHolder> CreateInputStreams(bool isArrow, const TString& token, const TString& clusterName, const ui64 timeout, bool unordered, const TVector<std::pair<NYT::TRichYPath, NYT::TFormat>>& tables, NYT::TNode samplingSpec) {
    auto connectionConfig = NYT::New<NYT::NApi::NRpcProxy::TConnectionConfig>();
    connectionConfig->ClusterUrl = clusterName;
    connectionConfig->DefaultTotalStreamingTimeout = TDuration::MilliSeconds(timeout);
    auto connection = CreateConnection(connectionConfig);
    auto clientOptions = NYT::NApi::TClientOptions();

    if (token) {
        clientOptions.Token = token;
    }

    auto client = DynamicPointerCast<NYT::NApi::NRpcProxy::TClient>(connection->CreateClient(clientOptions));
    Y_ABORT_UNLESS(client);
    auto apiServiceProxy = client->CreateApiServiceProxy();

    TVector<NYT::TFuture<NYT::NConcurrency::IAsyncZeroCopyInputStreamPtr>> waitFor;

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
        request->ClientAttachmentsStreamingParameters().ReadTimeout = TDuration::MilliSeconds(timeout);

        TString ppath;
        auto tableYPath = ConvertYPathFromOld(richYPath);

        NYT::NYPath::ToProto(&ppath, tableYPath);
        request->set_path(ppath);
        request->set_desired_rowset_format(isArrow ? NYT::NApi::NRpcProxy::NProto::ERowsetFormat::RF_ARROW : NYT::NApi::NRpcProxy::NProto::ERowsetFormat::RF_FORMAT);
        if (isArrow) {
            request->set_arrow_fallback_rowset_format(NYT::NApi::NRpcProxy::NProto::ERowsetFormat::RF_FORMAT);
        }

        request->set_enable_row_index(true);
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

        waitFor.emplace_back(std::move(CreateRpcClientInputStream(std::move(request)).ApplyUnique(BIND([](NYT::NConcurrency::IAsyncZeroCopyInputStreamPtr&& stream) {
            // first packet contains meta, skip it
            return stream->Read().ApplyUnique(BIND([stream = std::move(stream)](NYT::TSharedRef&&) {
                return std::move(stream);
            }));
        }))));
    }
    TVector<NYT::NConcurrency::IAsyncZeroCopyInputStreamPtr> rawInputs;
    NYT::NConcurrency::WaitFor(NYT::AllSucceeded(waitFor)).ValueOrThrow().swap(rawInputs);
    return std::make_unique<TSettingsHolder>(std::move(connection), std::move(client), std::move(rawInputs), std::move(originalIndexes));
}

}
