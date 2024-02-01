#include "dq_yt_rpc_helpers.h"

#include <ydb/library/yql/providers/yt/lib/yt_rpc_helpers/yt_convert_helpers.h>

namespace NYql::NDqs {

NYT::NTableClient::TUnversionedOwningRow ConvertRow(const TVector<NYT::TNode>& parts) {
    NYT::NTableClient::TUnversionedOwningRowBuilder builder;
    int id = 0;
    for (const auto& e: parts) {
        try {
            switch (e.GetType()) {
                #define XX(type,  otherType) \
                case NYT::TNode::EType::type: \
                    builder.AddValue(NYT::NTableClient::MakeUnversioned ## otherType ## Value(e.As ## type(), id)); \
                    break;
                XX(Int64, Int64)
                XX(Uint64, Uint64)
                XX(Double, Double)
                XX(Bool, Boolean)
                XX(String, String)
                #undef XX
                default:
                    THROW_ERROR_EXCEPTION("Key cannot contain %Qlv values",
                        e.GetType());
            }
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error deserializing key component #%v", id)
                << ex;
        }
        ++id;
    }
    return builder.FinishRow();
}

template<typename TOld, typename TNew>
void Convert(const TOld& old, TNew& newVal) {
    if (old.Offset_) {
        newVal.SetOffset(*old.Offset_);
    }

    if (old.TabletIndex_) {
        newVal.SetTabletIndex(*old.TabletIndex_);
    }

    if (old.RowIndex_) {
        newVal.SetRowIndex(*old.RowIndex_);
    }

    if (old.KeyBound_) {
        auto rel = old.KeyBound_->Relation();
        bool inclusive = NYT::ERelation::GreaterOrEqual == rel || NYT::ERelation::LessOrEqual == rel;
        bool isUpper = NYT::ERelation::Less == rel || NYT::ERelation::LessOrEqual == rel;
        newVal.KeyBound() = NYT::NTableClient::TOwningKeyBound::FromRow(ConvertRow(old.KeyBound_->Key().Parts_), inclusive, isUpper);
    }
}

NYT::NYPath::TRichYPath ConvertYPathFromOld(const NYT::TRichYPath& richYPath) {
    NYT::NYPath::TRichYPath tableYPath(richYPath.Path_);
    const auto& rngs = richYPath.GetRanges();
    if (rngs) {
        TVector<NYT::NChunkClient::TReadRange> ranges;
        for (const auto& rng: *rngs) {
            auto& range = ranges.emplace_back();
            Convert(rng.LowerLimit_, range.LowerLimit());
            Convert(rng.UpperLimit_, range.UpperLimit());
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

        waitFor.emplace_back(std::move(CreateRpcClientInputStream(std::move(request)).ApplyUnique(BIND([](NYT::NConcurrency::IAsyncZeroCopyInputStreamPtr&& stream) {
            // first packet contains meta, skip it
            return stream->Read().ApplyUnique(BIND([stream = std::move(stream)](NYT::TSharedRef&&) {
                return std::move(stream);
            }));
        }))));
    }
    TVector<NYT::NConcurrency::IAsyncZeroCopyInputStreamPtr> rawInputs;
    auto result = NYT::NConcurrency::WaitFor(NYT::AllSucceeded(waitFor));
    if (!result.IsOK()) {
        Cerr << "YT RPC Reader exception:\n";
    }
    result.ValueOrThrow().swap(rawInputs);
    return std::make_unique<TSettingsHolder>(std::move(connection), std::move(client), std::move(rawInputs), std::move(originalIndexes));
}

}
