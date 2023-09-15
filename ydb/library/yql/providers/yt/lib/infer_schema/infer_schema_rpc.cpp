#include "infer_schema_rpc.h"

#include "yt/cpp/mapreduce/common/helpers.h"

#include <yt/yt/library/auth/auth.h>

#include <yt/yt/client/api/client.h>

#include <yt/yt/client/api/rpc_proxy/client_impl.h>
#include <yt/yt/client/api/rpc_proxy/config.h>
#include <yt/yt/client/api/rpc_proxy/connection.h>
#include <yt/yt/client/api/rpc_proxy/row_stream.h>

#include <ydb/library/yql/utils/yql_panic.h>

#include <ydb/library/yql/providers/yt/lib/yt_rpc_helpers/yt_convert_helpers.h>

namespace NYql {
TVector<TMaybe<NYT::TNode>> InferSchemaFromTablesContents(const TString& cluster, const TString& token, const NYT::TTransactionId& tx, const std::vector<TTableInferSchemaRequest>& requests) {
    const ui32 Timeout = 300'000; // in ms
    auto connectionConfig = NYT::New<NYT::NApi::NRpcProxy::TConnectionConfig>();
    connectionConfig->ClusterUrl = cluster;
    connectionConfig->DefaultTotalStreamingTimeout = TDuration::MilliSeconds(Timeout);
    auto connection = CreateConnection(connectionConfig);
    auto clientOptions = NYT::NApi::TClientOptions();

    if (!token.empty()) {
        clientOptions.Token = token;
    }

    auto client = DynamicPointerCast<NYT::NApi::NRpcProxy::TClient>(connection->CreateClient(clientOptions));
    Y_VERIFY(client);
    auto apiServiceProxy = client->CreateApiServiceProxy();

    TVector<NYT::NConcurrency::IAsyncZeroCopyInputStreamPtr> inputs(requests.size());
    size_t i = 0;
    std::vector<NYT::TFuture<void>> futures;
    std::vector<NYT::TPromise<void>> promises;
    std::vector<TStreamSchemaInferer> inferers;
    inferers.reserve(requests.size());

    std::function<void(size_t)> runRead = [&](size_t i) {
        inputs[i]->Read().ApplyUnique(BIND([&inferers, &promises, &runRead, i = i](NYT::TErrorOr<NYT::TSharedRef>&& res){
            if (res.IsOK() && !res.Value()) {
                // EOS
                promises[i].Set();
                return;
            }
            if (!res.IsOK()) {
                promises[i].Set(res);
                return;
            }
            auto block = std::move(res.Value());
            NYT::NApi::NRpcProxy::NProto::TRowsetDescriptor descriptor;
            NYT::NApi::NRpcProxy::NProto::TRowsetStatistics statistics;
            auto currentPayload = std::move(NYT::NApi::NRpcProxy::DeserializeRowStreamBlockEnvelope(block, &descriptor, &statistics));
            if (descriptor.rowset_format() != NYT::NApi::NRpcProxy::NProto::RF_FORMAT) {
                runRead(i);
                return;
            }
            try {
                size_t prevIdx = 0;
                for (size_t j = 0; j < currentPayload.size(); ++j) {
                    if (currentPayload[j] == '\n') {
                        if (j - prevIdx > 1) {
                            NYT::TNode res;
                            TMemoryInput inp(currentPayload.begin() + prevIdx, j - prevIdx - 1); // without ; at the end
                            res.Load(&inp);
                            inferers[i].AddRow(res);
                        }
                        prevIdx = j + 1;
                    }
                }
            } catch (std::exception& e) {
                promises[i].Set(e);
            }
            runRead(i);
        }));
    };
    
    futures.reserve(requests.size());
    promises.reserve(requests.size());

    for (auto& req: requests) {
        inferers.emplace_back(req.TableName);

        auto request = apiServiceProxy.ReadTable();
        client->InitStreamingRequest(*request);
        request->ClientAttachmentsStreamingParameters().ReadTimeout = TDuration::MilliSeconds(Timeout);

        TString ppath;
        NYT::NYPath::TRichYPath tableYPath(req.TableId);
        NYT::NChunkClient::TReadRange range;

        range.LowerLimit().SetRowIndex(0);
        range.UpperLimit().SetRowIndex(req.Rows);
        tableYPath.SetRanges({range});

        NYT::NYPath::ToProto(&ppath, tableYPath);
        request->set_path(ppath);
        request->set_desired_rowset_format(NYT::NApi::NRpcProxy::NProto::ERowsetFormat::RF_FORMAT);

        request->set_unordered(true);
        NDqs::ConfigureTransaction(request, tx.dw);

        // Get skiff format yson string
        request->set_format("<format=text>yson");
        promises.push_back(NYT::NewPromise<void>());
        futures.push_back(promises.back().ToFuture());
        CreateRpcClientInputStream(std::move(request)).ApplyUnique(BIND([&runRead, &inputs, i](NYT::NConcurrency::IAsyncZeroCopyInputStreamPtr&& stream) {
            // first packet contains meta, skip it
            return stream->Read().ApplyUnique(BIND([&runRead, stream = std::move(stream), i, &inputs](NYT::TSharedRef&&) {
                inputs[i] = std::move(stream);
                runRead(i);
            }));
        }));
        ++i;
    }
    YQL_ENSURE(NYT::NConcurrency::WaitFor(AllSucceeded(futures)).IsOK(), "Excepted all promises to be resolved in InferSchemaFromTablesContents");
    TVector<TMaybe<NYT::TNode>> result;
    result.reserve(requests.size());
    std::transform(inferers.begin(), inferers.end(), std::back_inserter(result), [](auto& x) { return x.GetSchema();});
    return result;
}
}
