#pragma once

#include <ydb/library/yql/providers/yt/codec/yt_codec.h>
#include <ydb/library/yql/public/udf/udf_value_builder.h>

#include <yt/cpp/mapreduce/interface/io.h>
#include <yt/cpp/mapreduce/common/helpers.h>
#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/cpp/mapreduce/interface/serialize.h>
#include <yt/cpp/mapreduce/interface/config.h>
#include <yt/cpp/mapreduce/interface/common.h>

#include <yt/yt/library/auth/auth.h>
#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/rpc_proxy/client_impl.h>
#include <yt/yt/client/api/rpc_proxy/config.h>
#include <yt/yt/client/api/rpc_proxy/connection.h>
#include <yt/yt/client/api/rpc_proxy/row_stream.h>

#include <arrow/memory_pool.h>

namespace NYql::NDqs {
NYT::NYPath::TRichYPath ConvertYPathFromOld(const NYT::TRichYPath& richYPath);
class TPayloadRPCReader : public NYT::TRawTableReader {
public:
    TPayloadRPCReader(NYT::TSharedRef&& payload) : Payload_(std::move(payload)), PayloadStream_(Payload_.Begin(), Payload_.Size()) {}

    bool Retry(const TMaybe<ui32>&, const TMaybe<ui64>&, const std::exception_ptr&) override {
        return false;
    }

    void ResetRetries() override {

    }

    bool HasRangeIndices() const override {
        return true;
    };

    size_t DoRead(void* buf, size_t len) override {
        if (!PayloadStream_.Exhausted()) {
            return PayloadStream_.Read(buf, len);
        }
        return 0;
    };

    virtual ~TPayloadRPCReader() override {
    }
private:
    NYT::TSharedRef Payload_;
    TMemoryInput PayloadStream_;
};

struct TSettingsHolder : public TNonCopyable {
    TSettingsHolder(NYT::NApi::IConnectionPtr&& connection, NYT::TIntrusivePtr<NYT::NApi::NRpcProxy::TClient>&& client,
        TVector<NYT::NApi::NRpcProxy::TApiServiceProxy::TReqReadTablePtr>&& requests, TVector<size_t>&& originalIndexes)
        : Connection(std::move(connection))
        , Client(std::move(client))
        , Requests(std::move(requests))
        , OriginalIndexes(std::move(originalIndexes)) {};
    void SetColumns(const TVector<TString>& columnNames) {
        for (ui32 i = 0; i < columnNames.size(); ++i) {
            ColumnNameMapping[columnNames[i]] = i;
        }
    }
    NYT::NApi::IConnectionPtr Connection;
    NYT::TIntrusivePtr<NYT::NApi::NRpcProxy::TClient> Client;
    const TMkqlIOSpecs* Specs = nullptr;
    arrow::MemoryPool* Pool = nullptr;
    const NUdf::IPgBuilder* PgBuilder = nullptr;
    TVector<NYT::NApi::NRpcProxy::TApiServiceProxy::TReqReadTablePtr> Requests;

    TVector<size_t> OriginalIndexes;
    std::unordered_map<std::string, ui32> ColumnNameMapping;
};

std::unique_ptr<TSettingsHolder> CreateInputStreams(bool isArrow, const TString& token, const TString& clusterName, const ui64 timeout, bool unordered, const TVector<std::pair<NYT::TRichYPath, NYT::TFormat>>& tables, NYT::TNode samplingSpec);
NYT::TFuture<NYT::NConcurrency::IAsyncZeroCopyInputStreamPtr> CreateInputStream(NYT::NApi::NRpcProxy::TApiServiceProxy::TReqReadTablePtr requestPtr);

};
