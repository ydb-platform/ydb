#pragma once

#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>
#include <ydb/public/sdk/cpp/client/ydb_types/operation/operation.h>

#include <util/string/builder.h>

namespace NYdb {
namespace NOperation {

template <typename TOp>
class TOperationsList : public TStatus {
public:
    TOperationsList(TStatus&& status)
        : TStatus(std::move(status))
    {
    }

    TOperationsList(TStatus&& status, TVector<TOp>&& operations, const TString& nextPageToken)
        : TStatus(std::move(status))
        , Operations_(std::move(operations))
        , NextPageToken_(nextPageToken)
    {
    }

    const TVector<TOp>& GetList() const { return Operations_; }
    const TString& NextPageToken() const { return NextPageToken_; }

    TString ToJsonString() const {
        TStringBuilder json;
        json << "{\"operations\":[";

        bool first = true;
        for (const auto& operation : GetList()) {
            json << (first ? "" : ",") << operation.ToJsonString();
            first = false;
        }

        json << "],\"nextPageToken\":\"" << NextPageToken() << "\"}";
        return json;
    }

private:
    TVector<TOp> Operations_;
    TString NextPageToken_;
};

class TOperationClient {
    class TImpl;

public:
    TOperationClient(const TDriver& driver, const TCommonClientSettings& settings = TCommonClientSettings());

    template <typename TOp>
    NThreading::TFuture<TOp> Get(const TOperation::TOperationId& id);

    TAsyncStatus Cancel(const TOperation::TOperationId& id);
    TAsyncStatus Forget(const TOperation::TOperationId& id);

    template <typename TOp>
    NThreading::TFuture<TOperationsList<TOp>> List(ui64 pageSize = 0, const TString& pageToken = TString());

private:
    template <typename TOp>
    NThreading::TFuture<TOperationsList<TOp>> List(const TString& kind, ui64 pageSize, const TString& pageToken);

private:
    std::shared_ptr<TImpl> Impl_;
};

} // namespace NOperation
} // namespace NYdb
