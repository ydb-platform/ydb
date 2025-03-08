#pragma once

#include <ydb-cpp-sdk/client/driver/driver.h>
#include <ydb-cpp-sdk/client/types/operation/operation.h>

namespace NYdb::inline Dev {
namespace NOperation {

template <typename TOp>
class TOperationsList : public TStatus {
public:
    TOperationsList(TStatus&& status)
        : TStatus(std::move(status))
    {
    }

    TOperationsList(TStatus&& status, std::vector<TOp>&& operations, const std::string& nextPageToken)
        : TStatus(std::move(status))
        , Operations_(std::move(operations))
        , NextPageToken_(nextPageToken)
    {
    }

    const std::vector<TOp>& GetList() const { return Operations_; }
    const std::string& NextPageToken() const { return NextPageToken_; }

    std::string ToJsonString() const {
        std::stringstream json;
        json << "{\"operations\":[";

        bool first = true;
        for (const auto& operation : GetList()) {
            json << (first ? "" : ",") << operation.ToJsonString();
            first = false;
        }

        json << "],\"nextPageToken\":\"" << NextPageToken() << "\"}";
        return json.str();
    }

private:
    std::vector<TOp> Operations_;
    std::string NextPageToken_;
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
    NThreading::TFuture<TOperationsList<TOp>> List(ui64 pageSize = 0, const std::string& pageToken = std::string());

private:
    template <typename TOp>
    NThreading::TFuture<TOperationsList<TOp>> List(const std::string& kind, ui64 pageSize, const std::string& pageToken);

private:
    std::shared_ptr<TImpl> Impl_;
};

} // namespace NOperation
} // namespace NYdb
