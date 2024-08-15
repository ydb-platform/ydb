#pragma once

#include <ydb-cpp-sdk/client/types/fatal_error_handlers/handlers.h>
#include <ydb-cpp-sdk/client/types/ydb.h>

#include <ydb-cpp-sdk/library/yql_common/issue/yql_issue.h>

#include <library/cpp/threading/future/future.h>

namespace NYdb {

//! Internal status representation
struct TPlainStatus;

//! Represents status of call
class TStatus {
public:
    TStatus(EStatus statusCode, NYql::TIssues&& issues);
    TStatus(TPlainStatus&& plain);

    EStatus GetStatus() const;
    const NYql::TIssues& GetIssues() const;
    bool IsSuccess() const;
    bool IsTransportError() const;
    const std::string& GetEndpoint() const;
    const std::multimap<std::string, std::string>& GetResponseMetadata() const;
    float GetConsumedRu() const;

    void Out(IOutputStream& out) const;
    friend IOutputStream& operator<<(IOutputStream& out, const TStatus& st);
    friend std::ostream& operator<<(std::ostream& out, const TStatus& st);

protected:
    void CheckStatusOk(const std::string& str) const;
    void RaiseError(const std::string& str) const;
private:
    class TImpl;
    std::shared_ptr<TImpl> Impl_;
};

using TAsyncStatus = NThreading::TFuture<TStatus>;

class TStreamPartStatus : public TStatus {
public:
    TStreamPartStatus(TStatus&& status);
    bool EOS() const;
};

} // namespace NYdb
