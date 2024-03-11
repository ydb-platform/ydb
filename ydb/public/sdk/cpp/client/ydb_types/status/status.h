#pragma once

#include <ydb/public/sdk/cpp/client/ydb_types/fatal_error_handlers/handlers.h>
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/common/type_switcher.h>
#include <ydb/public/sdk/cpp/client/ydb_types/ydb.h>

#include <ydb/library/yql/public/issue/yql_issue.h>

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
    const TStringType& GetEndpoint() const;
    const std::multimap<TStringType, TStringType>& GetResponseMetadata() const;
    float GetConsumedRu() const;

    void Out(IOutputStream& out) const;
    friend IOutputStream& operator<<(IOutputStream& out, const TStatus& st);

protected:
    void CheckStatusOk(const TStringType& str) const;
    void RaiseError(const TStringType& str) const;
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
