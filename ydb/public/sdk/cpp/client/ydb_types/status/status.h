#pragma once

#include <ydb/public/sdk/cpp/client/ydb_types/fwd.h>

#include <ydb/public/sdk/cpp/client/ydb_types/exceptions/exceptions.h>
#include <ydb/public/sdk/cpp/client/ydb_types/fatal_error_handlers/handlers.h>
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/common/type_switcher.h>
#include <ydb/public/sdk/cpp/client/ydb_types/ydb.h>

#include <yql/essentials/public/issue/yql_issue.h>

#include <library/cpp/threading/future/future.h>

namespace NYdb::inline V2 {

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
    bool IsUnimplementedError() const;
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

namespace NStatusHelpers {

class TYdbErrorException : public TYdbException {
public:
    TYdbErrorException(TStatus status)
        : Status_(std::move(status))
    {
        *this << status;
    }

    friend IOutputStream& operator<<(IOutputStream& out, const TYdbErrorException& e) {
        return out << e.Status_;
    }

private:
    TStatus Status_;
};

void ThrowOnError(TStatus status, std::function<void(TStatus)> onSuccess = [](TStatus) {});
void ThrowOnErrorOrPrintIssues(TStatus status);

}

} // namespace NYdb
