#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/fwd.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status/status.h>

namespace NYdb::inline Dev {

using TPrecommitTransactionCallback = std::function<TAsyncStatus()>;
using TOnFailureTransactionCallback = std::function<NThreading::TFuture<void>()>;

class TTransactionBase {
public:
    const std::string& GetId() const {
        return *TxId_;
    }

    const std::string& GetSessionId() const {
        return *SessionId_;
    }

    virtual void AddPrecommitCallback(TPrecommitTransactionCallback cb) = 0;
    virtual void AddOnFailureCallback(TOnFailureTransactionCallback cb) = 0;

    virtual ~TTransactionBase() = default;

protected:
    TTransactionBase() = default;

    const std::string* SessionId_ = nullptr;
    const std::string* TxId_ = nullptr;
};

} // namespace NYdb
