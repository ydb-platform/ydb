#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

#include <mutex>

namespace NYdb::inline Dev::NTable {

class TTransaction::TImpl : public std::enable_shared_from_this<TImpl> {
public:
    TImpl(const TSession& session, const std::string& txId);

    const std::string& GetId() const {
        return TxId_;
    }

    const std::string& GetSessionId() const {
        return Session_.GetId();
    }

    bool IsActive() const {
        return !TxId_.empty();
    }

    TAsyncStatus Precommit() const;
    NThreading::TFuture<void> ProcessFailure() const;

    TAsyncCommitTransactionResult Commit(const TCommitTxSettings& settings = TCommitTxSettings());
    TAsyncStatus Rollback(const TRollbackTxSettings& settings = TRollbackTxSettings());

    TSession GetSession() const {
        return Session_;
    }

    void AddPrecommitCallback(TPrecommitTransactionCallback cb);
    void AddOnFailureCallback(TOnFailureTransactionCallback cb);

    TSession Session_;
    std::string TxId_;

private:
    bool ChangesAreAccepted = true; // haven't called Commit or Rollback yet
    mutable std::vector<TPrecommitTransactionCallback> PrecommitCallbacks;
    mutable std::vector<TOnFailureTransactionCallback> OnFailureCallbacks;

    std::mutex PrecommitCallbacksMutex;
    std::mutex OnFailureCallbacksMutex;
};

}
