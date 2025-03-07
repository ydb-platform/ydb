#pragma once

#include <ydb-cpp-sdk/client/table/table.h>

namespace NYdb::inline Dev::NTable {

class TTransaction::TImpl {
public:
    TImpl(const TSession& session, const std::string& txId);

    const std::string& GetId() const {
        return TxId_;
    }

    bool IsActive() const {
        return !TxId_.empty();
    }

    TAsyncStatus Precommit() const;
    TAsyncCommitTransactionResult Commit(const TCommitTxSettings& settings = TCommitTxSettings());
    TAsyncStatus Rollback(const TRollbackTxSettings& settings = TRollbackTxSettings());

    TSession GetSession() const {
        return Session_;
    }

    void AddPrecommitCallback(TPrecommitTransactionCallback cb);

private:
    TSession Session_;
    std::string TxId_;

    bool ChangesAreAccepted = true; // haven't called Commit or Rollback yet
    mutable std::vector<TPrecommitTransactionCallback> PrecommitCallbacks;
};

}
