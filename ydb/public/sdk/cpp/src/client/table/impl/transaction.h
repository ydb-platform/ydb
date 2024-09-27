#pragma once

#include <ydb-cpp-sdk/client/table/table.h>

namespace NYdb::NTable {

class TTransaction::TImpl {
public:
    TImpl(const TSession& session, const std::string& txId);

    const std::string& GetId() const {
        return TxId_;
    }

    bool IsActive() const {
        return !TxId_.empty();
    }

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
    std::vector<TPrecommitTransactionCallback> PrecommitCallbacks;
};

}
