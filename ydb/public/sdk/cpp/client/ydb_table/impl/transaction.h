#pragma once

#include <ydb/public/sdk/cpp/client/ydb_table/table.h>

namespace NYdb::NTable {

class TTransaction::TImpl {
public:
    TImpl(const TSession& session, const TString& txId);

    const TString& GetId() const {
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
    TString TxId_;

    bool ChangesAreAccepted = true; // haven't called Commit or Rollback yet
    TVector<TPrecommitTransactionCallback> PrecommitCallbacks;
};

}
