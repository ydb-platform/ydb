#include "transaction.h"
#include "table_client.h"

namespace NYdb::NTable {

TTransaction::TImpl::TImpl(const TSession& session, const TString& txId)
    : Session_(session)
    , TxId_(txId)
{
}

TAsyncCommitTransactionResult TTransaction::TImpl::Commit(const TCommitTxSettings& settings)
{
    ChangesAreAccepted = false;

    auto result = NThreading::MakeFuture(TCommitTransactionResult(TStatus(EStatus::SUCCESS, {}), Nothing()));

    for (auto& callback : PrecommitCallbacks) {
        auto action = [curr = callback()](const TAsyncCommitTransactionResult& prev) {
            if (const TStatus& status = prev.GetValue(); !status.IsSuccess()) {
                return prev;
            }

            return curr;
        };

        result = result.Apply(action);
    }

    auto precommitsCompleted = [this, settings](const TAsyncCommitTransactionResult& result) mutable {
        if (const TStatus& status = result.GetValue(); !status.IsSuccess()) {
            return result;
        }

        return Session_.Client_->CommitTransaction(Session_,
                                                   TxId_,
                                                   settings);
    };

    return result.Apply(precommitsCompleted);
}

TAsyncStatus TTransaction::TImpl::Rollback(const TRollbackTxSettings& settings)
{
    ChangesAreAccepted = false;
    return Session_.Client_->RollbackTransaction(Session_, TxId_, settings);
}

void TTransaction::TImpl::AddPrecommitCallback(TPrecommitTransactionCallback cb)
{
    if (!ChangesAreAccepted) {
        ythrow TContractViolation("Changes are no longer accepted");
    }

    PrecommitCallbacks.push_back(std::move(cb));
}

}
