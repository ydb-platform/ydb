#include "transaction.h"
#include "table_client.h"

namespace NYdb::NTable {

TTransaction::TImpl::TImpl(const TSession& session, const std::string& txId)
    : Session_(session)
    , TxId_(txId)
{
}

TAsyncStatus TTransaction::TImpl::Precommit() const
{
    auto result = NThreading::MakeFuture(TStatus(EStatus::SUCCESS, {}));

    for (auto& callback : PrecommitCallbacks) {
        auto action = [curr = callback()](const TAsyncStatus& prev) {
            if (const TStatus& status = prev.GetValue(); !status.IsSuccess()) {
                return prev;
            }

            return curr;
        };

        result = result.Apply(action);
    }

    return result;
}

TAsyncCommitTransactionResult TTransaction::TImpl::Commit(const TCommitTxSettings& settings)
{
    ChangesAreAccepted = false;

    auto result = Precommit();

    auto precommitsCompleted = [this, settings](const TAsyncStatus& result) mutable {
        if (const TStatus& status = result.GetValue(); !status.IsSuccess()) {
            return NThreading::MakeFuture(TCommitTransactionResult(TStatus(status), std::nullopt));
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
