#include "transaction.h"
#include "table_client.h"

namespace NYdb::inline Dev::NTable {

TTransaction::TImpl::TImpl(const TSession& session, const std::string& txId)
    : Session_(session)
    , TxId_(txId)
{
}

TAsyncStatus TTransaction::TImpl::Precommit() const
{
    TStatus status(EStatus::SUCCESS, {});

    for (auto& callback : PrecommitCallbacks) {
        if (!callback) {
            continue;
        }

        // If you send multiple requests in parallel, the `KQP` service can respond with `SESSION_BUSY`.
        // Therefore, precommit operations are performed sequentially. Here we move the callback to a local variable,
        // because otherwise it may be called twice.
        auto localCallback = std::move(callback);

        if (!status.IsSuccess()) {
            co_return status;
        }

        status = co_await localCallback();
    }

    co_return status;
}

NThreading::TFuture<void> TTransaction::TImpl::ProcessFailure() const
{
    for (auto& callback : OnFailureCallbacks) {
        if (!callback) {
            continue;
        }

        // If you send multiple requests in parallel, the `KQP` service can respond with `SESSION_BUSY`.
        // Therefore, precommit operations are performed sequentially. Here we move the callback to a local variable,
        // because otherwise it may be called twice.
        auto localCallback = std::move(callback);

        co_await localCallback();
    }

    co_return;
}

TAsyncCommitTransactionResult TTransaction::TImpl::Commit(const TCommitTxSettings& settings)
{
    ChangesAreAccepted = false;
    auto settingsCopy = settings;

    auto precommitResult = co_await Precommit();

    if (!precommitResult.IsSuccess()) {
        co_return TCommitTransactionResult(TStatus(precommitResult), std::nullopt);
    }

    PrecommitCallbacks.clear();

    auto commitResult = co_await Session_.Client_->CommitTransaction(Session_, TxId_, settingsCopy);

    if (!commitResult.IsSuccess()) {
        co_await ProcessFailure();
    }

    co_return commitResult;
}

TAsyncStatus TTransaction::TImpl::Rollback(const TRollbackTxSettings& settings)
{
    ChangesAreAccepted = false;

    auto rollbackResult = co_await Session_.Client_->RollbackTransaction(Session_, TxId_, settings);

    co_await ProcessFailure();
    co_return rollbackResult;
}

void TTransaction::TImpl::AddPrecommitCallback(TPrecommitTransactionCallback cb)
{
    if (!ChangesAreAccepted) {
        ythrow TContractViolation("Changes are no longer accepted");
    }

    PrecommitCallbacks.push_back(std::move(cb));
}

void TTransaction::TImpl::AddOnFailureCallback(TOnFailureTransactionCallback cb)
{
    if (!ChangesAreAccepted) {
        ythrow TContractViolation("Changes are no longer accepted");
    }

    OnFailureCallbacks.push_back(std::move(cb));
}

}
