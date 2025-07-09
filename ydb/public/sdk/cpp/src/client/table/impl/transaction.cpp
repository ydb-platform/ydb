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
    auto self = shared_from_this();

    TStatus status(EStatus::SUCCESS, {});

    for (auto& callback : self->PrecommitCallbacks) {
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
    auto self = shared_from_this();

    for (auto& callback : self->OnFailureCallbacks) {
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
    auto self = shared_from_this();

    self->ChangesAreAccepted = false;
    auto settingsCopy = settings;

    auto precommitResult = co_await self->Precommit();

    if (!precommitResult.IsSuccess()) {
        co_return TCommitTransactionResult(TStatus(precommitResult), std::nullopt);
    }

    self->PrecommitCallbacks.clear();

    auto commitResult = co_await self->Session_.Client_->CommitTransaction(self->Session_, self->TxId_, settingsCopy);

    if (!commitResult.IsSuccess()) {
        co_await self->ProcessFailure();
    }

    co_return commitResult;
}

TAsyncStatus TTransaction::TImpl::Rollback(const TRollbackTxSettings& settings)
{
    auto self = shared_from_this();
    self->ChangesAreAccepted = false;

    auto rollbackResult = co_await self->Session_.Client_->RollbackTransaction(self->Session_, self->TxId_, settings);

    co_await self->ProcessFailure();
    co_return rollbackResult;
}

void TTransaction::TImpl::AddPrecommitCallback(TPrecommitTransactionCallback cb)
{
    std::lock_guard lock(PrecommitCallbacksMutex);

    if (!ChangesAreAccepted) {
        ythrow TContractViolation("Changes are no longer accepted");
    }

    PrecommitCallbacks.push_back(std::move(cb));
}

void TTransaction::TImpl::AddOnFailureCallback(TOnFailureTransactionCallback cb)
{
    std::lock_guard lock(OnFailureCallbacksMutex);

    if (!ChangesAreAccepted) {
        ythrow TContractViolation("Changes are no longer accepted");
    }

    OnFailureCallbacks.push_back(std::move(cb));
}

}
