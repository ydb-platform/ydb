#include "sticky_transaction_pool.h"

#include "transaction.h"

#include <yt/yt/core/concurrency/lease_manager.h>

#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

ITransactionPtr IStickyTransactionPool::GetTransactionAndRenewLeaseOrThrow(
    TTransactionId transactionId)
{
    auto transaction = FindTransactionAndRenewLease(transactionId);
    if (!transaction) {
        THROW_ERROR_EXCEPTION(
            NTransactionClient::EErrorCode::NoSuchTransaction,
            "Sticky transaction %v is not found, "
            "this usually means that you use tablet transactions within HTTP API; "
            "consider using RPC API instead",
            transactionId);
    }

    return transaction;
}

////////////////////////////////////////////////////////////////////////////////

class TStickyTransactionPool
    : public IStickyTransactionPool
{
public:
    explicit TStickyTransactionPool(const NLogging::TLogger& logger)
        : Logger(logger)
    { }

    ITransactionPtr RegisterTransaction(ITransactionPtr transaction) override
    {
        auto transactionId = transaction->GetId();
        TStickyTransactionEntry entry{
            transaction,
            NConcurrency::TLeaseManager::CreateLease(
                transaction->GetTimeout(),
                BIND(&TStickyTransactionPool::OnStickyTransactionLeaseExpired, MakeWeak(this), transactionId, MakeWeak(transaction)))
        };

        bool inserted = false;
        {
            auto guard = WriterGuard(StickyTransactionLock_);
            inserted = IdToStickyTransactionEntry_.emplace(transactionId, entry).second;
        }

        if (!inserted) {
            NConcurrency::TLeaseManager::CloseLease(entry.Lease);
            THROW_ERROR_EXCEPTION(NTransactionClient::EErrorCode::InvalidTransactionState,
                "Failed to register duplicate sticky transaction %v",
                transactionId);
        }

        transaction->SubscribeCommitted(
            BIND(&TStickyTransactionPool::OnStickyTransactionCommitted, MakeWeak(this), transactionId));
        transaction->SubscribeAborted(
            BIND(&TStickyTransactionPool::OnStickyTransactionAborted, MakeWeak(this), transactionId));

        YT_LOG_DEBUG("Sticky transaction registered (TransactionId: %v)",
            transactionId);

        return transaction;
    }

    void UnregisterTransaction(TTransactionId transactionId) override
    {
        TStickyTransactionEntry entry;
        {
            auto guard = WriterGuard(StickyTransactionLock_);
            auto it = IdToStickyTransactionEntry_.find(transactionId);
            if (it == IdToStickyTransactionEntry_.end()) {
                return;
            }
            entry = std::move(it->second);
            IdToStickyTransactionEntry_.erase(it);
        }

        YT_LOG_DEBUG("Sticky transaction unregistered (TransactionId: %v)",
            transactionId);
    }

    ITransactionPtr FindTransactionAndRenewLease(TTransactionId transactionId) override
    {
        ITransactionPtr transaction;
        NConcurrency::TLease lease;
        {
            auto guard = ReaderGuard(StickyTransactionLock_);
            auto it = IdToStickyTransactionEntry_.find(transactionId);
            if (it == IdToStickyTransactionEntry_.end()) {
                return nullptr;
            }
            const auto& entry = it->second;
            transaction = entry.Transaction;
            lease = entry.Lease;
        }
        NConcurrency::TLeaseManager::RenewLease(std::move(lease));
        YT_LOG_DEBUG("Sticky transaction lease renewed (TransactionId: %v)",
            transactionId);
        return transaction;
    }

private:
    const NLogging::TLogger Logger;

    struct TStickyTransactionEntry
    {
        ITransactionPtr Transaction;
        NConcurrency::TLease Lease;
    };

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, StickyTransactionLock_);
    THashMap<TTransactionId, TStickyTransactionEntry> IdToStickyTransactionEntry_;

    void OnStickyTransactionLeaseExpired(TTransactionId transactionId, TWeakPtr<ITransaction> weakTransaction)
    {
        auto transaction = weakTransaction.Lock();
        if (!transaction) {
            return;
        }

        {
            auto guard = WriterGuard(StickyTransactionLock_);
            auto it = IdToStickyTransactionEntry_.find(transactionId);
            if (it == IdToStickyTransactionEntry_.end()) {
                return;
            }
            if (it->second.Transaction != transaction) {
                return;
            }
            IdToStickyTransactionEntry_.erase(it);
        }

        YT_LOG_DEBUG("Sticky transaction lease expired (TransactionId: %v)",
            transactionId);

        YT_UNUSED_FUTURE(transaction->Abort());
    }

    void OnStickyTransactionCommitted(TTransactionId transactionId)
    {
        OnStickyTransactionFinished(transactionId);
    }

    void OnStickyTransactionAborted(TTransactionId transactionId, const TError& /*error*/)
    {
        OnStickyTransactionFinished(transactionId);
    }

    void OnStickyTransactionFinished(TTransactionId transactionId)
    {
        NConcurrency::TLease lease;
        {
            auto guard = WriterGuard(StickyTransactionLock_);
            auto it = IdToStickyTransactionEntry_.find(transactionId);
            if (it == IdToStickyTransactionEntry_.end()) {
                return;
            }
            lease = it->second.Lease;
            IdToStickyTransactionEntry_.erase(it);
        }

        YT_LOG_DEBUG("Sticky transaction unregistered (TransactionId: %v)",
            transactionId);

        NConcurrency::TLeaseManager::CloseLease(lease);
    }
};

////////////////////////////////////////////////////////////////////////////////

IStickyTransactionPoolPtr CreateStickyTransactionPool(
    const NLogging::TLogger& logger)
{
    return New<TStickyTransactionPool>(logger);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
