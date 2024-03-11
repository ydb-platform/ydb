#pragma once

#include "abortable_registry.h"

#include <yt/cpp/mapreduce/http/requests.h>
#include <yt/cpp/mapreduce/http/retry_request.h>

#include <util/datetime/base.h>
#include <util/generic/maybe.h>
#include <util/generic/ptr.h>
#include <util/system/thread.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TPingableTransaction
{
public:
    //
    // Start a new transaction.
    TPingableTransaction(
        const IClientRetryPolicyPtr& retryPolicy,
        const TClientContext& context,
        const TTransactionId& parentId,
        ITransactionPingerPtr transactionPinger,
        const TStartTransactionOptions& options);

    //
    // Attach to an existing transaction.
    TPingableTransaction(
        const IClientRetryPolicyPtr& retryPolicy,
        const TClientContext& context,
        const TTransactionId& transactionId,
        ITransactionPingerPtr transactionPinger,
        const TAttachTransactionOptions& options);

    ~TPingableTransaction();

    const TTransactionId GetId() const;

    const std::pair<TDuration, TDuration> GetPingInterval() const;
    const TClientContext GetContext() const;

    void Commit();
    void Abort();
    void Detach();


private:
    enum class EStopAction
    {
        Detach,
        Abort,
        Commit,
    };

private:
    IClientRetryPolicyPtr ClientRetryPolicy_;
    TClientContext Context_;
    TTransactionId TransactionId_;
    TDuration MinPingInterval_;
    TDuration MaxPingInterval_;

    // We have to own an IntrusivePtr to registry to prevent use-after-free.
    ::TIntrusivePtr<NDetail::TAbortableRegistry> AbortableRegistry_;

    bool AbortOnTermination_;

    bool AutoPingable_;
    bool Finalized_ = false;
    ITransactionPingerPtr Pinger_;

private:
    void Init(
        const TClientContext& context,
        const TTransactionId& transactionId,
        TDuration timeout);

    void Stop(EStopAction action);
};

////////////////////////////////////////////////////////////////////////////////

TYPath Snapshot(
    const IClientRetryPolicyPtr& clientRetryPolicy,
    const TClientContext& context,
    const TTransactionId& transactionId,
    const TYPath& path);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
