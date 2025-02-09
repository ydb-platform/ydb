#include "transaction.h"

#include "transaction_pinger.h"

#include <yt/cpp/mapreduce/common/retry_lib.h>
#include <yt/cpp/mapreduce/common/retry_request.h>
#include <yt/cpp/mapreduce/common/wait_proxy.h>

#include <yt/cpp/mapreduce/interface/config.h>
#include <yt/cpp/mapreduce/interface/error_codes.h>
#include <yt/cpp/mapreduce/interface/raw_client.h>

#include <util/datetime/base.h>

#include <util/generic/scope.h>

#include <util/random/random.h>

#include <util/string/builder.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TPingableTransaction::TPingableTransaction(
    const IRawClientPtr& rawClient,
    const IClientRetryPolicyPtr& retryPolicy,
    const TClientContext& context,
    const TTransactionId& parentId,
    ITransactionPingerPtr transactionPinger,
    const TStartTransactionOptions& options)
    : RawClient_(rawClient)
    , ClientRetryPolicy_(retryPolicy)
    , Context_(context)
    , AbortableRegistry_(NDetail::TAbortableRegistry::Get())
    , AbortOnTermination_(true)
    , AutoPingable_(options.AutoPingable_)
    , Pinger_(std::move(transactionPinger))
{
    auto transactionId = NDetail::RequestWithRetry<TTransactionId>(
        ClientRetryPolicy_->CreatePolicyForGenericRequest(),
        [this, &parentId, &options] (TMutationId& mutationId) {
            return RawClient_->StartTransaction(mutationId, parentId, options);
        });
    auto actualTimeout = options.Timeout_.GetOrElse(Context_.Config->TxTimeout);
    Init(rawClient, context, transactionId, actualTimeout);
}

TPingableTransaction::TPingableTransaction(
    const IRawClientPtr& rawClient,
    const IClientRetryPolicyPtr& retryPolicy,
    const TClientContext& context,
    const TTransactionId& transactionId,
    ITransactionPingerPtr transactionPinger,
    const TAttachTransactionOptions& options)
    : RawClient_(rawClient)
    , ClientRetryPolicy_(retryPolicy)
    , Context_(context)
    , AbortableRegistry_(NDetail::TAbortableRegistry::Get())
    , AbortOnTermination_(options.AbortOnTermination_)
    , AutoPingable_(options.AutoPingable_)
    , Pinger_(std::move(transactionPinger))
{
    auto timeoutNode = NDetail::RequestWithRetry<TNode>(
        ClientRetryPolicy_->CreatePolicyForGenericRequest(),
        [this, &transactionId] (TMutationId /*mutationId*/) {
            return RawClient_->TryGet(
                TTransactionId(),
                "#" + GetGuidAsString(transactionId) + "/@timeout",
                TGetOptions());
        });
    if (timeoutNode.IsUndefined()) {
        throw yexception() << "Transaction " << GetGuidAsString(transactionId) << " does not exist";
    }
    auto timeout = TDuration::MilliSeconds(timeoutNode.AsInt64());
    Init(rawClient, context, transactionId, timeout);
}

void TPingableTransaction::Init(
    const IRawClientPtr& rawClient,
    const TClientContext& context,
    const TTransactionId& transactionId,
    TDuration timeout)
{
    TransactionId_ = transactionId;

    if (AbortOnTermination_) {
        AbortableRegistry_->Add(
            TransactionId_,
            ::MakeIntrusive<NDetail::TTransactionAbortable>(rawClient, context, TransactionId_));
    }

    if (AutoPingable_) {
        // Compute 'MaxPingInterval_' and 'MinPingInterval_' such that 'pingInterval == (max + min) / 2'.
        auto pingInterval = context.Config->PingInterval;
        auto safeTimeout = timeout - TDuration::Seconds(5);
        MaxPingInterval_ = Max(pingInterval, Min(safeTimeout, pingInterval * 1.5));
        MinPingInterval_ = pingInterval - (MaxPingInterval_ - pingInterval);

        Pinger_->RegisterTransaction(*this);
    }
}

TPingableTransaction::~TPingableTransaction()
{
    try {
        Stop(AbortOnTermination_ ? EStopAction::Abort : EStopAction::Detach);
    } catch (...) {
    }
}

const TTransactionId TPingableTransaction::GetId() const
{
    return TransactionId_;
}

const std::pair<TDuration, TDuration> TPingableTransaction::GetPingInterval() const
{
    return {MinPingInterval_, MaxPingInterval_};
}

const TClientContext TPingableTransaction::GetContext() const
{
    return Context_;
}

void TPingableTransaction::Ping() const
{
    RawClient_->PingTransaction(TransactionId_);
}

void TPingableTransaction::Commit()
{
    Stop(EStopAction::Commit);
}

void TPingableTransaction::Abort()
{
    Stop(EStopAction::Abort);
}

void TPingableTransaction::Detach()
{
    Stop(EStopAction::Detach);
}

void TPingableTransaction::Stop(EStopAction action)
{
    if (Finalized_) {
        return;
    }

    Y_DEFER {
        Finalized_ = true;
        if (AutoPingable_ && Pinger_->HasTransaction(*this)) {
            Pinger_->RemoveTransaction(*this);
        }
    };

    switch (action) {
        case EStopAction::Commit:
            NDetail::RequestWithRetry<void>(
                ClientRetryPolicy_->CreatePolicyForGenericRequest(),
                [this] (TMutationId& mutationId) {
                    RawClient_->CommitTransaction(mutationId, TransactionId_);
                });
            break;
        case EStopAction::Abort:
        NDetail::RequestWithRetry<void>(
                ClientRetryPolicy_->CreatePolicyForGenericRequest(),
                [this] (TMutationId& mutationId) {
                    RawClient_->AbortTransaction(mutationId, TransactionId_);
                });
            break;
        case EStopAction::Detach:
            // Do nothing.
            break;
    }

    AbortableRegistry_->Remove(TransactionId_);
}

////////////////////////////////////////////////////////////////////////////////

TYPath Snapshot(
    const IRawClientPtr& rawClient,
    const IClientRetryPolicyPtr& clientRetryPolicy,
    const TTransactionId& transactionId,
    const TYPath& path)
{
    auto lockId = NDetail::RequestWithRetry<TLockId>(
        clientRetryPolicy->CreatePolicyForGenericRequest(),
        [&rawClient, &transactionId, &path] (TMutationId& mutationId) {
            return rawClient->Lock(
                mutationId,
                transactionId,
                path,
                ELockMode::LM_SNAPSHOT);
        });

    auto lockedNodeId = NDetail::RequestWithRetry<TNode>(
        clientRetryPolicy->CreatePolicyForGenericRequest(),
        [&rawClient, &transactionId, &lockId] (TMutationId /*mutationId*/) {
            return rawClient->Get(
                transactionId,
                ::TStringBuilder() << '#' << GetGuidAsString(lockId) << "/@node_id");
        });
    return "#" + lockedNodeId.AsString();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
