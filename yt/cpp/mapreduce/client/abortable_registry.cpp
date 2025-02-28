#include "abortable_registry.h"

#include <yt/cpp/mapreduce/common/retry_lib.h>

#include <yt/cpp/mapreduce/common/retry_request.h>

#include <yt/cpp/mapreduce/interface/common.h>
#include <yt/cpp/mapreduce/interface/raw_client.h>

#include <yt/cpp/mapreduce/interface/logging/yt_log.h>

#include <util/generic/singleton.h>

namespace NYT::NDetail {

////////////////////////////////////////////////////////////////////////////////

TTransactionAbortable::TTransactionAbortable(
    const IRawClientPtr& rawClient,
    const TClientContext& context,
    const TTransactionId& transactionId)
    : RawClient_(rawClient)
    , Context_(context)
    , TransactionId_(transactionId)
{ }

void TTransactionAbortable::Abort()
{
    RequestWithRetry<void>(
        CreateDefaultRequestRetryPolicy(Context_.Config),
        [this] (TMutationId& mutationId) {
            RawClient_->AbortTransaction(mutationId, TransactionId_);
        });
}

TString TTransactionAbortable::GetType() const
{
    return "transaction";
}

////////////////////////////////////////////////////////////////////////////////

TOperationAbortable::TOperationAbortable(
    IRawClientPtr rawClient,
    IClientRetryPolicyPtr clientRetryPolicy,
    const TOperationId& operationId)
    : RawClient_(std::move(rawClient))
    , ClientRetryPolicy_(std::move(clientRetryPolicy))
    , OperationId_(operationId)
{ }

void TOperationAbortable::Abort()
{
    RequestWithRetry<void>(
        ClientRetryPolicy_->CreatePolicyForGenericRequest(),
        [this] (TMutationId& mutationId) {
            RawClient_->AbortOperation(mutationId, OperationId_);
        });
}

TString TOperationAbortable::GetType() const
{
    return "operation";
}

////////////////////////////////////////////////////////////////////////////////

void TAbortableRegistry::AbortAllAndBlockForever()
{
    auto guard = Guard(Lock_);

    for (const auto& entry : ActiveAbortables_) {
        const auto& id = entry.first;
        const auto& abortable = entry.second;
        try {
            abortable->Abort();
        } catch (std::exception& ex) {
            YT_LOG_ERROR("Exception while aborting %v %v: %v",
                abortable->GetType(),
                id,
                ex.what());
        }
    }

    Running_ = false;
}

void TAbortableRegistry::Add(const TGUID& id, IAbortablePtr abortable)
{
    auto guard = Guard(Lock_);

    if (!Running_) {
        Sleep(TDuration::Max());
    }

    ActiveAbortables_[id] = abortable;
}

void TAbortableRegistry::Remove(const TGUID& id)
{
    auto guard = Guard(Lock_);

    if (!Running_) {
        Sleep(TDuration::Max());
    }

    ActiveAbortables_.erase(id);
}

////////////////////////////////////////////////////////////////////////////////

namespace {

class TRegistryHolder
{
public:
    TRegistryHolder()
        : Registry_(::MakeIntrusive<TAbortableRegistry>())
    { }

    ::TIntrusivePtr<TAbortableRegistry> Get()
    {
        return Registry_;
    }

private:
    ::TIntrusivePtr<TAbortableRegistry> Registry_;
};

} // namespace

::TIntrusivePtr<TAbortableRegistry> TAbortableRegistry::Get()
{
    return Singleton<TRegistryHolder>()->Get();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDetail
