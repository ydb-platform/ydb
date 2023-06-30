#include "abortable_registry.h"

#include <yt/cpp/mapreduce/common/retry_lib.h>

#include <yt/cpp/mapreduce/interface/common.h>
#include <yt/cpp/mapreduce/interface/logging/yt_log.h>

#include <util/generic/singleton.h>

namespace NYT {
namespace NDetail {

using namespace NRawClient;

////////////////////////////////////////////////////////////////////////////////

TTransactionAbortable::TTransactionAbortable(const TClientContext& context, const TTransactionId& transactionId)
    : Context_(context)
    , TransactionId_(transactionId)
{ }

void TTransactionAbortable::Abort()
{
    AbortTransaction(nullptr, Context_, TransactionId_);
}

TString TTransactionAbortable::GetType() const
{
    return "transaction";
}

////////////////////////////////////////////////////////////////////////////////

TOperationAbortable::TOperationAbortable(IClientRetryPolicyPtr clientRetryPolicy, TClientContext context, const TOperationId& operationId)
    : ClientRetryPolicy_(std::move(clientRetryPolicy))
    , Context_(std::move(context))
    , OperationId_(operationId)
{ }


void TOperationAbortable::Abort()
{
    AbortOperation(ClientRetryPolicy_->CreatePolicyForGenericRequest(), Context_, OperationId_);
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

} // namespace NDetail
} // namespace NYT
