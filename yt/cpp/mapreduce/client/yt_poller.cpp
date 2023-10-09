#include "yt_poller.h"

#include <yt/cpp/mapreduce/raw_client/raw_batch_request.h>
#include <yt/cpp/mapreduce/raw_client/raw_requests.h>

#include <yt/cpp/mapreduce/common/debug_metrics.h>
#include <yt/cpp/mapreduce/common/retry_lib.h>
#include <yt/cpp/mapreduce/common/wait_proxy.h>

#include <yt/cpp/mapreduce/http/retry_request.h>

#include <yt/cpp/mapreduce/interface/config.h>

#include <yt/cpp/mapreduce/interface/logging/yt_log.h>

namespace NYT {
namespace NDetail {

using namespace NRawClient;

////////////////////////////////////////////////////////////////////////////////

TYtPoller::TYtPoller(
    TClientContext context,
    const IClientRetryPolicyPtr& retryPolicy)
    : Context_(std::move(context))
    , ClientRetryPolicy_(retryPolicy)
    , WaiterThread_(&TYtPoller::WatchLoopProc, this)
{
    WaiterThread_.Start();
}

TYtPoller::~TYtPoller()
{
    Stop();
}

void TYtPoller::Watch(IYtPollerItemPtr item)
{
    auto g = Guard(Lock_);
    Pending_.emplace_back(std::move(item));
    HasData_.Signal();
}


void TYtPoller::Stop()
{
    {
        auto g = Guard(Lock_);
        if (!IsRunning_) {
            return;
        }
        IsRunning_ = false;
        HasData_.Signal();
    }
    WaiterThread_.Join();
}

void TYtPoller::DiscardQueuedItems()
{
    for (auto& item : Pending_) {
        item->OnItemDiscarded();
    }
    for (auto& item : InProgress_) {
        item->OnItemDiscarded();
    }
}

void TYtPoller::WatchLoop()
{
    TInstant nextRequest = TInstant::Zero();
    while (true) {
        {
            auto g = Guard(Lock_);
            if (IsRunning_ && Pending_.empty() && InProgress_.empty()) {
                TWaitProxy::Get()->WaitCondVar(HasData_, Lock_);
            }

            if (!IsRunning_) {
                DiscardQueuedItems();
                return;
            }

            {
                auto ug = Unguard(Lock_);  // allow adding new items into Pending_
                TWaitProxy::Get()->SleepUntil(nextRequest);
                nextRequest = TInstant::Now() + Context_.Config->WaitLockPollInterval;
            }
            if (!Pending_.empty()) {
                InProgress_.splice(InProgress_.end(), Pending_);
            }
            Y_ABORT_UNLESS(!InProgress_.empty());
        }

        TRawBatchRequest rawBatchRequest(Context_.Config);

        for (auto& item : InProgress_) {
            item->PrepareRequest(&rawBatchRequest);
        }

        try {
            ExecuteBatch(ClientRetryPolicy_->CreatePolicyForGenericRequest(), Context_, rawBatchRequest);
        } catch (const std::exception& ex) {
            YT_LOG_ERROR("Exception while executing batch request: %v", ex.what());
        }

        for (auto it = InProgress_.begin(); it != InProgress_.end();) {
            auto& item = *it;

            IYtPollerItem::EStatus status = item->OnRequestExecuted();

            if (status == IYtPollerItem::PollBreak) {
                it = InProgress_.erase(it);
            } else {
                ++it;
            }
        }

        IncDebugMetric(TStringBuf("yt_poller_top_loop_repeat_count"));
    }
}

void* TYtPoller::WatchLoopProc(void* data)
{
    static_cast<TYtPoller*>(data)->WatchLoop();
    return nullptr;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail
} // namespace NYT
