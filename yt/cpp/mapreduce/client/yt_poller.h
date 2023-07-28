#pragma once

#include <yt/cpp/mapreduce/common/fwd.h>

#include <yt/cpp/mapreduce/http/context.h>
#include <yt/cpp/mapreduce/http/requests.h>

#include <yt/cpp/mapreduce/interface/client.h>

#include <util/generic/list.h>
#include <util/system/mutex.h>
#include <util/system/thread.h>
#include <util/system/condvar.h>

namespace NYT {
namespace NDetail {

namespace NRawClient {
    class TRawBatchRequest;
}

////////////////////////////////////////////////////////////////////////////////

class IYtPollerItem
    : public TThrRefBase
{
public:
    enum EStatus {
        PollContinue,
        PollBreak,
    };

public:
    virtual ~IYtPollerItem() = default;

    virtual void PrepareRequest(NRawClient::TRawBatchRequest* batchRequest) = 0;

    // Should return PollContinue if poller should continue polling this item.
    // Should return PollBreak if poller should stop polling this item.
    virtual EStatus OnRequestExecuted() = 0;

    virtual void OnItemDiscarded() = 0;

};
using IYtPollerItemPtr = ::TIntrusivePtr<IYtPollerItem>;

////////////////////////////////////////////////////////////////////////////////

class TYtPoller
    : public TThrRefBase
{
public:
    TYtPoller(TClientContext context, const IClientRetryPolicyPtr& retryPolicy);
    ~TYtPoller();

    void Watch(IYtPollerItemPtr item);

    void Stop();

private:
    void DiscardQueuedItems();

    void WatchLoop();
    static void* WatchLoopProc(void*);

private:
    struct TItem;

    const TClientContext Context_;
    const IClientRetryPolicyPtr ClientRetryPolicy_;


    TList<IYtPollerItemPtr> InProgress_;
    TList<IYtPollerItemPtr> Pending_;

    TThread WaiterThread_;
    TMutex Lock_;
    TCondVar HasData_;

    bool IsRunning_ = true;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail
} // namespace NYT
