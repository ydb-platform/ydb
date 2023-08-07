#pragma once

#include "public.h"

#include <yt/yt/core/concurrency/async_stream.h>
#include <yt/yt/core/concurrency/nonblocking_queue.h>


namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

class TAsyncStreamPipe
    : public IAsyncZeroCopyInputStream
    , public IAsyncOutputStream
{
public:
    TFuture<TSharedRef> Read() override;

    TFuture<void> Write(const TSharedRef& buffer) override;
    TFuture<void> Close() override;

    TFuture<void> Abort(const TError& error);

private:
    struct TItem
    {
        // If Data is empty it means close was requested.
        TSharedRef Data;
        TPromise<void> WriteComplete;

        TItem(TSharedRef sharedRef, TPromise<void> writeComplete);
    };

    TNonblockingQueue<TItem> Queue_;
};

DEFINE_REFCOUNTED_TYPE(TAsyncStreamPipe)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
