#include "async_stream_pipe.h"

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

struct TAsyncStreamPipeTag
{ };

////////////////////////////////////////////////////////////////////////////////

TAsyncStreamPipe::TItem::TItem(TSharedRef sharedRef, TPromise<void> writeComplete)
    : Data(std::move(sharedRef))
    , WriteComplete(std::move(writeComplete))
{ }

////////////////////////////////////////////////////////////////////////////////

TFuture<void> TAsyncStreamPipe::Write(const TSharedRef& buffer)
{
    if (!buffer) {
        // Empty buffer has special meaning in our queue, so we don't write it.
        return VoidFuture;
    }

    auto writeComplete = NewPromise<void>();
    Queue_.Enqueue(TItem(TSharedRef::MakeCopy<TAsyncStreamPipeTag>(buffer), writeComplete));
    return writeComplete;
}

TFuture<TSharedRef> TAsyncStreamPipe::Read()
{
    auto result = Queue_.Dequeue();
    return result.Apply(BIND([] (TItem item) {
        item.WriteComplete.Set();
        return item.Data;
    }));
}

TFuture<void> TAsyncStreamPipe::Close()
{
    Queue_.Enqueue(TItem(TSharedRef(), NewPromise<void>()));
    return VoidFuture;
}

TFuture<void> TAsyncStreamPipe::Abort(const TError& error)
{
    auto writeComplete = NewPromise<void>();
    Queue_.Enqueue(error);
    return writeComplete;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
