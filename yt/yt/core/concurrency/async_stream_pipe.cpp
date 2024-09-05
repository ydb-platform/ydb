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

TBoundedAsyncStreamPipe::TBoundedAsyncStreamPipe(i64 sizeLimit)
    : Queue_(sizeLimit)
{ }

TFuture<void> TBoundedAsyncStreamPipe::Write(const TSharedRef& buffer)
{
    if (!buffer) {
        // Empty buffer has special meaning in our queue, so we don't write it.
        return VoidFuture;
    }

    if (Aborted_.load()) {
        return MakeFuture(Error_);
    }

    auto result = Queue_.Enqueue(TSharedRef::MakeCopy<TAsyncStreamPipeTag>(buffer));

    if (Aborted_.load()) {
        Queue_.Drain(Error_);
        return MakeFuture(Error_);
    }

    return result.Apply(BIND([this, this_ = MakeStrong(this)] () -> TFuture<void> {
        if (Aborted_.load()) {
            return MakeFuture(Error_);
        }
        return VoidFuture;
    }));
}

TFuture<TSharedRef> TBoundedAsyncStreamPipe::Read()
{
    if (Aborted_.load()) {
        return MakeFuture<TSharedRef>(Error_);
    }

    auto result = Queue_.Dequeue();

    if (Aborted_.load()) {
        Queue_.Drain(Error_);
        return MakeFuture<TSharedRef>(Error_);
    }

    return result.ApplyUnique(BIND([this, this_ = MakeStrong(this)] (TSharedRef&& data) -> TFuture<TSharedRef> {
        if (Aborted_.load()) {
            return MakeFuture<TSharedRef>(Error_);
        }
        return MakeFuture(std::move(data));
    }));
}

TFuture<void> TBoundedAsyncStreamPipe::Close()
{
    return Queue_.Enqueue(TSharedRef());
}

void TBoundedAsyncStreamPipe::Abort(const TError& error)
{
    if (Aborting_.exchange(true)) {
        return;
    }
    Error_ = error;
    Aborted_.store(true);

    Queue_.Drain(Error_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
