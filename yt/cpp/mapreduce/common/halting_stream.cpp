#include "halting_stream.h"

#include <yt/yt/core/actions/bind.h>
#include <yt/yt/core/concurrency/async_stream.h>

namespace NYT::NDetail {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class THaltingAsyncStream
    : public IAsyncInputStream
{
public:
    THaltingAsyncStream(
        IAsyncInputStreamPtr underlying,
        i64 bytesBeforeHalt)
        : Underlying_(std::move(underlying))
        , BytesBeforeHalt_(bytesBeforeHalt)
    { }

private:
    void OnRead(TPromise<size_t> promise, const TErrorOr<size_t>& result)
    {
        if (result.IsOK()) {
            BytesRead_ += result.Value();
        }
        promise.TrySet(result);
    }

    TFuture<size_t> Read(const TSharedMutableRef& buffer) override
    {
        if (BytesRead_ >= BytesBeforeHalt_) {
            HaltPromise_ = NewPromise<size_t>();
            return HaltPromise_.ToFuture();
        }

        auto limit = std::min(buffer.Size(), static_cast<size_t>(BytesBeforeHalt_ - BytesRead_));
        auto promise = NewPromise<size_t>();
        auto future = promise.ToFuture();

        Underlying_->Read(buffer.Slice(0, limit)).Subscribe(
            BIND(&THaltingAsyncStream::OnRead, MakeStrong(this), std::move(promise)));

        return future;
    }

private:
    IAsyncInputStreamPtr Underlying_;
    const i64 BytesBeforeHalt_;
    i64 BytesRead_ = 0;
    TPromise<size_t> HaltPromise_;
};

////////////////////////////////////////////////////////////////////////////////

IAsyncInputStreamPtr CreateHaltingAsyncStream(
    IAsyncInputStreamPtr underlying,
    i64 bytesBeforeHalt)
{
    return New<THaltingAsyncStream>(std::move(underlying), bytesBeforeHalt);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDetail
