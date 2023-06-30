#include "coro_pipe.h"

#include <yt/yt/core/actions/bind.h>

namespace NYT {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TCoroStream
    : public IZeroCopyInput
{
public:
    TCoroStream(TCoroutine<void(TStringBuf)>* coroutine, TStringBuf data)
        : Coroutine_(coroutine)
        , PendingData_(data)
        , Finished_(data.empty())
    { }

    size_t DoNext(const void** ptr, size_t len) override
    {
        if (PendingData_.empty()) {
            if (Finished_) {
                *ptr = nullptr;
                return 0;
            }
            std::tie(PendingData_) = Coroutine_->Yield();
            if (PendingData_.Empty()) {
                Finished_ = true;
                *ptr = nullptr;
                return 0;
            }
        }
        *ptr = PendingData_.Data();
        len = Min(len, PendingData_.Size());
        PendingData_.Skip(len);
        return len;
    }

    void Complete()
    {
        if (!Finished_) {
            const void* ptr;
            if (!PendingData_.Empty() || DoNext(&ptr, 1)) {
                THROW_ERROR_EXCEPTION("Stray data in stream");
            }
        }
    }

private:
    TCoroutine<void(TStringBuf)>* const Coroutine_;
    TStringBuf PendingData_;
    bool Finished_ = false;
};

////////////////////////////////////////////////////////////////////////////////

TCoroPipe::TCoroPipe(std::function<void(IZeroCopyInput*)> streamReader)
    : Coroutine_(BIND(
        [streamReader=std::move(streamReader)] (TCoroutine& self, TStringBuf data) {
            TCoroStream stream(&self, data);
            streamReader(&stream);
            stream.Complete();
        }))
{ }

void TCoroPipe::Feed(TStringBuf data)
{
    if (data) {
        Coroutine_.Run(data);
    }
}

void TCoroPipe::Finish()
{
    Coroutine_.Run(TStringBuf());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
