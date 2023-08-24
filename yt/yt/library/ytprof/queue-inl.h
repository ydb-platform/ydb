#ifndef QUEUE_INL_H_
#error "Direct inclusion of this file is not allowed, include queue.h"
#include "queue.h"
#endif
#undef QUEUE_INL_H_

namespace NYT::NYTProf {

////////////////////////////////////////////////////////////////////////////////

TStaticQueue::TStaticQueue(size_t logSize)
    : Size_(1 << logSize)
    , Buffer_(Size_)
{ }

template <class TFn>
bool TStaticQueue::TryPush(const TFn& getIp)
{
    auto start = WriteSeq_.load(std::memory_order::relaxed);
    auto end = ReadSeq_.load(std::memory_order::acquire) + Size_;

    if (start + 1 >= end) {
        return false;
    }

    int count = 0;
    while (true) {
        auto [ip, ok] = getIp();
        if (!ok) {
            break;
        }

        if (start + count + 1 >= end) {
            return false;
        }

        Buffer_[ToIndex(start+count+1)] = reinterpret_cast<intptr_t>(ip);
        count++;
    }

    Buffer_[ToIndex(start)] = count;
    WriteSeq_.store(start+count+1, std::memory_order::release);
    return true;
}

template <class TFn>
bool TStaticQueue::TryPop(const TFn& onIp)
{
    auto start = ReadSeq_.load(std::memory_order::relaxed);
    auto end = WriteSeq_.load(std::memory_order::acquire);

    if (start == end) {
        return false;
    }

    auto count = Buffer_[ToIndex(start)];
    for (int i = 0; i < count; i++) {
        onIp(reinterpret_cast<void*>(Buffer_[ToIndex(start+i+1)]));
    }

    ReadSeq_.store(start+count+1, std::memory_order::release);
    return true;
}

size_t TStaticQueue::ToIndex(i64 seq) const
{
    return seq & (Size_ - 1);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTProf
