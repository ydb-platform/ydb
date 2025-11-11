#pragma once

#include <atomic>
#include <optional>
#include <vector>

#include <util/generic/fwd.h>

namespace NYT::NYTProf {

////////////////////////////////////////////////////////////////////////////////

/*!
 *  TStaticQueue is SPSC thread and signal safe queue that does not perform
 *  any allocations.
 */
class TStaticQueue
{
public:
    inline TStaticQueue(size_t logSize);

    template <class TFn>
    bool TryPush(const TFn& getIp);

    template <class TFn>
    bool TryPop(const TFn& onIp);

private:
    const i64 Size_;

    std::atomic<i64> WriteSeq_{0};
    std::atomic<i64> ReadSeq_{0};

    std::vector<intptr_t> Buffer_;

    inline size_t ToIndex(i64 seq) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTProf

#define QUEUE_INL_H_
#include "queue-inl.h"
#undef QUEUE_INL_H_
