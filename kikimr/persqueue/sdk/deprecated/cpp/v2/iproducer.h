#pragma once

#include "types.h"
#include "responses.h"
#include <library/cpp/threading/future/future.h>

namespace NPersQueue {

// IProducer is threadsafe.
// If one creates several producers with same SourceId in same topic - first Producer (from point of server view) will die.
// There could be only one producer with concrete SourceId in concrete topic at once.

class IProducer {
public:
    // Start producer.
    // Producer can be used after its start will be finished.
    virtual NThreading::TFuture<TProducerCreateResponse> Start(TInstant deadline = TInstant::Max()) noexcept = 0;

    NThreading::TFuture<TProducerCreateResponse> Start(TDuration timeout) noexcept {
        return Start(TInstant::Now() + timeout);
    }

    // Add write request to queue.
    virtual NThreading::TFuture<TProducerCommitResponse> Write(TProducerSeqNo seqNo, TData data) noexcept = 0;

    // Add write request to queue without specifying seqNo. So, without deduplication (at least once guarantee).
    virtual NThreading::TFuture<TProducerCommitResponse> Write(TData data) noexcept = 0;

    // Get future that is signalled when producer is dead.
    virtual NThreading::TFuture<TError> IsDead() noexcept = 0;

    virtual ~IProducer() = default;
};

} // namespace NPersQueue
