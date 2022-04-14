#pragma once

#include "types.h"
#include "responses.h"

#include <kikimr/yndx/api/protos/persqueue.pb.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/vector.h>

namespace NPersQueue {

class IConsumer {
public:
    virtual ~IConsumer() = default;

    // Start consumer.
    // Consumer can be used after its start will be finished.
    virtual NThreading::TFuture<TConsumerCreateResponse> Start(TInstant deadline = TInstant::Max()) noexcept = 0;

    NThreading::TFuture<TConsumerCreateResponse> Start(TDuration timeout) noexcept {
        return Start(TInstant::Now() + timeout);
    }

    //read result according to settings or Lock/Release requests from server(if UseLockSession is true)
    virtual NThreading::TFuture<TConsumerMessage> GetNextMessage() noexcept = 0;

    //commit processed reads
    virtual void Commit(const TVector<ui64>& cookies) noexcept = 0;

    //will request status - response will be received in GetNextMessage with EMT_STATUS
    virtual void RequestPartitionStatus(const TString& topic, ui64 partition, ui64 generation) noexcept { Y_UNUSED(topic); Y_UNUSED(partition); Y_UNUSED(generation); };

    //will be signalled in case of errors - you may use this future if you can't use GetNextMessage() //out of memory or any other reason
    virtual NThreading::TFuture<TError> IsDead() noexcept = 0;
};

}
