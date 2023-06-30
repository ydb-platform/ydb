#pragma once

#include "public.h"

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/profiling/timing.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

struct IReadyEventReaderBase
    : public virtual TRefCounted
{
    virtual TFuture<void> GetReadyEvent() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IReadyEventReaderBase)

////////////////////////////////////////////////////////////////////////////////

class TReadyEventReaderBase
    : public virtual NChunkClient::IReadyEventReaderBase
{
protected:
    //! Return ready event without starting wait timer. Intended for internal use in subclasses.
    const TFuture<void>& ReadyEvent() const;

    //! Set ready event. Ready event is wrapped with a callback which
    //! stops wait timer when ready event is ready.
    void SetReadyEvent(TFuture<void> readyEvent);

    //! Return how much time caller spent waiting on ready event.
    TDuration GetWaitTime() const;

private:
    TFuture<void> ReadyEvent_ = VoidFuture;

    //! This timer is started when GetReadyEvent() is invoked and stopped when ready event is set.
    //! In other words, it shows how much time caller spends waiting on ready event.
    mutable NProfiling::TWallTimer WaitTimer_ = NProfiling::TWallTimer(false /* start */);

    //! Return ready event and start wait timer if it is not already active.
    //! This method is intended for external use, but not for accessing ready event from subclasses, thus private.
    TFuture<void> GetReadyEvent() const override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
