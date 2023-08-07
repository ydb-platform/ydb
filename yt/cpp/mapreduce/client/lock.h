#pragma once

#include "client.h"

#include <yt/cpp/mapreduce/interface/client.h>

namespace NYT {
namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

class TLock
    : public ILock
{
public:
    TLock(const TLockId& lockId, TClientPtr client, bool waitable);

    virtual const TLockId& GetId() const override;
    virtual TNodeId GetLockedNodeId() const override;
    virtual const ::NThreading::TFuture<void>& GetAcquiredFuture() const override;

private:
    const TLockId LockId_;
    mutable TMaybe<::NThreading::TFuture<void>> Acquired_;
    TClientPtr Client_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail
} // namespace NYT
