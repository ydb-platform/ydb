#pragma once

#include "public.h"

#include <yt/yt/core/net/public.h>

namespace NYT::NHttp::NDetail {

////////////////////////////////////////////////////////////////////////////////

//! Responsible for returning the connection to the owning pool
//! if it could be reused
struct TReusableConnectionState final
{
    std::atomic<bool> Reusable = true;
    NNet::IConnectionPtr Connection;
    TConnectionPoolPtr OwningPool;

    TReusableConnectionState(NNet::IConnectionPtr connection, TConnectionPoolPtr owningPool);
    ~TReusableConnectionState();
};

using TReusableConnectionStatePtr = TIntrusivePtr<TReusableConnectionState>;

//! Reports to the shared state whether the connection could be reused
//! (by calling T::IsSafeToReuse() in the destructor)
template <class T>
class TConnectionReuseWrapper
    : public T
{
public:
    using T::T;

    ~TConnectionReuseWrapper() override;

    void SetReusableState(TReusableConnectionStatePtr reusableState);

private:
    TReusableConnectionStatePtr ReusableState_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttp::NDetail

#define CONNECTION_REUSE_HELPERS_INL_H
#include "connection_reuse_helpers-inl.h"
#undef CONNECTION_REUSE_HELPERS_INL_H
