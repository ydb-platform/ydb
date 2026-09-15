#pragma once

#include <ydb/library/actors/interconnect/interconnect_direct_session.h>

#include <util/generic/hash.h>
#include <util/system/spinlock.h>

#include <array>
#include <atomic>
#include <bit>

namespace NYdb::NBS::NBlockStore::NStorage::NTransport {

////////////////////////////////////////////////////////////////////////////////

// Per-request reply demultiplexer entry owned by TSessionReplyRouter.
// Receive() returns true when the handler is finished and should be erased
// from the router. Destructors complete still-pending work with OUTDATED /
// "Session broken" when the router (or the session) dies.
class IReplyHandler: public TThrRefBase
{
public:
    ~IReplyHandler() override = default;

    // Returns true when the handler is finished and may be erased.
    [[nodiscard]] virtual bool Receive(TAutoPtr<NActors::IEventHandle> ev) = 0;
};

////////////////////////////////////////////////////////////////////////////////

// Long-lived IReceiveCallback registered once per IDirectSession. Datapath
// replies are demultiplexed by cookie into IReplyHandler instances that NBS
// executor threads register via Add().
class TSessionReplyRouter: public NActors::IReceiveCallback
{
public:
    TSessionReplyRouter() = default;
    ~TSessionReplyRouter() override;

    // NBS executor threads. Returns the cookie to stamp on the outbound event.
    [[nodiscard]] ui64 Add(TIntrusivePtr<IReplyHandler> handler);

    // Drop a handler that was never sent (Send returned false). Destructor of
    // the handler completes the associated promise with session-broken.
    void Remove(ui64 cookie);

    // IC shard / input-session thread.
    void Receive(TAutoPtr<NActors::IEventHandle> ev) override;

private:
    static constexpr size_t InFlightTrackingMapCount = 64;
    static_assert(std::has_single_bit(InFlightTrackingMapCount));

    struct TInFlightTrackingMap
    {
        TAdaptiveLock Lock;
        THashMap<ui64, TIntrusivePtr<IReplyHandler>> InFlight;
    };

    [[nodiscard]] TInFlightTrackingMap& InFlightTrackingMapFor(ui64 cookie);

    std::array<TInFlightTrackingMap, InFlightTrackingMapCount>
        InFlightTrackingMaps;
    std::atomic<ui64> NextCookie{1};
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NTransport
