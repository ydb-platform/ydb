#include "session_reply_router.h"

namespace NYdb::NBS::NBlockStore::NStorage::NTransport {

////////////////////////////////////////////////////////////////////////////////

TSessionReplyRouter::~TSessionReplyRouter()
{
    // Dropping InFlight refs runs IReplyHandler destructors, which complete
    // still-pending promises with OUTDATED / "Session broken".
    for (auto& trackingMap: InFlightTrackingMaps) {
        with_lock (trackingMap.Lock) {
            trackingMap.InFlight.clear();
        }
    }
}

ui64 TSessionReplyRouter::Add(TIntrusivePtr<IReplyHandler> handler)
{
    Y_ABORT_UNLESS(handler);
    const ui64 cookie = NextCookie.fetch_add(1, std::memory_order_relaxed);
    auto& trackingMap = InFlightTrackingMapFor(cookie);
    with_lock (trackingMap.Lock) {
        auto [it, inserted] =
            trackingMap.InFlight.emplace(cookie, std::move(handler));
        Y_ABORT_UNLESS(inserted);
    }
    return cookie;
}

void TSessionReplyRouter::Remove(ui64 cookie)
{
    auto& trackingMap = InFlightTrackingMapFor(cookie);
    with_lock (trackingMap.Lock) {
        trackingMap.InFlight.erase(cookie);
    }
}

void TSessionReplyRouter::Receive(TAutoPtr<NActors::IEventHandle> ev)
{
    const ui64 cookie = ev->Cookie;
    auto& trackingMap = InFlightTrackingMapFor(cookie);

    TIntrusivePtr<IReplyHandler> handler;
    {
        with_lock (trackingMap.Lock) {
            if (auto* ptr = trackingMap.InFlight.FindPtr(cookie)) {
                handler = *ptr;
            }
        }
    }
    if (!handler) {
        // Stale / already-completed cookie. Drop rather than forwarding into
        // the actor system toward a nonexistent virtual ActorId.
        return;
    }

    if (handler->Receive(ev)) {
        with_lock (trackingMap.Lock) {
            trackingMap.InFlight.erase(cookie);
        }
    }
}

TSessionReplyRouter::TInFlightTrackingMap&
TSessionReplyRouter::InFlightTrackingMapFor(ui64 cookie)
{
    return InFlightTrackingMaps[cookie & (InFlightTrackingMapCount - 1)];
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NTransport
