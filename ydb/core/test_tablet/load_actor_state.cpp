#include "load_actor_impl.h"

namespace NKikimr::NTestShard {

    void TLoadActor::RegisterTransition(TKey& key, ::NTestShard::TStateServer::EEntityState from,
            ::NTestShard::TStateServer::EEntityState to, std::unique_ptr<TEvKeyValue::TEvRequest> ev) {
        STLOG(PRI_DEBUG, TEST_SHARD, TS14, "RegisterTransition", (TabletId, TabletId), (Key, key.first), (From, from),
            (To, to));

        // some sanity checks
        Y_VERIFY(key.second.ConfirmedState == key.second.PendingState);
        Y_VERIFY(key.second.ConfirmedState == from);
        Y_VERIFY(!key.second.Request);
        Y_VERIFY(from != to);
        Y_VERIFY(from != ::NTestShard::TStateServer::DELETED);
        Y_VERIFY(to != ::NTestShard::TStateServer::ABSENT);

        if (!Settings.HasStorageServerHost()) {
            if (from == ::NTestShard::TStateServer::WRITE_PENDING && to == ::NTestShard::TStateServer::CONFIRMED) {
                BytesOfData += key.second.Len;
            }
            if (to == ::NTestShard::TStateServer::DELETED) {
                Keys.erase(key.first);
            } else {
                key.second.ConfirmedState = key.second.PendingState = to;
            }
            if (ev) {
                Send(TabletActorId, ev.release());
            }
            if (!DoSomeActionInFlight) {
                TActivationContext::Send(new IEventHandle(EvDoSomeAction, 0, SelfId(), {}, nullptr, 0));
                DoSomeActionInFlight = true;
            }
            return;
        }

        // generate transition command and send it to state server
        auto request = std::make_unique<TEvStateServerRequest>();
        auto& r = request->Record;
        auto *write = r.MutableWrite();
        write->SetTabletId(TabletId);
        write->SetGeneration(Generation);
        write->SetKey(key.first);
        write->SetOriginState(from);
        write->SetTargetState(to);
        Send(MakeStateServerInterfaceActorId(), request.release());

        // update local state
        key.second.PendingState = to;
        key.second.Request = std::move(ev);
        TransitionInFlight.push_back(&key);
    }

    void TLoadActor::Handle(TEvStateServerWriteResult::TPtr ev) {
        STLOG(PRI_DEBUG, TEST_SHARD, TS15, "received TEvStateServerWriteResult", (TabletId, TabletId));

        // check response
        auto& r = ev->Get()->Record;
        switch (r.GetStatus()) {
            case ::NTestShard::TStateServer::OK:
                break;

            case ::NTestShard::TStateServer::ERROR:
                Y_FAIL_S("ERROR from StateServer TabletId# " << TabletId);

            case ::NTestShard::TStateServer::RACE:
                TActivationContext::Send(new IEventHandle(TEvents::TSystem::Poison, 0, TabletActorId, SelfId(), nullptr, 0));
                PassAway();
                return;

            default:
                Y_FAIL();
        }

        // obtain current key
        Y_VERIFY(!TransitionInFlight.empty());
        auto& key = *TransitionInFlight.front();
        TransitionInFlight.pop_front();

        // account data bytes if confirming written key
        Y_VERIFY(key.second.ConfirmedState != key.second.PendingState);
        if (key.second.ConfirmedState == ::NTestShard::TStateServer::WRITE_PENDING &&
                key.second.PendingState == ::NTestShard::TStateServer::CONFIRMED) {
            BytesOfData += key.second.Len;
        }

        // switch to correct state
        key.second.ConfirmedState = key.second.PendingState;
        if (auto& r = key.second.Request) {
            if (const auto it = WritesInFlight.find(r->Record.GetCookie()); it != WritesInFlight.end()) {
                StateServerWriteLatency.Add(TActivationContext::Monotonic(), TDuration::Seconds(it->second.Timer.PassedReset()));
            }
            Send(TabletActorId, r.release());
        }
        if (key.second.ConfirmedState == ::NTestShard::TStateServer::DELETED) {
            Keys.erase(key.first);
        }

        // perform some action if possible
        Action();
    }

} // NKikimr::NTestShard
