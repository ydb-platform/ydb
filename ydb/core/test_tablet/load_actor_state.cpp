#include "load_actor_impl.h"

namespace NKikimr::NTestShard {

    void TLoadActor::RegisterTransition(TKey& key, ::NTestShard::TStateServer::EEntityState from,
            ::NTestShard::TStateServer::EEntityState to, std::unique_ptr<TEvKeyValue::TEvRequest> ev,
            NWilson::TTraceId traceId) {
        STLOG(PRI_DEBUG, TEST_SHARD, TS14, "RegisterTransition", (TabletId, TabletId), (Key, key.first), (From, from),
            (To, to));

        // some sanity checks
        Y_VERIFY_S(key.second.ConfirmedState == key.second.PendingState, "key# " << key.first
                << " ConfirmedState# " << key.second.ConfirmedState
                << " PendingState# " << key.second.PendingState
                << " from# " << from
                << " to# " << to
                << " Request# "<< bool(key.second.Request));
        Y_ABORT_UNLESS(key.second.ConfirmedState == from);
        Y_ABORT_UNLESS(!key.second.Request);
        Y_ABORT_UNLESS(from != to);
        Y_ABORT_UNLESS(from != ::NTestShard::TStateServer::DELETED);
        Y_ABORT_UNLESS(to != ::NTestShard::TStateServer::ABSENT);

        if (!Settings.HasStorageServerHost()) {
            if (from == ::NTestShard::TStateServer::WRITE_PENDING && to == ::NTestShard::TStateServer::CONFIRMED) {
                BytesOfData += key.second.Len;
            }
            if (from == ::NTestShard::TStateServer::CONFIRMED) {
                MakeUnconfirmed(key);
            } else if (to == ::NTestShard::TStateServer::CONFIRMED) {
                MakeConfirmed(key);
            }
            if (to == ::NTestShard::TStateServer::DELETED) {
                Keys.erase(key.first);
            } else {
                key.second.ConfirmedState = key.second.PendingState = to;
                Y_ABORT_UNLESS(key.second.ConfirmedState == ::NTestShard::TStateServer::CONFIRMED
                    ? key.second.ConfirmedKeyIndex < ConfirmedKeys.size() && ConfirmedKeys[key.second.ConfirmedKeyIndex] == key.first
                    : key.second.ConfirmedKeyIndex == Max<size_t>());
            }
            if (ev) {
                Send(TabletActorId, ev.release(), 0, 0, std::move(traceId));
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
        key.second.TraceId = std::move(traceId);
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
                Y_ABORT();
        }

        // obtain current key
        Y_ABORT_UNLESS(!TransitionInFlight.empty());
        auto& key = *TransitionInFlight.front();
        TransitionInFlight.pop_front();

        // account data bytes if confirming written key
        Y_ABORT_UNLESS(key.second.ConfirmedState != key.second.PendingState);
        if (key.second.ConfirmedState == ::NTestShard::TStateServer::WRITE_PENDING &&
                key.second.PendingState == ::NTestShard::TStateServer::CONFIRMED) {
            BytesOfData += key.second.Len;
        }

        // switch to correct state
        if (key.second.ConfirmedState == ::NTestShard::TStateServer::CONFIRMED) {
            MakeUnconfirmed(key);
        } else if (key.second.PendingState == ::NTestShard::TStateServer::CONFIRMED) {
            MakeConfirmed(key);
        }
        key.second.ConfirmedState = key.second.PendingState;
        if (auto& r = key.second.Request) {
            if (const auto it = WritesInFlight.find(r->Record.GetCookie()); it != WritesInFlight.end()) {
                StateServerWriteLatency.Add(TActivationContext::Monotonic(), TDuration::Seconds(it->second.Timer.PassedReset()));
            }
            Send(TabletActorId, r.release(), 0, 0, std::move(key.second.TraceId));
        }
        if (key.second.ConfirmedState == ::NTestShard::TStateServer::DELETED) {
            Y_ABORT_UNLESS(key.second.ConfirmedKeyIndex == Max<size_t>());
            Keys.erase(key.first);
        } else {
            Y_ABORT_UNLESS(key.second.ConfirmedState == ::NTestShard::TStateServer::CONFIRMED
                ? key.second.ConfirmedKeyIndex < ConfirmedKeys.size() && ConfirmedKeys[key.second.ConfirmedKeyIndex] == key.first
                : key.second.ConfirmedKeyIndex == Max<size_t>());
        }

        // perform some action if possible
        Action();
    }

    void TLoadActor::MakeConfirmed(TKey& key) {
        Y_ABORT_UNLESS(key.second.ConfirmedKeyIndex == Max<size_t>());
        key.second.ConfirmedKeyIndex = ConfirmedKeys.size();
        ConfirmedKeys.push_back(key.first);
    }

    void TLoadActor::MakeUnconfirmed(TKey& key) {
        Y_ABORT_UNLESS(key.second.ConfirmedKeyIndex < ConfirmedKeys.size());
        Y_ABORT_UNLESS(ConfirmedKeys[key.second.ConfirmedKeyIndex] == key.first);
        if (key.second.ConfirmedKeyIndex + 1 != ConfirmedKeys.size()) {
            auto& cell = ConfirmedKeys[key.second.ConfirmedKeyIndex];
            std::swap(cell, ConfirmedKeys.back());
            const auto it = Keys.find(cell);
            Y_ABORT_UNLESS(it != Keys.end());
            auto& otherKey = it->second;
            Y_ABORT_UNLESS(otherKey.ConfirmedKeyIndex + 1 == ConfirmedKeys.size());
            otherKey.ConfirmedKeyIndex = key.second.ConfirmedKeyIndex;
        }
        ConfirmedKeys.pop_back();
        key.second.ConfirmedKeyIndex = Max<size_t>();
    }

} // NKikimr::NTestShard

template<>
void Out<::NTestShard::TStateServer::EEntityState>(IOutputStream& s, ::NTestShard::TStateServer::EEntityState value) {
    s << ::NTestShard::TStateServer::EEntityState_Name(value);
}
