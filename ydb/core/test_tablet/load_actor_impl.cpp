#include "load_actor_impl.h"

namespace NKikimr::NTestShard {

    TLoadActor::TLoadActor(ui64 tabletId, ui32 generation, TActorId tablet,
            const NKikimrClient::TTestShardControlRequest::TCmdInitialize& settings)
        : TabletId(tabletId)
        , Generation(generation)
        , Tablet(tablet)
        , Settings(settings)
        , StateServerWriteLatency(1024)
        , WriteLatency(1024)
        , ReadLatency(1024)
    {}

    void TLoadActor::Bootstrap(const TActorId& parentId) {
        STLOG(PRI_DEBUG, TEST_SHARD, TS31, "TLoadActor::Bootstrap", (TabletId, TabletId));
        TabletActorId = parentId;
        Send(MakeStateServerInterfaceActorId(), new TEvStateServerConnect(Settings.GetStorageServerHost(),
            Settings.GetStorageServerPort()));
        Send(parentId, new TTestShard::TEvSwitchMode(TTestShard::EMode::STATE_SERVER_CONNECT));
        Become(&TThis::StateFunc);
    }

    void TLoadActor::PassAway() {
        Send(MakeStateServerInterfaceActorId(), new TEvStateServerDisconnect);
        if (ValidationActorId) {
            TActivationContext::Send(new IEventHandle(TEvents::TSystem::Poison, 0, ValidationActorId, SelfId(), nullptr, 0));
        }
        TActorBootstrapped::PassAway();
    }

    void TLoadActor::HandleWakeup() {
        STLOG(PRI_NOTICE, TEST_SHARD, TS00, "voluntary restart", (TabletId, TabletId));
        TActivationContext::Send(new IEventHandle(TEvents::TSystem::Poison, 0, Tablet, TabletActorId, nullptr, 0));
    }

    void TLoadActor::Action() {
        if (ValidationActorId) { // do nothing while validation is in progress
            return;
        }
        if (StallCounter > 500) {
            if (WritesInFlight.empty() && DeletesInFlight.empty() && ReadsInFlight.empty() && TransitionInFlight.empty()) {
                StallCounter = 0;
            } else {
                return;
            }
        }
        ui64 barrier = 2 * Settings.GetMaxDataBytes();
        if (Settings.HasValidateAfterBytes()) {
            barrier = Settings.GetValidateAfterBytes();
        }
        if (BytesProcessed > barrier) { // time to perform validation
            if (WritesInFlight.empty() && DeletesInFlight.empty() && ReadsInFlight.empty() && TransitionInFlight.empty()) {
                RunValidation(false);
            }
        } else { // resume load
            if (WritesInFlight.size() < Settings.GetMaxInFlight()) { // write until there is space in inflight
                IssueWrite();
                if (WritesInFlight.size() < Settings.GetMaxInFlight()) {
                    TActivationContext::Send(new IEventHandle(EvDoSomeAction, 0, SelfId(), {}, nullptr, 0));
                }
            }
            if (ReadsInFlight.size() < 10 && IssueRead()) {
                TActivationContext::Send(new IEventHandle(EvDoSomeAction, 0, SelfId(), {}, nullptr, 0));
            }
            if (BytesOfData > Settings.GetMaxDataBytes()) { // delete some data if needed
                IssueDelete();
            }
        }
    }

    void TLoadActor::Handle(TEvStateServerStatus::TPtr ev) {
        if (ev->Get()->Connected) {
            RunValidation(true);
        } else {
            TActivationContext::Send(new IEventHandle(TEvents::TSystem::Poison, 0, TabletActorId, SelfId(), nullptr, 0));
            PassAway();
        }
    }

    TDuration TLoadActor::GenerateRandomInterval(const NKikimrClient::TTestShardControlRequest::TTimeInterval& interval) {
        Y_VERIFY(interval.HasFrequency() && interval.HasMaxIntervalMs());
        const double frequency = interval.GetFrequency();
        const double xMin = exp(-frequency * interval.GetMaxIntervalMs() * 1e-3);
        const double x = Max(xMin, TAppData::RandomProvider->GenRandReal2());
        return TDuration::Seconds(-log(x) / frequency);
    }

    TDuration TLoadActor::GenerateRandomInterval(const google::protobuf::RepeatedPtrField<NKikimrClient::TTestShardControlRequest::TTimeInterval>& intervals) {
        return intervals.empty()
            ? TDuration::Zero()
            : GenerateRandomInterval(intervals[intervals.size() == 1 ? 0 : PickInterval(intervals)]);
    }

    size_t TLoadActor::GenerateRandomSize(const google::protobuf::RepeatedPtrField<NKikimrClient::TTestShardControlRequest::TSizeInterval>& intervals,
            bool *isInline) {
        Y_VERIFY(!intervals.empty());
        const auto& interval = intervals[PickInterval(intervals)];
        Y_VERIFY(interval.HasMin() && interval.HasMax() && interval.GetMin() <= interval.GetMax());
        *isInline = interval.GetInline();
        return TAppData::RandomProvider->Uniform(interval.GetMin(), interval.GetMax());
    }

    std::unique_ptr<TEvKeyValue::TEvRequest> TLoadActor::CreateRequest() {
        auto request = std::make_unique<TEvKeyValue::TEvRequest>();
        auto& r = request->Record;
        r.SetTabletId(TabletId);
        r.SetCookie(++LastCookie);
        ++StallCounter;
        return request;
    }

    void TLoadActor::Handle(TEvKeyValue::TEvResponse::TPtr ev) {
        Y_VERIFY(!ValidationActorId); // no requests during validation
        auto& record = ev->Get()->Record;
        if (record.GetStatus() != NMsgBusProxy::MSTATUS_OK) {
            STLOG(PRI_ERROR, TEST_SHARD, TS26, "TEvKeyValue::TEvRequest failed", (TabletId, TabletId),
                (Status, record.GetStatus()), (ErrorReason, record.GetErrorReason()));
            if (const auto it = WritesInFlight.find(record.GetCookie()); it != WritesInFlight.end()) {
                for (const TString& key : it->second.KeysInQuery) {
                    const auto it = Keys.find(key);
                    Y_VERIFY_S(it != Keys.end(), "Key# " << key << " not found in Keys dict");
                    STLOG(PRI_WARN, TEST_SHARD, TS27, "write failed", (TabletId, TabletId), (Key, key));
                    RegisterTransition(*it, ::NTestShard::TStateServer::WRITE_PENDING, ::NTestShard::TStateServer::DELETED);
                }
                WritesInFlight.erase(it);
            }
            if (const auto it = DeletesInFlight.find(record.GetCookie()); it != DeletesInFlight.end()) {
                for (const TString& key : it->second.KeysInQuery) {
                    const auto it = Keys.find(key);
                    Y_VERIFY_S(it != Keys.end(), "Key# " << key << " not found in Keys dict");
                    STLOG(PRI_WARN, TEST_SHARD, TS28, "delete failed", (TabletId, TabletId), (Key, key));
                    RegisterTransition(*it, ::NTestShard::TStateServer::DELETE_PENDING, ::NTestShard::TStateServer::CONFIRMED);
                    BytesOfData += it->second.Len;
                }
                DeletesInFlight.erase(it);
            }
            if (const auto it = ReadsInFlight.find(record.GetCookie()); it != ReadsInFlight.end()) {
                const auto& [key, offset, size, timestamp] = it->second;
                const auto jt = KeysBeingRead.find(key);
                Y_VERIFY(jt != KeysBeingRead.end() && jt->second);
                if (!--jt->second) {
                    KeysBeingRead.erase(jt);
                }
                ReadsInFlight.erase(it);
            }
        } else {
            auto makeResponse = [&] {
                NKikimrClient::TResponse copy;
                copy.CopyFrom(record);
                for (auto& m : *copy.MutableReadResult()) {
                    if (m.HasValue()) {
                        m.SetValue(TStringBuilder() << m.GetValue().size() << " bytes of data");
                    }
                }
                return SingleLineProto(copy);
            };
            STLOG(PRI_INFO, TEST_SHARD, TS04, "TEvKeyValue::TEvResponse", (TabletId, TabletId), (Msg, makeResponse()));
            ProcessWriteResult(record.GetCookie(), record.GetWriteResult());
            ProcessDeleteResult(record.GetCookie(), record.GetDeleteRangeResult());
            ProcessReadResult(record.GetCookie(), record.GetReadResult());
        }
        Action();
    }

    void TTestShard::StartActivities() {
        if (!ActivityActorId && Settings) {
            ActivityActorId = Register(new TLoadActor(TabletID(), Executor()->Generation(), Tablet(), *Settings),
                TMailboxType::ReadAsFilled, AppData()->UserPoolId);
        }
    }

    void TTestShard::PassAway() {
        TActivationContext::Send(new IEventHandle(TEvents::TSystem::Poison, 0, ActivityActorId, {}, {}, 0));
        TKeyValueFlat::PassAway();
    }

} // NKikimr::NTestShard
