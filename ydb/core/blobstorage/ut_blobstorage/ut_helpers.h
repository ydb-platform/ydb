#pragma once

#include <ydb/core/util/hp_timer_helpers.h>
#include <ydb/core/base/blobstorage_common.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>

namespace NKikimr {

TString MakeData(ui32 dataSize);

template<typename Int1 = ui32, typename Int2 = ui32>
inline Int1 GenerateRandom(Int1 min, Int2 max) {
    return min + RandomNumber(max - min);
}

class TInflightActor : public TActorBootstrapped<TInflightActor> {
public:
    struct TSettings {
        ui32 Requests;
        ui32 MaxInFlight;
        TDuration Delay = TDuration::Zero();
        TGroupId GroupId = TGroupId::Zero();
        ui32 GroupGeneration = 1;
    };

public:
    TInflightActor(TSettings settings)
        : RequestsToSend(settings.Requests)
        , RequestInFlight(settings.MaxInFlight)
        , GroupId(settings.GroupId.GetRawId())
        , Settings(settings)
    {}

    virtual ~TInflightActor() = default;

    void SetGroupId(TGroupId groupId) {
        GroupId = groupId.GetRawId();
    }

    void Bootstrap(const TActorContext&/* ctx*/) {
        LastTs = TAppData::TimeProvider->Now();
        EstablishSession();
    }

protected:
    void ScheduleRequests() {
        while (RequestInFlight > 0 && RequestsToSend > 0) {
            TInstant now = TAppData::TimeProvider->Now();
            TDuration timePassed = now - LastTs;
            if (timePassed >= Settings.Delay) {
                LastTs = now;
                RequestInFlight--;
                RequestsToSend--;
                RequestsSent++;
                SendRequest();
                continue;
            } else if (!WakeupScheduled) {
                Schedule(Settings.Delay - timePassed, new TEvents::TEvWakeup);
                WakeupScheduled = true;
            }
            break;
        }
    }

    void WakeupAndSchedule() {
        WakeupScheduled = false;
        ScheduleRequests();
    }

    void HandleReply(NKikimrProto::EReplyStatus status) {
        ResponsesByStatus[status]++;
        ++RequestInFlight;
        ScheduleRequests();
    }

    virtual void EstablishSession() = 0;
    virtual void SendRequest() = 0;

protected:
    ui32 RequestsToSend;
    ui32 RequestInFlight;
    ui32 GroupId;
    TInstant LastTs;
    TSettings Settings;
    bool WakeupScheduled = false;

public:
    std::map<NKikimrProto::EReplyStatus, ui32> ResponsesByStatus;
    ui32 RequestsSent = 0;

protected:
    static ui64 Cookie;
};

/////////////////////////////////// TInflightActorPut ///////////////////////////////////

class TInflightActorPut : public TInflightActor {
public:
    TInflightActorPut(TSettings settings, ui32 dataSize = 1024)
        : TInflightActor(settings)
        , DataSize(dataSize)
    {}

    STRICT_STFUNC(StateWork,
        cFunc(TEvBlobStorage::TEvStatusResult::EventType, ScheduleRequests);
        cFunc(TEvents::TEvWakeup::EventType, WakeupAndSchedule);
        hFunc(TEvBlobStorage::TEvPutResult, Handle);
    )

    void EstablishSession() override {
        // dummy request to establish the session
        auto ev = new TEvBlobStorage::TEvStatus(TInstant::Max());
        SendToBSProxy(SelfId(), GroupId, ev, 0);
        Become(&TInflightActorPut::StateWork);
    }

protected:
    void SendRequest() override {
        TString data = MakeData(DataSize);
        auto ev = new TEvBlobStorage::TEvPut(TLogoBlobID(1, 1, 1, 10, DataSize, Cookie++),
                data, TInstant::Max(), NKikimrBlobStorage::UserData);
        SendToBSProxy(SelfId(), GroupId, ev, 0);
    }

    void Handle(TEvBlobStorage::TEvPutResult::TPtr res) {
        HandleReply(res->Get()->Status);
    }

private:
    std::string Data;
    ui32 DataSize;
};

/////////////////////////////////// TInflightActorGet ///////////////////////////////////

class TInflightActorGet : public TInflightActor {
public:
    TInflightActorGet(TSettings settings, ui32 dataSize = 1024)
        : TInflightActor(settings)
        , DataSize(dataSize)
    {}

    STRICT_STFUNC(StateWork,
        cFunc(TEvBlobStorage::TEvStatusResult::EventType, InitializeData);
        hFunc(TEvBlobStorage::TEvPutResult, Handle);
        cFunc(TEvents::TEvWakeup::EventType, WakeupAndSchedule);
        hFunc(TEvBlobStorage::TEvGetResult, Handle);
    )

    void EstablishSession() override {
        // dummy request to establish the session
        auto ev = new TEvBlobStorage::TEvStatus(TInstant::Max());
        SendToBSProxy(SelfId(), GroupId, ev, 0);
        Become(&TInflightActorGet::StateWork);
    }

    void InitializeData() {
        for (ui32 i = 0; i < Settings.Requests; ++i) {
            TString data = MakeData(DataSize);
            TLogoBlobID blobId = TLogoBlobID(1, 1, 1, 10, DataSize, Cookie++);
            auto ev = new TEvBlobStorage::TEvPut(blobId, data, TInstant::Max());
            SendToBSProxy(SelfId(), GroupId, ev, 0);
        }
    }

protected:
    void SendRequest() override {
        auto ev = new TEvBlobStorage::TEvGet(BlobIds[RequestsSent - 1], 0, DataSize, TInstant::Max(),
                NKikimrBlobStorage::EGetHandleClass::FastRead);
        SendToBSProxy(SelfId(), GroupId, ev, 0);
    }

    void Handle(TEvBlobStorage::TEvGetResult::TPtr res) {
        HandleReply(res->Get()->Status);
    }

    void Handle(TEvBlobStorage::TEvPutResult::TPtr res) {
        Y_VERIFY_S(res->Get()->Status == NKikimrProto::OK, res->Get()->ErrorReason);
        BlobIds.push_back(res->Get()->Id);
        if (++BlobsWritten == Settings.Requests) {
            ScheduleRequests();
        }
    }

private:
    std::string Data;
    ui32 DataSize;
    std::vector<TLogoBlobID> BlobIds;
    ui32 BlobsWritten = 0;
};

/////////////////////////////////// TInflightActorPatch ///////////////////////////////////

class TInflightActorPatch : public TInflightActor {
public:
    TInflightActorPatch(TSettings settings, ui32 dataSize = 1024)
        : TInflightActor(settings)
        , DataSize(dataSize)
    {}

    STRICT_STFUNC(StateWork,
        cFunc(TEvBlobStorage::TEvStatusResult::EventType, InitializeData);
        cFunc(TEvents::TEvWakeup::EventType, WakeupAndSchedule);
        hFunc(TEvBlobStorage::TEvPatchResult, Handle);
        hFunc(TEvBlobStorage::TEvPutResult, Handle);
    )

    void EstablishSession() override {
        // dummy request to establish the session
        auto ev = new TEvBlobStorage::TEvStatus(TInstant::Max());
        SendToBSProxy(SelfId(), GroupId, ev, 0);
        Become(&TInflightActorPatch::StateWork);
    }

    void InitializeData() {
        TString data = MakeData(DataSize);
        for (ui32 i = 0; i < RequestInFlight; ++i) {
            TLogoBlobID blobId(1, 1, 1, 10, DataSize, Cookie++);
            auto ev = new TEvBlobStorage::TEvPut(blobId, data, TInstant::Max());
            SendToBSProxy(SelfId(), GroupId, ev, 0);
        }
    }

protected:
    void SendRequest() override {
        TLogoBlobID oldId = Blobs.front();
        Blobs.pop_front();
        TLogoBlobID newId(1, 1, oldId.Step() + 1, 10, DataSize, oldId.Cookie());
        Y_ABORT_UNLESS(TEvBlobStorage::TEvPatch::GetBlobIdWithSamePlacement(oldId, &newId, BlobIdMask, GroupId, GroupId));
        TArrayHolder<TEvBlobStorage::TEvPatch::TDiff> diffs(new TEvBlobStorage::TEvPatch::TDiff[1]);
        char c = 'a' + RequestsToSend % 26;
        diffs[0].Set(TString(DataSize, c), 0);
        auto ev = new TEvBlobStorage::TEvPatch(GroupId, oldId, newId, BlobIdMask, std::move(diffs), 1, TInstant::Max());
        SendToBSProxy(SelfId(), GroupId, ev, 0);
    }


    void Handle(TEvBlobStorage::TEvPatchResult::TPtr res) {
        Blobs.push_back(res->Get()->Id);
        HandleReply(res->Get()->Status);
    }

    void Handle(TEvBlobStorage::TEvPutResult::TPtr res) {
        Y_VERIFY_S(res->Get()->Status == NKikimrProto::OK, res->Get()->ErrorReason);
        Blobs.push_back(res->Get()->Id);
        if (++BlobsWritten == RequestInFlight) {
            ScheduleRequests();
        }
    }

protected:
    std::deque<TLogoBlobID> Blobs;
    ui32 BlobIdMask = TLogoBlobID::MaxCookie & 0xfffff000;
    ui32 BlobsWritten = 0;
    std::string Data;
    ui32 DataSize;
};

} // namespace NKikimr
