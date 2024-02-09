#pragma once

#include <ydb/core/util/hp_timer_helpers.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>

namespace NKikimr {

TString MakeData(ui32 dataSize);

class TInflightActor : public TActorBootstrapped<TInflightActor> {
public:
    struct TSettings {
        ui32 Requests;
        ui32 MaxInFlight;
        TDuration Delay = TDuration::Zero();
        ui32 GroupId = 0;
        ui32 GroupGeneration = 1;
    };

public:
    TInflightActor(TSettings settings)
        : RequestsToSend(settings.Requests)
        , RequestInFlight(settings.MaxInFlight)
        , GroupId(settings.GroupId)
        , Settings(settings)
    {}

    virtual ~TInflightActor() = default;

    void SetGroupId(ui32 groupId) {
        GroupId = groupId;
    }

    void Bootstrap(const TActorContext &ctx) {
        LastTs = TAppData::TimeProvider->Now();
        BootstrapImpl(ctx);
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
            } else if (!WakeupScheduled) {
                Schedule(Settings.Delay - timePassed, new TEvents::TEvWakeup);
                WakeupScheduled = true;
                break;
            }
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

    virtual void BootstrapImpl(const TActorContext &ctx) = 0;
    virtual void SendRequest() = 0;

protected:
    ui32 RequestsToSend;
    ui32 RequestInFlight;
    ui32 GroupId;
    TInstant LastTs;
    TSettings Settings;
    bool WakeupScheduled = false;

public:
    std::unordered_map<NKikimrProto::EReplyStatus, ui32> ResponsesByStatus;
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

    virtual void BootstrapImpl(const TActorContext&/* ctx*/) override {
        // dummy request to establish the session
        auto ev = new TEvBlobStorage::TEvStatus(TInstant::Max());
        SendToBSProxy(SelfId(), GroupId, ev, 0);
        Become(&TInflightActorPut::StateWork);
    }

protected:
    virtual void SendRequest() override {
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
        cFunc(TEvBlobStorage::TEvPutResult::EventType, ScheduleRequests);
        cFunc(TEvents::TEvWakeup::EventType, WakeupAndSchedule);
        hFunc(TEvBlobStorage::TEvGetResult, Handle);
    )

    virtual void BootstrapImpl(const TActorContext&/* ctx*/) override {
        TString data = MakeData(DataSize);
        BlobId = TLogoBlobID(1, 1, 1, 10, DataSize, Cookie++);
        auto ev = new TEvBlobStorage::TEvPut(BlobId, data, TInstant::Max());
        SendToBSProxy(SelfId(), GroupId, ev, 0);
        Become(&TInflightActorGet::StateWork);
    }

protected:
    virtual void SendRequest() override {
        auto ev = new TEvBlobStorage::TEvGet(BlobId, 0, 10, TInstant::Max(), NKikimrBlobStorage::EGetHandleClass::FastRead);
        SendToBSProxy(SelfId(), GroupId, ev, 0);
    }

    void Handle(TEvBlobStorage::TEvGetResult::TPtr res) {
        HandleReply(res->Get()->Status);
    }

private:
    TLogoBlobID BlobId;
    std::string Data;
    ui32 DataSize;
};

/////////////////////////////////// TInflightActorPatch ///////////////////////////////////

class TInflightActorPatch : public TInflightActor {
public:
    TInflightActorPatch(TSettings settings, ui32 dataSize = 1024)
        : TInflightActor(settings)
        , DataSize(dataSize)
    {}

    STRICT_STFUNC(StateWork,
        cFunc(TEvents::TEvWakeup::EventType, WakeupAndSchedule);
        hFunc(TEvBlobStorage::TEvPatchResult, Handle);
        hFunc(TEvBlobStorage::TEvPutResult, Handle);
    )

    virtual void BootstrapImpl(const TActorContext&/* ctx*/) override {
        TString data = MakeData(DataSize);
        for (ui32 i = 0; i < RequestInFlight; ++i) {
            TLogoBlobID blobId(1, 1, 1, 10, DataSize, Cookie++);
            auto ev = new TEvBlobStorage::TEvPut(blobId, data, TInstant::Max());
            SendToBSProxy(SelfId(), GroupId, ev, 0);
        }
        Become(&TInflightActorPatch::StateWork);
    }

protected:
    virtual void SendRequest() override {
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
