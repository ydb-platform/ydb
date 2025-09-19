#pragma once

#include <ydb/core/util/hp_timer_helpers.h>
#include <ydb/core/base/blobstorage_common.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>

namespace NKikimr {

TString MakeData(ui32 dataSize, ui32 step = 1);

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
    TInflightActorPut(TSettings settings, ui32 dataSize = 1024, ui32 putsInBatch = 1)
        : TInflightActor(settings)
        , DataSize(dataSize)
        , PutsInBatch(putsInBatch)
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
        for (ui32 i = 0; i < PutsInBatch; ++i) {
            TString data = MakeData(DataSize);
            auto ev = new TEvBlobStorage::TEvPut(TLogoBlobID(1, 1, 1, 10, DataSize, Cookie++),
                    data, TInstant::Max(), NKikimrBlobStorage::TabletLog);
            SendToBSProxy(SelfId(), GroupId, ev, 0);
        }
    }

    void Handle(TEvBlobStorage::TEvPutResult::TPtr res) {
        HandleReply(res->Get()->Status);
    }

private:
    std::string Data;
    ui32 DataSize;
    ui32 PutsInBatch;
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


struct TTestCtxBase {
public:
    TTestCtxBase(TEnvironmentSetup::TSettings settings)
        : NodeCount(settings.NodeCount)
        , Erasure(settings.Erasure)
        , Env(new TEnvironmentSetup(std::move(settings)))
    {}

    virtual ~TTestCtxBase() = default;

    void CreateOneGroup() {
        Env->CreateBoxAndPool(1, 1);
        Env->Sim(TDuration::Minutes(1));

        FetchBaseConfig();

        UNIT_ASSERT_VALUES_EQUAL(BaseConfig.GroupSize(), 1);
        const auto& group = BaseConfig.GetGroup(0);
        GroupId = group.GetGroupId();
    }

    void AllocateEdgeActor(bool findNodeWithoutVDisks = false) {
        ui32 chosenNodeId = 0;
        if (!findNodeWithoutVDisks) {
            chosenNodeId = NodeCount;
        } else {
            std::set<ui32> nodesWOVDisks = GetComplementNodeSet(GetNodesWithVDisks());
            chosenNodeId = *nodesWOVDisks.begin();
        }

        Y_VERIFY_S(chosenNodeId != 0, "No available nodes to allocate");
        Edge = Env->Runtime->AllocateEdgeActor(chosenNodeId);
    }

    void AllocateEdgeActorOnSpecificNode(ui32 nodeId) {
        Edge = Env->Runtime->AllocateEdgeActor(nodeId);
    }

    void FetchBaseConfig() {
        BaseConfig = Env->FetchBaseConfig();
    }

    TAutoPtr<TEventHandle<TEvBlobStorage::TEvStatusResult>> GetGroupStatus(ui32 groupId,
            TDuration waitTime = TDuration::Max()) {
        TInstant ts = (waitTime == TDuration::Max()) ? TInstant::Max() : Env->Now() + waitTime;
        Env->Runtime->WrapInActorContext(Edge, [&] {
            SendToBSProxy(Edge, groupId, new TEvBlobStorage::TEvStatus(ts));
        });
        return Env->WaitForEdgeActorEvent<TEvBlobStorage::TEvStatusResult>(Edge, false, ts);
    }

    virtual void Initialize() {
        CreateOneGroup();
        AllocateEdgeActor();
        GetGroupStatus(GroupId);
    }

public:
    struct TDataProfile {
    public:
        enum class ECookieStrategy {
            SimpleIncrement = 0,
            WithSamePlacement,
        };

        enum class EContentType {
            Zeros = 0,
            RepetitivePattern,
        };

    public:
        ui32 GroupId;
        std::optional<ui64> TotalSize;
        std::optional<ui32> TotalBlobs;
        ui64 BlobSize;
        ui32 BatchSize = 1;
        EContentType ContentType = EContentType::Zeros;

        TDuration DelayBetweenBatches = TDuration::Zero();

        // must be specified when using ECookieStrategy::WithSamePlacement
        std::optional<TBlobStorageGroupType> Erasure = std::nullopt;

        ui64 TabletId = 123218421;
        ui32 Channel = 1;
        ui32 Generation = 1;
        ui32 Step = 1;
        ECookieStrategy CookieStrategy = ECookieStrategy::SimpleIncrement;

    public:
        ui64 NextCookie(ui64 prevCookie) const {
            switch (CookieStrategy) {
                case TDataProfile::ECookieStrategy::SimpleIncrement:
                    return ++prevCookie;
                case TDataProfile::ECookieStrategy::WithSamePlacement: {
                    ui64 originalHash = TLogoBlobID(TabletId, Generation, Step, Channel, BlobSize, prevCookie).Hash();
                    while (prevCookie < TLogoBlobID::MaxCookie) {
                        TLogoBlobID next(TabletId, Generation, Step, Channel, BlobSize, ++prevCookie);
                        if (next.Hash() % Erasure->BlobSubgroupSize() == originalHash % Erasure->BlobSubgroupSize()) {
                            return prevCookie;
                        }
                    }
                }
                default:
                    Y_FAIL();
            }
        }
    };

    std::vector<TLogoBlobID> WriteCompressedData(TDataProfile profile) {
        Y_VERIFY(profile.TotalSize || profile.TotalBlobs);
        std::vector<TLogoBlobID> blobs;

        static ui64 cookie = 0;
        std::vector<TLogoBlobID> batch;

        ui32 blobsToWrite;

        if (profile.TotalBlobs) {
            blobsToWrite = *profile.TotalBlobs;
        } else {
            blobsToWrite = (*profile.TotalSize / profile.BlobSize) + !!(*profile.TotalSize % profile.BlobSize);
        }

        for (ui64 i = 0; i < blobsToWrite; ++i) {
            cookie = profile.NextCookie(cookie);
            batch.emplace_back(profile.TabletId, profile.Generation, profile.Step, profile.Channel,
                    profile.BlobSize, cookie);

            Env->Runtime->WrapInActorContext(Edge, [&] {
                TString data;

                switch (profile.ContentType) {
                    case TDataProfile::EContentType::Zeros:
                        data = TString(profile.BlobSize, '\0');
                        break;
                    case TDataProfile::EContentType::RepetitivePattern:
                        data = MakeData(profile.BlobSize);
                        break;
                    default:
                        Y_FAIL();
                }

                SendToBSProxy(Edge, profile.GroupId, new TEvBlobStorage::TEvPut(batch.back(), data, TInstant::Max()),
                        NKikimrBlobStorage::TabletLog);
            });


            if (batch.size() == profile.BatchSize || i == blobsToWrite - 1) {
                for (ui32 i = 0; i < batch.size(); ++i) {
                    auto res = Env->WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(
                            Edge, false, TInstant::Max());
                    UNIT_ASSERT(res->Get()->Status == NKikimrProto::OK);
                }

                if (profile.DelayBetweenBatches != TDuration::Zero()) {
                    Env->Sim(profile.DelayBetweenBatches);
                }

                blobs.insert(blobs.end(), batch.begin(), batch.end());
                batch.clear();
            }
        }
        return blobs;
    }

    std::set<ui32> GetNodesWithVDisks() {
        std::set<ui32> res;
        for (const auto& vslot : BaseConfig.GetVSlot()) {
            res.insert(vslot.GetVSlotId().GetNodeId());
        }
        return res;
    }

    std::set<ui32> GetComplementNodeSet(std::set<ui32> nodes) {
        std::set<ui32> res;
        for (ui32 nodeId = 1; nodeId <= NodeCount; ++nodeId) {
            if (!nodes.contains(nodeId)) {
                res.insert(nodeId);
            }
        }
        return res;
    }

public:
    ui32 NodeCount;
    TBlobStorageGroupType Erasure;
    std::shared_ptr<TEnvironmentSetup> Env;

    NKikimrBlobStorage::TBaseConfig BaseConfig;
    ui32 GroupId;
    TActorId Edge;
};

} // namespace NKikimr
