#pragma once

#include "defs.h"

#include "events.h"

class TVDisk : TNonCopyable {
    TVDiskID VDiskId;
    const ui32 NodeId;
    const ui32 PDiskId;
    const ui32 VSlotId;
    const ui64 PDiskGuid;
    NKikimrBlobStorage::EVDiskStatus Status = NKikimrBlobStorage::INIT_PENDING;
    std::optional<NKikimrBlobStorage::EVDiskStatus> ReportedStatus;
    TInstant NextStatusChange = TInstant::Zero();

private:
    class TVDiskMockActor : public TActor<TVDiskMockActor> {
        TVDisk& Self;

    public:
        TVDiskMockActor(TVDisk& self)
            : TActor(&TThis::StateFunc)
            , Self(self)
        {
            Y_UNUSED(Self);
        }

        void Handle(TEvBlobStorage::TEvVStatus::TPtr ev) {
            Send(ev->Sender, new TEvBlobStorage::TEvVStatusResult(NKikimrProto::OK, Self.VDiskId,
                Self.Status >= NKikimrBlobStorage::REPLICATING,  Self.Status >= NKikimrBlobStorage::READY, false, 0));
        }

        STRICT_STFUNC(StateFunc, {
            cFunc(TEvents::TSystem::Poison, PassAway);
            hFunc(TEvBlobStorage::TEvVStatus, Handle);
        })
    };

public:
    TVDisk(const TVDiskID& vdiskId, ui32 nodeId, ui32 pdiskId, ui32 vslotId, ui64 pdiskGuid)
        : VDiskId(vdiskId)
        , NodeId(nodeId)
        , PDiskId(pdiskId)
        , VSlotId(vslotId)
        , PDiskGuid(pdiskGuid)
    {
        LOG_DEBUG(*TlsActivationContext, NKikimrServices::BS_NODE, "[%u] VDiskId# %s PDiskId# %u VSlotId# %u created",
            NodeId, VDiskId.ToString().data(), PDiskId, VSlotId);
        auto *actorSystem = TActivationContext::ActorSystem();
        actorSystem->RegisterLocalService(GetActorId(), actorSystem->Register(new TVDiskMockActor(*this)));
    }

    void StopActor() {
        TActivationContext::Send(new IEventHandle(TEvents::TSystem::Poison, 0, GetActorId(), {}, {}, 0));
        LOG_DEBUG(*TlsActivationContext, NKikimrServices::BS_NODE, "[%u] VDiskId# %s destroyed", NodeId, VDiskId.ToString().data());
    }

    void UpdateVDiskId(const TVDiskID& newVDiskId) {
        if (VDiskId != newVDiskId) {
            UNIT_ASSERT_VALUES_EQUAL(VDiskId.GroupID, newVDiskId.GroupID);
            UNIT_ASSERT_VALUES_EQUAL(VDiskId.FailRealm, newVDiskId.FailRealm);
            UNIT_ASSERT_VALUES_EQUAL(VDiskId.FailDomain, newVDiskId.FailDomain);
            UNIT_ASSERT_VALUES_EQUAL(VDiskId.VDisk, newVDiskId.VDisk);
            LOG_DEBUG(*TlsActivationContext, NKikimrServices::BS_NODE, "[%u] VDiskId# %s -> %s",
                NodeId, VDiskId.ToString().data(), newVDiskId.ToString().data());
            VDiskId = newVDiskId;
        }
    }

    TActorId GetActorId() const {
        return MakeBlobStorageVDiskID(NodeId, PDiskId, VSlotId);
    }

    void Serialize(NKikimrBlobStorage::TVDiskStatus *pb) const {
        VDiskIDFromVDiskID(VDiskId, pb->MutableVDiskId());
        pb->SetNodeId(NodeId);
        pb->SetPDiskId(PDiskId);
        pb->SetVSlotId(VSlotId);
        pb->SetPDiskGuid(PDiskGuid);
        pb->SetStatus(Status);
    }

    TInstant HandleStatusChange(const TInstant now) {
        if (now >= NextStatusChange) {
            const TDuration duration = OnStatusChange();
            LOG_DEBUG(*TlsActivationContext, NKikimrServices::BS_NODE, "[%u] VDiskId# %s status changed to %s",
                NodeId, VDiskId.ToString().data(), EVDiskStatus_Name(Status).data());
            NextStatusChange = duration != TDuration::Max() ? now + duration : TInstant::Max();
        }
        return NextStatusChange;
    }

    TDuration OnStatusChange() {
        switch (Status) {
            case NKikimrBlobStorage::INIT_PENDING:
                if (NextStatusChange == TInstant::Zero()) {
                    return TDuration::MilliSeconds(1000 + RandomNumber(5000u));
                } else {
                    Status = NKikimrBlobStorage::REPLICATING;
                    return TDuration::MilliSeconds(5000 + RandomNumber(30000u));
                }

            case NKikimrBlobStorage::REPLICATING:
                Status = NKikimrBlobStorage::READY;

            case NKikimrBlobStorage::READY:
                return TDuration::Max();

            case NKikimrBlobStorage::ERROR:
                break;
        }

        Y_ABORT();
    }

    void Report(NKikimrBlobStorage::TEvControllerUpdateDiskStatus *pb) {
        if (std::exchange(ReportedStatus, Status) != Status) {
            Serialize(pb->AddVDiskStatus());
        }
    }
};
