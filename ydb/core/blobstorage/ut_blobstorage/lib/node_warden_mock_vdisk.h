#pragma once

#include "defs.h"
#include "node_warden_mock.h"

class TNodeWardenMockActor::TVDiskMockActor : public TActorBootstrapped<TVDiskMockActor> {
    TNodeWardenMockActor *NodeWardenMockActor;
    std::weak_ptr<TVDiskState> VDisk;

    enum class EState {
        INITIAL,
        READ_LOG,
        REPLICATION,
        READY,
    } State = EState::INITIAL;

public:
    TVDiskMockActor(TNodeWardenMockActor *nodeWardenMockActor, std::shared_ptr<TVDiskState> vdisk)
        : NodeWardenMockActor(nodeWardenMockActor)
        , VDisk(vdisk)
    {
        vdisk->Actor = this;
    }

    void Bootstrap() {
        if (const auto& vdisk = VDisk.lock()) {
            STLOG(PRI_INFO, BS_NODE, NWM06, "VDisk starting", (VDiskId, vdisk->VDiskId), (VSlotId, vdisk->VSlotId));
            if (vdisk->AllocatedSize) {
                StartReadLog();
            } else {
                StartInitial();
            }
        }
        Become(&TThis::StateFunc);
    }

    void Handle(TEvBlobStorage::TEvVStatus::TPtr ev) {
        auto& record = ev->Get()->Record;
        if (const auto& vdisk = VDisk.lock()) {
            auto response = std::make_unique<TEvBlobStorage::TEvVStatusResult>(NKikimrProto::ERROR, vdisk->GetVDiskId(),
                false, false, false, 0);
            auto& r = response->Record;
            if (VDiskIDFromVDiskID(record.GetVDiskID()) != vdisk->GetVDiskId()) { // RACE
                r.SetStatus(NKikimrProto::RACE);
            } else {
                r.SetStatus(NKikimrProto::OK);
                switch (State) {
                    case EState::INITIAL:
                    case EState::READ_LOG:
                        r.SetStatus(NKikimrProto::NOTREADY);
                        break;

                    case EState::REPLICATION:
                        r.SetJoinedGroup(true);
                        break;

                    case EState::READY:
                        r.SetJoinedGroup(true);
                        r.SetReplicated(true);
                        break;
                }
            }
            Send(ev->Sender, response.release(), 0, ev->Cookie);
        } else {
            Send(ev->Sender, new TEvBlobStorage::TEvVStatusResult(NKikimrProto::ERROR, record.GetVDiskID()), 0, ev->Cookie);
        }
    }

    void StartInitial() {
        Schedule(TDuration::Seconds(15), new TEvents::TEvWakeup);
        State = EState::INITIAL;
    }

    void StartReadLog() {
        Schedule(TDuration::MilliSeconds(100 + RandomNumber<ui64>(4900)), new TEvents::TEvWakeup);
        State = EState::READ_LOG;
    }

    void StartReplication() {
        State = EState::REPLICATION;
        if (const auto& vdisk = VDisk.lock()) {
            STLOG(PRI_INFO, BS_NODE, NWM09, "VDisk REPLICATING", (VDiskId, vdisk->VDiskId), (VSlotId, vdisk->VSlotId));
            vdisk->Status = NKikimrBlobStorage::EVDiskStatus::REPLICATING;
            InvokeOtherActor(*NodeWardenMockActor, &TNodeWardenMockActor::SendUpdateVDiskStatus, vdisk.get());
        }
        ResumeReplication();
    }

    void ResumeReplication() {
        if (const auto& vdisk = VDisk.lock()) {
            if (const ui64 quantum = Min(vdisk->TargetDataSize - vdisk->AllocatedSize, (400 << 20) + RandomNumber<ui64>(500 << 20))) {
                vdisk->AllocatedSize += quantum;
                UNIT_ASSERT(vdisk->PDisk->GetAllocatedSize() <= 85 * vdisk->PDisk->Size / 100);
                InvokeOtherActor(*NodeWardenMockActor, &TNodeWardenMockActor::SendUpdateVDiskStatus, vdisk.get());
                Schedule(TDuration::MilliSeconds(100 + RandomNumber<ui64>(100)), new TEvents::TEvWakeup);
            } else {
                BecomeReady();
            }
        }
    }

    void BecomeReady() {
        if (const auto& vdisk = VDisk.lock()) {
            STLOG(PRI_INFO, BS_NODE, NWM08, "VDisk READY", (VDiskId, vdisk->VDiskId), (VSlotId, vdisk->VSlotId));
            vdisk->Status = NKikimrBlobStorage::EVDiskStatus::READY;
            InvokeOtherActor(*NodeWardenMockActor, &TNodeWardenMockActor::SendUpdateVDiskStatus, vdisk.get());
            State = EState::READY;
        }
    }

    void HandleWakeup() {
        switch (State) {
            case EState::INITIAL:
                StartReadLog();
                break;

            case EState::READ_LOG:
                StartReplication();
                break;

            case EState::REPLICATION:
                ResumeReplication();
                break;

            case EState::READY:
                Y_ABORT();
        }
    }

    void PassAway() override {
        if (const auto& vdisk = VDisk.lock()) {
            STLOG(PRI_INFO, BS_NODE, NWM07, "VDisk stopping", (VDiskId, vdisk->VDiskId), (VSlotId, vdisk->VSlotId));
            UNIT_ASSERT_EQUAL(vdisk->Actor, this);
            vdisk->Actor = nullptr;
        }
        TActorBootstrapped::PassAway();
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvBlobStorage::TEvVStatus, Handle);
        cFunc(TEvents::TSystem::Poison, PassAway);
        cFunc(TEvents::TSystem::Wakeup, HandleWakeup);
    )
};
