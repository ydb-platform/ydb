#include "write_persistent_buffers_request_actor.h"

#include <ydb/core/util/pb.h>

namespace NKikimr::NDDisk {

    TWritePersistentBuffersRequest::TWritePersistentBuffersRequest()
        : TActor(&TThis::StateFunc)
        {}

    void TWritePersistentBuffersRequest::Reply() {
        auto msg = std::make_unique<TEvWritePersistentBuffersResult>();
        for (auto& inflight : Inflights) {
            if (!inflight.Replied && inflight.Received) {
                inflight.Replied = true;
                auto* res = msg->Record.AddResult();
                auto* pbId = res->MutablePersistentBufferId();
                pbId->SetNodeId(inflight.NodeId);
                pbId->SetPDiskId(inflight.PDiskId);
                pbId->SetDDiskSlotId(inflight.SlotId);
                auto* res2 = res->MutableResult();
                res2->SetStatus(inflight.Status);
                res2->SetErrorReason(inflight.ErrorReason);
                res2->SetFreeSpace(inflight.FreeSpace);
            }
        }

        Send(Sender, msg.release(), 0, Cookie);
    }

    void TWritePersistentBuffersRequest::ReplyAndDie() {
        Reply();
        PassAway();
    }

    void TWritePersistentBuffersRequest::Timeout() {
        if (Received == Inflights.size()) {
            ReplyAndDie();
        } else {
            Reply();
        }
    }

    void TWritePersistentBuffersRequest::Handle(TEvents::TEvUndelivered::TPtr ev) {
        auto sourceType = ev->Get()->SourceType;
        if (sourceType == TEv::EvWritePersistentBuffer) {
            auto cookie = ev->Cookie;
            Y_ABORT_UNLESS(cookie < Inflights.size());
            auto& inflight = Inflights[cookie];
            if (!inflight.Received) {
                inflight.Status = NKikimrBlobStorage::NDDisk::TReplyStatus::UNDELIVERED;
                inflight.Received = true;
                Received++;
            }
        }
        CheckReply();
    }

    void TWritePersistentBuffersRequest::Handle(TEvInterconnect::TEvNodeDisconnected::TPtr ev) {
        const ui32 node = ev->Get()->NodeId;
        for (auto& inflight : Inflights) {
            if (!inflight.Received && inflight.NodeId == node) {
                inflight.Status = NKikimrBlobStorage::NDDisk::TReplyStatus::DISCONNECTED;
                inflight.Received = true;
                Received++;
            }
        }
        CheckReply();
    }

    void TWritePersistentBuffersRequest::CheckReply() {
        if (Received == Inflights.size()) {
            ReplyAndDie();
        }
    }

    void TWritePersistentBuffersRequest::Handle(TEvWritePersistentBufferResult::TPtr ev) {
        auto cookie = ev->Cookie;
        Y_ABORT_UNLESS(cookie < Inflights.size());
        auto& inflight = Inflights[cookie];
        inflight.Status = ev->Get()->Record.GetStatus();
        inflight.ErrorReason = ev->Get()->Record.GetErrorReason();
        inflight.FreeSpace = ev->Get()->Record.GetFreeSpace();
        if (!inflight.Received) {
            inflight.Received = true;
            Received++;
        }
        CheckReply();
    }

    void TWritePersistentBuffersRequest::Handle(TEvWritePersistentBuffers::TPtr ev) {
        Sender = ev->Sender;
        Cookie = ev->Cookie;
        const auto& record = ev->Get()->Record;
        TQueryCredentials creds;
        auto recordCreds = record.GetCredentials();
        creds.TabletId = recordCreds.GetTabletId();
        creds.Generation = recordCreds.GetGeneration();
        creds.FromPersistentBuffer = true;
        const TBlockSelector selector(record.GetSelector());
        const ui64 lsn = record.GetLsn();
        const TWriteInstruction instr(record.GetInstruction());
        TRope payload;
        if (instr.PayloadId) {
            payload = ev->Get()->GetPayload(*instr.PayloadId);
        }

        for (auto& pbId : record.GetPersistentBufferIds()) {
            auto msg = std::make_unique<TEvWritePersistentBuffer>(creds, selector, lsn, NDDisk::TWriteInstruction(0));
            msg->AddPayload(TRope(payload));
            auto pbServiceId = MakeBlobStorageDDiskId(pbId.GetNodeId(), pbId.GetPDiskId(), pbId.GetDDiskSlotId());
            auto h = std::make_unique<IEventHandle>(pbServiceId, SelfId(), msg.release(), 0, Inflights.size());
            TActivationContext::Send(h.release());
            Inflights.emplace_back(TPersistentBufferInflight{
                pbId.GetNodeId(),
                pbId.GetPDiskId(),
                pbId.GetDDiskSlotId(),
                false,
                false,
                NKikimrBlobStorage::NDDisk::TReplyStatus::UNKNOWN,
                "",
                -1,
            });
        }
        Schedule(TDuration::MicroSeconds(record.GetReplyTimeoutMicroseconds()), new TEvents::TEvWakeup());
    }

    STFUNC(TWritePersistentBuffersRequest::StateFunc) {
        STRICT_STFUNC_BODY(
            hFunc(TEvWritePersistentBufferResult, Handle)
            hFunc(TEvWritePersistentBuffers, Handle)
            hFunc(TEvents::TEvUndelivered, Handle)
            cFunc(TEvents::TSystem::Wakeup, Timeout);
            hFunc(TEvInterconnect::TEvNodeDisconnected, Handle);

            cFunc(TEvents::TSystem::Poison, PassAway)
        )
    }

} // NKikimr::NDDisk
