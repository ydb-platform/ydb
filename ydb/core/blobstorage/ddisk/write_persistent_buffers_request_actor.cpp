#include "write_persistent_buffers_request_actor.h"

#include <ydb/core/util/pb.h>

namespace NKikimr::NDDisk {

    TWritePersistentBuffersRequestActor::TWritePersistentBuffersRequestActor()
        : TActor(&TThis::StateFunc)
        {}

    void TWritePersistentBuffersRequestActor::Reply() {
        auto msg = std::make_unique<TEvWritePersistentBuffersResult>();
        for (auto& inflight : Inflights) {
            if (!inflight.Replied && inflight.Received) {
                inflight.Replied = true;
                auto* res = msg->Record.AddResult();
                auto* pbId = res->MutablePersistentBufferId();
                pbId->SetNodeId(inflight.NodeId);
                pbId->SetPDiskId(inflight.PDiskId);
                pbId->SetDDiskSlotId(inflight.DDiskSlotId);
                auto* res2 = res->MutableResult();
                res2->SetStatus(inflight.Status);
                res2->SetErrorReason(inflight.ErrorReason);
                res2->SetFreeSpace(inflight.FreeSpace);
            }
        }

        Send(Sender, msg.release(), 0, Cookie);
    }

    void TWritePersistentBuffersRequestActor::ReplyAndDie() {
        Reply();
        PassAway();
    }

    void TWritePersistentBuffersRequestActor::Timeout() {
        if (Received == Inflights.size()) {
            ReplyAndDie();
        } else {
            Reply();
        }
    }

    void TWritePersistentBuffersRequestActor::Handle(TEvInterconnect::TEvNodeDisconnected::TPtr ev) {
        const ui32 node = ev->Get()->NodeId;
        for (auto& inflight : Inflights) {
            if (!inflight.Received && inflight.NodeId == node) {
                inflight.Status = NKikimrBlobStorage::NDDisk::TReplyStatus::ERROR;
                inflight.ErrorReason = TStringBuilder() << "Node " << node << " disconnected";
                inflight.Received = true;
                Received++;
            }
        }
        CheckReply();
    }

    void TWritePersistentBuffersRequestActor::CheckReply() {
        if (Received == Inflights.size()) {
            ReplyAndDie();
        }
    }

    void TWritePersistentBuffersRequestActor::Handle(TEvWritePersistentBufferResult::TPtr ev) {
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

    void TWritePersistentBuffersRequestActor::Handle(TEvWritePersistentBuffers::TPtr ev) {
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
            auto h = std::make_unique<IEventHandle>(pbServiceId, SelfId(), msg.release(), IEventHandle::FlagSubscribeOnSession, Inflights.size());
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

    void TWritePersistentBuffersRequestActor::PassAway() {
        for (auto& inflight : Inflights) {
            if (inflight.NodeId != SelfId().NodeId()) {
                Send(TActivationContext::InterconnectProxy(inflight.NodeId), new TEvents::TEvUnsubscribe());
            }
        }
        TActor::PassAway();
    }

    STFUNC(TWritePersistentBuffersRequestActor::StateFunc) {
        STRICT_STFUNC_BODY(
            hFunc(TEvWritePersistentBufferResult, Handle)
            hFunc(TEvWritePersistentBuffers, Handle)
            cFunc(TEvents::TSystem::Wakeup, Timeout);
            hFunc(TEvInterconnect::TEvNodeDisconnected, Handle);

            cFunc(TEvents::TSystem::Poison, PassAway)
        )
    }

} // NKikimr::NDDisk
