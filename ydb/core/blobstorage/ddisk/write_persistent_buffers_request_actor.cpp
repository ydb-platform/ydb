#include "write_persistent_buffers_request_actor.h"

#include <ydb/core/util/pb.h>

namespace NKikimr::NDDisk {

    TWritePersistentBuffersRequestActor::TWritePersistentBuffersRequestActor(TActorId parentId)
        : TActor(&TThis::StateFunc)
        , ParentId(parentId)
        {}

    void TWritePersistentBuffersRequestActor::Reply(ui64 cookie) {
        auto itInflight = Inflights.find(cookie);
        Y_ABORT_UNLESS(itInflight != Inflights.end());
        auto& i = itInflight->second;
        auto msg = std::make_unique<TEvWritePersistentBuffersResult>();
        for (auto& [_, inflight] : i.Inflights) {
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
                res2->SetPDiskNormalizedOccupancy(inflight.PDiskNormalizedOccupancy);
            }
        }

        Send(i.Sender, msg.release(), 0, i.Cookie);
    }

    void TWritePersistentBuffersRequestActor::ReplyAndFinish(ui64 cookie) {
        Reply(cookie);
        Inflights[cookie].Span.End();
        auto cnt = Inflights.erase(cookie);
        Y_ABORT_UNLESS(cnt == 1);
    }

    void TWritePersistentBuffersRequestActor::Timeout(TEvents::TEvWakeup::TPtr &ev) {
        ui64 cookie = ev->Get()->Tag;
        auto itInflight = Inflights.find(cookie);
        if (itInflight == Inflights.end()) {
            return;
        }
        auto& inflight = itInflight->second;

        if (inflight.Received == inflight.Inflights.size()) {
            ReplyAndFinish(cookie);
        } else {
            Reply(cookie);
        }
    }

    void TWritePersistentBuffersRequestActor::Handle(TEvInterconnect::TEvNodeDisconnected::TPtr ev) {
        const ui32 node = ev->Get()->NodeId;
        for (auto it = Inflights.begin(); it != Inflights.end(); ) {
            auto current = it++;
            for (auto& [partCookie, inflight] : current->second.Inflights) {
                if (!inflight.Received && inflight.NodeId == node) {
                    inflight.Status = NKikimrBlobStorage::NDDisk::TReplyStatus::ERROR;
                    inflight.ErrorReason = TStringBuilder() << "Node " << node << " disconnected";
                    inflight.Received = true;
                    current->second.Received++;
                    auto cnt = InflightParts.erase(partCookie);
                    Y_ABORT_UNLESS(cnt == 1);
                }
            }
            CheckReply(current->first);
        }
    }

    void TWritePersistentBuffersRequestActor::CheckReply(ui64 cookie) {
        auto itInflight = Inflights.find(cookie);
        Y_ABORT_UNLESS(itInflight != Inflights.end());
        auto& inflight = itInflight->second;
        if (inflight.Received == inflight.Inflights.size()) {
            ReplyAndFinish(cookie);
        }
    }

    void TWritePersistentBuffersRequestActor::Handle(TEvWritePersistentBufferResult::TPtr ev) {
        auto partCookie = ev->Cookie;
        auto itCookie = InflightParts.find(partCookie);
        Y_ABORT_UNLESS(itCookie != InflightParts.end());
        auto cookie = itCookie->second;
        InflightParts.erase(partCookie);
        auto itInflight = Inflights.find(cookie);
        Y_ABORT_UNLESS(itInflight != Inflights.end());
        auto& i = itInflight->second;

        auto itInflight2 = i.Inflights.find(partCookie);
        Y_ABORT_UNLESS(itInflight2 != i.Inflights.end());
        auto& inflight = itInflight2->second;
        inflight.Status = ev->Get()->Record.GetStatus();
        inflight.ErrorReason = ev->Get()->Record.GetErrorReason();
        inflight.FreeSpace = ev->Get()->Record.GetFreeSpace();
        inflight.PDiskNormalizedOccupancy = ev->Get()->Record.GetPDiskNormalizedOccupancy();
        if (!inflight.Received) {
            inflight.Received = true;
            i.Received++;
        }
        CheckReply(cookie);
    }

    void TWritePersistentBuffersRequestActor::Handle(TEvReadPersistentBufferResult::TPtr ev) {
        auto cookie = ev->Cookie;
        auto it = ReadInflights.find(cookie);
        Y_ABORT_UNLESS(it != ReadInflights.end());
        auto& inflight = it->second;
        auto& record = ev->Get()->Record;

        if (record.GetStatus() != NKikimrBlobStorage::NDDisk::TReplyStatus::OK) {
            auto msg = std::make_unique<TEvWritePersistentBuffersResult>();
            for (auto& pb : inflight.PersistentBufferIds) {
                auto* res = msg->Record.AddResult();
                auto* pbId = res->MutablePersistentBufferId();
                pbId->SetNodeId(std::get<0>(pb));
                pbId->SetPDiskId(std::get<1>(pb));
                pbId->SetDDiskSlotId(std::get<2>(pb));
                auto* res2 = res->MutableResult();
                res2->SetStatus(record.GetStatus());
                res2->SetErrorReason(record.GetErrorReason());
                res2->SetFreeSpace(-1);
                res2->SetPDiskNormalizedOccupancy(-1);
            }
            Send(inflight.Sender, msg.release(), 0, inflight.Cookie);

            ReadInflights.erase(it);
            return;
        }

        const auto payloadId = record.GetReadResult().GetPayloadId();

        TRope payload = ev->Get()->GetPayload(payloadId);

        NDDisk::TQueryCredentials creds{inflight.TabletId, inflight.TabletGeneration, true};
        const NDDisk::TBlockSelector selector{record.GetVChunkIndex(), record.GetOffsetInBytes(), record.GetSizeInBytes()};

        auto msg = std::make_unique<TEvWritePersistentBuffers>(creds, selector, inflight.Lsn, NDDisk::TWriteInstruction(0),
            inflight.PersistentBufferIds, inflight.Timeout);
        msg->AddPayload(TRope(payload));
        auto h = std::make_unique<IEventHandle>(SelfId(), inflight.Sender, msg.release(), 0, inflight.Cookie);
        TActivationContext::Send(h.release());

        ReadInflights.erase(it);
    }

    void TWritePersistentBuffersRequestActor::Handle(TEvReadThenWritePersistentBuffers::TPtr ev) {
        auto cookie = NextCookie++;
        const auto& record = ev->Get()->Record;
        TQueryCredentials creds;
        auto recordCreds = record.GetCredentials();
        creds.TabletId = recordCreds.GetTabletId();
        creds.Generation = recordCreds.GetGeneration();
        creds.FromPersistentBuffer = true;
        auto requestGeneration = record.GetGeneration();
        auto lsn = record.GetLsn();
        auto timeout = record.GetReplyTimeoutMicroseconds();

        auto [it, inserted] = ReadInflights.try_emplace(cookie, TReadInflight{
            .Sender = ev->Sender,
            .Cookie = ev->Cookie,
            .TabletId = creds.TabletId,
            .TabletGeneration = creds.Generation,
            .RequestGeneration = requestGeneration,
            .Lsn = lsn,
            .Timeout = timeout,
        });
        Y_ABORT_UNLESS(inserted);
        for (auto& pbId : record.GetPersistentBufferIds()) {
            it->second.PersistentBufferIds.emplace_back(pbId.GetNodeId(), pbId.GetPDiskId(), pbId.GetDDiskSlotId());
        }

        auto msg = std::make_unique<TEvReadPersistentBuffer>();
        creds.Serialize(msg->Record.MutableCredentials());
        msg->Record.SetLsn(lsn);
        msg->Record.SetGeneration(requestGeneration);
        NDDisk::TReadInstruction(true).Serialize(msg->Record.MutableInstruction());

        auto h = std::make_unique<IEventHandle>(ParentId, SelfId(), msg.release(), 0, cookie);
        TActivationContext::Send(h.release());
    }

    void TWritePersistentBuffersRequestActor::Handle(TEvWritePersistentBuffers::TPtr ev) {
        auto cookie = NextCookie++;
        auto span = std::move(NWilson::TSpan(TWilson::DDiskTopLevel, std::move(ev->TraceId), "DDisk.WritePersistentBuffers",
            NWilson::EFlags::NONE, TActivationContext::ActorSystem()));
        auto [it, inserted] = Inflights.try_emplace(cookie, TInflight{
            .Sender = ev->Sender,
            .Cookie = ev->Cookie,
            .Span = std::move(span),
        });

        Y_ABORT_UNLESS(inserted);
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
            auto partCookie = NextCookie++;
            auto msg = std::make_unique<TEvWritePersistentBuffer>(creds, selector, lsn, NDDisk::TWriteInstruction(0));
            msg->AddPayload(TRope(payload));
            auto pbServiceId = MakeBlobStoragePersistentBufferId(pbId.GetNodeId(), pbId.GetPDiskId(), pbId.GetDDiskSlotId());
            auto h = std::make_unique<IEventHandle>(pbServiceId, SelfId(), msg.release(), IEventHandle::FlagSubscribeOnSession, partCookie);
            TActivationContext::Send(h.release());
            auto [_, inserted2] = InflightParts.try_emplace(partCookie, cookie);
            Y_ABORT_UNLESS(inserted2);

            auto [__, inserted3] = it->second.Inflights.try_emplace(partCookie, TInflight::TPersistentBufferInflight{
                pbId.GetNodeId(),
                pbId.GetPDiskId(),
                pbId.GetDDiskSlotId(),
                false,
                false,
                NKikimrBlobStorage::NDDisk::TReplyStatus::UNKNOWN,
                "",
                -1,
                -1,
            });
            Y_ABORT_UNLESS(inserted3);
        }
        Schedule(TDuration::MicroSeconds(record.GetReplyTimeoutMicroseconds()), new TEvents::TEvWakeup(cookie));
    }

    void TWritePersistentBuffersRequestActor::PassAway() {
        for (auto& [_, i] : Inflights) {
            for (auto& [__, inflight] : i.Inflights) {
                if (inflight.NodeId != SelfId().NodeId()) {
                    Send(TActivationContext::InterconnectProxy(inflight.NodeId), new TEvents::TEvUnsubscribe());
                }
            }
        }
        TActor::PassAway();
    }

    STFUNC(TWritePersistentBuffersRequestActor::StateFunc) {
        STRICT_STFUNC_BODY(
            hFunc(TEvReadPersistentBufferResult, Handle)
            hFunc(TEvWritePersistentBufferResult, Handle)
            hFunc(TEvWritePersistentBuffers, Handle)
            hFunc(TEvReadThenWritePersistentBuffers, Handle)
            hFunc(TEvents::TEvWakeup, Timeout)
            hFunc(TEvInterconnect::TEvNodeDisconnected, Handle)

            cFunc(TEvents::TSystem::Poison, PassAway)
            IgnoreFunc(TEvInterconnect::TEvNodeConnected)
        )
    }

} // NKikimr::NDDisk
