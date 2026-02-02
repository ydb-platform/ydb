#include "ddisk_actor.h"

#include <ydb/core/util/stlog.h>

#include <ydb/core/util/pb.h>

namespace NKikimr::NDDisk {

    void TDDiskActor::Handle(TEvPrivate::TEvHandlePersistentBufferEventForChunk::TPtr ev) {
        auto chunkIdx = ev->Get()->ChunkIndex;
        Y_ABORT_UNLESS(chunkIdx);
        ProcessPersistentBufferQueue();
    }

    void TDDiskActor::ProcessPersistentBufferQueue() {
        Y_ABORT_UNLESS(!PendingPersistentBufferEvents.empty());
        auto& temp = PendingPersistentBufferEvents.front().Ev;
        const auto& record = temp->Get<TEvWritePersistentBuffer>()->Record;
        const TQueryCredentials creds(record.GetCredentials());
        const TBlockSelector selector(record.GetSelector());
        const ui64 lsn = record.GetLsn();
        if (PersistentBufferOwnedChunks.empty()
            || PersistentBufferEmptyChunkOffset + SectorSize + selector.Size >= ChunkSize) {
            IssuePersistentBufferChunkAllocation();
            return;
        }
        TChunkIdx chunkIdx = PersistentBufferOwnedChunks.back();

        TRope data;
        TString buffer;
        buffer.resize(SectorSize);
        TPersistentBufferHeader *header = (TPersistentBufferHeader*)buffer.data();
        header->Signature[0] = TPersistentBufferHeader::PersistentBufferHeaderSignature[0];
        header->Signature[1] = TPersistentBufferHeader::PersistentBufferHeaderSignature[1];
        header->TabletId = creds.TabletId;
        header->VChunkIndex = selector.VChunkIndex;
        header->OffsetInBytes = selector.OffsetInBytes;
        header->Size = selector.Size;
        header->Lsn = lsn;
        data.Insert(data.End(), TRope(buffer));
        const TWriteInstruction instr(record.GetInstruction());
        if (instr.PayloadId) {
            data.Insert(data.End(), TRope(temp->Get<TEvWritePersistentBuffer>()->GetPayload(*instr.PayloadId)));
        }

        auto span = std::move(NWilson::TSpan(TWilson::DDiskTopLevel, std::move(temp->TraceId), "DDisk.WritePersistentBuffer",
                NWilson::EFlags::NONE, TActivationContext::ActorSystem())
            .Attribute("chunk_idx", chunkIdx)
            .Attribute("offset_in_bytes", PersistentBufferEmptyChunkOffset));

        Counters.Interface.WritePersistentBuffer.Request(selector.Size);
        const ui64 cookie = NextCookie++;
        Send(BaseInfo.PDiskActorID, new NPDisk::TEvChunkWriteRaw(
            PDiskParams->Owner,
            PDiskParams->OwnerRound,
            chunkIdx,
            PersistentBufferEmptyChunkOffset,
            std::move(data)), 0, cookie);

        WriteCallbacks.try_emplace(cookie, std::move(span), [this, sender = temp->Sender, cookie = temp->Cookie,
                tabletId = creds.TabletId, vchunkIndex = selector.VChunkIndex, offsetInBytes = selector.OffsetInBytes,
                size = selector.Size, data = std::move(temp->Get<TEvWritePersistentBuffer>()->GetPayload(*instr.PayloadId)), lsn = lsn,
                session = temp->InterconnectSession, temp = temp.release()](NPDisk::TEvChunkWriteRawResult& /*ev*/, NWilson::TSpan&& span) {
            Counters.Interface.WritePersistentBuffer.Reply(true);
            span.End();
            PersistentBufferEmptyChunkOffset += data.size() + SectorSize;
            auto& buffer = PersistentBuffers[{tabletId, vchunkIndex}];
            auto [it, inserted] = buffer.Records.try_emplace(lsn);
            TPersistentBuffer::TRecord& pr = it->second;
            if (inserted) {
                pr = {
                    .OffsetInBytes = offsetInBytes,
                    .Size = size,
                    .Data = std::move(data),
                };
            } else {
                Y_ABORT_UNLESS(pr.OffsetInBytes == offsetInBytes);
                Y_ABORT_UNLESS(pr.Size == size);
                Y_ABORT_UNLESS(pr.Data == data);
            }
            SendReply(*temp, std::make_unique<TEvWritePersistentBufferResult>(NKikimrBlobStorage::NDDisk::TReplyStatus::OK));
        });
        PendingPersistentBufferEvents.pop();
        if (!PendingPersistentBufferEvents.empty()) {
            ProcessPersistentBufferQueue();
        }
    }

    void TDDiskActor::Handle(TEvWritePersistentBuffer::TPtr ev) {
        if (!CheckQuery(*ev, &Counters.Interface.WritePersistentBuffer)) {
            return;
        }
        PendingPersistentBufferEvents.emplace(ev, "WaitingPersistentBufferWrite");
        ProcessPersistentBufferQueue();
    }

    void TDDiskActor::Handle(TEvReadPersistentBuffer::TPtr ev) {
        if (!CheckQuery(*ev, &Counters.Interface.ReadPersistentBuffer)) {
            return;
        }

        const auto& record = ev->Get()->Record;
        const TQueryCredentials creds(record.GetCredentials());
        const TBlockSelector selector(record.GetSelector());
        const ui64 lsn = record.GetLsn();

        Counters.Interface.ReadPersistentBuffer.Request();

        auto span = std::move(NWilson::TSpan(TWilson::DDiskTopLevel, std::move(ev->TraceId), "DDisk.ReadPersistentBuffer",
                NWilson::EFlags::NONE, TActivationContext::ActorSystem())
            .Attribute("tablet_id", static_cast<long>(creds.TabletId))
            .Attribute("vchunk_index", static_cast<long>(selector.VChunkIndex))
            .Attribute("offset_in_bytes", selector.OffsetInBytes)
            .Attribute("size", selector.Size)
            .Attribute("lsn", static_cast<long>(lsn)));

        const auto it = PersistentBuffers.find({creds.TabletId, selector.VChunkIndex});
        if (it == PersistentBuffers.end()) {
            Counters.Interface.ReadPersistentBuffer.Reply(false);
            span.End();
            SendReply(*ev, std::make_unique<TEvReadPersistentBufferResult>(
                NKikimrBlobStorage::NDDisk::TReplyStatus::MISSING_RECORD));
            return;
        }
        const TPersistentBuffer& buffer = it->second;

        const auto jt = buffer.Records.find(lsn);
        if (jt == buffer.Records.end()) {
            Counters.Interface.ReadPersistentBuffer.Reply(false);
            span.End();
            SendReply(*ev, std::make_unique<TEvReadPersistentBufferResult>(
                NKikimrBlobStorage::NDDisk::TReplyStatus::MISSING_RECORD));
            return;
        }
        const TPersistentBuffer::TRecord& pr = jt->second;
        Y_ABORT_UNLESS(pr.OffsetInBytes == selector.OffsetInBytes);
        Y_ABORT_UNLESS(pr.Size == selector.Size);

        Counters.Interface.ReadPersistentBuffer.Reply(true, selector.Size);
        span.End();
        SendReply(*ev, std::make_unique<TEvReadPersistentBufferResult>(NKikimrBlobStorage::NDDisk::TReplyStatus::OK,
            std::nullopt, pr.Data));
    }

    void TDDiskActor::Handle(TEvFlushPersistentBuffer::TPtr ev) {
        if (!CheckQuery(*ev, &Counters.Interface.FlushPersistentBuffer)) {
            return;
        }

        const auto& record = ev->Get()->Record;
        const TQueryCredentials creds(record.GetCredentials());
        const TBlockSelector selector(record.GetSelector());
        const ui64 lsn = record.GetLsn();

        Counters.Interface.FlushPersistentBuffer.Request();

        auto span = std::move(NWilson::TSpan(TWilson::DDiskTopLevel, std::move(ev->TraceId), "DDisk.FlushPersistentBuffer",
                NWilson::EFlags::NONE, TActivationContext::ActorSystem())
            .Attribute("tablet_id", static_cast<long>(creds.TabletId))
            .Attribute("vchunk_index", static_cast<long>(selector.VChunkIndex))
            .Attribute("offset_in_bytes", selector.OffsetInBytes)
            .Attribute("size", selector.Size)
            .Attribute("lsn", static_cast<long>(lsn)));

        const auto it = PersistentBuffers.find({creds.TabletId, selector.VChunkIndex});
        if (it == PersistentBuffers.end()) {
            Counters.Interface.FlushPersistentBuffer.Reply(false);
            span.End();
            SendReply(*ev, std::make_unique<TEvFlushPersistentBufferResult>(
                NKikimrBlobStorage::NDDisk::TReplyStatus::MISSING_RECORD));
            return;
        }
        TPersistentBuffer& buffer = it->second;

        const auto jt = buffer.Records.find(lsn);
        if (jt == buffer.Records.end()) {
            Counters.Interface.FlushPersistentBuffer.Reply(false);
            span.End();
            SendReply(*ev, std::make_unique<TEvFlushPersistentBufferResult>(
                NKikimrBlobStorage::NDDisk::TReplyStatus::MISSING_RECORD));
            return;
        }
        TPersistentBuffer::TRecord& pr = jt->second;
        TRope data = std::move(pr.Data);
        Y_ABORT_UNLESS(pr.OffsetInBytes == selector.OffsetInBytes);
        Y_ABORT_UNLESS(pr.Size == selector.Size);
        Y_DEBUG_ABORT_UNLESS(record.HasDDiskId());

        auto query = std::make_unique<TEvWrite>(TQueryCredentials(creds.TabletId, creds.Generation,
            record.GetDDiskInstanceGuid(), true), selector, TWriteInstruction(0));
        query->AddPayload(std::move(data));

        const ui64 cookie = NextWriteCookie++;
        const auto& ddiskId = record.GetDDiskId();
        Send(MakeBlobStorageDDiskId(ddiskId.GetNodeId(), ddiskId.GetPDiskId(), ddiskId.GetDDiskSlotId()),
            query.release(), IEventHandle::FlagTrackDelivery, cookie, span.GetTraceId());
        WritesInFlight.emplace(cookie, TWriteInFlight{ev->Sender, ev->Cookie, ev->InterconnectSession,
            std::move(span), selector.Size});
    }

    void TDDiskActor::Handle(TEvErasePersistentBuffer::TPtr ev) {
        if (!CheckQuery(*ev, &Counters.Interface.ErasePersistentBuffer)) {
            return;
        }

        const auto& record = ev->Get()->Record;
        const TQueryCredentials creds(record.GetCredentials());
        const TBlockSelector selector(record.GetSelector());
        const ui64 lsn = record.GetLsn();

        Counters.Interface.ErasePersistentBuffer.Request();

        auto span = std::move(NWilson::TSpan(TWilson::DDiskTopLevel, std::move(ev->TraceId), "DDisk.ErasePersistentBuffer",
                NWilson::EFlags::NONE, TActivationContext::ActorSystem())
            .Attribute("tablet_id", static_cast<long>(creds.TabletId))
            .Attribute("vchunk_index", static_cast<long>(selector.VChunkIndex))
            .Attribute("offset_in_bytes", selector.OffsetInBytes)
            .Attribute("size", selector.Size)
            .Attribute("lsn", static_cast<long>(lsn))
            .Attribute("erase", true));

        const auto it = PersistentBuffers.find({creds.TabletId, selector.VChunkIndex});
        if (it == PersistentBuffers.end()) {
            Counters.Interface.ErasePersistentBuffer.Reply(false);
            span.End();
            SendReply(*ev, std::make_unique<TEvErasePersistentBufferResult>(
                NKikimrBlobStorage::NDDisk::TReplyStatus::MISSING_RECORD));
            return;
        }
        TPersistentBuffer& buffer = it->second;

        const auto jt = buffer.Records.find(lsn);
        if (jt == buffer.Records.end()) {
            Counters.Interface.ErasePersistentBuffer.Reply(false);
            span.End();
            SendReply(*ev, std::make_unique<TEvErasePersistentBufferResult>(
                NKikimrBlobStorage::NDDisk::TReplyStatus::MISSING_RECORD));
            return;
        }
        TPersistentBuffer::TRecord& pr = jt->second;
        TRope data = std::move(pr.Data);
        Y_ABORT_UNLESS(pr.OffsetInBytes == selector.OffsetInBytes);
        Y_ABORT_UNLESS(pr.Size == selector.Size);

        buffer.Records.erase(jt);
        if (buffer.Records.empty()) {
            PersistentBuffers.erase(it);
        }
        Counters.Interface.ErasePersistentBuffer.Reply(true);
        span.End();
        SendReply(*ev, std::make_unique<TEvErasePersistentBufferResult>(NKikimrBlobStorage::NDDisk::TReplyStatus::OK));
    }

    void TDDiskActor::Handle(TEvWriteResult::TPtr ev) {
        HandleWriteInFlight(ev->Cookie, [&] {
            const auto& record = ev->Get()->Record;

            auto reply = std::make_unique<TEvFlushPersistentBufferResult>();
            auto& rr = reply->Record;

            rr.SetStatus(record.GetStatus());
            if (record.HasErrorReason()) {
                rr.SetErrorReason(record.GetErrorReason());
            }

            return reply;
        });
    }

    void TDDiskActor::Handle(TEvents::TEvUndelivered::TPtr ev) {
        if (ev->Get()->SourceType == TEv::EvWrite) {
            HandleWriteInFlight(ev->Cookie, [&] {
                return std::make_unique<TEvFlushPersistentBufferResult>(NKikimrBlobStorage::NDDisk::TReplyStatus::ERROR,
                    "write undelivered");
            });
        }
    }

    void TDDiskActor::HandleWriteInFlight(ui64 cookie, const std::function<std::unique_ptr<IEventBase>()>& factory) {
        if (const auto it = WritesInFlight.find(cookie); it != WritesInFlight.end()) {
            TWriteInFlight& wif = it->second;
            auto h = std::make_unique<IEventHandle>(wif.Sender, SelfId(), factory().release(), 0, wif.Cookie);
            if (wif.InterconnectionSessionId) {
                h->Rewrite(TEvInterconnect::EvForward, wif.InterconnectionSessionId);
            }
            wif.Span.End();
            if (auto *ptr = h->CastAsLocal<TEvFlushPersistentBufferResult>()) {
                const bool success = ptr->Record.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::OK;
                Counters.Interface.FlushPersistentBuffer.Reply(success, success ? wif.Size : 0);
            }
            TActivationContext::Send(h.release());
            WritesInFlight.erase(it);
        }
    }

    void TDDiskActor::Handle(TEvListPersistentBuffer::TPtr ev) {
        if (!CheckQuery(*ev, &Counters.Interface.ListPersistentBuffer)) {
            return;
        }

        const auto& record = ev->Get()->Record;
        const TQueryCredentials creds(record.GetCredentials());

        Counters.Interface.ListPersistentBuffer.Request();

        auto reply = std::make_unique<TEvListPersistentBufferResult>(NKikimrBlobStorage::NDDisk::TReplyStatus::OK);
        auto& rr = reply->Record;

        for (auto it = PersistentBuffers.lower_bound({creds.TabletId, 0}); it != PersistentBuffers.end() &&
                std::get<0>(it->first) == creds.TabletId; ++it) {
            const TPersistentBuffer& buffer = it->second;
            for (const auto& [lsn, pr] : buffer.Records) {
                auto *pb = rr.AddRecords();
                auto *sel = pb->MutableSelector();
                sel->SetVChunkIndex(std::get<1>(it->first));
                sel->SetOffsetInBytes(pr.OffsetInBytes);
                sel->SetSize(pr.Size);
                pb->SetLsn(lsn);
            }
        }

        Counters.Interface.ListPersistentBuffer.Reply(true, reply->CalculateSerializedSizeCached());
        SendReply(*ev, std::move(reply));
    }

} // NKikimr::NDDisk
