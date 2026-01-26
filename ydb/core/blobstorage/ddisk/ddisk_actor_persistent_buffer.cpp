#include "ddisk_actor.h"

#include <ydb/core/util/stlog.h>

#include <ydb/core/util/pb.h>

namespace NKikimr::NDDisk {

    void TDDiskActor::Handle(TEvDDiskWritePersistentBuffer::TPtr ev) {
        if (!CheckQuery(*ev)) {
            return;
        }

        const auto& record = ev->Get()->Record;
        const TQueryCredentials creds(record.GetCredentials());
        const TBlockSelector selector(record.GetSelector());

        const TWriteInstruction instr(record.GetInstruction());
        TRope data;
        if (instr.PayloadId) {
            data = ev->Get()->GetPayload(*instr.PayloadId);
        }

        auto& buffer = PersistentBuffers[{creds.TabletId, selector.VChunkIndex}];
        auto [it, inserted] = buffer.Records.try_emplace(record.GetLsn());
        TPersistentBuffer::TRecord& pr = it->second;
        if (inserted) {
            pr = {
                .OffsetInBytes = selector.OffsetInBytes,
                .Size = selector.Size,
                .Data = std::move(data),
            };
        } else {
            Y_ABORT_UNLESS(pr.OffsetInBytes == selector.OffsetInBytes);
            Y_ABORT_UNLESS(pr.Size == selector.Size);
            Y_ABORT_UNLESS(pr.Data == data);
        }

        SendReply(*ev, std::make_unique<TEvDDiskWritePersistentBufferResult>(NKikimrBlobStorage::TDDiskReplyStatus::OK));
    }

    void TDDiskActor::Handle(TEvDDiskReadPersistentBuffer::TPtr ev) {
        if (!CheckQuery(*ev)) {
            return;
        }

        const auto& record = ev->Get()->Record;
        const TQueryCredentials creds(record.GetCredentials());
        const TBlockSelector selector(record.GetSelector());

        const auto it = PersistentBuffers.find({creds.TabletId, selector.VChunkIndex});
        if (it == PersistentBuffers.end()) {
            SendReply(*ev, std::make_unique<TEvDDiskReadPersistentBufferResult>(
                NKikimrBlobStorage::TDDiskReplyStatus::MISSING_RECORD));
            return;
        }
        const TPersistentBuffer& buffer = it->second;

        const auto jt = buffer.Records.find(record.GetLsn());
        if (jt == buffer.Records.end()) {
            SendReply(*ev, std::make_unique<TEvDDiskReadPersistentBufferResult>(
                NKikimrBlobStorage::TDDiskReplyStatus::MISSING_RECORD));
            return;
        }
        const TPersistentBuffer::TRecord& pr = jt->second;
        Y_ABORT_UNLESS(pr.OffsetInBytes == selector.OffsetInBytes);
        Y_ABORT_UNLESS(pr.Size == selector.Size);

        SendReply(*ev, std::make_unique<TEvDDiskReadPersistentBufferResult>(NKikimrBlobStorage::TDDiskReplyStatus::OK,
            std::nullopt, pr.Data));
    }

    void TDDiskActor::Handle(TEvDDiskFlushPersistentBuffer::TPtr ev) {
        if (!CheckQuery(*ev)) {
            return;
        }

        const auto& record = ev->Get()->Record;
        const TQueryCredentials creds(record.GetCredentials());
        const TBlockSelector selector(record.GetSelector());

        const auto it = PersistentBuffers.find({creds.TabletId, selector.VChunkIndex});
        if (it == PersistentBuffers.end()) {
            SendReply(*ev, std::make_unique<TEvDDiskFlushPersistentBufferResult>(
                NKikimrBlobStorage::TDDiskReplyStatus::MISSING_RECORD));
            return;
        }
        TPersistentBuffer& buffer = it->second;

        const auto jt = buffer.Records.find(record.GetLsn());
        if (jt == buffer.Records.end()) {
            SendReply(*ev, std::make_unique<TEvDDiskFlushPersistentBufferResult>(
                NKikimrBlobStorage::TDDiskReplyStatus::MISSING_RECORD));
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

        if (record.HasDDiskId()) {
            Y_DEBUG_ABORT_UNLESS(record.HasDDiskInstanceGuid());

            auto query = std::make_unique<TEvDDiskWrite>(TQueryCredentials(creds.TabletId, creds.Generation,
                record.GetDDiskInstanceGuid(), true), selector, TWriteInstruction(0));
            query->AddPayload(std::move(data));

            const ui64 cookie = NextWriteCookie++;
            WritesInFlight.emplace(cookie, TWriteInFlight{ev->Sender, ev->Cookie, ev->InterconnectSession});
            const auto& ddiskId = record.GetDDiskId();
            Send(MakeBlobStorageDDiskId(ddiskId.GetNodeId(), ddiskId.GetPDiskId(), ddiskId.GetDDiskSlotId()),
                query.release(), IEventHandle::FlagTrackDelivery, cookie);
        } else {
            SendReply(*ev, std::make_unique<TEvDDiskFlushPersistentBufferResult>(NKikimrBlobStorage::TDDiskReplyStatus::OK));
        }
    }

    void TDDiskActor::Handle(TEvDDiskWriteResult::TPtr ev) {
        HandleWriteInFlight(ev->Cookie, [&] {
            const auto& record = ev->Get()->Record;

            auto reply = std::make_unique<TEvDDiskFlushPersistentBufferResult>();
            auto& rr = reply->Record;

            rr.SetStatus(record.GetStatus());
            if (record.HasErrorReason()) {
                rr.SetErrorReason(record.GetErrorReason());
            }

            return reply;
        });
    }

    void TDDiskActor::Handle(TEvents::TEvUndelivered::TPtr ev) {
        if (ev->Get()->SourceType == TEv::EvDDiskWrite) {
            HandleWriteInFlight(ev->Cookie, [&] {
                return std::make_unique<TEvDDiskFlushPersistentBufferResult>(NKikimrBlobStorage::TDDiskReplyStatus::ERROR,
                    "write undelivered");
            });
        }
    }

    void TDDiskActor::Handle(TEvDDiskListPersistentBuffer::TPtr ev) {
        if (!CheckQuery(*ev)) {
            return;
        }

        const auto& record = ev->Get()->Record;
        const TQueryCredentials creds(record.GetCredentials());

        auto reply = std::make_unique<TEvDDiskListPersistentBufferResult>(NKikimrBlobStorage::TDDiskReplyStatus::OK);
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

        SendReply(*ev, std::move(reply));
    }

    void TDDiskActor::HandleWriteInFlight(ui64 cookie, const std::function<std::unique_ptr<IEventBase>()>& factory) {
        if (const auto it = WritesInFlight.find(cookie); it != WritesInFlight.end()) {
            TWriteInFlight wif = it->second;
            WritesInFlight.erase(it);
            auto h = std::make_unique<IEventHandle>(wif.Sender, SelfId(), factory().release(), 0, wif.Cookie);
            if (wif.InterconnectionSessionId) {
                h->Rewrite(TEvInterconnect::EvForward, wif.InterconnectionSessionId);
            }
            TActivationContext::Send(h.release());
        }
    }

} // NKikimr::NDDisk
