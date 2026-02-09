#include "ddisk_actor.h"

#include <ydb/core/util/stlog.h>

#include <ydb/core/util/pb.h>


namespace NKikimr::NDDisk {

    void TDDiskActor::Handle(TEvSync::TPtr ev) {
        if (!CheckQuery(*ev, &Counters.Interface.Sync)) {
            return;
        }

        const auto& record = ev->Get()->Record;
        TQueryCredentials creds(record.GetCredentials());
        TBlockSelector selector(record.GetSelector());
        const ui64 lsn = record.GetLsn();

        Counters.Interface.PullFromPersistentBuffer.Request();

        auto span = std::move(NWilson::TSpan(TWilson::DDiskTopLevel, std::move(ev->TraceId), "DDisk.PullFromPersistentBuffer",
                NWilson::EFlags::NONE, TActivationContext::ActorSystem())
            .Attribute("tablet_id", static_cast<long>(creds.TabletId))
            .Attribute("vchunk_index", static_cast<long>(selector.VChunkIndex))
            .Attribute("offset_in_bytes", selector.OffsetInBytes)
            .Attribute("size", selector.Size)
            .Attribute("lsn", static_cast<long>(lsn)));

        auto query = std::make_unique<TEvReadPersistentBuffer>(TQueryCredentials(creds.TabletId, creds.Generation,
            record.GetDDiskInstanceGuid(), true), selector, lsn, TReadInstruction(true));

        const ui64 cookie = NextPullingCookie++;
        const auto& ddiskId = record.GetDDiskId();
        Send(MakeBlobStorageDDiskId(ddiskId.GetNodeId(), ddiskId.GetPDiskId(), ddiskId.GetDDiskSlotId()),
            query.release(), IEventHandle::FlagTrackDelivery, cookie, span.GetTraceId());

        DropSegmentFromPulling(selector);

        PullingsInFlight.emplace(cookie, TPullingInFlight{ev->Sender, ev->Cookie, ev->InterconnectSession,
            std::move(span), selector.OffsetInBytes, selector.OffsetInBytes + selector.Size, {}, std::move(creds), std::move(selector)});
    }

    void TDDiskActor::Handle(TEvReadPersistentBufferResult::TPtr ev) {
        auto it = PullingsInFlight.find(ev->Cookie);
        if (it == PullingsInFlight.end()) {
            return;
        }

        const auto& record = ev->Get()->Record;
        if (record.GetStatus() != NKikimrBlobStorage::NDDisk::TReplyStatus::OK) {
            ReplyPullingResult(it, record.GetStatus(), record.GetErrorReason());
            return;
        }

        const TWriteInstruction instr(0);
        auto callback = [
            this,
            sender = ev->Sender,
            cookie = ev->Cookie,
            session = ev->InterconnectSession
        ] (NPDisk::TEvChunkWriteRawResult& evResult, NWilson::TSpan&& /*span*/) {
            if (auto it = PullingsInFlight.find(cookie); it != PullingsInFlight.end()) {
                if (evResult.Status != NKikimrProto::EReplyStatus::OK) {
                    ReplyPullingResult(it, NKikimrBlobStorage::NDDisk::TReplyStatus::ERROR, "Failed to write into device");
                    return;
                }
                ReplyPullingResult(it, NKikimrBlobStorage::NDDisk::TReplyStatus::OK);
            }
        };
        auto &pulling = it->second;
        InternalWrite(ev, pulling.Creds, pulling.Selector, instr, std::move(callback));
    }

    void TDDiskActor::ReplyPullingResult(TPullingInFlightIterator it, NKikimrBlobStorage::NDDisk::TReplyStatus::E status, TString errorReason) {
        TPullingInFlight& pif = it->second;
        auto ev = std::make_unique<TEvPullFromPersistentBufferResult>(status, errorReason);
        auto h = std::make_unique<IEventHandle>(pif.Sender, SelfId(), ev.release(), 0, pif.Cookie);
        if (pif.InterconnectionSessionId) {
            h->Rewrite(TEvInterconnect::EvForward, pif.InterconnectionSessionId);
        }
        pif.Span.End();
        if (auto *ptr = h->CastAsLocal<TEvPullFromPersistentBufferResult>()) {
            const bool success = ptr->Record.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::OK;
            Counters.Interface.PullFromPersistentBuffer.Reply(success, success ? pif.End - pif.Offset: 0);
        }
        TActivationContext::Send(h.release());
        PullingsInFlight.erase(it);
    }
}
