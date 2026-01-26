#include "ddisk_actor.h"

#include <ydb/core/util/stlog.h>

namespace NKikimr::NDDisk {

    void TDDiskActor::Handle(TEvWrite::TPtr ev) {
        if (!CheckQuery(*ev)) {
            return;
        }

        const auto& record = ev->Get()->Record;
        const TQueryCredentials creds(record.GetCredentials());
        const TBlockSelector selector(record.GetSelector());
        const TWriteInstruction instr(record.GetInstruction());

        TChunkRef& chunkRef = ChunkRefs[creds.TabletId][selector.VChunkIndex];
        if (!chunkRef.PendingEventsForChunk.empty() || !chunkRef.ChunkIdx) {
            chunkRef.PendingEventsForChunk.emplace(ev.Release());
            if (!chunkRef.ChunkIdx) {
                IssueChunkAllocation(creds.TabletId, selector.VChunkIndex);
            }
            return;
        }

        TRope data;
        if (instr.PayloadId) {
            data = ev->Get()->GetPayload(*instr.PayloadId);
        }

        Y_ABORT_UNLESS(chunkRef.ChunkIdx);

        const ui64 cookie = NextCookie++;
        Send(BaseInfo.PDiskActorID, new NPDisk::TEvChunkWriteRaw(
            PDiskParams->Owner,
            PDiskParams->OwnerRound,
            chunkRef.ChunkIdx,
            selector.OffsetInBytes,
            std::move(data)), 0, cookie);

        WriteCallbacks.emplace(cookie, [this, sender = ev->Sender, cookie = ev->Cookie, session = ev->InterconnectSession](
                NPDisk::TEvChunkWriteRawResult& /*ev*/) {
            auto reply = std::make_unique<TEvWriteResult>(NKikimrBlobStorage::NDDisk::TReplyStatus::OK);
            auto h = std::make_unique<IEventHandle>(sender, SelfId(), reply.release(), 0, cookie);
            if (session) {
                h->Rewrite(TEvInterconnect::EvForward, session);
            }
            TActivationContext::Send(h.release());
        });
    }

	void TDDiskActor::Handle(NPDisk::TEvChunkWriteRawResult::TPtr ev) {
        auto& msg = *ev->Get();
        STLOG(PRI_DEBUG, BS_DDISK, BSDD07, "TDDiskActor::Handle(TEvChunkWriteRawResult)", (DDiskId, DDiskId), (Msg, msg.ToString()));
        
        if (msg.Status != NKikimrProto::OK) {
            Y_ABORT();
        }

        const auto it = WriteCallbacks.find(ev->Cookie);
        Y_ABORT_UNLESS(it != WriteCallbacks.end());
        it->second(msg);
        WriteCallbacks.erase(it);
    }

    void TDDiskActor::Handle(TEvRead::TPtr ev) {
        if (!CheckQuery(*ev)) {
            return;
        }

        const auto& record = ev->Get()->Record;
        const TQueryCredentials creds(record.GetCredentials());
        const TBlockSelector selector(record.GetSelector());

        TRope result;

        TChunkRef& chunkRef = ChunkRefs[creds.TabletId][selector.VChunkIndex];
        if (!chunkRef.PendingEventsForChunk.empty()) {
            chunkRef.PendingEventsForChunk.emplace(ev.Release());
        } else if (!chunkRef.ChunkIdx) {
            auto zero = TRcBuf::Uninitialized(selector.Size);
            memset(zero.GetDataMut(), 0, zero.size());
            result.Insert(result.End(), std::move(zero));
            SendReply(*ev, std::make_unique<TEvReadResult>(NKikimrBlobStorage::NDDisk::TReplyStatus::OK, std::nullopt,
                std::move(result)));
        } else {
            const ui64 cookie = NextCookie++;
            Send(BaseInfo.PDiskActorID, new NPDisk::TEvChunkReadRaw(
                PDiskParams->Owner,
                PDiskParams->OwnerRound,
                chunkRef.ChunkIdx,
                selector.OffsetInBytes,
                selector.Size), 0, cookie);

            ReadCallbacks.emplace(cookie, [this, sender = ev->Sender, cookie = ev->Cookie, session = ev->InterconnectSession](
                    NPDisk::TEvChunkReadRawResult& ev) {
                auto reply = std::make_unique<TEvReadResult>(NKikimrBlobStorage::NDDisk::TReplyStatus::OK, std::nullopt,
                    std::move(ev.Data));
                auto h = std::make_unique<IEventHandle>(sender, SelfId(), reply.release(), 0, cookie);
                if (session) {
                    h->Rewrite(TEvInterconnect::EvForward, session);
                }
                TActivationContext::Send(h.release());
            });
        }
    }

	void TDDiskActor::Handle(NPDisk::TEvChunkReadRawResult::TPtr ev) {
        auto& msg = *ev->Get();
        STLOG(PRI_DEBUG, BS_DDISK, BSDD08, "TDDiskActor::Handle(TEvChunkReadRawResult)", (DDiskId, DDiskId), (Msg, msg.ToString()));

        if (msg.Status != NKikimrProto::OK) {
            Y_ABORT();
        }

        const auto it = ReadCallbacks.find(ev->Cookie);
        Y_ABORT_UNLESS(it != ReadCallbacks.end());
        it->second(msg);
        ReadCallbacks.erase(it);
    }

} // NKikimr::NDDisk
