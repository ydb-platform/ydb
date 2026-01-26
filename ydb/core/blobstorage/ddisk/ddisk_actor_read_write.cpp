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

//        void *cookie = reinterpret_cast<void*>(NextCookie++);
//        Send(BaseInfo.PDiskActorId, new NPDisk::TEvChunkWrite(
//            PDiskParams->Owner,
//            PDiskParams->OwnerRound,
//            chunkRef.ChunkIdx,
//            selector.OffsetInBytes,
//            MakeIntrusive<NPDisk::TEvChunkWrite::TRopeAlignedParts>(std::move(data), data.size()),
//            cookie,
//            false /*doFlush*/,
//            NPriWrite::HullHugeUserData,
//            false /*isSeqWrite*/));
//        WriteCallbacks.emplace(cookie, [&](NPDisk::TEvChunkWriteResult& ev) {
//        });

        std::tuple<ui64, ui64, ui32> blockId{
            creds.TabletId,
            selector.VChunkIndex,
            selector.OffsetInBytes / BlockSize,
        };
        auto dataIter = data.Begin();
        for (size_t offs = 0; offs < selector.Size; offs += BlockSize, ++std::get<2>(blockId)) {
            TString block = TString::Uninitialized(BlockSize);
            dataIter.ExtractPlainDataAndAdvance(block.Detach(), block.size());

            // insert block into block map
            const auto [it, _] = BlockRefCount.try_emplace(std::move(block));
            ++it->second;

            // replace block data
            if (const TString *prev = std::exchange(Blocks[blockId], &it->first)) {
                const auto kt = BlockRefCount.find(*prev);
                Y_ABORT_UNLESS(kt != BlockRefCount.end());
                if (!--kt->second) {
                    BlockRefCount.erase(kt);
                }
            }
        }


        SendReply(*ev, std::make_unique<TEvWriteResult>(NKikimrBlobStorage::NDDisk::TReplyStatus::OK));
    }

	void TDDiskActor::Handle(NPDisk::TEvChunkWriteResult::TPtr ev) {
        auto& msg = *ev->Get();
        STLOG(PRI_DEBUG, BS_DDISK, BSDD07, "TDDiskActor::Handle(TEvChunkWriteResult)", (DDiskId, DDiskId), (Msg, msg.ToString()));
        
        if (msg.Status != NKikimrProto::OK) {
            Y_ABORT();
        }

        const auto it = WriteCallbacks.find(msg.Cookie);
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
            return;
        } else if (!chunkRef.ChunkIdx) {
            auto zero = TRcBuf::Uninitialized(selector.Size);
            memset(zero.GetDataMut(), 0, zero.size());
            result.Insert(result.End(), std::move(zero));
        } else {
            std::tuple<ui64, ui64, ui32> blockId{
                creds.TabletId,
                selector.VChunkIndex,
                selector.OffsetInBytes / BlockSize,
            };
            for (size_t offs = 0; offs < selector.Size; offs += BlockSize, ++std::get<2>(blockId)) {
                if (const auto it = Blocks.find(blockId); it != Blocks.end()) {
                    result.Insert(result.End(), TRope(*it->second));
                } else {
                    TString zero = TString::Uninitialized(BlockSize);
                    memset(zero.Detach(), 0, zero.size());
                    result.Insert(result.End(), TRope(std::move(zero)));
                }
            }
        }

        SendReply(*ev, std::make_unique<TEvReadResult>(NKikimrBlobStorage::NDDisk::TReplyStatus::OK, std::nullopt,
            std::move(result)));
    }

	void TDDiskActor::Handle(NPDisk::TEvChunkReadResult::TPtr ev) {
        auto& msg = *ev->Get();
        STLOG(PRI_DEBUG, BS_DDISK, BSDD08, "TDDiskActor::Handle(TEvChunkReadResult)", (DDiskId, DDiskId), (Msg, msg.ToString()));

        if (msg.Status != NKikimrProto::OK) {
            Y_ABORT();
        }

        const auto it = ReadCallbacks.find(msg.Cookie);
        Y_ABORT_UNLESS(it != ReadCallbacks.end());
        it->second(msg);
        ReadCallbacks.erase(it);
    }

} // NKikimr::NDDisk
