#include "ddisk_actor.h"

#include <ydb/core/util/stlog.h>

namespace NKikimr::NDDisk {

    void TDDiskActor::Handle(TEvDDiskWrite::TPtr ev) {
        const auto& record = ev->Get()->Record;

        const TQueryCredentials creds(record.GetCredentials());
        if (!ValidateConnection(*ev, creds)) {
            SendReply(*ev, std::make_unique<TEvDDiskWriteResult>(NKikimrBlobStorage::TDDiskReplyStatus::SESSION_MISMATCH));
            return;
        }

        const TBlockSelector selector(record.GetSelector());
        if (selector.OffsetInBytes % BlockSize || selector.Size % BlockSize) {
            SendReply(*ev, std::make_unique<TEvDDiskWriteResult>(NKikimrBlobStorage::TDDiskReplyStatus::INCORRECT_REQUEST,
                "offset and must must be multiple of block size"));
            return;
        }

        const TWriteInstruction instr(record.GetInstruction());
        TRope data;
        if (instr.PayloadId) {
            data = ev->Get()->GetPayload(*instr.PayloadId);
        }
        if (data.size() != selector.Size) {
            SendReply(*ev, std::make_unique<TEvDDiskWriteResult>(NKikimrBlobStorage::TDDiskReplyStatus::INCORRECT_REQUEST,
                "declared size in selector doesn't match actually provided data"));
            return;
        }

        std::tuple<ui64, ui64, ui32> blockId{
            creds.TabletId,
            selector.VChunkIndex,
            selector.OffsetInBytes / BlockSize,
        };
        auto dataIter = data.Begin();
        for (size_t offs = 0; offs < selector.Size; offs += BlockSize) {
            TString block = TString::Uninitialized(BlockSize);
            dataIter.ExtractPlainDataAndAdvance(block.Detach(), block.size());

            // insert block into block map
            const auto [it, _] = BlockRefCount.try_emplace(std::move(block));
            ++it->second;

            // replace block data
            if (const auto [jt, inserted] = Blocks.try_emplace(blockId, &it->first); !inserted) {
                const auto kt = BlockRefCount.find(*std::exchange(jt->second, &it->first));
                Y_ABORT_UNLESS(kt != BlockRefCount.end());
                if (!--kt->second) {
                    BlockRefCount.erase(kt);
                }
            }

            ++std::get<2>(blockId);
        }

        SendReply(*ev, std::make_unique<TEvDDiskWriteResult>(NKikimrBlobStorage::TDDiskReplyStatus::OK));
    }

    void TDDiskActor::Handle(TEvDDiskRead::TPtr ev) {
        const auto& record = ev->Get()->Record;

        const TQueryCredentials creds(record.GetCredentials());
        if (!ValidateConnection(*ev, creds)) {
            SendReply(*ev, std::make_unique<TEvDDiskReadResult>(NKikimrBlobStorage::TDDiskReplyStatus::SESSION_MISMATCH));
            return;
        }

        const TBlockSelector selector(record.GetSelector());
        if (selector.OffsetInBytes % BlockSize || selector.Size % BlockSize) {
            SendReply(*ev, std::make_unique<TEvDDiskReadResult>(NKikimrBlobStorage::TDDiskReplyStatus::INCORRECT_REQUEST,
                "offset and must must be multiple of block size"));
            return;
        }

        TRope result;

        std::tuple<ui64, ui64, ui32> blockId{
            creds.TabletId,
            selector.VChunkIndex,
            selector.OffsetInBytes / BlockSize,
        };
        for (size_t offs = 0; offs < selector.Size; offs += BlockSize) {
            if (const auto it = Blocks.find(blockId); it != Blocks.end()) {
                result.Insert(result.End(), TRope(*it->second));
            } else {
                TString zero = TString::Uninitialized(BlockSize);
                memset(zero.Detach(), 0, zero.size());
                result.Insert(result.End(), TRope(std::move(zero)));
            }
            ++std::get<2>(blockId);
        }

        SendReply(*ev, std::make_unique<TEvDDiskReadResult>(NKikimrBlobStorage::TDDiskReplyStatus::OK, std::nullopt,
            std::move(result)));
    }

} // NKikimr::NDDisk
