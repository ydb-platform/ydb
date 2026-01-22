#include "ddisk_actor.h"

#include <ydb/core/util/stlog.h>

namespace NKikimr::NDDisk {

    void TDDiskActor::Handle(TEvDDiskWrite::TPtr ev) {
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

        SendReply(*ev, std::make_unique<TEvDDiskWriteResult>(NKikimrBlobStorage::TDDiskReplyStatus::OK));
    }

    void TDDiskActor::Handle(TEvDDiskRead::TPtr ev) {
        if (!CheckQuery(*ev)) {
            return;
        }

        const auto& record = ev->Get()->Record;
        const TQueryCredentials creds(record.GetCredentials());
        const TBlockSelector selector(record.GetSelector());

        TRope result;

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

        SendReply(*ev, std::make_unique<TEvDDiskReadResult>(NKikimrBlobStorage::TDDiskReplyStatus::OK, std::nullopt,
            std::move(result)));
    }

} // NKikimr::NDDisk
