#include "scrub_actor_impl.h"

namespace NKikimr {

    std::optional<TRcBuf> TScrubCoroImpl::Read(const TDiskPart& part) {
        Y_ABORT_UNLESS(part.ChunkIdx);
        Y_ABORT_UNLESS(part.Size);
        auto msg = std::make_unique<NPDisk::TEvChunkRead>(ScrubCtx->PDiskCtx->Dsk->Owner,
            ScrubCtx->PDiskCtx->Dsk->OwnerRound, part.ChunkIdx, part.Offset, part.Size, NPriRead::HullLow, nullptr);
        ScrubCtx->VCtx->CountScrubCost(*msg);
        Send(ScrubCtx->PDiskCtx->PDiskId, msg.release());
        CurrentState = TStringBuilder() << "reading data from " << part.ToString();
        auto res = WaitForPDiskEvent<NPDisk::TEvChunkReadResult>();
        if (ScrubCtx->VCtx->CostTracker) {
            ScrubCtx->VCtx->CostTracker->CountPDiskResponse();
        }
        auto *m = res->Get();
        Y_VERIFY_S(m->Status == NKikimrProto::OK || m->Status == NKikimrProto::CORRUPTED,
            "Status# " << NKikimrProto::EReplyStatus_Name(m->Status));
        return m->Status == NKikimrProto::OK && m->Data.IsReadable() ? std::make_optional(m->Data.ToString()) : std::nullopt;
    }

    bool TScrubCoroImpl::IsReadable(const TDiskPart& part) {
        return Read(part).has_value();
    }

    void TScrubCoroImpl::Write(const TDiskPart& part, TString data) {
        Y_ABORT_UNLESS(part.ChunkIdx);
        Y_ABORT_UNLESS(part.Size);
        size_t alignedSize = data.size();
        if (const size_t offset = alignedSize % ScrubCtx->PDiskCtx->Dsk->AppendBlockSize) {
            alignedSize += ScrubCtx->PDiskCtx->Dsk->AppendBlockSize - offset;
        }
        auto msg = std::make_unique<NPDisk::TEvChunkWrite>(
            ScrubCtx->PDiskCtx->Dsk->Owner,
            ScrubCtx->PDiskCtx->Dsk->OwnerRound,
            part.ChunkIdx,
            part.Offset,
            MakeIntrusive<NPDisk::TEvChunkWrite::TAlignedParts>(std::move(data), alignedSize),
            nullptr,
            true,
            NPriWrite::HullComp);
        ScrubCtx->VCtx->CountScrubCost(*msg);
        Send(ScrubCtx->PDiskCtx->PDiskId, msg.release());
        CurrentState = TStringBuilder() << "writing index to " << part.ToString();
        auto res = WaitForPDiskEvent<NPDisk::TEvChunkWriteResult>();
        if (ScrubCtx->VCtx->CostTracker) {
            ScrubCtx->VCtx->CostTracker->CountPDiskResponse();
        }
        Y_ABORT_UNLESS(res->Get()->Status == NKikimrProto::OK); // FIXME: good logic
    }

} // NKikimr
