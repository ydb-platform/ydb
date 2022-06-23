#include "agent_impl.h"

namespace NKikimr::NBlobDepot {

    bool TBlobDepotAgent::IssueRead(const NProtoBuf::RepeatedPtrField<NKikimrBlobDepot::TValueChain>& values, ui64 offset,
            ui64 size, NKikimrBlobStorage::EGetHandleClass getHandleClass, bool mustRestoreFirst, TQuery *query,
            ui64 tag, bool vg, TString *error) {
        ui64 outputOffset = 0;

        struct TReadItem {
            ui32 GroupId;
            TLogoBlobID Id;
            ui32 Offset;
            ui32 Size;
            ui64 OutputOffset;
        };
        std::vector<TReadItem> items;

        for (const auto& value : values) {
            if (!value.HasLocator()) {
                *error = "TValueChain.Locator is missing";
                return false;
            }
            const auto& locator = value.GetLocator();
            const ui64 totalDataLen = locator.GetTotalDataLen();
            if (!totalDataLen) {
                *error = "TBlobLocator.TotalDataLen is missing or zero";
                return false;
            }
            const ui64 begin = value.GetSubrangeBegin();
            const ui64 end = value.HasSubrangeEnd() ? value.GetSubrangeEnd() : totalDataLen;
            if (end <= begin || totalDataLen < end) {
                *error = "incorrect SubrangeBegin/SubrangeEnd pair";
                return false;
            }

            const ui64 partLen = end - begin;
            if (offset >= partLen) {
                // just skip this part
                offset -= partLen;
                continue;
            }

            const ui64 partSize = Min(size ? size : Max<ui64>(), partLen - offset);

            auto cgsi = TCGSI::FromProto(locator.GetBlobSeqId());

            if (vg) {
                const bool composite = totalDataLen + sizeof(TVirtualGroupBlobFooter) <= MaxBlobSize;
                const EBlobType type = composite ? EBlobType::VG_COMPOSITE_BLOB : EBlobType::VG_DATA_BLOB;
                const ui32 blobSize = totalDataLen + (composite ? sizeof(TVirtualGroupBlobFooter) : 0);
                const auto id = cgsi.MakeBlobId(TabletId, type, 0, blobSize);
                items.push_back(TReadItem{locator.GetGroupId(), id, static_cast<ui32>(offset + begin),
                    static_cast<ui32>(partSize), outputOffset});
            } else {
                Y_FAIL();
            }

            if (size) {
                size -= partSize;
                if (!size) {
                    break;
                }
            }
            offset = 0;
            outputOffset += partSize;
        }

        if (size) {
            *error = "incorrect offset/size provided";
            return false;
        }

        auto& reads = query->Reads;
        auto iter = reads.insert(reads.end(), TQuery::TReadContext{
            .Query = query,
            .Tag = tag,
            .Size = outputOffset,
        });

        for (const TReadItem& item : items) {
            const ui64 id = NextReadId++;
            SendToBSProxy(SelfId(), item.GroupId, new TEvBlobStorage::TEvGet(item.Id, item.Offset, item.Size,
                TInstant::Max(), getHandleClass, mustRestoreFirst), id);
            ReadsInFlight.emplace(id, iter);
            iter->ReadsToOffset.emplace(id, item.OutputOffset);
        }

        return true;
    }

    void TBlobDepotAgent::Handle(TEvBlobStorage::TEvGetResult::TPtr ev) {
        if (const auto it = ReadsInFlight.find(ev->Cookie); it != ReadsInFlight.end()) {
            auto ctx = it->second;
            ReadsInFlight.erase(it);

            const auto offsetIt = ctx->ReadsToOffset.find(ev->Cookie);
            Y_VERIFY(offsetIt != ctx->ReadsToOffset.end());
            const ui64 offset = offsetIt->second;
            ctx->ReadsToOffset.erase(offsetIt);

            auto& msg = *ev->Get();
            if (msg.Status != NKikimrProto::OK) {
                ctx->Status = msg.Status;
                ctx->Buffer = std::move(msg.ErrorReason);
            } else if (ctx->Status == NKikimrProto::OK) {
                Y_VERIFY(msg.ResponseSz == 1);
                auto& blob = msg.Responses[1];
                Y_VERIFY(offset + blob.Buffer.size() <= ctx->Size);
                if (!ctx->Buffer && !offset) {
                    ctx->Buffer = std::move(blob.Buffer);
                    ctx->Buffer.resize(ctx->Size);
                } else {
                    if (!ctx->Buffer) {
                        ctx->Buffer = TString::Uninitialized(ctx->Size);
                    }
                    memcpy(ctx->Buffer.Detach() + offset, blob.Buffer.data(), blob.Buffer.size());
                }
            }

            if (ctx->ReadsToOffset.empty()) {
                ctx->Query->OnRead(ctx->Tag, ctx->Status, std::move(ctx->Buffer));
                ctx->Query->Reads.erase(ctx);
            }
        }
    }

} // NKikimr::NBlobDepot
