#include "agent_impl.h"

namespace NKikimr::NBlobDepot {

    struct TBlobDepotAgent::TReadContext : TRequestContext {
        TQuery *Query;
        const ui64 Tag;
        const ui64 Size;
        THashMap<std::tuple<TLogoBlobID, ui32, ui32>, TStackVec<ui64, 1>> ReadOffsets;
        TString Buffer;
        bool Terminated = false;

        TReadContext(TQuery *query, ui64 tag, ui64 size)
            : Query(query)
            , Tag(tag)
            , Size(size)
        {}

        void EndWithError(NKikimrProto::EReplyStatus status, TString errorReason) {
            Query->OnRead(Tag, status, errorReason);
            Terminated = true;
        }

        void EndWithSuccess() {
            Query->OnRead(Tag, NKikimrProto::OK, std::move(Buffer));
        }
    };

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

            auto blobSeqId = TBlobSeqId::FromProto(locator.GetBlobSeqId());

            if (vg) {
                const bool composite = totalDataLen + sizeof(TVirtualGroupBlobFooter) <= MaxBlobSize;
                const EBlobType type = composite ? EBlobType::VG_COMPOSITE_BLOB : EBlobType::VG_DATA_BLOB;
                const ui32 blobSize = totalDataLen + (composite ? sizeof(TVirtualGroupBlobFooter) : 0);
                const auto id = blobSeqId.MakeBlobId(TabletId, type, 0, blobSize);
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

        auto context = std::make_shared<TReadContext>(query, tag, outputOffset);
        for (const TReadItem& item : items) {
            auto key = std::make_tuple(item.Id, item.Offset, item.Size);
            auto& v = context->ReadOffsets[key];
            v.push_back(item.OutputOffset);
            if (v.size() == 1) {
                SendToProxy(item.GroupId, std::make_unique<TEvBlobStorage::TEvGet>(item.Id, item.Offset, item.Size,
                    TInstant::Max(), getHandleClass, mustRestoreFirst), query, context);
            }
        }

        return true;
    }

    void TBlobDepotAgent::HandleGetResult(const TRequestContext::TPtr& context, TEvBlobStorage::TEvGetResult& msg) {
        auto& readContext = context->Obtain<TReadContext>();
        if (readContext.Terminated) {
            return;
        }

        if (msg.Status != NKikimrProto::OK) {
            readContext.EndWithError(msg.Status, std::move(msg.ErrorReason));
        } else {
            for (ui32 i = 0; i < msg.ResponseSz; ++i) {
                auto& blob = msg.Responses[i];
                if (blob.Status != NKikimrProto::OK) {
                    return readContext.EndWithError(blob.Status, TStringBuilder() << "failed to read BlobId# " << blob.Id);
                }

                const auto it = readContext.ReadOffsets.find(std::make_tuple(blob.Id, blob.Shift, blob.RequestedSize));
                Y_VERIFY(it != readContext.ReadOffsets.end());
                auto v = std::move(it->second);
                readContext.ReadOffsets.erase(it);

                for (const ui64 offset : v) {
                    Y_VERIFY(offset + blob.Buffer.size() <= readContext.Size);
                    if (!readContext.Buffer && !offset) {
                        readContext.Buffer = std::move(blob.Buffer);
                        readContext.Buffer.resize(readContext.Size);
                    } else {
                        if (!readContext.Buffer) {
                            readContext.Buffer = TString::Uninitialized(readContext.Size);
                        }
                        memcpy(readContext.Buffer.Detach() + offset, blob.Buffer.data(), blob.Buffer.size());
                    }
                }

                if (readContext.ReadOffsets.empty()) {
                    readContext.EndWithSuccess();
                }
            }
        }
    }

} // NKikimr::NBlobDepot
