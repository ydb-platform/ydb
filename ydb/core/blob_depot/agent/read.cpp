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

    bool TBlobDepotAgent::IssueRead(const TReadArg& arg, TString& error) {
        ui64 outputOffset = 0;

        struct TReadItem {
            ui32 GroupId;
            TLogoBlobID Id;
            ui32 Offset;
            ui32 Size;
            ui64 OutputOffset;
        };
        std::vector<TReadItem> items;

        const ui64 offsetOnEntry = arg.Offset;
        ui64 offset = arg.Offset;
        const ui64 sizeOnEntry = arg.Size;
        ui64 size = arg.Size;

        for (const auto& value : arg.Values) {
            const ui32 groupId = value.GetGroupId();
            const auto blobId = LogoBlobIDFromLogoBlobID(value.GetBlobId());
            const ui64 begin = value.GetSubrangeBegin();
            const ui64 end = value.HasSubrangeEnd() ? value.GetSubrangeEnd() : blobId.BlobSize();

            if (end <= begin || blobId.BlobSize() < end) {
                error = "incorrect SubrangeBegin/SubrangeEnd pair";
                STLOG(PRI_CRIT, BLOB_DEPOT_AGENT, BDA24, error, (VirtualGroupId, VirtualGroupId), (TabletId, TabletId),
                    (Values, FormatList(arg.Values)));
                return false;
            }

            // calculate the whole length of current part
            ui64 partLen = end - begin;
            if (offset >= partLen) {
                // just skip this part
                offset -= partLen;
                continue;
            }

            // adjust it to fit size and offset
            partLen = Min(size ? size : Max<ui64>(), partLen - offset);
            Y_VERIFY(partLen);

            items.push_back(TReadItem{groupId, blobId, ui32(offset + begin), ui32(partLen), outputOffset});

            outputOffset += partLen;
            offset = 0;

            if (size) {
                size -= partLen;
                if (!size) {
                    break;
                }
            }
        }

        if (size) {
            error = "incorrect offset/size provided";
            STLOG(PRI_ERROR, BLOB_DEPOT_AGENT, BDA25, error, (VirtualGroupId, VirtualGroupId), (TabletId, TabletId),
                (Offset, offsetOnEntry), (Size, sizeOnEntry), (Values, FormatList(arg.Values)));
            return false;
        }

        auto context = std::make_shared<TReadContext>(arg.Query, arg.Tag, outputOffset);
        for (const TReadItem& item : items) {
            auto key = std::make_tuple(item.Id, item.Offset, item.Size);
            auto& v = context->ReadOffsets[key];
            v.push_back(item.OutputOffset);
            if (v.size() == 1) {
                auto event = std::make_unique<TEvBlobStorage::TEvGet>(
                    item.Id,
                    item.Offset,
                    item.Size,
                    TInstant::Max(),
                    arg.GetHandleClass,
                    arg.MustRestoreFirst);
                event->ReaderTabletData = arg.ReaderTabletData;
                SendToProxy(item.GroupId, std::move(event), arg.Query, context);
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
                    Y_VERIFY_S(offset + blob.Buffer.size() <= readContext.Size, "offset# " << offset << " Buffer.size# "
                        << blob.Buffer.size() << " Size# " << readContext.Size);
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
