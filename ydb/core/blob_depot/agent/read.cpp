#include "agent_impl.h"

namespace NKikimr::NBlobDepot {

    struct TBlobDepotAgent::TReadContext : TRequestContext {
        TQuery *Query;
        const ui64 Tag;
        const ui64 Size;
        TString Buffer;
        bool Terminated = false;
        ui32 NumPartsPending = 0;

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

        struct TPartContext : TRequestContext {
            std::shared_ptr<TReadContext> Read;
            std::vector<ui64> Offsets;

            TPartContext(std::shared_ptr<TReadContext> read)
                : Read(std::move(read))
            {}
        };
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

        ui64 offset = arg.Offset;
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
                (Offset, arg.Offset), (Size, arg.Size), (Values, FormatList(arg.Values)));
            return false;
        }

        auto context = std::make_shared<TReadContext>(arg.Query, arg.Tag, outputOffset);
        if (!outputOffset) {
            context->EndWithSuccess();
            return true;
        }

        THashMap<ui32, std::vector<std::tuple<ui64 /*offset*/, TEvBlobStorage::TEvGet::TQuery>>> queriesPerGroup;
        for (const TReadItem& item : items) {
            TEvBlobStorage::TEvGet::TQuery query;
            query.Set(item.Id, item.Offset, item.Size);
            queriesPerGroup[item.GroupId].emplace_back(item.OutputOffset, query);
        }

        for (const auto& [groupId, queries] : queriesPerGroup) {
            const ui32 sz = queries.size();
            TArrayHolder<TEvBlobStorage::TEvGet::TQuery> q(new TEvBlobStorage::TEvGet::TQuery[sz]);
            auto partContext = std::make_shared<TReadContext::TPartContext>(context);
            for (ui32 i = 0; i < sz; ++i) {
                ui64 outputOffset;
                std::tie(outputOffset, q[i]) = queries[i];
                partContext->Offsets.push_back(outputOffset);
            }

            auto event = std::make_unique<TEvBlobStorage::TEvGet>(q, sz, TInstant::Max(), arg.GetHandleClass, arg.MustRestoreFirst);
            event->ReaderTabletData = arg.ReaderTabletData;
            SendToProxy(groupId, std::move(event), arg.Query, std::move(partContext));
            ++context->NumPartsPending;
        }

        Y_VERIFY(context->NumPartsPending);

        return true;
    }

    void TBlobDepotAgent::HandleGetResult(const TRequestContext::TPtr& context, TEvBlobStorage::TEvGetResult& msg) {
        auto& partContext = context->Obtain<TReadContext::TPartContext>();
        auto& readContext = *partContext.Read;
        if (readContext.Terminated) {
            return; // just ignore this read
        }

        if (msg.Status != NKikimrProto::OK) {
            readContext.EndWithError(msg.Status, std::move(msg.ErrorReason));
        } else {
            Y_VERIFY(msg.ResponseSz == partContext.Offsets.size());

            for (ui32 i = 0; i < msg.ResponseSz; ++i) {
                auto& blob = msg.Responses[i];
                if (blob.Status != NKikimrProto::OK) {
                    return readContext.EndWithError(blob.Status, TStringBuilder() << "failed to read BlobId# " << blob.Id);
                }

                auto& buffer = readContext.Buffer;
                const ui64 offset = partContext.Offsets[i];

                Y_VERIFY(offset < readContext.Size && blob.Buffer.size() <= readContext.Size - offset);

                if (!buffer && !offset) {
                    buffer = std::move(blob.Buffer);
                    buffer.resize(readContext.Size);
                } else {
                    if (!buffer) {
                        buffer = TString::Uninitialized(readContext.Size);
                    }
                    memcpy(buffer.Detach() + offset, blob.Buffer.data(), blob.Buffer.size());
                }
            }

            if (!--readContext.NumPartsPending) {
                readContext.EndWithSuccess();
            }
        }
    }

} // NKikimr::NBlobDepot
