#include "agent_impl.h"

namespace NKikimr::NBlobDepot {

    struct TBlobDepotAgent::TQuery::TReadContext
        : TRequestContext
        , std::enable_shared_from_this<TReadContext>
    {
        TReadArg ReadArg;
        const ui64 Size;
        TFragmentedBuffer Buffer;
        bool Terminated = false;
        bool StopProcessingParts = false;
        ui32 NumPartsPending = 0;
        TLogoBlobID BlobWithoutData;

        TReadContext(TReadArg&& readArg, ui64 size)
            : ReadArg(std::move(readArg))
            , Size(size)
        {}

        void Abort() {
            Terminated = true;
        }

        void EndWithSuccess(TQuery *query) {
            Y_ABORT_UNLESS(!Terminated);
            Y_ABORT_UNLESS(Buffer.IsMonolith());
            Y_ABORT_UNLESS(Buffer.GetMonolith().size() == Size);
            query->OnRead(ReadArg.Tag, TReadOutcome{TReadOutcome::TOk{Buffer.GetMonolith()}});
            Abort();
        }

        void EndWithError(TQuery *query, NKikimrProto::EReplyStatus status, TString errorReason) {
            Y_ABORT_UNLESS(!Terminated);
            Y_ABORT_UNLESS(status != NKikimrProto::NODATA && status != NKikimrProto::OK);
            query->OnRead(ReadArg.Tag, TReadOutcome{TReadOutcome::TError{status, std::move(errorReason)}});
            Abort();
        }

        void EndWithNoData(TQuery *query) {
            Y_ABORT_UNLESS(!Terminated);
            query->OnRead(ReadArg.Tag, TReadOutcome{TReadOutcome::TNodata{}});
            Abort();
        }

        ui64 GetTag() const {
            return ReadArg.Tag;
        }

        struct TPartContext : TRequestContext {
            std::shared_ptr<TReadContext> Read;
            std::vector<ui64> Offsets;

            TPartContext(std::shared_ptr<TReadContext> read)
                : Read(std::move(read))
            {}
        };
    };

    bool TBlobDepotAgent::TQuery::IssueRead(TReadArg&& arg, TString& error) {
        STLOG(PRI_DEBUG, BLOB_DEPOT_AGENT, BDA34, "IssueRead", (AgentId, Agent.LogId), (QueryId, GetQueryId()),
            (ReadId, arg.Tag), (Key, Agent.PrettyKey(arg.Key)), (Offset, arg.Offset), (Size, arg.Size),
            (Value, arg.Value));

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

        for (const auto& value : arg.Value.Chain) {
            const ui32 groupId = value.GroupId;
            const auto& blobId = value.BlobId;
            const ui32 begin = value.SubrangeBegin;
            const ui32 end = value.SubrangeEnd;

            if (end <= begin || blobId.BlobSize() < end) {
                error = "incorrect SubrangeBegin/SubrangeEnd pair";
                STLOG(PRI_CRIT, BLOB_DEPOT_AGENT, BDA24, error, (AgentId, Agent.LogId), (QueryId, GetQueryId()),
                    (ReadId, arg.Tag), (Key, Agent.PrettyKey(arg.Key)), (Offset, arg.Offset), (Size, arg.Size),
                    (Value, arg.Value));
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
            Y_ABORT_UNLESS(partLen);

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
            STLOG(PRI_ERROR, BLOB_DEPOT_AGENT, BDA25, error, (AgentId, Agent.LogId), (QueryId, GetQueryId()),
                (ReadId, arg.Tag), (Key, Agent.PrettyKey(arg.Key)), (Offset, arg.Offset), (Size, arg.Size),
                (Value, arg.Value));
            return false;
        }

        auto context = std::make_shared<TReadContext>(std::move(arg), outputOffset);
        if (!outputOffset) {
            context->EndWithSuccess(this);
            return true;
        }

        THashMap<ui32, std::vector<std::tuple<ui64 /*offset*/, TEvBlobStorage::TEvGet::TQuery>>> queriesPerGroup;
        for (const TReadItem& item : items) {
            TEvBlobStorage::TEvGet::TQuery query;
            query.Set(item.Id, item.Offset, item.Size);
            queriesPerGroup[item.GroupId].emplace_back(item.OutputOffset, query);
            Agent.BytesRead += item.Size;
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

            auto event = std::make_unique<TEvBlobStorage::TEvGet>(q, sz, TInstant::Max(), context->ReadArg.GetHandleClass);
            event->ReaderTabletData = context->ReadArg.ReaderTabletData;
            STLOG(PRI_DEBUG, BLOB_DEPOT_AGENT, BDA39, "issuing TEvGet", (AgentId, Agent.LogId), (QueryId, GetQueryId()),
                (ReadId, context->GetTag()), (Key, Agent.PrettyKey(context->ReadArg.Key)), (GroupId, groupId), (Msg, *event));
            Agent.SendToProxy(groupId, std::move(event), this, std::move(partContext));
            ++context->NumPartsPending;
        }

        Y_ABORT_UNLESS(context->NumPartsPending);

        return true;
    }

    void TBlobDepotAgent::TQuery::HandleGetResult(const TRequestContext::TPtr& context, TEvBlobStorage::TEvGetResult& msg) {
        auto& partContext = context->Obtain<TReadContext::TPartContext>();
        auto& readContext = *partContext.Read;
        STLOG(PRI_DEBUG, BLOB_DEPOT_AGENT, BDA41, "HandleGetResult", (AgentId, Agent.LogId), (QueryId, GetQueryId()),
            (ReadId, readContext.GetTag()), (Key, Agent.PrettyKey(readContext.ReadArg.Key)), (Msg, msg),
            (Terminated, readContext.Terminated));
        if (readContext.Terminated || readContext.StopProcessingParts) {
            return; // just ignore this read
        }

        Y_ABORT_UNLESS(msg.ResponseSz == partContext.Offsets.size());

        for (ui32 i = 0; i < msg.ResponseSz; ++i) {
            auto& blob = msg.Responses[i];
            if (blob.Status == NKikimrProto::NODATA) {
                NKikimrBlobDepot::TEvResolve resolve;
                auto *item = resolve.AddItems();
                item->SetExactKey(readContext.ReadArg.Key);
                item->SetMustRestoreFirst(readContext.ReadArg.MustRestoreFirst);
                STLOG(PRI_DEBUG, BLOB_DEPOT_AGENT, BDA48, "issuing extra resolve", (Agent, Agent.LogId), (QueryId, GetQueryId()),
                    (ReadId, readContext.GetTag()), (Key, Agent.PrettyKey(readContext.ReadArg.Key)), (Msg, resolve));
                Agent.Issue(std::move(resolve), this, readContext.shared_from_this());
                readContext.StopProcessingParts = true;
                readContext.BlobWithoutData = blob.Id;
                return;
            } else if (blob.Status != NKikimrProto::OK) {
                return readContext.EndWithError(this, blob.Status, TStringBuilder() << "failed to read BlobId# " << blob.Id
                    << " Status# " << blob.Status << " ErrorReason# '" << msg.ErrorReason << "'");
            }

            const ui64 offset = partContext.Offsets[i];
            Y_ABORT_UNLESS(offset < readContext.Size && blob.Buffer.size() <= readContext.Size - offset);
            readContext.Buffer.Write(offset, std::move(blob.Buffer));
        }

        if (!--readContext.NumPartsPending) {
            readContext.EndWithSuccess(this);
        }
    }

    void TBlobDepotAgent::TQuery::HandleResolveResult(const TRequestContext::TPtr& context, TEvBlobDepot::TEvResolveResult& msg) {
        auto& readContext = context->Obtain<TReadContext>();
        if (readContext.Terminated) {
            return;
        }
        STLOG(PRI_DEBUG, BLOB_DEPOT_AGENT, BDA42, "HandleResolveResult", (AgentId, Agent.LogId), (QueryId, GetQueryId()),
            (ReadId, readContext.GetTag()), (Key, Agent.PrettyKey(readContext.ReadArg.Key)), (Msg, msg.Record));
        if (msg.Record.GetStatus() != NKikimrProto::OK) {
            readContext.EndWithError(this, msg.Record.GetStatus(), msg.Record.GetErrorReason());
        } else if (msg.Record.ResolvedKeysSize() == 1) {
            const auto& item = msg.Record.GetResolvedKeys(0);
            if (TResolvedValue value(item); value.Supersedes(readContext.ReadArg.Value)) { // value chain has changed, we have to try again
                readContext.ReadArg.Value = std::move(value);
                TString error;
                if (!IssueRead(std::move(readContext.ReadArg), error)) {
                    readContext.EndWithError(this, NKikimrProto::ERROR, TStringBuilder() << "failed to restart read Error# " << error);
                }
            } else if (!item.GetReliablyWritten()) { // this was unassimilated value and we got NODATA for it
                readContext.EndWithNoData(this);
            } else {
                STLOG(PRI_CRIT, BLOB_DEPOT_AGENT, BDA40, "failed to read blob: data seems to be lost", (AgentId, Agent.LogId),
                    (QueryId, GetQueryId()), (ReadId, readContext.GetTag()), (Key, Agent.PrettyKey(readContext.ReadArg.Key)),
                    (BlobId, readContext.BlobWithoutData));
                Y_VERIFY_DEBUG_S(false, "data seems to be lost AgentId# " << Agent.LogId << " QueryId# " << GetQueryId()
                    << " ReadId# " << readContext.GetTag() << " BlobId# " << readContext.BlobWithoutData);
                readContext.EndWithError(this, NKikimrProto::ERROR, TStringBuilder() << "failed to read BlobId# "
                    << readContext.BlobWithoutData << ": data seems to be lost");
            }
        } else {
            Y_ABORT_UNLESS(!msg.Record.ResolvedKeysSize());
            readContext.EndWithNoData(this);
        }
    }

} // NKikimr::NBlobDepot
