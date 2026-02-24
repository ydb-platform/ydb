#include "ddisk_actor.h"
#include "direct_io_op.h"

#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_data.h>

#include <util/generic/overloaded.h>
#include <ydb/core/util/stlog.h>

#include <cerrno>

namespace NKikimr::NDDisk {

#if defined(__linux__)

    struct TDDiskIoOp : TDDiskActor::TDirectIoOpBase {
        TActorId Sender;                    // original requester
        ui64 Cookie = 0;                    // original event cookie
        TActorId InterconnectSession;
        NWilson::TSpan Span;
        TCounters& Counters;

        TDDiskIoOp(std::atomic<ui32>& inFlightCount, TCounters& counters)
            : TDDiskActor::TDirectIoOpBase(inFlightCount)
            , Counters(counters)
        {}

        virtual void Drop() override {
            Span.End();
        }
        
        virtual void Reply() override {
            std::unique_ptr<IEventBase> reply;
            if (Result >= 0) {
                if (IsRead) {
                    TRope data(std::move(AlignedDataHolder));
                    reply = std::make_unique<TEvReadResult>(
                        NKikimrBlobStorage::NDDisk::TReplyStatus::OK, std::nullopt, std::move(data));
                } else {
                    reply = std::make_unique<TEvWriteResult>(
                        NKikimrBlobStorage::NDDisk::TReplyStatus::OK);
                }
            } else {
                auto status = UringErrorToStatus(Result, IsRead);
                TString reason = TStringBuilder()
                    << (IsRead ? "read" : "write")
                    << " failed: " << strerror(-Result)
                    << " (errno " << (-Result) << ")";
                if (IsRead) {
                    reply = std::make_unique<TEvReadResult>(status, reason);
                } else {
                    reply = std::make_unique<TEvWriteResult>(status, reason);
                }
            }

            auto h = std::make_unique<IEventHandle>(Sender, DDiskId, reply.release(),
                0, Cookie, nullptr, Span.GetTraceId());
            if (InterconnectSession) {
                h->Rewrite(TEvInterconnect::EvForward, InterconnectSession);
            }
            Span.End();
            actorSystem->Send(h.release());

            const bool ok = Result >= 0;
            if (IsRead) {
                Counters.Interface.Read.Reply(ok, ok ? Size : 0);
            } else {
                Counters.Interface.Write.Reply(ok, ok ? Size : 0);
            }
        }
    };
#endif

    void TDDiskActor::SendInternalWrite(
            TChunkRef& chunkRef,
            const TBlockSelector &selector,
            NWilson::TSpan&& span,
            TRope &&data,
            std::function<void(NPDisk::TEvChunkWriteRawResult&, NWilson::TSpan&&)> callback
    ) {
        Y_ABORT_UNLESS(chunkRef.ChunkIdx);

        const ui64 cookie = NextCookie++;
        Send(BaseInfo.PDiskActorID, new NPDisk::TEvChunkWriteRaw(
            PDiskParams->Owner,
            PDiskParams->OwnerRound,
            chunkRef.ChunkIdx,
            selector.OffsetInBytes,
            std::move(data)), 0, cookie);

        WriteCallbacks.try_emplace(cookie, TPendingWrite{std::move(span), callback});
    }

    void TDDiskActor::Handle(TEvWrite::TPtr ev) {
        if (!CheckQuery(*ev, &Counters.Interface.Write)) {
            return;
        }

        const auto& record = ev->Get()->Record;
        const TQueryCredentials creds(record.GetCredentials());
        const TBlockSelector selector(record.GetSelector());

        TChunkRef& chunkRef = ChunkRefs[creds.TabletId][selector.VChunkIndex];
        if (!chunkRef.PendingEventsForChunk.empty() || !chunkRef.ChunkIdx) {
            if (chunkRef.PendingEventsForChunk.empty() && !chunkRef.ChunkIdx) {
                IssueChunkAllocation(creds.TabletId, selector.VChunkIndex);
            }
            chunkRef.PendingEventsForChunk.emplace(ev, "WaitChunkAllocation");
            return;
        }

        const TWriteInstruction instr(record.GetInstruction());

        auto span = std::move(NWilson::TSpan(TWilson::DDiskTopLevel, std::move(ev->TraceId), "DDisk.Write",
                NWilson::EFlags::NONE, TActivationContext::ActorSystem())
            .Attribute("tablet_id", static_cast<long>(creds.TabletId))
            .Attribute("vchunk_index", static_cast<long>(selector.VChunkIndex))
            .Attribute("offset_in_bytes", selector.OffsetInBytes)
            .Attribute("size", selector.Size));

#if defined(__linux__)
        if (UringRouter) {
            Counters.Interface.Write.Request();
            if (InFlightCount.load(std::memory_order_relaxed) >= MaxInFlight) {
                span.End();
                Counters.Interface.Write.Reply(false);
                SendReply(*ev, std::make_unique<TEvWriteResult>(
                    NKikimrBlobStorage::NDDisk::TReplyStatus::OVERLOADED, "direct I/O inflight limit exceeded"));
                return;
            }
            DirectWrite(ev, selector, instr, chunkRef, std::move(span));
            return;
        }
#endif

        TRope data;
        if (instr.PayloadId) {
            data = ev->Get()->GetPayload(*instr.PayloadId);
        }

        Counters.Interface.Write.Request();

        auto callback = [this, sender = ev->Sender, cookie = ev->Cookie,
                session = ev->InterconnectSession, size = selector.Size](NPDisk::TEvChunkWriteRawResult& /*ev*/, NWilson::TSpan&& span) {
            auto reply = std::make_unique<TEvWriteResult>(NKikimrBlobStorage::NDDisk::TReplyStatus::OK);
            auto h = std::make_unique<IEventHandle>(sender, SelfId(), reply.release(), 0, cookie, nullptr, span.GetTraceId());
            if (session) {
                h->Rewrite(TEvInterconnect::EvForward, session);
            }
            Counters.Interface.Write.Reply(true, size);
            span.End();
            TActivationContext::Send(h.release());
        };

        SendInternalWrite(chunkRef, selector, std::move(span), std::move(data), std::move(callback));
    }

	void TDDiskActor::Handle(NPDisk::TEvChunkWriteRawResult::TPtr ev) {
        auto& msg = *ev->Get();
        STLOG(PRI_DEBUG, BS_DDISK, BSDD07, "TDDiskActor::Handle(TEvChunkWriteRawResult)", (DDiskId, DDiskId), (Msg, msg.ToString()));

        if (msg.Status != NKikimrProto::OK) {
            Y_ABORT();
        }

        const auto it = WriteCallbacks.find(ev->Cookie);
        Y_ABORT_UNLESS(it != WriteCallbacks.end());
        std::visit(TOverloaded{
            [&](TPendingWrite& w) {
                w.Callback(msg, std::move(w.Span));
            },
            [&](const TPersistentBufferPendingWrite& callback) {
                callback(msg);
            }
        }, it->second);
        WriteCallbacks.erase(it);
    }

    void TDDiskActor::Handle(TEvRead::TPtr ev) {
        if (!CheckQuery(*ev, &Counters.Interface.Read)) {
            return;
        }

        const auto& record = ev->Get()->Record;
        const TQueryCredentials creds(record.GetCredentials());
        const TBlockSelector selector(record.GetSelector());

        TRope result;

        TChunkRef& chunkRef = ChunkRefs[creds.TabletId][selector.VChunkIndex];
        if (!chunkRef.PendingEventsForChunk.empty()) {
            chunkRef.PendingEventsForChunk.emplace(ev, "WaitChunkAllocation");
            return;
        }

        Counters.Interface.Read.Request();

        auto span = std::move(NWilson::TSpan(TWilson::DDiskTopLevel, std::move(ev->TraceId), "DDisk.Read",
                NWilson::EFlags::NONE, TActivationContext::ActorSystem())
            .Attribute("tablet_id", static_cast<long>(creds.TabletId))
            .Attribute("vchunk_index", static_cast<long>(selector.VChunkIndex))
            .Attribute("offset_in_bytes", selector.OffsetInBytes)
            .Attribute("size", selector.Size));

        if (!chunkRef.ChunkIdx) {
            auto zero = TRcBuf::Uninitialized(selector.Size);
            memset(zero.GetDataMut(), 0, zero.size());
            result.Insert(result.End(), std::move(zero));
            Counters.Interface.Read.Reply(true, selector.Size);
            span.End();
            SendReply(*ev, std::make_unique<TEvReadResult>(NKikimrBlobStorage::NDDisk::TReplyStatus::OK, std::nullopt,
                std::move(result)));
            return;
        }

#if defined(__linux__)
        if (UringRouter) {
            if (InFlightCount.load(std::memory_order_relaxed) >= MaxInFlight) {
                span.End();
                Counters.Interface.Read.Reply(false);
                SendReply(*ev, std::make_unique<TEvReadResult>(
                    NKikimrBlobStorage::NDDisk::TReplyStatus::OVERLOADED, "direct I/O inflight limit exceeded"));
                return;
            }
            DirectRead(ev, selector, chunkRef, std::move(span));
            return;
        }
#endif

        {
            const ui64 cookie = NextCookie++;
            Send(BaseInfo.PDiskActorID, new NPDisk::TEvChunkReadRaw(
                PDiskParams->Owner,
                PDiskParams->OwnerRound,
                chunkRef.ChunkIdx,
                selector.OffsetInBytes,
                selector.Size), 0, cookie);

            ReadCallbacks.try_emplace(cookie, TPendingRead{std::move(span), [this, sender = ev->Sender, cookie = ev->Cookie,
                    session = ev->InterconnectSession, size = selector.Size](NPDisk::TEvChunkReadRawResult& ev,
                    NWilson::TSpan&& span) {
                auto reply = std::make_unique<TEvReadResult>(NKikimrBlobStorage::NDDisk::TReplyStatus::OK, std::nullopt,
                    std::move(ev.Data));
                auto h = std::make_unique<IEventHandle>(sender, SelfId(), reply.release(), 0, cookie, nullptr,
                    span.GetTraceId());
                if (session) {
                    h->Rewrite(TEvInterconnect::EvForward, session);
                }
                Counters.Interface.Read.Reply(true, size);
                span.End();
                TActivationContext::Send(h.release());
            }});
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
        std::visit(TOverloaded{
            [&](TPendingRead& w) {
                w.Callback(msg, std::move(w.Span));
            },
            [&](const TPersistentBufferPendingRead& callback) {
                callback(msg);
            }
        }, it->second);
        ReadCallbacks.erase(it);
    }

#if defined(__linux__)

    void TDDiskActor::DirectWrite(TEvWrite::TPtr ev, const TBlockSelector& selector,
            const TWriteInstruction& instr, TChunkRef& chunkRef, NWilson::TSpan span) {
        Y_ABORT_UNLESS(chunkRef.ChunkIdx);
        Y_ABORT_UNLESS(DiskFormat);

        // TODO: use pool
        auto op = std::make_unique<TDDiskIoOp>(InFlightCount, Counters);
        op->Sender = ev->Sender;
        op->Cookie = ev->Cookie;
        op->InterconnectSession = ev->InterconnectSession;
        op->DDiskId = SelfId();
        op->Span = std::move(span);
        op->IsRead = false;
        op->Size = selector.Size;

        TRope data;
        if (instr.PayloadId) {
            data = ev->Get()->GetPayload(*instr.PayloadId);
        }

        // Zero-copy path: if the payload is contiguous and page-aligned, reuse the buffer directly.
        // TODO: should we check page size? And for large writes and huge pages should properly align?
        auto iter = data.Begin();
        if (iter.ContiguousSize() == data.size() &&
                reinterpret_cast<uintptr_t>(iter.ContiguousData()) % BlockSize == 0) {
            op->AlignedDataHolder = iter.GetChunk(); // zero-copy: ref-count bump
        } else {
            op->AlignedDataHolder = TRcBuf::UninitializedPageAligned(data.size());
            data.Begin().ExtractPlainDataAndAdvance(op->AlignedDataHolder.GetDataMut(), data.size());
        }

        op->DiskOffset = DiskFormat->Offset(chunkRef.ChunkIdx, 0, selector.OffsetInBytes);

        InFlightCount.fetch_add(1, std::memory_order_relaxed);
        const bool submitted = UringRouter->Write(op->AlignedDataHolder.data(), op->Size, op->DiskOffset, op.get());
        if (submitted) {
            op.release();
            // with SQ polling – no syscall
            // TODO: without polling do we need batching?
            UringRouter->Flush();
        } else {
            // SQ ring full -- should not happen if MaxInFlight == QueueDepth, but handle gracefully
            InFlightCount.fetch_sub(1, std::memory_order_relaxed);
            op->Span.End();
            Counters.Interface.Write.Reply(false);
            SendReply(*ev, std::make_unique<TEvWriteResult>(
                NKikimrBlobStorage::NDDisk::TReplyStatus::OVERLOADED, "io_uring SQ ring full"));
        }
    }

    void TDDiskActor::DirectRead(TEvRead::TPtr ev, const TBlockSelector& selector,
            TChunkRef& chunkRef, NWilson::TSpan span) {
        Y_ABORT_UNLESS(chunkRef.ChunkIdx);
        Y_ABORT_UNLESS(DiskFormat);

        // TODO: use pool
        auto op = std::make_unique<TDDiskIoOp>(InFlightCount, Counters);
        op->Sender = ev->Sender;
        op->Cookie = ev->Cookie;
        op->InterconnectSession = ev->InterconnectSession;
        op->DDiskId = SelfId();
        op->Span = std::move(span);
        op->IsRead = true;
        op->Size = selector.Size;
        op->AlignedDataHolder = TRcBuf::UninitializedPageAligned(selector.Size);
        op->DiskOffset = DiskFormat->Offset(chunkRef.ChunkIdx, 0, selector.OffsetInBytes);

        InFlightCount.fetch_add(1, std::memory_order_relaxed);
        const bool submitted = UringRouter->Read(op->AlignedDataHolder.GetDataMut(), op->Size, op->DiskOffset, op.get());
        if (submitted) {
            op.release();
            UringRouter->Flush();
        } else {
            // SQ ring full
            InFlightCount.fetch_sub(1, std::memory_order_relaxed);
            op->Span.End();
            Counters.Interface.Read.Reply(false);
            SendReply(*ev, std::make_unique<TEvReadResult>(
                NKikimrBlobStorage::NDDisk::TReplyStatus::OVERLOADED, "io_uring SQ ring full"));
        }
    }

    TDDiskActor::TEvPrivate::TEvShortIO::TEvShortIO(std::unique_ptr<TDirectIoOpBase> op)
        : Op(std::move(op))
    {}

    TDDiskActor::TEvPrivate::TEvShortIO::~TEvShortIO() = default;

    void TDDiskActor::HandleShortIO(TEvPrivate::TEvShortIO::TPtr ev) {
        std::unique_ptr<TDirectIoOpBase> op = std::move(ev->Get()->Op);

        ui32 remaining = op->Size - op->BufferOffset;
        bool submitted;
        if (op->IsRead) {
            submitted = UringRouter->Read(
                op->AlignedDataHolder.GetDataMut() + op->BufferOffset,
                remaining, op->DiskOffset, op.get());
        } else {
            submitted = UringRouter->Write(
                op->AlignedDataHolder.data() + op->BufferOffset,
                remaining, op->DiskOffset, op.get());
        }

        if (submitted) {
            op.release();
            UringRouter->Flush();
        } else {
            InFlightCount.fetch_sub(1, std::memory_order_relaxed);

            std::unique_ptr<IEventBase> reply;
            if (op->IsRead) {
                reply = std::make_unique<TEvReadResult>(
                    NKikimrBlobStorage::NDDisk::TReplyStatus::OVERLOADED, "io_uring SQ ring full (short I/O retry)");
            } else {
                reply = std::make_unique<TEvWriteResult>(
                    NKikimrBlobStorage::NDDisk::TReplyStatus::OVERLOADED, "io_uring SQ ring full (short I/O retry)");
            }
            auto h = std::make_unique<IEventHandle>(op->Sender, SelfId(), reply.release(),
                0, op->Cookie, nullptr, op->Span.GetTraceId());
            if (op->InterconnectSession) {
                h->Rewrite(TEvInterconnect::EvForward, op->InterconnectSession);
            }
            op->Span.End();
            TActivationContext::Send(h.release());

            auto& ctr = op->IsRead ? Counters.Interface.Read : Counters.Interface.Write;
            ctr.Reply(false);
        }
    }

#endif // defined(__linux__)

} // NKikimr::NDDisk
