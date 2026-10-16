#include "ddisk_actor.h"

namespace NKikimr::NDDisk {
    using TStatus = NKikimrBlobStorage::NDDisk::TReplyStatus;

    void TDDiskActor::Handle(TEvSync::TPtr ev) {
        TQueryCredentials creds;
        if (!CheckQueryImpl<false>(*ev, &Counters.Interface.Sync, creds)) {
            return;
        }

        const auto& record = ev->Get()->Record;
        const TQueryCredentials original(record.GetCredentials());

        auto reject = [&](TStatus::E status, TString reason) {
            Counters.Interface.Sync.Request(0);
            Counters.Interface.Sync.Reply(false);
            SendReply(*ev, std::make_unique<TEvSyncResult>(status, std::move(reason)));
        };

        if (TabletChunkDeletionsInFlight.contains(creds.TabletId)) {
            reject(TStatus::BUSY, "tablet chunk deletion is in flight");
            return;
        }

        if (!record.SourcesSize()) {
            reject(TStatus::INCORRECT_REQUEST, "sources must be non-empty");
            return;
        }

        // Build and validate the entire request before sending even the first source read.
        auto sync = std::make_shared<TSyncInFlight>();
        sync->Id = NextSyncId++;
        sync->Creds = creds;
        std::optional<ui64> vchunk;
        for (const auto& source : record.GetSources()) {
            if (!source.HasDDiskId()) {
                reject(TStatus::INCORRECT_REQUEST, "source ddisk id must be set");
                return;
            }
            const auto& id = source.GetDDiskId();
            const auto sourceCreds = TQueryCredentials::ForInternal(creds.TabletId, creds.Generation,
                std::make_optional(source.GetDDiskInstanceGuid()), creds.DirectBlockGroupIndex);
            for (const auto& segment : source.GetSegments()) {
                const TBlockSelector selector(segment.GetSelector());
                const bool pb = segment.HasPersistentBufferSegment();
                if (pb == segment.HasDDiskSegment()
                        || !selector.Size || selector.OffsetInBytes % IntegrityUnitSize
                        || selector.Size % IntegrityUnitSize
                        || selector.OffsetInBytes > DiskFormat->ChunkSize
                        || selector.Size > DiskFormat->ChunkSize - selector.OffsetInBytes
                        || (vchunk && *vchunk != selector.VChunkIndex)) {
                    reject(TStatus::INCORRECT_REQUEST,
                        "segments must have exactly one kind and aligned nonempty ranges within one VChunk");
                    return;
                }
                vchunk = selector.VChunkIndex;
                auto& input = sync->Requests.emplace_back();
                input.Selector = selector;
                input.PersistentBufferSource = pb;
                input.Credentials = sourceCreds;
                if (pb) {
                    input.Source = MakeBlobStoragePersistentBufferId(
                        id.GetNodeId(), id.GetPDiskId(), id.GetDDiskSlotId());
                    input.Lsn = segment.GetPersistentBufferSegment().GetLsn();
                    input.Generation = segment.GetPersistentBufferSegment().GetGeneration();
                } else {
                    input.Source = MakeBlobStorageDDiskId(
                        id.GetNodeId(), id.GetPDiskId(), id.GetDDiskSlotId());
                }
            }
        }

        if (sync->Requests.empty()) {
            reject(TStatus::INCORRECT_REQUEST, "segments must be non-empty");
            return;
        }

        for (const auto& input : sync->Requests) {
            // Logical source bytes admitted to this Sync, including overlapping ranges.
            sync->RequestedBytes += input.Selector.Size;
        }

        Counters.Interface.Sync.Request(0);
        CountTabletIo(creds.TabletId, ETabletOperation::Sync, 1, sync->RequestedBytes);
        *Counters.Interface.Sync.Bytes += sync->RequestedBytes;
        // From here ExecuteDataWrite owns the in-flight bytes and the SyncsInFlight entry.
        *Counters.Interface.Sync.BytesInFlight += sync->RequestedBytes;
        sync->VChunkIndex = *vchunk;

        TDataWrite write{
            .OriginalCredentials = original,
            .ResolvedCredentials = creds,
            .Selector = TBlockSelector(*vchunk, 0, 0),
            .Reply = {ev->Sender, ev->InterconnectSession, ev->Cookie},
            .Sync = sync,
        };

        write.Span = NWilson::TSpan(TWilson::DDiskTopLevel, std::move(ev->TraceId), "DDisk.Sync",
            NWilson::EFlags::NONE, TActivationContext::ActorSystem());
        NPrivate::AddMessageWaitAttributes(write.Span);
        write.Span.Attribute("tablet_id", static_cast<i64>(creds.TabletId))
            .Attribute("sync_id", static_cast<i64>(sync->Id));

        Y_ABORT_UNLESS(SyncsInFlight.emplace(sync->Id, sync).second);

        ev.Reset(nullptr);
        ExecuteDataWrite(std::move(write), nullptr);
    }

    void TDDiskActor::SendSyncSourceRead(TDataWrite& write, const TSyncReadRequest& input) {
        auto& sync = *write.Sync;
        sync.Selector = write.Selector;
        sync.SourceResult.reset();
        sync.SourceCookie = NActors::AllocateWaitCookie();
        SyncSourceCookies.emplace(sync.SourceCookie, TSyncSourceCookie{sync.Id, input.PersistentBufferSource});
        IEventBase* query = input.PersistentBufferSource
            ? static_cast<IEventBase*>(new TEvReadPersistentBuffer(input.Credentials, write.Selector,
                input.Lsn, input.Generation, TReadInstruction(true)))
            : static_cast<IEventBase*>(new TEvRead(input.Credentials, write.Selector, TReadInstruction(true)));
        Send(input.Source, query, IEventHandle::FlagTrackDelivery, sync.SourceCookie, write.Span.GetTraceId());
    }

    bool TDDiskActor::AcceptSyncSource(TDataWrite& write, TSyncReadRequest& input) {
        auto source = std::move(*write.Sync->SourceResult);
        write.Sync->SourceResult.reset();
        if (source.Status != TStatus::OK) {
            input.Status = source.Status;
            input.ErrorReason = std::move(source.ErrorReason);
            return false;
        }
        if (!source.HasPayload || source.Data.size() != write.Selector.Size) {
            input.Status = TStatus::INCORRECT_REQUEST;
            input.ErrorReason = "source payload size mismatch or missing payload";
            return false;
        }
        if (Config.EnableChecksums && !HasRequiredBlockChecksums(source.Checksums.size(),
                write.Selector.OffsetInBytes, write.Selector.Size)) {
            input.Status = TStatus::INCORRECT_REQUEST;
            input.ErrorReason = "source read must return one checksum per aligned 4 KiB block";
            return false;
        }
        if (Config.EnableChecksums && Config.CheckChecksumBeforeWrite) {
            if (const auto validation = ValidatePayloadChecksums(source.Checksums, source.Data)) {
                if (validation->Status == TStatus::CORRUPTED) {
                    Counters.Checksums.ChecksumMismatch->Inc();
                }
                input.Status = validation->Status;
                input.ErrorReason = validation->ErrorReason;
                return false;
            }
        }
        write.Data = std::move(source.Data);
        write.Checksums = std::move(source.Checksums);
        return true;
    }

    void TDDiskActor::FinishSync(TDataWrite& write) {
        auto& sync = *write.Sync;
        const bool committed = IsChunkCommitted(write.ResolvedCredentials.TabletId, sync.VChunkIndex);
        TStringBuilder errors;
        for (auto& input : sync.Requests) {
            if (IsBroken() || (Stopping && (!committed
                    || (input.Status == TStatus::OK && input.Retired != input.Selector.Size)))) {
                input.Status = IsBroken() ? TStatus::ERROR : TStatus::SESSION_MISMATCH;
                input.ErrorReason = IsBroken() ? GetBrokenReason() : TString(StoppingReason);
            }
            if (input.Status != TStatus::OK) {
                errors << input.ErrorReason << "; ";
            }
        }
        auto reply = std::make_unique<TEvSyncResult>(errors
            ? (Stopping && !IsBroken() ? TStatus::SESSION_MISMATCH : TStatus::ERROR) : TStatus::OK, errors);
        for (const auto& input : sync.Requests) {
            reply->AddSegmentResult(input.Status, input.ErrorReason);
        }
        Counters.Interface.Sync.Reply(!errors);
        auto h = std::make_unique<IEventHandle>(write.Reply.OriginalRequester, SelfId(), reply.release(),
            0, write.Reply.Cookie, nullptr, write.Span.GetTraceId());
        if (write.Reply.InterconnectSession) {
            h->Rewrite(TEvInterconnect::EvForward, write.Reply.InterconnectSession);
        }
        write.Span.End();
        SyncsInFlight.erase(sync.Id);
        TActivationContext::Send(h.release());
    }

    void TDDiskActor::QueueSync(ui64 id) {
        if (const auto it = SyncsInFlight.find(id); it != SyncsInFlight.end()) {
            it->second->Changed.NotifyAll();
        }
    }

    void TDDiskActor::QueueSyncsForChunk(ui64 tabletId, ui64 vChunkIndex) {
        for (const auto& [id, sync] : SyncsInFlight) {
            if (sync->Creds.TabletId == tabletId && sync->VChunkIndex == vChunkIndex) {
                QueueSync(id);
            }
        }
    }

    void TDDiskActor::CompleteSyncSource(ui64 cookie, bool persistentBuffer,
            TStatus::E status, TString reason, bool hasPayload, TRope data, std::vector<ui64> checksums)
    {
        const auto it = SyncSourceCookies.find(cookie);
        if (it == SyncSourceCookies.end() || it->second.PersistentBuffer != persistentBuffer) {
            return;
        }
        const auto route = it->second;
        SyncSourceCookies.erase(it);
        if (const auto parent = SyncsInFlight.find(route.SyncId); parent != SyncsInFlight.end()) {
            auto& sync = *parent->second;
            sync.SourceCookie = 0;
            sync.SourceResult.emplace();
            auto& result = *sync.SourceResult;
            result.Status = status;
            result.ErrorReason = std::move(reason);
            result.HasPayload = hasPayload;
            // Bound retained payload even when the remote source is malformed.
            result.Data = data.size() <= sync.Selector.Size ? std::move(data) : TRope{};
            result.Checksums = std::move(checksums);
            QueueSync(route.SyncId);
        }
    }

    void TDDiskActor::HandleSyncSourceUndelivered(ui64 cookie, bool persistentBuffer) {
        CompleteSyncSource(cookie, persistentBuffer, TStatus::ERROR, "source read event undelivered", false, {}, {});
    }

    void TDDiskActor::CancelPendingSyncSources() {
        for (const auto& [id, sync] : SyncsInFlight) {
            SyncSourceCookies.erase(std::exchange(sync->SourceCookie, 0));
            QueueSync(id);
        }
    }

    template<class TResult>
    void TDDiskActor::HandleSyncSourceResult(typename TResult::TPtr ev, bool persistentBuffer) {
        const auto cookie = SyncSourceCookies.find(ev->Cookie);
        if (cookie == SyncSourceCookies.end() || cookie->second.PersistentBuffer != persistentBuffer) {
            return;
        }
        auto& msg = *ev->Get();
        const bool hasPayload = msg.GetPayloadCount() > 0;
        TRope data = hasPayload ? msg.GetPayload(0) : TRope{};
        CompleteSyncSource(ev->Cookie, persistentBuffer, msg.Record.GetStatus(), msg.Record.GetErrorReason(),
            hasPayload, std::move(data),
            {msg.Record.GetChecksums().begin(), msg.Record.GetChecksums().end()});
    }

    void TDDiskActor::Handle(TEvReadResult::TPtr ev) {
        HandleSyncSourceResult<TEvReadResult>(std::move(ev), false);
    }

    void TDDiskActor::Handle(TEvReadPersistentBufferResult::TPtr ev) {
        HandleSyncSourceResult<TEvReadPersistentBufferResult>(std::move(ev), true);
    }
}
