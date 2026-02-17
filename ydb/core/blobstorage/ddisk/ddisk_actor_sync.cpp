#include "ddisk_actor.h"

#include <ydb/core/util/stlog.h>

#include <ydb/core/util/pb.h>


namespace NKikimr::NDDisk {

    struct TDDiskActor::TSyncWithPersistentBufferPolicy {
        using TResultEvent = TEvSyncWithPersistentBufferResult;

        static constexpr TSyncInFlight::ESourceKind SourceKind = TSyncInFlight::ESK_PERSISTENT_BUFFER;

        static auto& GetCounters(TDDiskActor& actor) {
            return actor.Counters.Interface.SyncWithPersistentBuffer;
        }

        static std::unique_ptr<IEventBase> MakeResult(
                NKikimrBlobStorage::NDDisk::TReplyStatus::E status,
                const std::optional<TString>& errorReason = std::nullopt) {
            return std::make_unique<TResultEvent>(status, errorReason);
        }

        template <typename TSegment>
        static IEventBase* MakeReadQuery(const TQueryCredentials& sourceCreds,
                const TBlockSelector& selector, const TSegment& segment) {
            return new TEvReadPersistentBuffer(sourceCreds, selector, segment.GetLsn(), TReadInstruction(true));
        }
    };

    struct TDDiskActor::TSyncWithDDiskPolicy {
        using TResultEvent = TEvSyncWithDDiskResult;

        static constexpr TSyncInFlight::ESourceKind SourceKind = TSyncInFlight::ESK_DDISK;

        static auto& GetCounters(TDDiskActor& actor) {
            return actor.Counters.Interface.SyncWithDDisk;
        }

        static std::unique_ptr<IEventBase> MakeResult(
                NKikimrBlobStorage::NDDisk::TReplyStatus::E status,
                const std::optional<TString>& errorReason = std::nullopt) {
            return std::make_unique<TResultEvent>(status, errorReason);
        }

        template <typename TSegment>
        static IEventBase* MakeReadQuery(const TQueryCredentials& sourceCreds,
                const TBlockSelector& selector, const TSegment&) {
            return new TEvRead(sourceCreds, selector, TReadInstruction(true));
        }
    };

    template <typename TPolicy, typename TEventPtr>
    void TDDiskActor::HandleSync(TEventPtr ev) {
        auto& counters = TPolicy::GetCounters(*this);
        if (!CheckQuery(*ev, &counters)) {
            return;
        }

        const auto& record = ev->Get()->Record;
        const TQueryCredentials creds(record.GetCredentials());
        TSyncIt syncIt = SyncsInFlight.end();
        counters.Request();

        auto cleanupSyncState = [&] {
            if (syncIt == SyncsInFlight.end()) {
                return;
            }

            auto& sync = syncIt->second;
            std::vector<TSegmentManager::TSegment> removedSegments;
            if (sync.FirstRequestId != Max<ui64>()) {
                for (ui64 requestId = sync.FirstRequestId; requestId < sync.FirstRequestId + sync.Requests.size(); ++requestId) {
                    SegmentManager.PopRequest(requestId, &removedSegments);
                }
            }
            sync.Span.End();
            SyncsInFlight.erase(syncIt);
            syncIt = SyncsInFlight.end();
        };

        auto reject = [&](NKikimrBlobStorage::NDDisk::TReplyStatus::E status, TString errorReason) {
            cleanupSyncState();
            counters.Reply(false);
            SendReply(*ev, TPolicy::MakeResult(status, std::move(errorReason)));
        };

        if (!record.SegmentsSize()) {
            reject(NKikimrBlobStorage::NDDisk::TReplyStatus::INCORRECT_REQUEST, "segments must be non-empty");
            return;
        }

        if (!record.HasDDiskId()) {
            reject(NKikimrBlobStorage::NDDisk::TReplyStatus::INCORRECT_REQUEST, "source ddisk id must be set");
            return;
        }

        const ui64 syncId = NextSyncId++;
        auto span = std::move(NWilson::TSpan(TWilson::DDiskTopLevel, std::move(ev->TraceId), "DDisk.Sync",
                NWilson::EFlags::NONE, TActivationContext::ActorSystem())
            .Attribute("tablet_id", static_cast<long>(creds.TabletId))
            .Attribute("sync_id", static_cast<long>(syncId)));

        syncIt = SyncsInFlight.emplace(syncId, TSyncInFlight{
            .Sender=ev->Sender,
            .Cookie=ev->Cookie,
            .InterconnectionSessionId=ev->InterconnectSession,
            .Span={},
            .Creds=creds,
            .Requests={},
            .ErrorReason={},
            .SourceKind=TPolicy::SourceKind
        }).first;
        auto& sync = syncIt->second;
        sync.Span = std::move(span);

        const auto& ddiskId = record.GetDDiskId();
        const TQueryCredentials sourceCreds(creds.TabletId, creds.Generation, record.GetDDiskInstanceGuid(), true);
        const TActorId sourceDDiskId = MakeBlobStorageDDiskId(ddiskId.GetNodeId(), ddiskId.GetPDiskId(), ddiskId.GetDDiskSlotId());
        std::optional<ui64> vChunkIndex;

        sync.Requests.reserve(record.SegmentsSize());

        for (const auto& segment : record.GetSegments()) {
            const TBlockSelector selector(segment.GetSelector());

            if (!vChunkIndex) {
                vChunkIndex = selector.VChunkIndex;
            } else if (*vChunkIndex != selector.VChunkIndex) {
                reject(NKikimrBlobStorage::NDDisk::TReplyStatus::INCORRECT_REQUEST,
                    "Segments must be in one VChunk");
                return;
            }

            if (selector.OffsetInBytes % BlockSize || selector.Size % BlockSize || !selector.Size) {
                reject(NKikimrBlobStorage::NDDisk::TReplyStatus::INCORRECT_REQUEST,
                    "offset and size must be multiple of block size and size must be nonzero");
                return;
            }

            std::vector<TSegmentManager::TOutdatedRequest> outdated;
            const TSegmentManager::TSegment segmentRange{selector.OffsetInBytes, selector.OffsetInBytes + selector.Size};
            ui64 requestId = 0;
            SegmentManager.PushRequest(*vChunkIndex, syncId, segmentRange, &requestId, &outdated);

            if (sync.FirstRequestId == Max<ui64>()) {
                sync.FirstRequestId = requestId;
            } else {
                Y_ABORT_UNLESS(requestId == sync.FirstRequestId + sync.Requests.size());
            }

            sync.Requests.emplace_back(TSyncReadRequest{
                .Status=NKikimrBlobStorage::NDDisk::TReplyStatus::UNKNOWN,
                .Selector=selector
            });

            for (auto& [outdatedSyncId, outdatedRequestId] : outdated) {
                auto outdatedIt = SyncsInFlight.find(outdatedSyncId);
                if (outdatedIt == SyncsInFlight.end()) {
                    continue;
                }
                auto& outdatedSync = outdatedIt->second;
                auto& request = outdatedSync.Requests[outdatedRequestId - outdatedSync.FirstRequestId];
                const auto prevStatus = std::exchange(request.Status, NKikimrBlobStorage::NDDisk::TReplyStatus::OUTDATED);
                if (prevStatus == NKikimrBlobStorage::NDDisk::TReplyStatus::UNKNOWN) {
                    if (--outdatedSync.RequestsInFlight == 0) {
                        ReplySync(outdatedIt);
                    }
                }
            }

            Send(sourceDDiskId,
                TPolicy::MakeReadQuery(sourceCreds, selector, segment),
                IEventHandle::FlagTrackDelivery,
                requestId,
                sync.Span.GetTraceId());
            sync.RequestsInFlight++;
        }

        sync.VChunkIndex = *vChunkIndex;
    }

    void TDDiskActor::Handle(TEvSyncWithPersistentBuffer::TPtr ev) {
        HandleSync<TSyncWithPersistentBufferPolicy>(std::move(ev));
    }

    void TDDiskActor::Handle(TEvSyncWithDDisk::TPtr ev) {
        HandleSync<TSyncWithDDiskPolicy>(std::move(ev));
    }


    template <typename TEventPtr>
    void TDDiskActor::InternalSyncReadResult(TEventPtr ev) {
        ui64 syncId = SegmentManager.GetSync(ev->Cookie);

        if (syncId == Max<ui64>()) {
            return;
        }

        auto it = SyncsInFlight.find(syncId);
        if (it == SyncsInFlight.end()) {
            return;
        }
        auto& sync = it->second;

        if (ev->Cookie < sync.FirstRequestId || ev->Cookie >= sync.FirstRequestId + sync.Requests.size()) {
            // TODO(kruall): log error
            return;
        }
        auto& request = sync.Requests[ev->Cookie - sync.FirstRequestId];

        if (request.Status != NKikimrBlobStorage::NDDisk::TReplyStatus::UNKNOWN) {
            return;
        }

        const auto& record = ev->Get()->Record;
        if (record.GetStatus() != NKikimrBlobStorage::NDDisk::TReplyStatus::OK) {
            request.Status = record.GetStatus();
            request.ErrorReason << "[" << request.Selector.OffsetInBytes << ';' << request.Selector.OffsetInBytes + request.Selector.Size << "] failed to read; reason: " << record.GetErrorReason();
            sync.ErrorReason << "[request_idx=" << ev->Cookie - sync.FirstRequestId << "] failed to read; ";
            if (--sync.RequestsInFlight == 0) {
                ReplySync(it);
            }
            return;
        }

        TChunkRef& chunkRef = ChunkRefs[sync.Creds.TabletId][sync.VChunkIndex];
        if (!chunkRef.PendingEventsForChunk.empty() || !chunkRef.ChunkIdx) {
            if (chunkRef.PendingEventsForChunk.empty() && !chunkRef.ChunkIdx) {
                IssueChunkAllocation(sync.Creds.TabletId, sync.VChunkIndex);
            }
            chunkRef.PendingEventsForChunk.emplace(ev, "WaitChunkAllocation");
            return;
        }

        std::vector<TSegmentManager::TSegment> segments;
        SegmentManager.PopRequest(ev->Cookie, &segments);
        Y_VERIFY(segments.size());

        TRope data = ev->Get()->GetPayload(0);

        std::sort(segments.begin(), segments.end());
        ui64 cuttedFromData = request.Selector.OffsetInBytes;
        request.SegmentsInFlight = segments.size();

        for (auto& [begin, end] : segments) {
            if (cuttedFromData < begin) {
                data.EraseFront(begin - cuttedFromData);
            }
            TRope segmentData;
            data.ExtractFront(end - begin, &segmentData);
            cuttedFromData = end;

            auto callback = [
                this,
                begin = begin,
                end = end,
                syncId = syncId,
                requestId = ev->Cookie
            ] (NPDisk::TEvChunkWriteRawResult& evResult, NWilson::TSpan&& /*span*/) {
                if (auto it = SyncsInFlight.find(syncId); it != SyncsInFlight.end()) {
                    auto &sync = it->second;
                    auto &request = sync.Requests[requestId - sync.FirstRequestId];

                    if (request.Status != NKikimrBlobStorage::NDDisk::TReplyStatus::UNKNOWN) {
                        return;
                    }

                    if (evResult.Status != NKikimrProto::EReplyStatus::OK) {
                        request.Status = NKikimrBlobStorage::NDDisk::TReplyStatus::ERROR;
                        request.ErrorReason << "[" << begin << ";" << end << ") failed to write; reason: " << evResult.ErrorReason << "; ";
                        sync.ErrorReason << "[request_idx=" << requestId - sync.FirstRequestId << "]" << "failed to write; ";
                        if (--sync.RequestsInFlight == 0) {
                            ReplySync(it);
                        }
                        return;
                    }

                    if (--request.SegmentsInFlight == 0) {
                        request.Status = NKikimrBlobStorage::NDDisk::TReplyStatus::OK;

                        if (--sync.RequestsInFlight == 0) {
                            ReplySync(it);
                        }
                    }
                }
            };
            TBlockSelector segmentSelector(sync.VChunkIndex, begin, end - begin);
            SendInternalWrite(chunkRef, sync.Creds, segmentSelector, NWilson::TTraceId{ev->TraceId}, std::move(segmentData), std::move(callback));
        }
    }

    void TDDiskActor::Handle(TEvReadPersistentBufferResult::TPtr ev) {
        InternalSyncReadResult(ev);
    }

    void TDDiskActor::Handle(TEvReadResult::TPtr ev) {
        InternalSyncReadResult(ev);
    }

    template <typename TResultEvent, typename TCounters>
    std::unique_ptr<IEventHandle> TDDiskActor::MakeSyncResult(const TSyncInFlight& sync, TCounters& counters) const {
        std::unique_ptr<TResultEvent> ev;
        if (sync.ErrorReason) {
            ev = std::make_unique<TResultEvent>(NKikimrBlobStorage::NDDisk::TReplyStatus::ERROR, sync.ErrorReason);
            counters.Reply(false);
        } else {
            ev = std::make_unique<TResultEvent>(NKikimrBlobStorage::NDDisk::TReplyStatus::OK);
            counters.Reply(true);
        }

        for (const auto& request : sync.Requests) {
            ev->AddSegmentResult(request.Status, request.ErrorReason);
        }

        return std::make_unique<IEventHandle>(sync.Sender, SelfId(), ev.release(), 0, sync.Cookie);
    }


    void TDDiskActor::ReplySync(TSyncIt it) {
        Y_VERIFY(it != SyncsInFlight.end());
        auto& sync = it->second;
        std::unique_ptr<IEventHandle> h;

        switch (sync.SourceKind) {
            case TSyncInFlight::ESK_PERSISTENT_BUFFER:
                h = MakeSyncResult<TEvSyncWithPersistentBufferResult>(
                    sync,
                    Counters.Interface.SyncWithPersistentBuffer);
                break;
            case TSyncInFlight::ESK_DDISK:
                h = MakeSyncResult<TEvSyncWithDDiskResult>(
                    sync,
                    Counters.Interface.SyncWithDDisk);
                break;
            default:
                Y_ABORT("Unexpected sync source kind");
                break;
        }

        if (sync.InterconnectionSessionId) {
            h->Rewrite(TEvInterconnect::EvForward, sync.InterconnectionSessionId);
        }
        sync.Span.End();
        TActivationContext::Send(h.release());
        SyncsInFlight.erase(it);
    }
}
