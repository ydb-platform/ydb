#include "ddisk_actor.h"
#include "direct_io_op.h"

#include <ydb/core/util/stlog.h>

#include <ydb/core/util/pb.h>


namespace NKikimr::NDDisk {

    void TDDiskActor::Handle(TEvSync::TPtr ev) {
        YDB_LOG_TRACE_COMP(BS_DDISK, "TDDiskActor::HandleSync",
            {"marker", "BSDD22"},
            {"DDiskId", DDiskId},
            {"msg", ev->Get()->Record});

        auto& counters = Counters.Interface.Sync;
        if (!CheckQuery(*ev, &counters)) {
            return;
        }

        const auto& record = ev->Get()->Record;
        const TQueryCredentials creds(record.GetCredentials());
        TSyncIt syncIt = SyncsInFlight.end();
        counters.Request(0);

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
            YDB_LOG_DEBUG_CTX_COMP(*TActivationContext::ActorSystem(), NKikimrServices::BS_DDISK, "TDDiskActor::HandleSync reject",
                {"reason", errorReason},
                {"DDiskId", DDiskId});
            cleanupSyncState();
            counters.Reply(false);
            SendReply(*ev, std::make_unique<TEvSyncResult>(status, std::move(errorReason)));
        };

        if (!record.SourcesSize()) {
            reject(NKikimrBlobStorage::NDDisk::TReplyStatus::INCORRECT_REQUEST, "sources must be non-empty");
            return;
        }

        struct TSourceInfo {
            TActorId DDiskActorId;
            TActorId PersistentBufferActorId;
            TQueryCredentials Creds;
        };

        auto getSource = [&](const auto& source, TSourceInfo *sourceInfo) {
            if (!source.HasDDiskId()) {
                reject(NKikimrBlobStorage::NDDisk::TReplyStatus::INCORRECT_REQUEST,
                    "source ddisk id must be set");
                return false;
            }
            const auto& ddiskId = source.GetDDiskId();
            sourceInfo->DDiskActorId =
                MakeBlobStorageDDiskId(ddiskId.GetNodeId(), ddiskId.GetPDiskId(), ddiskId.GetDDiskSlotId());
            sourceInfo->PersistentBufferActorId =
                MakeBlobStoragePersistentBufferId(ddiskId.GetNodeId(), ddiskId.GetPDiskId(), ddiskId.GetDDiskSlotId());
            sourceInfo->Creds = TQueryCredentials::ForInternal(
                creds.TabletId,
                creds.Generation,
                std::make_optional(source.GetDDiskInstanceGuid()));
            return true;
        };

        std::vector<TSourceInfo> sourceInfos;
        sourceInfos.reserve(record.SourcesSize());
        size_t segmentCount = 0;
        for (const auto& source : record.GetSources()) {
            auto& sourceInfo = sourceInfos.emplace_back();
            if (!getSource(source, &sourceInfo)) {
                return;
            }
            segmentCount += source.SegmentsSize();
        }
        if (!segmentCount) {
            reject(NKikimrBlobStorage::NDDisk::TReplyStatus::INCORRECT_REQUEST, "segments must be non-empty");
            return;
        }

        const ui64 syncId = NextSyncId++;
        auto span = NWilson::TSpan(TWilson::DDiskTopLevel, std::move(ev->TraceId), "DDisk.Sync",
                NWilson::EFlags::NONE, TActivationContext::ActorSystem());
        NPrivate::AddMessageWaitAttributes(span);
        span
            .Attribute("tablet_id", static_cast<i64>(creds.TabletId))
            .Attribute("sync_id", static_cast<i64>(syncId));

        syncIt = SyncsInFlight.emplace(syncId, TSyncInFlight{
            .Sender=ev->Sender,
            .Cookie=ev->Cookie,
            .InterconnectionSessionId=ev->InterconnectSession,
            .Span={},
            .Creds=creds,
            .Requests={},
            .ErrorReason={}
        }).first;
        auto& sync = syncIt->second;
        sync.Span = std::move(span);

        std::optional<ui64> vChunkIndex;

        sync.Requests.reserve(segmentCount);

        auto validateSelector = [&](const TBlockSelector& selector, TStringBuf selectorName) {
            if (selector.OffsetInBytes % DiskFormat->SectorSize || selector.Size % DiskFormat->SectorSize || !selector.Size) {
                reject(NKikimrBlobStorage::NDDisk::TReplyStatus::INCORRECT_REQUEST,
                    TStringBuilder() << selectorName
                        << " offset and size must be multiple of sector size and size must be nonzero");
                return false;
            }

            if (selector.OffsetInBytes > DiskFormat->ChunkSize ||
                    selector.Size > DiskFormat->ChunkSize - selector.OffsetInBytes) {
                reject(NKikimrBlobStorage::NDDisk::TReplyStatus::INCORRECT_REQUEST,
                    TStringBuilder() << selectorName << " exceeds chunk bounds");
                return false;
            }

            return true;
        };

        ui32 sourceIndex = 0;
        for (const auto& source : record.GetSources()) {
            const auto& sourceInfo = sourceInfos[sourceIndex++];
            if (!source.SegmentsSize()) {
                continue;
            }

            for (const auto& segment : source.GetSegments()) {
                const TBlockSelector selector(segment.GetSelector());
                const bool fromPersistentBuffer =
                    segment.HasPersistentBufferSegment();
                if (!segment.HasDDiskSegment() &&
                    !segment.HasPersistentBufferSegment())
                {
                    reject(NKikimrBlobStorage::NDDisk::TReplyStatus::INCORRECT_REQUEST,
                        "segment kind must be set");
                    return;
                }

                if (!vChunkIndex) {
                    vChunkIndex = selector.VChunkIndex;
                } else if (*vChunkIndex != selector.VChunkIndex) {
                    reject(NKikimrBlobStorage::NDDisk::TReplyStatus::INCORRECT_REQUEST,
                        "Segments must be in one VChunk");
                    return;
                }

                if (!validateSelector(selector, "segment")) {
                    return;
                }

                sync.RequestsInFlight++;
                std::vector<TSegmentManager::TOutdatedRequest> outdated;
                const TSegmentManager::TSegment segmentRange{selector.OffsetInBytes, selector.OffsetInBytes + selector.Size};
                ui64 requestId = 0;
                SegmentManager.PushRequest(creds.TabletId, *vChunkIndex, syncId, segmentRange, &requestId, &outdated);

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

                Y_DEBUG_ABORT_UNLESS(SyncReadCookiesInFlight.emplace(requestId).second);
                const TActorId& sourceActorId = fromPersistentBuffer
                    ? sourceInfo.PersistentBufferActorId
                    : sourceInfo.DDiskActorId;
                std::unique_ptr<IEventBase> readQuery;
                if (fromPersistentBuffer) {
                    const auto& persistentBufferSegment =
                        segment.GetPersistentBufferSegment();
                    readQuery = std::make_unique<TEvReadPersistentBuffer>(
                        sourceInfo.Creds,
                        selector,
                        persistentBufferSegment.GetLsn(),
                        persistentBufferSegment.GetGeneration(),
                        TReadInstruction(true));
                } else {
                    readQuery = std::make_unique<TEvRead>(
                        sourceInfo.Creds,
                        selector,
                        TReadInstruction(true));
                }
                Send(
                    sourceActorId,
                    readQuery.release(),
                    IEventHandle::FlagTrackDelivery,
                    requestId,
                    sync.Span.GetTraceId());
            }
        }

        sync.VChunkIndex = *vChunkIndex;
    }

    template <typename TEventPtr>
    void TDDiskActor::InternalSyncReadResult(TEventPtr ev) {
        YDB_LOG_TRACE_COMP(BS_DDISK, "TDDiskActor::InternalSyncReadResult",
            {"marker", "BSDD26"},
            {"DDiskId", DDiskId},
            {"cookie", ev->Cookie},
            {"msg", ev->Get()->Record});

        ui64 syncId = SegmentManager.GetSync(ev->Cookie);

        if (syncId == Max<ui64>()) {
            if (SyncReadCookiesInFlight.erase(ev->Cookie)) {
                return;
            }
            YDB_LOG_ERROR_COMP(BS_DDISK, "TDDiskActor::InternalSyncReadResult unknown sync for cookie",
                {"marker", "BSDD24"},
                {"DDiskId", DDiskId},
                {"cookie", ev->Cookie});
            return;
        }

        auto it = SyncsInFlight.find(syncId);
        if (it == SyncsInFlight.end()) {
            SyncReadCookiesInFlight.erase(ev->Cookie);
            return;
        }
        auto& sync = it->second;

        if (ev->Cookie < sync.FirstRequestId || ev->Cookie >= sync.FirstRequestId + sync.Requests.size()) {
            SyncReadCookiesInFlight.erase(ev->Cookie);
            YDB_LOG_ERROR_COMP(BS_DDISK, "TDDiskActor::InternalSyncReadResult request cookie out of range",
                {"marker", "BSDD25"},
                {"DDiskId", DDiskId},
                {"cookie", ev->Cookie},
                {"syncId", syncId},
                {"firstRequestId", sync.FirstRequestId},
                {"requestsCount", sync.Requests.size()});
            return;
        }
        auto& request = sync.Requests[ev->Cookie - sync.FirstRequestId];

        if (request.Status != NKikimrBlobStorage::NDDisk::TReplyStatus::UNKNOWN) {
            SyncReadCookiesInFlight.erase(ev->Cookie);
            return;
        }

        const auto& record = ev->Get()->Record;
        if (record.GetStatus() != NKikimrBlobStorage::NDDisk::TReplyStatus::OK) {
            SyncReadCookiesInFlight.erase(ev->Cookie);
            request.Status = record.GetStatus();
            request.ErrorReason << "[" << request.Selector.OffsetInBytes << ';'
                << request.Selector.OffsetInBytes + request.Selector.Size
                << "] failed to read; reason: " << record.GetErrorReason();
            sync.ErrorReason << "[request_idx=" << ev->Cookie - sync.FirstRequestId << "] failed to read; ";
            YDB_LOG_DEBUG_CTX_COMP(*TActivationContext::ActorSystem(), NKikimrServices::BS_DDISK, "TDDiskActor::InternalSyncReadResult read failed",
                {"DDiskId", DDiskId},
                {"cookie", ev->Cookie},
                {"syncId", syncId},
                {"status", static_cast<int>(record.GetStatus())},
                {"errorReason", record.GetErrorReason()});
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
        SyncReadCookiesInFlight.erase(ev->Cookie);
        Y_VERIFY(segments.size());

        TRope data = ev->Get()->GetPayload(0);

        std::sort(segments.begin(), segments.end());
        ui64 cuttedFromData = request.Selector.OffsetInBytes;
        request.SegmentsInFlight = segments.size();

        YDB_LOG_DEBUG_CTX_COMP(*TActivationContext::ActorSystem(), NKikimrServices::BS_DDISK, "TDDiskActor::InternalSyncReadResult writing segments",
            {"DDiskId", DDiskId},
            {"cookie", ev->Cookie},
            {"syncId", syncId},
            {"chunkIdx", chunkRef.ChunkIdx},
            {"segmentsInFlight", request.SegmentsInFlight},
            {"dataSize", data.size()});

        // TODO: don't flush each time, write as a single op?
        for (auto& [begin, end] : segments) {
            if (cuttedFromData < begin) {
                data.EraseFront(begin - cuttedFromData);
            }
            TRope segmentData;
            data.ExtractFront(end - begin, &segmentData);
            cuttedFromData = end;

            auto diskOffset = DiskFormat->Offset(chunkRef.ChunkIdx, 0, begin);
            std::unique_ptr<TDirectIoOpBase> op = AllocateOp<TInternalSyncWriteOp>();
            auto* syncWriteOp = static_cast<TInternalSyncWriteOp*>(op.get());
            syncWriteOp->SetSyncId(syncId);
            syncWriteOp->SetRequestId(ev->Cookie);
            syncWriteOp->SetSegment(begin, end);
            syncWriteOp->PrepareWrite(std::move(segmentData), diskOffset, chunkRef.ChunkIdx, begin);

            DirectUringOp(op);
        }
    }

    void TDDiskActor::Handle(TEvReadPersistentBufferResult::TPtr ev) {
        InternalSyncReadResult(ev);
    }

    void TDDiskActor::Handle(TEvReadResult::TPtr ev) {
        InternalSyncReadResult(ev);
    }

    void TDDiskActor::Handle(TEvPrivate::TEvInternalSyncWriteResult::TPtr ev) {
        auto it = SyncsInFlight.find(ev->Get()->SyncId);
        if (it == SyncsInFlight.end()) {
            return;
        }
        auto& sync = it->second;

        const ui64 requestId = ev->Get()->RequestId;
        if (requestId < sync.FirstRequestId || requestId >= sync.FirstRequestId + sync.Requests.size()) {
            return;
        }
        auto& request = sync.Requests[requestId - sync.FirstRequestId];

        if (request.Status != NKikimrBlobStorage::NDDisk::TReplyStatus::UNKNOWN) {
            return;
        }

        const ui64 begin = ev->Get()->SegmentBegin;
        const ui64 end = ev->Get()->SegmentEnd;
        if (ev->Get()->Status != NKikimrBlobStorage::NDDisk::TReplyStatus::OK) {
            request.Status = NKikimrBlobStorage::NDDisk::TReplyStatus::ERROR;
            request.ErrorReason << "[" << begin << ";" << end << ") failed to write; reason: " << ev->Get()->ErrorMessage << "; ";
            sync.ErrorReason << "[request_idx=" << requestId - sync.FirstRequestId << "] failed to write; ";
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

    std::unique_ptr<IEventHandle> TDDiskActor::MakeSyncResult(const TSyncInFlight& sync) {
        std::unique_ptr<TEvSyncResult> ev;
        if (sync.ErrorReason) {
            ev = std::make_unique<TEvSyncResult>(NKikimrBlobStorage::NDDisk::TReplyStatus::ERROR, sync.ErrorReason);
            Counters.Interface.Sync.Reply(false);
        } else {
            ev = std::make_unique<TEvSyncResult>(NKikimrBlobStorage::NDDisk::TReplyStatus::OK);
            Counters.Interface.Sync.Reply(true);
        }

        for (const auto& request : sync.Requests) {
            ev->AddSegmentResult(request.Status, request.ErrorReason);
        }

        return std::make_unique<IEventHandle>(sync.Sender, SelfId(), ev.release(), 0, sync.Cookie);
    }


    void TDDiskActor::ReplySync(TSyncIt it) {
        Y_VERIFY(it != SyncsInFlight.end());
        auto& sync = it->second;
        auto h = MakeSyncResult(sync);

        if (sync.InterconnectionSessionId) {
            h->Rewrite(TEvInterconnect::EvForward, sync.InterconnectionSessionId);
        }
        sync.Span.End();
        TActivationContext::Send(h.release());
        SyncsInFlight.erase(it);
    }
}
