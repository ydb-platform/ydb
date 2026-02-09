#include "ddisk_actor.h"

#include <ydb/core/util/stlog.h>

#include <ydb/core/util/pb.h>


namespace NKikimr::NDDisk {

    void TDDiskActor::Handle(TEvSync::TPtr ev) {
        if (!CheckQuery(*ev, &Counters.Interface.Sync)) {
            return;
        }

        const auto& record = ev->Get()->Record;
        const TQueryCredentials creds(record.GetCredentials());

        auto reject = [&](NKikimrBlobStorage::NDDisk::TReplyStatus::E status, TString errorReason) {
            Counters.Interface.Sync.Request();
            Counters.Interface.Sync.Reply(false);
            SendReply(*ev, std::make_unique<TEvSyncResult>(status, std::move(errorReason)));
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
        Counters.Interface.Sync.Request();
        auto span = std::move(NWilson::TSpan(TWilson::DDiskTopLevel, std::move(ev->TraceId), "DDisk.Sync",
                NWilson::EFlags::NONE, TActivationContext::ActorSystem())
            .Attribute("tablet_id", static_cast<long>(creds.TabletId))
            .Attribute("sync_id", static_cast<long>(syncId)));

        auto &sync = SyncsInFlight.emplace(syncId, TSyncInFlight{
            .Sender=ev->Sender, 
            .Cookie=ev->Cookie,
            .InterconnectionSessionId=ev->InterconnectSession,
            .Span={}, 
            .Creds=creds,
            .Requests={},
            .ErrorReason={}
        }).first->second;
            //.Attribute("offset_in_bytes", selector.OffsetInBytes)
            //.Attribute("size", selector.Size)
            //.Attribute("source", lsn ? "persistent_buffer" : "ddisk");
        //if (lsn) {
        //    span.Attribute("lsn", static_cast<long>(*lsn));
        //}

        const auto& ddiskId = record.GetDDiskId();
        const TQueryCredentials sourceCreds(creds.TabletId, creds.Generation, record.GetDDiskInstanceGuid(), true);
        const TActorId sourceDDiskId = MakeBlobStorageDDiskId(ddiskId.GetNodeId(), ddiskId.GetPDiskId(), ddiskId.GetDDiskSlotId());
        std::optional<ui64> vChunkIndex;

        sync.Requests.reserve(record.SegmentsSize());

        for (auto &segment : record.GetSegments()) {
            TBlockSelector selector;
            std::optional<ui64> lsn;
            const NKikimrBlobStorage::NDDisk::TDDiskId ddiskId;

            switch (segment.GetSourceCase()) {
                case NKikimrBlobStorage::NDDisk::TEvSync::TSegments::kPersistentBuffer: {
                    const auto& src = segment.GetPersistentBuffer();
                    selector = TBlockSelector(src.GetSelector());
                    lsn = src.GetLsn();
                    break;
                }
                case NKikimrBlobStorage::NDDisk::TEvSync::TSegments::kDDisk: {
                    const auto& src = segment.GetDDisk();
                    selector = TBlockSelector(src.GetSelector());
                    break;
                }
                case NKikimrBlobStorage::NDDisk::TEvSync::TSegments::SOURCE_NOT_SET: {
                    // TODO(kruall): clean already sent requests
                    reject(NKikimrBlobStorage::NDDisk::TReplyStatus::INCORRECT_REQUEST, "segment source must be set");
                    return;
                }
            }

            if (!vChunkIndex) {
                vChunkIndex = selector.VChunkIndex;
            } else if (*vChunkIndex != selector.VChunkIndex) {
                // TODO(kruall): clean
                reject(NKikimrBlobStorage::NDDisk::TReplyStatus::INCORRECT_REQUEST, "Segments must be in one VChunk");
            }

            if (selector.OffsetInBytes % BlockSize || selector.Size % BlockSize || !selector.Size) {
                // TODO(kruall): clean already sent requests
                reject(NKikimrBlobStorage::NDDisk::TReplyStatus::INCORRECT_REQUEST,
                    "offset and size must be multiple of block size and size must be nonzero");
                return;
            }

            std::vector<TSegmentManager::TOutdatedRequest> outdated;
            TSegmentManager::TSegment segmentRange{selector.OffsetInBytes, selector.OffsetInBytes + selector.Size};
            ui64 requestId = 0;
            SegmentManager.PushRequest(*vChunkIndex, syncId, segmentRange, &requestId, &outdated);
            if (sync.FirstRequestId != Max<ui64>()) {
                sync.FirstRequestId = requestId;
            }
            sync.Requests.emplace_back(TSyncReadRequest{
                .Status=NKikimrBlobStorage::NDDisk::TReplyStatus::UNKNOWN,
                .Selector=selector});

            for (auto &[outdatedSyncId, outdatedRequestId] : outdated) {
                auto syncIt = SyncsInFlight.find(outdatedSyncId);
                if (syncIt == SyncsInFlight.end()) {
                    continue;
                }
                auto &outdatedSync = syncIt->second;
                auto &request = outdatedSync.Requests[outdatedRequestId - outdatedSync.FirstRequestId];
                auto prevStatus = std::exchange(request.Status, NKikimrBlobStorage::NDDisk::TReplyStatus::OUTDATED);
                if (prevStatus == NKikimrBlobStorage::NDDisk::TReplyStatus::UNKNOWN) {
                    if (--outdatedSync.RequestsInFlight == 0) {
                        // TODO(kruall): send reply
                        ReplySync(syncIt);
                    }
                }
            }

            if (lsn) {
                auto query = std::make_unique<TEvReadPersistentBuffer>(sourceCreds, selector, *lsn, TReadInstruction(true));
                Send(sourceDDiskId, query.release(), IEventHandle::FlagTrackDelivery, syncId, span.GetTraceId());
            } else {
                auto query = std::make_unique<TEvRead>(sourceCreds, selector, TReadInstruction(true));
                Send(sourceDDiskId, query.release(), IEventHandle::FlagTrackDelivery, syncId, span.GetTraceId());
            }
        }

        sync.VChunkIndex = *vChunkIndex;
    }

    void TDDiskActor::Handle(TEvReadPersistentBufferResult::TPtr ev) {
        std::vector<TSegmentManager::TSegment> segments;
        ui64 syncId = 0;
        SegmentManager.PopRequest(ev->Cookie, &segments, &syncId);

        if (syncId == Max<ui64>() || segments.empty()) {
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

        TRope data = ev->Get()->GetPayload(0);

        std::sort(segments.begin(), segments.end());
        ui64 cuttedFromData = 0;
        request.SegmentsInFlight = segments.size();

        for (auto& [begin, end] : segments) {
            TRope::TIterator ropeBegin = data.Position(begin - cuttedFromData);
            TRope::TIterator ropeEnd = data.Position(end - cuttedFromData);
            TRope segmentData = data.Extract(ropeBegin, ropeEnd);
            cuttedFromData += end - begin;

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

    void TDDiskActor::ReplySync(TSyncIt it) {
        Y_VERIFY(it != SyncsInFlight.end());
        auto &sync = it->second;
        std::unique_ptr<TEvSyncResult> ev;
        if (sync.ErrorReason) {
            ev = std::make_unique<TEvSyncResult>(NKikimrBlobStorage::NDDisk::TReplyStatus::ERROR, sync.ErrorReason);
            Counters.Interface.Sync.Reply(false);
        } else {
            ev = std::make_unique<TEvSyncResult>(NKikimrBlobStorage::NDDisk::TReplyStatus::OK);
            Counters.Interface.Sync.Reply(true);
        }

        for (auto &request : sync.Requests) {
            ev->AddSegmentResult(request.Status, request.ErrorReason);
        }
        auto h = std::make_unique<IEventHandle>(sync.Sender, SelfId(), ev.release(), 0, sync.Cookie);
        if (sync.InterconnectionSessionId) {
            h->Rewrite(TEvInterconnect::EvForward, sync.InterconnectionSessionId);
        }
        sync.Span.End();
        TActivationContext::Send(h.release());
        SyncsInFlight.erase(it);
    }
}
