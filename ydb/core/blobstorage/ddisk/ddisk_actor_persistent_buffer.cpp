#include "ddisk_actor.h"
#include "direct_io_op.h"

#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_data.h>
#include <ydb/core/util/hp_timer_helpers.h>
#include <ydb/core/util/stlog.h>
#include <ydb/core/util/pb.h>

#define XXH_INLINE_ALL
#include <contrib/libs/xxhash/xxhash.h>

namespace NKikimr::NDDisk {

#define STLOG_E(MESSAGE, ...) STLOG(PRI_ERROR, NKikimrServices::BS_PERSISTENT_BUFFER, BSPB, TStringBuilder() << "PBufferId: " << SelfId() << ", " << MESSAGE, __VA_ARGS__)
#define STLOG_D(MESSAGE, ...) STLOG(PRI_DEBUG, NKikimrServices::TActivity::BS_PERSISTENT_BUFFER, BSPB, TStringBuilder() << "PBufferId: " << SelfId() << ", " << MESSAGE, __VA_ARGS__)
#define STLOG_I(MESSAGE, ...) STLOG(PRI_INFO, NKikimrServices::TActivity::BS_PERSISTENT_BUFFER, BSPB, TStringBuilder() << "PBufferId: " << SelfId() << ", " << MESSAGE, __VA_ARGS__)

    void TDDiskActor::IssuePersistentBufferChunkAllocation() {
        Y_ABORT_UNLESS(IsPersistentBufferActor);
        if (!IssuePersistentBufferChunkAllocationInflight) {
            IssuePersistentBufferChunkAllocationInflight = true;
            auto ddiskActorId = MakeBlobStorageDDiskId(SelfId().NodeId(), BaseInfo.PDiskId, BaseInfo.VDiskSlotId);
            Send(ddiskActorId, new TEvPrivate::TEvIssuePersistentBufferChunkAllocation());
            STLOG_D("TDDiskActor::ProcessPersistentBufferWrite empty space, request new chunk", (FreeSpace, PersistentBufferSpaceAllocator.GetFreeSpace()), (PersistentBufferSpaceAllocator, PersistentBufferSpaceAllocator.ToString()));
        }
    }

    void TDDiskActor::InitPersistentBuffer() {
        Y_ABORT_UNLESS(IsPersistentBufferActor);
        Y_ABORT_UNLESS(DiskFormat);
        SectorSize = DiskFormat->SectorSize;
        Y_ABORT_UNLESS(SectorSize >= sizeof(TPersistentBufferHeader));
        ChunkSize = DiskFormat->ChunkSize;
        Y_ABORT_UNLESS(ChunkSize % SectorSize == 0);
        SectorInChunk = ChunkSize / SectorSize;
        PersistentBufferSpaceAllocator = TPersistentBufferSpaceAllocator(SectorInChunk);
        PersistentBufferBarriersManager.Initialize(PersistentBufferUniqueId, BaseInfo.PDiskActorID.NodeId(), BaseInfo.PDiskId, BaseInfo.VDiskSlotId);
    }

    void TDDiskActor::CollectPbStatsSnapshot() {
        const TInstant now = TActivationContext::Now();
        auto take = [&](const TString& name, const auto& opCounters) {
            TPbOpSnapshot snap;
            snap.Timestamp = now;
            snap.Requests = opCounters.Requests ? opCounters.Requests->Val() : 0;
            if (opCounters.ResponseTime) {
                auto s = opCounters.ResponseTime->Snapshot();
                const ui32 n = s->Count();
                snap.BucketCounts.resize(n);
                for (ui32 i = 0; i < n; ++i) {
                    snap.BucketCounts[i] = s->Value(i);
                }
            }
            auto& dq = PbStatsHistory[name];
            dq.push_back(std::move(snap));
            // Keep snapshots within [now - PbStatsWindow - PbStatsSnapshotPeriod, now]
            // so we can always find one ~PbStatsWindow old.
            const TInstant cutoff = now - PbStatsWindow - PbStatsSnapshotPeriod;
            while (dq.size() > 1 && dq.front().Timestamp < cutoff) {
                dq.pop_front();
            }
        };
#define DDISK_TAKE_PB_SNAP(NAME) take(#NAME, Counters.Interface.NAME);
        DDISK_TAKE_PB_SNAP(WritePersistentBuffer)
        DDISK_TAKE_PB_SNAP(ReadPersistentBuffer)
        DDISK_TAKE_PB_SNAP(ErasePersistentBuffer)
#undef DDISK_TAKE_PB_SNAP
        Schedule(PbStatsSnapshotPeriod, new TEvents::TEvWakeup(EWakeupTag::WakeupCollectPbStats));
    }

    void TDDiskActor::UpdateFreeSpaceInfo() {
        Send(BaseInfo.PDiskActorID, new NPDisk::TEvCheckSpace(PDiskParams->Owner, PDiskParams->OwnerRound));
        Schedule(TDuration::MilliSeconds(PersistentBufferFormat.UpdateFreeSpaceInfoMilliseconds), new TEvents::TEvWakeup(EWakeupTag::WakeupUpdateFreeSpaceInfo));
    }

    void TDDiskActor::Handle(NPDisk::TEvCheckSpaceResult::TPtr ev) {
        if (ev->Get()->Status == NKikimrProto::EReplyStatus::OK) {
            NormalizedOccupancy = ev->Get()->NormalizedOccupancy;
        }
    }

    void TDDiskActor::Handle(TEvPrivate::TEvHandlePersistentBufferEventForChunk::TPtr ev) {
        auto chunkIdx = ev->Get()->ChunkIndex;
        Y_ABORT_UNLESS(chunkIdx);
        IssuePersistentBufferChunkAllocationInflight = false;
        PersistentBufferSpaceAllocator.AddNewChunk(chunkIdx);
        PersistentBufferAllocatedChunks.insert(chunkIdx);
        *Counters.PersistentBuffer.AllocatedChunks = PersistentBufferSpaceAllocator.OwnedChunks.size();

        if (!PersistentBufferReady) {
            StartRestorePersistentBuffer();
        } else {
            ProcessPersistentBufferQueue();
        }
    }

    ui64 TDDiskActor::CalculateChecksum(const TRope::TIterator begin) {
        Y_ABORT_UNLESS(PersistentBufferUniqueId != 0);

        XXH3_state_t state;
        XXH3_64bits_reset(&state);
        size_t numBytes = SectorSize;
        for (auto it = begin; numBytes && it.Valid(); it.AdvanceToNextContiguousBlock()) {
            const size_t n = Min(numBytes, it.ContiguousSize());
            XXH3_64bits_update(&state, it.ContiguousData(), n);
            numBytes -= n;
        }
        XXH3_64bits_update(&state, &PersistentBufferUniqueId, sizeof(PersistentBufferUniqueId));
        return XXH3_64bits_digest(&state);
    }

    void TDDiskActor::StartRestorePersistentBuffer() {
        Y_ABORT_UNLESS(IsPersistentBufferActor);
        if (PersistentBufferReady) {
            return;
        }

        if (PersistentBufferSpaceAllocator.OwnedChunks.size() < PersistentBufferFormat.InitChunks) {
            IssuePersistentBufferChunkAllocation();
            return;
        }

        if (PersistentBufferSpaceAllocator.OwnedChunks.size() == PersistentBufferAllocatedChunks.size()) {
            STLOG_D("TDDiskActor::StartRestorePersistentBuffer ready");
            PersistentBufferReady = true;
            UpdateFreeSpaceInfo();
            *Counters.PersistentBuffer.AllocatedChunks = PersistentBufferSpaceAllocator.OwnedChunks.size();
            *Counters.PersistentBuffer.TotalBytes =
                (PersistentBufferSpaceAllocator.OwnedChunks.size() * SectorInChunk - PersistentBufferSpaceAllocator.GetFreeSpace()) * SectorSize;
            return;
        }
        for (ui32 pos = 0; pos < PersistentBufferSpaceAllocator.OwnedChunks.size() && PersistentBufferRestoreChunksInflight < PersistentBufferFormat.MaxChunkRestoreInflight; pos++) {
            auto chunkIdx = PersistentBufferSpaceAllocator.OwnedChunks[pos];
            if (PersistentBufferAllocatedChunks.count(chunkIdx) > 0 || PersistentBufferRestoringChunks.count(chunkIdx) > 0) {
                continue;
            }
            STLOG_D("TDDiskActor::StartRestorePersistentBuffer restoring chunk from DDisk", (ChunkIdx, chunkIdx));
            PersistentBufferRestoringChunks.insert(chunkIdx);
            const ui64 cookie = NextCookie++;
            PersistentBufferRestoreChunksInflight++;


            std::unique_ptr<TDirectIoOpBase> op = AllocateOp<TPersistentBufferPartIoOp>();
            auto* partOp = static_cast<TPersistentBufferPartIoOp*>(op.get());
            partOp->SetCookie(cookie);
            partOp->SetPartCookie(chunkIdx);
            partOp->SetIsRestore(true);
            auto offset = DiskFormat->Offset(chunkIdx, 0, 0);
            op->PrepareRead(ChunkSize, offset, chunkIdx, 0);
            DirectUringOp(op);
        }
    }

    std::vector<std::tuple<ui32, ui32, TRope>> TDDiskActor::SlicePersistentBuffer(ui64 tabletId, ui32 generation, ui64 vchunkIndex,
        ui64 lsn, ui32 offsetInBytes, ui32 sizeInBytes, TRope&& payload, std::vector<TPersistentBufferSectorInfo>& sectors) {
        auto headerData = TRcBuf::UninitializedPageAligned(SectorSize);
        TPersistentBufferHeader *header = (TPersistentBufferHeader*)headerData.GetDataMut();
        memset(header, 0, SectorSize);
        memcpy(header->Signature, TPersistentBufferHeader::PersistentBufferHeaderSignature, 16);
        header->PersistentBufferUniqueId = PersistentBufferUniqueId;
        header->NodeId = BaseInfo.PDiskActorID.NodeId();
        header->PDiskId = BaseInfo.PDiskId;
        header->SlotId = BaseInfo.VDiskSlotId;
        header->RecordLsn = 0;

        header->Flags = 0;
        header->Record.TabletId = tabletId;
        header->Record.Generation = generation;
        header->Record.VChunkIndex = vchunkIndex;
        header->Record.OffsetInBytes = offsetInBytes;
        header->Record.Size = sizeInBytes;
        header->Record.Lsn = lsn;

        for (ui32 i = 1; i < sectors.size(); ++i) {
            auto& loc = header->Record.Locations[i - 1];
            loc = sectors[i];
            auto it = payload.Position(SectorSize * (i - 1));
            if ((ui8)it.ContiguousData()[0] == TPersistentBufferHeader::PersistentBufferHeaderSignature[0]) {
                loc.HasSignatureCorrection = true;
                *it.ContiguousDataMut() = 0;
            }
            loc.Checksum = CalculateChecksum(it);
            sectors[i].Checksum = loc.Checksum;
        }
        header->HeaderChecksum = 0;
        std::vector<std::tuple<ui32, ui32, TRope>> parts;
        parts.reserve(sectors.size());
        for (ui32 sectorIdx = 0, first = 0; sectorIdx <= sectors.size(); sectorIdx++) {
            if (sectorIdx == sectors.size()
                || sectors[first].ChunkIdx != sectors[sectorIdx].ChunkIdx
                || sectors[first].SectorIdx + sectorIdx - first != sectors[sectorIdx].SectorIdx) {
                TRope data;
                ui32 partSize = (sectorIdx - (first == 0 ? 1 : first)) * SectorSize;
                if (first == 0) {
                    data = headerData;
                    ((TPersistentBufferHeader*)data.Begin().ContiguousDataMut())->HeaderChecksum = CalculateChecksum(data.Begin());
                }
                payload.ExtractFront(partSize, &data);
                parts.push_back({(ui32)sectors[first].ChunkIdx, sectors[first].SectorIdx * SectorSize, std::move(data)});
                first = sectorIdx;
            }
        }
        return parts;
    }

    void TDDiskActor::ProcessPersistentBufferQueue() {
        if (PendingPersistentBufferEvents.empty() || !PersistentBufferReady) {
            return;
        }
        STLOG_D("TDDiskActor::ProcessPersistentBufferQueue start processing",
            (PendingPersistentBufferEvents.size(), PendingPersistentBufferEvents.size()));
        while (!PendingPersistentBufferEvents.empty()) {
            auto temp = PendingPersistentBufferEvents.front().Release();
            PendingPersistentBufferEvents.pop();
            Counters.PersistentBuffer.PendingEventsQueueSize->Dec();
            auto size = PendingPersistentBufferEvents.size();
            Receive(temp);
            if (PendingPersistentBufferEvents.size() != size) {
                if (IssuePersistentBufferChunkAllocationInflight) {
                    STLOG_D("TDDiskActor::ProcessPersistentBufferQueue waiting next chunk allocation",
                        (PendingPersistentBufferEvents.size(), PendingPersistentBufferEvents.size()), (size, size));
                    return;
                }
                Y_ABORT("TDDiskActor::ProcessPersistentBufferQueue pending queue growth");
            }
        }
    }

    void TDDiskActor::RestorePersistentBufferChunk(TDDiskActor::TEvPrivate::TEvReadPersistentBufferPart::TPtr ev) {
        ui32 chunkIdx = ev->Get()->PartCookie;
        auto& data = ev->Get()->Data;
        PersistentBufferRestoreChunksInflight--;
        Y_ABORT_UNLESS(data.size() == ChunkSize);
        for (ui32 sectorIdx = 0; sectorIdx < SectorInChunk; sectorIdx++) {
            auto dataPos = data.Position(sectorIdx * SectorSize);
            auto headerData = TRcBuf::UninitializedPageAligned(SectorSize);
            auto sigPos = dataPos;
            sigPos.ExtractPlainDataAndAdvance(headerData.GetDataMut(), SectorSize);
            if (memcmp(headerData.GetDataMut(), TPersistentBufferHeader::PersistentBufferHeaderSignature, 16) == 0) {
                TPersistentBufferHeader* header = (TPersistentBufferHeader*)headerData.GetDataMut();
                ui64 headerChecksum = header->HeaderChecksum;
                header->HeaderChecksum = 0;
                TRope headerRope(std::move(headerData));
                ui64 sectorChecksum = CalculateChecksum(headerRope.Begin());
                if (headerChecksum != sectorChecksum || header->PersistentBufferUniqueId != PersistentBufferUniqueId
                    || (header->NodeId != BaseInfo.PDiskActorID.NodeId()) || header->PDiskId != BaseInfo.PDiskId
                    || header->SlotId != BaseInfo.VDiskSlotId) {
                    STLOG_E("TDDiskActor::StartRestorePersistentBuffer header checksum failed",
                        (TabletId, header->Record.TabletId), (VChunkIndex, header->Record.VChunkIndex),
                        (Lsn, header->Record.Lsn),
                        (header->PersistentBufferUniqueId, header->PersistentBufferUniqueId),
                        (PersistentBufferUniqueId, PersistentBufferUniqueId),
                        (headerChecksum, headerChecksum),
                        (sectorChecksum, sectorChecksum),
                        (header->NodeId, header->NodeId),
                        (NodeId, BaseInfo.PDiskActorID.NodeId()),
                        (header->PDiskId, header->PDiskId),
                        (PDiskId, BaseInfo.PDiskId),
                        (header->SlotId, header->SlotId),
                        (VDiskSlotId, BaseInfo.VDiskSlotId)
                    );
                    continue;
                }
                if (PersistentBufferBarriersManager.AddBarrier(header, chunkIdx, sectorIdx)) {
                    continue;
                }
                if (header->Flags & TPersistentBufferHeader::IS_ERASE) {
                    PersistentBufferBarriersManager.AddErase(header, chunkIdx, sectorIdx);
                    continue;
                }
                auto& buffer = PersistentBuffers[{header->Record.TabletId, header->Record.Generation}];
                auto [it, inserted] = buffer.Records.try_emplace(header->Record.Lsn);
                if (!inserted) {
                    STLOG_D("TDDiskActor::StartRestorePersistentBuffer duplicated lsn for tablet in persistent buffer", (TabletId, header->Record.TabletId), (VChunkIndex, header->Record.VChunkIndex), (Lsn, header->Record.Lsn));
                }
                TPersistentBuffer::TRecord& pr = it->second;
                pr = {
                    .OffsetInBytes = header->Record.OffsetInBytes,
                    .Size = header->Record.Size,
                    .VChunkIndex = header->Record.VChunkIndex,
                    .Timestamp = TInstant::Now()
                };
                ui32 sectorsCnt = header->Record.Size / SectorSize;
                pr.Sectors.reserve(sectorsCnt + 1);
                pr.Sectors.push_back({
                    .ChunkIdx = chunkIdx,
                    .SectorIdx = sectorIdx,
                });
                for (ui32 i = 0; i < sectorsCnt; i++) {
                    pr.Sectors.push_back(header->Record.Locations[i]);
                }
            } else {
                PersistentBufferSectorsChecksum[chunkIdx][sectorIdx] = CalculateChecksum(dataPos);
            }
        }

        StartRestorePersistentBuffer();
        if (PersistentBufferRestoreChunksInflight == 0) {
            for (auto& [_, pb] : PersistentBuffers) {
                std::erase_if(pb.Records, [this](const auto& pair) {
                    for (auto sector : pair.second.Sectors) {
                        if (PersistentBufferSectorsChecksum[sector.ChunkIdx][sector.SectorIdx] != sector.Checksum) {
                            return true;
                        }
                    }
                    return false;
                });
                for (auto [_, r] : pb.Records) {
                    pb.Size += r.Size;
                }
            }
            std::erase_if(PersistentBuffers, [](const auto& pb) { return pb.second.Records.empty(); });
            PersistentBufferSectorsChecksum.clear();

            PersistentBufferBarriersManager.RestoreBarriers(PersistentBuffers, PersistentBufferSpaceAllocator);
            PersistentBufferBarriersManager.RestoreErases(PersistentBuffers, PersistentBufferSpaceAllocator);

            for (auto& [_, pb] : PersistentBuffers) {
                for (auto& [__, record] : pb.Records) {
                    PersistentBufferSpaceAllocator.MarkOccupied(record.Sectors);
                }
            }

            STLOG_D("TDDiskActor::StartRestorePersistentBuffer ready");
            PersistentBufferReady = true;
            *Counters.PersistentBuffer.AllocatedChunks = PersistentBufferSpaceAllocator.OwnedChunks.size();
            *Counters.PersistentBuffer.TotalBytes =
                (PersistentBufferSpaceAllocator.OwnedChunks.size() * SectorInChunk - PersistentBufferSpaceAllocator.GetFreeSpace()) * SectorSize;
            ProcessPersistentBufferQueue();
        }
    }

    void TDDiskActor::Handle(TDDiskActor::TEvPrivate::TEvReadPersistentBufferPart::TPtr ev) {
        if (ev->Get()->IsRestore) {
            RestorePersistentBufferChunk(ev);
            return;
        }
        auto opCookie = ev->Get()->InflightCookie;
        auto partCookie = ev->Get()->PartCookie;

        auto itInflight = PersistentBufferDiskOperationInflight.find(opCookie);
        if (itInflight == PersistentBufferDiskOperationInflight.end()) {
            return;
        }
        auto& inflight = itInflight->second;
        auto status = ev->Get()->Status;
        if (status != NKikimrBlobStorage::NDDisk::TReplyStatus::OK) {
            inflight.Status = status;
            inflight.ErrorMessage = ev->Get()->ErrorMessage;
        }
        auto eraseCnt = inflight.OperationCookies.erase(partCookie);
        Y_ABORT_UNLESS(eraseCnt == 1);

        auto it = PersistentBuffers.find({inflight.TabletId, inflight.Generation});
        if (it == PersistentBuffers.end()) {
            inflight.Status = NKikimrBlobStorage::NDDisk::TReplyStatus::MISSING_RECORD;
        } else {
            auto recordIt = it->second.Records.find(inflight.Lsn);
            if (recordIt == it->second.Records.end()) {
                inflight.Status = NKikimrBlobStorage::NDDisk::TReplyStatus::MISSING_RECORD;
            } else {
                auto& pr = recordIt->second;
                inflight.DataParts.emplace(partCookie, std::move(ev->Get()->Data));
                if (inflight.OperationCookies.empty()) {
                    if (inflight.PartsCount == 0 || inflight.DataParts.size() != inflight.PartsCount
                        || inflight.Status != NKikimrBlobStorage::NDDisk::TReplyStatus::OK) {
                        inflight.Status = NKikimrBlobStorage::NDDisk::TReplyStatus::MISSING_RECORD;
                        ReplyReadPersistentBuffer(pr, inflight.Status, inflight.ErrorMessage);
                    } else {
                        pr.Data = std::move(inflight.JoinData(SectorSize));
                        PersistentBufferInMemoryCacheSize += pr.Size;
                        *Counters.PersistentBuffer.InMemoryCacheSize = PersistentBufferInMemoryCacheSize;

                        auto [_, inserted2] = PersistentBuffersInMemoryCacheUptime[pr.Timestamp].emplace(inflight.TabletId, inflight.Generation, inflight.Lsn);
                        Y_ABORT_UNLESS(inserted2);
                        ReplyReadPersistentBuffer(pr, inflight.Status, inflight.ErrorMessage);
                    }
                    return;
                }
            }
        }

        if (inflight.OperationCookies.empty()) {
            ReplyReadPersistentBuffer(opCookie);
        }
    }

    void TDDiskActor::HandleWritePart(TPersistentBufferDiskOperationInFlight& inflight, ui64 partCookie) {
        auto eraseCnt = inflight.OperationCookies.erase(partCookie);
        Y_ABORT_UNLESS(eraseCnt == 1);
        inflight.Span.Event("Part written");

        if (inflight.OperationCookies.empty()) {
            Counters.Interface.WritePersistentBuffer.Reply(!inflight.ErrorMessage, inflight.Size,
                HPMilliSecondsFloat(HPNow() - inflight.StartTs));
            if (!inflight.ErrorMessage) {
                auto& buffer = PersistentBuffers[{inflight.TabletId, inflight.Generation}];
                auto [it, inserted] = buffer.Records.try_emplace(inflight.Lsn);
                TPersistentBuffer::TRecord& pr = it->second;
                Y_ABORT_UNLESS(inflight.DataParts.size() == 1 && inflight.PartsCount == 1);
                Y_ABORT_UNLESS(inserted);
                pr = {
                    .OffsetInBytes = inflight.OffsetInBytes,
                    .Size = (ui32)inflight.DataParts.begin()->second.size(),
                    .Sectors = std::move(inflight.Sectors),
                    .VChunkIndex = inflight.VChunkIndex,
                    .Timestamp = TInstant::Now()
                };
                buffer.Size += pr.Size;
                pr.Data = std::move(inflight.DataParts.begin()->second);
                PersistentBufferInMemoryCacheSize += pr.Size;
                *Counters.PersistentBuffer.InMemoryCacheSize = PersistentBufferInMemoryCacheSize;
                auto [_, inserted2] = PersistentBuffersInMemoryCacheUptime[pr.Timestamp].emplace(inflight.TabletId, inflight.Generation, inflight.Lsn);
                Y_ABORT_UNLESS(inserted2);
                SanitizePersistentBufferInMemoryCache();
            } else {
                PersistentBufferSpaceAllocator.Free(inflight.OccupiedSectors);
            }
            auto it = PersistentBufferWriteInflightsByRecord.find({inflight.TabletId, inflight.Generation, inflight.Lsn});
            Y_ABORT_UNLESS(it != PersistentBufferWriteInflightsByRecord.end());
            Y_ABORT_UNLESS(!it->second.empty());
            auto status = inflight.Status;
            auto errorMessage = inflight.ErrorMessage;
            for (ui64 replyCookie : it->second) {
                auto replyIt = PersistentBufferDiskOperationInflight.find(replyCookie);
                Y_ABORT_UNLESS(replyIt != PersistentBufferDiskOperationInflight.end());
                auto& replyInflight = replyIt->second;
                auto replyEv = std::make_unique<TEvWritePersistentBufferResult>(
                    status, errorMessage, GetPersistentBufferFreeSpace(), NormalizedOccupancy);
                auto h = std::make_unique<IEventHandle>(replyInflight.Sender, SelfId(), replyEv.release(), 0, replyInflight.Cookie);
                if (replyInflight.Session) {
                    h->Rewrite(TEvInterconnect::EvForward, replyInflight.Session);
                }
                TActivationContext::Send(h.release());
                replyInflight.Span.End();
                PersistentBufferDiskOperationInflight.erase(replyIt);
            }
            PersistentBufferWriteInflightsByRecord.erase(it);
            *Counters.PersistentBuffer.TotalBytes =
                (PersistentBufferSpaceAllocator.OwnedChunks.size() * SectorInChunk - PersistentBufferSpaceAllocator.GetFreeSpace()) * SectorSize;
        }
    }

    void TDDiskActor::HandleErasePart(TPersistentBufferDiskOperationInFlight& inflight, ui64 opCookie, ui64 partCookie, bool resultStatus) {
        auto eraseCnt = inflight.OperationCookies.erase(partCookie);
        Y_ABORT_UNLESS(eraseCnt == 1);
        inflight.Span.Event("Part erased");
        if (resultStatus) {
            ClearPersistentBufferRecords(inflight, partCookie);
        }
        if (inflight.OperationCookies.empty()) {
            Counters.Interface.ErasePersistentBuffer.Reply(!inflight.ErrorMessage, inflight.Size,
                HPMilliSecondsFloat(HPNow() - inflight.StartTs));

            if (!inflight.ErrorMessage) {
                PersistentBufferSpaceAllocator.Free(inflight.Sectors);
            } else {
                PersistentBufferSpaceAllocator.Free(inflight.OccupiedSectors);
            }

            auto replyEv = std::make_unique<TEvErasePersistentBufferResult>(
                inflight.Status, inflight.ErrorMessage, GetPersistentBufferFreeSpace(), NormalizedOccupancy);
            auto h = std::make_unique<IEventHandle>(inflight.Sender, SelfId(), replyEv.release(), 0, inflight.Cookie);
            if (inflight.Session) {
                h->Rewrite(TEvInterconnect::EvForward, inflight.Session);
            }
            TActivationContext::Send(h.release());
            inflight.Span.End();
            PersistentBufferDiskOperationInflight.erase(opCookie);

            *Counters.PersistentBuffer.TotalBytes =
                (PersistentBufferSpaceAllocator.OwnedChunks.size() * SectorInChunk - PersistentBufferSpaceAllocator.GetFreeSpace()) * SectorSize;
        }
    }

    void TDDiskActor::Handle(TDDiskActor::TEvPrivate::TEvWritePersistentBufferPart::TPtr ev) {
        auto opCookie = ev->Get()->InflightCookie;
        auto partCookie = ev->Get()->PartCookie;
        auto status = ev->Get()->Status;
        auto errorMessage = ev->Get()->ErrorMessage;

        auto itInflight = PersistentBufferDiskOperationInflight.find(opCookie);
        if (itInflight == PersistentBufferDiskOperationInflight.end()) {
            return;
        }
        auto& inflight = itInflight->second;
        bool resultStatus = status == NKikimrBlobStorage::NDDisk::TReplyStatus::OK;
        if (!resultStatus) {
            inflight.Status = status;
            inflight.ErrorMessage = ev->Get()->ErrorMessage;
        }
        if (ev->Get()->IsErase) {
            auto itErase = inflight.Erases.find(partCookie);
            Y_ABORT_UNLESS(itErase != inflight.Erases.end());
            for (auto& e : itErase->second) {
                auto it = PersistentBufferEraseInflightsByRecord.find(TPersistentBufferRecordId{inflight.TabletId, std::get<1>(e), std::get<0>(e)});
                if (it == PersistentBufferEraseInflightsByRecord.end()) {
                    break;
                }
                Y_ABORT_UNLESS(it->second.EraseCookie == partCookie);
                for (auto opCookie2 : it->second.OperationsCookie) {
                    if (opCookie2 != opCookie) {
                        auto itInflight2 = PersistentBufferDiskOperationInflight.find(opCookie2);
                        if (itInflight2 == PersistentBufferDiskOperationInflight.end()) {
                            continue;
                        }
                        auto& inflight2 = itInflight2->second;
                        if (!resultStatus) {
                            inflight2.Status = status;
                            inflight2.ErrorMessage = ev->Get()->ErrorMessage;
                        }
                        HandleErasePart(inflight2, opCookie2, partCookie, resultStatus);
                    }
                }
                PersistentBufferEraseInflightsByRecord.erase(it);
            }
            HandleErasePart(inflight, opCookie, partCookie, resultStatus);
        } else {
            HandleWritePart(inflight, partCookie);
        }
    }

    void TDDiskActor::ProcessPersistentBufferWrite(TEvWritePersistentBuffer::TPtr ev) {
        const auto& record = ev->Get()->Record;
        const TQueryCredentials creds(record.GetCredentials());
        const TBlockSelector selector(record.GetSelector());
        const ui64 lsn = record.GetLsn();

        if (auto barrier = PersistentBufferBarriersManager.GetBarrier(creds.TabletId); lsn <= barrier) {
            STLOG(PRI_DEBUG, BS_DDISK, BSDD15, "TDDiskActor::ProcessPersistentBufferWrite write before barrier",
                (TabletId, creds.TabletId), (Generation, creds.Generation), (Lsn, lsn), (Barrier, barrier));
            SendReply(*ev, std::make_unique<TEvWritePersistentBufferResult>(
                NKikimrBlobStorage::NDDisk::TReplyStatus::OUTDATED,
                TStringBuilder() << "write before barrier"
                    << " tabletId# " << creds.TabletId
                    << " generation# " << creds.Generation
                    << " lsn# " << lsn
                    << " barrier# " << barrier));
            return;
        }

        ui32 sectorsCnt = selector.Size / SectorSize + 1;
        const TWriteInstruction instr(record.GetInstruction());
        TRope payload;
        if (instr.PayloadId) {
            payload = ev->Get()->GetPayload(*instr.PayloadId);
        }
        STLOG_I("TDDiskActor::ProcessPersistentBufferWrite",
            (TabletId, creds.TabletId), (Generation, creds.Generation), (Lsn, lsn));
        auto checkIsSameRequest = [&](auto &data) {
            bool dataEqual = data.Size == selector.Size;
            if (dataEqual) {
                for (ui32 i = 0; i < selector.Size / SectorSize; ++i) {
                    auto it = payload.Position(SectorSize * i);
                    if ((ui8)it.ContiguousData()[0] == TPersistentBufferHeader::PersistentBufferHeaderSignature[0]) {
                        *it.ContiguousDataMut() = 0;
                    }
                    if (data.Sectors[i + 1].Checksum != CalculateChecksum(it)) {
                        dataEqual = false;
                        break;
                    }
                }
            }

            if (!dataEqual || data.OffsetInBytes != selector.OffsetInBytes || data.Size != selector.Size
                || data.VChunkIndex != selector.VChunkIndex) {
                STLOG_D("TDDiskActor::ProcessPersistentBufferWrite duplicate record with incorrect data",
                    (TabletId, creds.TabletId), (Generation, creds.Generation), (Lsn, lsn));
                SendReply(*ev, std::make_unique<TEvWritePersistentBufferResult>(
                    NKikimrBlobStorage::NDDisk::TReplyStatus::INCORRECT_REQUEST,
                    TStringBuilder() << "duplicate record with incorrect data"));
                return false;
            }
            return true;
        };
        if (auto it = PersistentBuffers.find({creds.TabletId, creds.Generation}); it != PersistentBuffers.end()) {
            if (auto recordIt = it->second.Records.find(lsn); recordIt != it->second.Records.end()) {
                auto& record = recordIt->second;
                if (!checkIsSameRequest(record)) {
                    return;
                }
                STLOG_D("TDDiskActor::ProcessPersistentBufferWrite duplicate record",
                    (TabletId, creds.TabletId), (Generation, creds.Generation), (Lsn, lsn));
                SendReply(*ev, std::make_unique<TEvWritePersistentBufferResult>(NKikimrBlobStorage::NDDisk::TReplyStatus::OK));
                return;
            }

            if (selector.Size + it->second.Size > PersistentBufferFormat.PerTabletStorageLimit) {
                STLOG_D("TDDiskActor::ProcessPersistentBufferWrite tablet space occupation limit is reached",
                    (TabletId, creds.TabletId), (Generation, creds.Generation), (TabletSpaceOccupied, it->second.Size),
                    (WriteDataSize, selector.Size), (PerTabletStorageLimit, PersistentBufferFormat.PerTabletStorageLimit));
                SendReply(*ev, std::make_unique<TEvWritePersistentBufferResult>(
                    NKikimrBlobStorage::NDDisk::TReplyStatus::OVERFILL,
                    TStringBuilder() << "persistent buffer overfill "
                        << selector.Size << " bytes requested, current tablet space occupation " << it->second.Size << " bytes"));
                return;
            }
        }
        if (auto it = PersistentBufferWriteInflightsByRecord.find({creds.TabletId, creds.Generation, lsn}); it != PersistentBufferWriteInflightsByRecord.end()) {
            Y_ABORT_UNLESS(!it->second.empty());

            auto inflightIt = PersistentBufferDiskOperationInflight.find(*it->second.begin());
            Y_ABORT_UNLESS(inflightIt != PersistentBufferDiskOperationInflight.end());
            auto& inflight = inflightIt->second;

            if (!checkIsSameRequest(inflight)) {
                return;
            }

            auto span = NWilson::TSpan(TWilson::DDiskTopLevel, std::move(ev->TraceId), "DDisk.WritePersistentBuffer",
                NWilson::EFlags::NONE, TActivationContext::ActorSystem());
            NPrivate::AddMessageWaitAttributes(span);
            span
                .Attribute("tablet_id", static_cast<i64>(creds.TabletId))
                .Attribute("vchunk_index", static_cast<i64>(selector.VChunkIndex))
                .Attribute("offset_in_bytes", selector.OffsetInBytes)
                .Attribute("size", selector.Size)
                .Attribute("lsn", static_cast<i64>(lsn));

            auto opCookie = NextCookie++;
            auto& inflightRecord = PersistentBufferDiskOperationInflight[opCookie];
            inflightRecord = {
                .Sender = ev->Sender,
                .Cookie = ev->Cookie,
                .Session = ev->InterconnectSession,
                .Span = std::move(span),
            };
            it->second.emplace_back(opCookie);
            return;
        }
        auto sectors = PersistentBufferSpaceAllocator.Occupy(sectorsCnt);
        if (sectors.size() == 0) {
            if (PersistentBufferSpaceAllocator.OwnedChunks.size() < PersistentBufferFormat.MaxChunks) {
                if (PendingPersistentBufferEvents.size() >= PersistentBufferFormat.MaxPendingEventsQueueSize) {
                    STLOG_D("TDDiskActor::ProcessPersistentBufferWrite pending queue overfill", (PendingPersistentBufferEvents.size(), PendingPersistentBufferEvents.size()));
                    SendReply(*ev, std::make_unique<TEvWritePersistentBufferResult>(
                        NKikimrBlobStorage::NDDisk::TReplyStatus::OVERLOADED,
                        TStringBuilder() << "pending queue overfill, size:"
                            << PendingPersistentBufferEvents.size() << " limit: " << PersistentBufferFormat.MaxPendingEventsQueueSize));
                    return;
                }
                STLOG_D("TDDiskActor::ProcessPersistentBufferWrite pending",
                    (PendingPersistentBufferEvents.size(), PendingPersistentBufferEvents.size()));
                PendingPersistentBufferEvents.emplace(ev, "WaitingPersistentBufferWrite");
                Counters.PersistentBuffer.PendingEventsQueueSize->Inc();
                IssuePersistentBufferChunkAllocation();
            } else {
                STLOG_D("TDDiskActor::ProcessPersistentBufferWrite not enough space", (FreeSpace, PersistentBufferSpaceAllocator.GetFreeSpace() * SectorSize), (NeedSpace, sectorsCnt * SectorSize));
                SendReply(*ev, std::make_unique<TEvWritePersistentBufferResult>(
                    NKikimrBlobStorage::NDDisk::TReplyStatus::OVERFILL,
                    TStringBuilder() << "persistent buffer overfill "
                        << (sectorsCnt * SectorSize) << " bytes requested, free " << (PersistentBufferSpaceAllocator.GetFreeSpace() * SectorSize) << " bytes"));
            }
            return;
        }
        Y_ABORT_UNLESS(sectors.size() == sectorsCnt && sectorsCnt <= TPersistentBufferHeader::MaxSectorsPerBufferRecord + 1 && sectorsCnt > 1);

        auto span = NWilson::TSpan(TWilson::DDiskTopLevel, std::move(ev->TraceId), "DDisk.WritePersistentBuffer",
                NWilson::EFlags::NONE, TActivationContext::ActorSystem());
        NPrivate::AddMessageWaitAttributes(span);
        span
            .Attribute("tablet_id", static_cast<i64>(creds.TabletId))
            .Attribute("vchunk_index", static_cast<i64>(selector.VChunkIndex))
            .Attribute("offset_in_bytes", selector.OffsetInBytes)
            .Attribute("size", selector.Size)
            .Attribute("lsn", static_cast<i64>(lsn));
        Counters.Interface.WritePersistentBuffer.Request(selector.Size);

        auto parts = SlicePersistentBuffer(creds.TabletId, creds.Generation,
            selector.VChunkIndex, lsn, selector.OffsetInBytes, selector.Size, TRope(payload), sectors);

        auto opCookie = NextCookie++;
        auto& inflightRecord = PersistentBufferDiskOperationInflight[opCookie];
        inflightRecord = {
            .Sender = ev->Sender,
            .Cookie = ev->Cookie,
            .Session = ev->InterconnectSession,
            .Span = std::move(span),

            .TabletId = creds.TabletId,
            .Generation = creds.Generation,
            .VChunkIndex = selector.VChunkIndex,
            .Lsn = lsn,
            .OffsetInBytes = selector.OffsetInBytes,
            .Size = selector.Size,
            .Sectors = sectors,
            .DataParts = {{0, std::move(payload)}},
            .PartsCount = 1,
            .OccupiedSectors = std::move(sectors),
            .StartTs = HPNow(),
        };
        PersistentBufferWriteInflightsByRecord[TPersistentBufferRecordId{creds.TabletId, creds.Generation, lsn}].emplace_back(opCookie);

        // TODO: make uring to call flush just once
        for(auto& [chunkIdx, offset, data] : parts) {
            const ui64 cookie = NextCookie++;
            inflightRecord.OperationCookies.insert(cookie);
            auto diskOffset = DiskFormat->Offset(chunkIdx, 0, offset);
            std::unique_ptr<TDirectIoOpBase> op = AllocateOp<TPersistentBufferPartIoOp>();
            auto* partOp = static_cast<TPersistentBufferPartIoOp*>(op.get());
            partOp->SetCookie(opCookie);
            partOp->SetPartCookie(cookie);
            partOp->PrepareWrite(std::move(data), diskOffset, chunkIdx, offset);
            inflightRecord.Span.Event(
#if defined(__linux__)
                UringRouter ? "DirectUringOp" :
#endif
                "Send to pdisk");

            DirectUringOp(op);
        }
    }

    void TDDiskActor::Handle(TEvWritePersistentBuffer::TPtr ev) {
        if (!CheckQuery(*ev, &Counters.Interface.WritePersistentBuffer)) {
            return;
        }
        const auto& record = ev->Get()->Record;
        const TBlockSelector selector(record.GetSelector());
        if (selector.Size > TPersistentBufferHeader::MaxSectorsPerBufferRecord * SectorSize) {
            Counters.Interface.WritePersistentBuffer.Request(selector.Size);
            Counters.Interface.WritePersistentBuffer.Reply(false, selector.Size);
            STLOG_D("TDDiskActor::Handle(TEvWritePersistentBuffer) persistent buffer write limit", (selector.Size, selector.Size));
            SendReply(*ev, std::make_unique<TEvWritePersistentBufferResult>(
                NKikimrBlobStorage::NDDisk::TReplyStatus::INCORRECT_REQUEST,
                TStringBuilder() << "persistent buffer write limit "
                    << (TPersistentBufferHeader::MaxSectorsPerBufferRecord * SectorSize) << " bytes, received " << selector.Size << " bytes"));
            return;
        }
        if (!PersistentBufferReady) {
            if (PendingPersistentBufferEvents.size() >= PersistentBufferFormat.MaxPendingEventsQueueSize) {
                STLOG_D("TDDiskActor::Handle(TEvWritePersistentBuffer) pending queue overfill", (PendingPersistentBufferEvents.size(), PendingPersistentBufferEvents.size()));
                SendReply(*ev, std::make_unique<TEvWritePersistentBufferResult>(
                    NKikimrBlobStorage::NDDisk::TReplyStatus::OVERLOADED,
                    TStringBuilder() << "pending queue overfill, size:"
                        << PendingPersistentBufferEvents.size() << " limit: " << PersistentBufferFormat.MaxPendingEventsQueueSize));
                return;
            }
            STLOG_D("TDDiskActor::Handle(TEvWritePersistentBuffer) pending",
                (PendingPersistentBufferEvents.size(), PendingPersistentBufferEvents.size()));
            PendingPersistentBufferEvents.emplace(ev, "WaitingPersistentBufferWrite");
            Counters.PersistentBuffer.PendingEventsQueueSize->Inc();
            return;
        }
        ProcessPersistentBufferWrite(ev);
    }

    void TDDiskActor::Handle(TEvReadPersistentBuffer::TPtr ev) {
        const auto& record = ev->Get()->Record;
        const TQueryCredentials creds(record.GetCredentials());
        if ((!creds.FromPersistentBuffer || record.HasSelector()) &&
            !CheckQuery(*ev, &Counters.Interface.ReadPersistentBuffer)) {
            return;
        }

        if (!PersistentBufferReady) {
            if (PendingPersistentBufferEvents.size() >= PersistentBufferFormat.MaxPendingEventsQueueSize) {
                STLOG_D("TDDiskActor::Handle(TEvReadPersistentBuffer) pending queue overfill", (PendingPersistentBufferEvents.size(), PendingPersistentBufferEvents.size()));
                SendReply(*ev, std::make_unique<TEvReadPersistentBufferResult>(
                    NKikimrBlobStorage::NDDisk::TReplyStatus::OVERLOADED,
                    TStringBuilder() << "pending queue overfill, size:"
                        << PendingPersistentBufferEvents.size() << " limit: " << PersistentBufferFormat.MaxPendingEventsQueueSize));
                return;
            }
            STLOG_D("TDDiskActor::Handle(TEvReadPersistentBuffer) pending",
                (PendingPersistentBufferEvents.size(), PendingPersistentBufferEvents.size()));
            PendingPersistentBufferEvents.emplace(ev, "WaitingPersistentBufferRead");
            Counters.PersistentBuffer.PendingEventsQueueSize->Inc();
            return;
        }

        const ui64 lsn = record.GetLsn();
        const ui32 generation = record.GetGeneration();
        TBlockSelector selector(record.GetSelector());

        STLOG_I("TDDiskActor::Handle(TEvReadPersistentBuffer)",
            (TabletId, creds.TabletId), (Generation, creds.Generation), (Lsn, lsn));

        Counters.Interface.ReadPersistentBuffer.Request(selector.Size);

        auto span = NWilson::TSpan(TWilson::DDiskTopLevel, std::move(ev->TraceId), "DDisk.ReadPersistentBuffer",
                NWilson::EFlags::NONE, TActivationContext::ActorSystem());
        NPrivate::AddMessageWaitAttributes(span);
        span
            .Attribute("tablet_id", static_cast<i64>(creds.TabletId))
            .Attribute("vchunk_index", static_cast<i64>(selector.VChunkIndex))
            .Attribute("offset_in_bytes", selector.OffsetInBytes)
            .Attribute("size", selector.Size)
            .Attribute("lsn", static_cast<i64>(lsn));

        auto it = PersistentBuffers.find({creds.TabletId, generation});
        if (it == PersistentBuffers.end()) {
            Counters.Interface.ReadPersistentBuffer.Reply(false, selector.Size);
            span.End();
            SendReply(*ev, std::make_unique<TEvReadPersistentBufferResult>(
                NKikimrBlobStorage::NDDisk::TReplyStatus::MISSING_RECORD));
            return;
        }
        TPersistentBuffer& buffer = it->second;

        auto jt = buffer.Records.find(lsn);
        if (jt == buffer.Records.end()) {
            Counters.Interface.ReadPersistentBuffer.Reply(false, selector.Size);
            span.End();
            SendReply(*ev, std::make_unique<TEvReadPersistentBufferResult>(
                NKikimrBlobStorage::NDDisk::TReplyStatus::MISSING_RECORD));
            return;
        }
        TPersistentBuffer::TRecord& pr = jt->second;

        if (!record.HasSelector()) {
            selector.VChunkIndex = pr.VChunkIndex;
            selector.OffsetInBytes = pr.OffsetInBytes;
            selector.Size = pr.Size;
        }

        if (pr.OffsetInBytes > selector.OffsetInBytes ||
            pr.OffsetInBytes + pr.Size < selector.Size + selector.OffsetInBytes) {
            Counters.Interface.ReadPersistentBuffer.Reply(false, selector.Size);
            span.End();
            SendReply(*ev, std::make_unique<TEvReadPersistentBufferResult>(
                NKikimrBlobStorage::NDDisk::TReplyStatus::INCORRECT_REQUEST, "Selector range is out of record bounds"));
            return;
        }

        ui64 operationCookie = NextCookie++;
        auto [inflightIt, inserted] = PersistentBufferDiskOperationInflight.try_emplace(operationCookie, TPersistentBufferDiskOperationInFlight{
            .Sender = ev->Sender,
            .Cookie = ev->Cookie,
            .Session = ev->InterconnectSession,
            .Span = std::move(span),

            .TabletId = creds.TabletId,
            .Generation = generation,
            .VChunkIndex = selector.VChunkIndex,
            .Lsn = lsn,
            .OffsetInBytes = selector.OffsetInBytes,
            .Size = selector.Size,
            .PartsCount = 0,
            .StartTs = HPNow(),
        });
        Y_ABORT_UNLESS(inserted);
        pr.ReadInflight.insert(operationCookie);
        if (pr.ReadInflight.size() > 1) {
            return;
        }
        if (pr.Data.size() > 0) {
            Y_ABORT_UNLESS(pr.Data.size() == pr.Size);
            ReplyReadPersistentBuffer(pr, NKikimrBlobStorage::NDDisk::TReplyStatus::OK, {});
        } else {
            Y_ABORT_UNLESS(pr.Sectors.size() > 1);
            // Zero sector contains persistent buffer header, we skip it
            for (ui32 sectorIdx = 1, first = 1; sectorIdx <= pr.Sectors.size(); sectorIdx++) {
                if (sectorIdx == pr.Sectors.size()
                    || pr.Sectors[first].ChunkIdx != pr.Sectors[sectorIdx].ChunkIdx
                    || pr.Sectors[first].SectorIdx + sectorIdx - first != pr.Sectors[sectorIdx].SectorIdx) {
                    const ui64 cookie = NextCookie++;
                    inflightIt->second.OperationCookies.emplace(cookie);
                    std::unique_ptr<TDirectIoOpBase> op = AllocateOp<TPersistentBufferPartIoOp>();
                    auto* partOp = static_cast<TPersistentBufferPartIoOp*>(op.get());
                    partOp->SetCookie(operationCookie);
                    partOp->SetPartCookie(cookie);
                    auto dataOffset = pr.Sectors[first].SectorIdx * SectorSize;
                    auto size = (pr.Sectors[sectorIdx - 1].SectorIdx - pr.Sectors[first].SectorIdx + 1) * SectorSize;
                    auto offset = DiskFormat->Offset(pr.Sectors[first].ChunkIdx, 0, dataOffset);
                    op->PrepareRead(size, offset, pr.Sectors[first].ChunkIdx, dataOffset);
                    DirectUringOp(op);
                    inflightIt->second.PartsCount++;
                    first = sectorIdx;
                }
            }
        }
    }

    TRope TrimData(TRope data, ui32 offset, ui32 size, ui32 selectorOffset, ui32 selectorSize) {
        Y_ABORT_UNLESS(selectorOffset >= offset && offset + size >= selectorOffset + selectorSize);
        Y_ABORT_UNLESS(data.size() == size);
        Y_ABORT_UNLESS(data.size() >= selectorOffset - offset + selectorSize);
        auto start = data.Begin() + (selectorOffset - offset);
        return data.Extract(start, start + selectorSize);
    }

    void TDDiskActor::ReplyReadPersistentBuffer(TPersistentBuffer::TRecord& pr, NKikimrBlobStorage::NDDisk::TReplyStatus::E status, std::optional<TString> errorMessage) {
        for (auto inflightCookie : pr.ReadInflight) {
            auto inflightIt = PersistentBufferDiskOperationInflight.find(inflightCookie);
            Y_ABORT_UNLESS(inflightIt != PersistentBufferDiskOperationInflight.end());
            auto& inflight = inflightIt->second;
            TRope data;
            if (pr.Data) {
                data = std::move(TrimData(pr.Data, pr.OffsetInBytes, pr.Size, inflight.OffsetInBytes, inflight.Size));
            }
            auto replyEv = std::make_unique<TEvReadPersistentBufferResult>(status, errorMessage,
                pr.VChunkIndex, pr.OffsetInBytes, pr.Size, std::move(data));
            auto h = std::make_unique<IEventHandle>(inflight.Sender, SelfId(), replyEv.release(), 0, inflight.Cookie);
            if (inflight.Session) {
                h->Rewrite(TEvInterconnect::EvForward, inflight.Session);
            }
            TActivationContext::Send(h.release());

            const bool ok = (status == NKikimrBlobStorage::NDDisk::TReplyStatus::OK);
            Counters.Interface.ReadPersistentBuffer.Reply(ok, inflight.Size,
                HPMilliSecondsFloat(HPNow() - inflight.StartTs));
            inflight.Span.End();
            PersistentBufferDiskOperationInflight.erase(inflightIt);
        }
        pr.ReadInflight.clear();
        SanitizePersistentBufferInMemoryCache();
    }

    void TDDiskActor::ReplyReadPersistentBuffer(ui64 operationCookie) {
        auto inflightIt = PersistentBufferDiskOperationInflight.find(operationCookie);
        Y_ABORT_UNLESS(inflightIt != PersistentBufferDiskOperationInflight.end());
        auto& inflight = inflightIt->second;

        auto replyEv = std::make_unique<TEvReadPersistentBufferResult>(inflight.Status, inflight.ErrorMessage);
        auto h = std::make_unique<IEventHandle>(inflight.Sender, SelfId(), replyEv.release(), 0, inflight.Cookie);
        if (inflight.Session) {
            h->Rewrite(TEvInterconnect::EvForward, inflight.Session);
        }
        TActivationContext::Send(h.release());

        Counters.Interface.ReadPersistentBuffer.Reply(false, inflight.Size,
            HPMilliSecondsFloat(HPNow() - inflight.StartTs));
        inflight.Span.End();
        PersistentBufferDiskOperationInflight.erase(inflightIt);
    }

    ui64 TDDiskActor::CalcPersistentBufferInMemoryCacheSize() {
        ui64 res = 0;
        for (auto& [_, v] : PersistentBuffers) {
            for (auto& [__, r] : v.Records) {
                if (r.Size == r.Data.size()) {
                    res += r.Size;
                }
            }
        }
        return res;
    }

    void TDDiskActor::SanitizePersistentBufferInMemoryCache() {
        while (PersistentBufferInMemoryCacheSize > PersistentBufferFormat.MaxInMemoryCache) {
            Y_DEBUG_ABORT_UNLESS(PersistentBufferInMemoryCacheSize == CalcPersistentBufferInMemoryCacheSize());
            auto recordIt = PersistentBuffersInMemoryCacheUptime.begin();
            Y_ABORT_UNLESS(recordIt != PersistentBuffersInMemoryCacheUptime.end());
            auto lsnIt = recordIt->second.begin();
            Y_ABORT_UNLESS(lsnIt != recordIt->second.end());
            auto& pb = PersistentBuffers.at({lsnIt->TabletId, lsnIt->Generation});
            auto& pr = pb.Records.at(lsnIt->Lsn);
            Y_ABORT_UNLESS(pr.Data.size() == pr.Size);
            PersistentBufferInMemoryCacheSize -= pr.Size;
            *Counters.PersistentBuffer.InMemoryCacheSize = PersistentBufferInMemoryCacheSize;
            recordIt->second.erase(lsnIt);
            if (recordIt->second.empty()) {
                PersistentBuffersInMemoryCacheUptime.erase(recordIt);
            }
            pr.Data.clear();
        }
    }

    void TDDiskActor::SanitizePersistentBufferInMemoryCache(ui64 tabletId, ui32 generation, ui64 lsn, TPersistentBuffer::TRecord& record) {
        if (!record.Data.empty()) {
            Y_ABORT_UNLESS(record.Data.size() == record.Size);
            Y_DEBUG_ABORT_UNLESS(PersistentBufferInMemoryCacheSize == CalcPersistentBufferInMemoryCacheSize());
            Y_ABORT_UNLESS(PersistentBufferInMemoryCacheSize >= record.Size);
            PersistentBufferInMemoryCacheSize -= record.Size;
            *Counters.PersistentBuffer.InMemoryCacheSize = PersistentBufferInMemoryCacheSize;
            auto icuIt = PersistentBuffersInMemoryCacheUptime.find(record.Timestamp);
            Y_ABORT_UNLESS(icuIt != PersistentBuffersInMemoryCacheUptime.end());
            auto count = icuIt->second.erase(TPersistentBufferRecordId{tabletId, generation, lsn});
            Y_ABORT_UNLESS(count == 1);
            if (icuIt->second.empty()) {
                PersistentBuffersInMemoryCacheUptime.erase(icuIt);
            }

            record.Data.clear();
        }
    }

    TRope TDDiskActor::TPersistentBufferDiskOperationInFlight::JoinData(ui32 sectorSize) {
        Y_ABORT_UNLESS(DataParts.size() == PartsCount && PartsCount > 0);
        while (DataParts.size() > 1) {
            auto first = DataParts.begin();
            auto second = std::next(first);
            first->second.Insert(first->second.Begin(), std::move(second->second));
            DataParts.erase(second);
        }
        PartsCount = 1;
        auto& payload = DataParts.begin()->second;
        for (ui32 i = 1; i < Sectors.size(); i++) {
            if (Sectors[i].HasSignatureCorrection) {
                *payload.Position(sectorSize * (i - 1)).ContiguousDataMut() = TPersistentBufferHeader::PersistentBufferHeaderSignature[0];
            }
        }
        return DataParts.begin()->second;
    }

    void TDDiskActor::BarrierErasePersistentBuffer(IEventHandle& queryEv, const TQueryCredentials& creds, const std::vector<std::tuple<ui64, ui32>>& erases, ui64 lsn) {
        Counters.Interface.ErasePersistentBuffer.Request(0);
        STLOG_I("TDDiskActor::BarrierErasePersistentBuffer", (tabletId, creds.TabletId), (lsn, lsn));
        auto span = std::move(NWilson::TSpan(TWilson::DDiskTopLevel, std::move(queryEv.TraceId), "DDisk.BarrierErasePersistentBuffer",
                NWilson::EFlags::NONE, TActivationContext::ActorSystem())
            .Attribute("tablet_id", static_cast<long>(creds.TabletId)));
        const ui64 barrierEraseCookie = NextCookie++;
        auto [inflightRecord, inserted] = PersistentBufferDiskOperationInflight.try_emplace(barrierEraseCookie, TPersistentBufferDiskOperationInFlight{
            .Sender = queryEv.Sender,
            .Cookie = queryEv.Cookie,
            .Session = queryEv.InterconnectSession,
            .Span = std::move(span),
            .TabletId = creds.TabletId,
            .StartTs = HPNow(),
        });
        Y_ABORT_UNLESS(inserted);

        const auto sectors = PersistentBufferSpaceAllocator.Occupy(1);
        Y_ABORT_UNLESS(sectors.size() == 1);
        auto [oldChunkIdx, oldSectorIdx, barrier] = PersistentBufferBarriersManager.MoveBarrier(creds.TabletId, lsn, sectors[0]);

        if (oldChunkIdx != Max<ui32>()) {
            inflightRecord->second.Sectors.push_back({.ChunkIdx = oldChunkIdx, .SectorIdx = oldSectorIdx});
        }
        const ui64 cookie = NextCookie++;
        inflightRecord->second.OperationCookies.insert(cookie);
        inflightRecord->second.Erases[cookie] = erases;
        auto headerData = TRcBuf::UninitializedPageAligned(SectorSize);
        barrier.Header.HeaderChecksum = 0;
        memcpy(headerData.GetDataMut(), &barrier.Header, sizeof(TPersistentBufferHeader));
        TRope headerRope(headerData);
        ((TPersistentBufferHeader*)headerRope.Begin().ContiguousDataMut())->HeaderChecksum = CalculateChecksum(headerRope.Begin());
        inflightRecord->second.OccupiedSectors.emplace_back(TPersistentBufferSectorInfo{barrier.ChunkIdx, barrier.SectorIdx, 0, 0, 0});

        auto chunkOffset = barrier.SectorIdx * SectorSize;
        auto diskOffset = DiskFormat->Offset(barrier.ChunkIdx, 0, chunkOffset);
        std::unique_ptr<TDirectIoOpBase> op = AllocateOp<TPersistentBufferPartIoOp>();
        auto* partOp = static_cast<TPersistentBufferPartIoOp*>(op.get());
        partOp->SetCookie(barrierEraseCookie);
        partOp->SetPartCookie(cookie);
        partOp->SetIsErase(true);
        partOp->PrepareWrite(std::move(headerRope), diskOffset, barrier.ChunkIdx, chunkOffset);
        DirectUringOp(op);
    }

    void TDDiskActor::ErasePersistentBuffer(IEventHandle& queryEv, const TQueryCredentials& creds, const std::vector<std::tuple<ui64, ui32>>& erases) {
        Counters.Interface.ErasePersistentBuffer.Request(0);

        auto span = NWilson::TSpan(TWilson::DDiskTopLevel, std::move(queryEv.TraceId), "DDisk.ErasePersistentBuffer",
                NWilson::EFlags::NONE, TActivationContext::ActorSystem());
        NPrivate::AddMessageWaitAttributes(span);
        span.Attribute("tablet_id", static_cast<i64>(creds.TabletId));

        const ui64 batchEraseCookie = NextCookie++;

        auto [inflightRecord, inserted] = PersistentBufferDiskOperationInflight.try_emplace(batchEraseCookie, TPersistentBufferDiskOperationInFlight{
            .Sender = queryEv.Sender,
            .Cookie = queryEv.Cookie,
            .Session = queryEv.InterconnectSession,
            .Span = std::move(span),
            .TabletId = creds.TabletId,
            .StartTs = HPNow(),
        });
        Y_ABORT_UNLESS(inserted);

        // TODO: flush once
        for (auto& e : erases) {
            auto lsn = std::get<0>(e);
            auto generation = std::get<1>(e);
            STLOG_I("TDDiskActor::ErasePersistentBuffer", (tabletId, creds.TabletId), (lsn, lsn), (generation, generation));

            const auto it = PersistentBuffers.find({creds.TabletId, generation});
            TPersistentBuffer& buffer = it->second;
            const auto jt = buffer.Records.find(lsn);
            TPersistentBuffer::TRecord& pr = jt->second;

            auto &itErase = PersistentBufferEraseInflightsByRecord[TPersistentBufferRecordId{creds.TabletId, generation, lsn}];
            if (!itErase.OperationsCookie.empty()) {
                inflightRecord->second.OperationCookies.insert(itErase.EraseCookie);
                inflightRecord->second.Erases[itErase.EraseCookie].push_back(e);
                itErase.OperationsCookie.push_back(batchEraseCookie);
                continue;
            }

            const ui64 cookie = NextCookie++;
            itErase.EraseCookie = cookie;
            itErase.OperationsCookie.push_back(batchEraseCookie);
            inflightRecord->second.Erases[cookie].push_back(e);
            inflightRecord->second.OperationCookies.insert(cookie);

            auto zeroingData = TRcBuf::UninitializedPageAligned(SectorSize);
            memset(zeroingData.GetDataMut(), 0, SectorSize);

            auto chunkOffset = pr.Sectors[0].SectorIdx * SectorSize;
            auto diskOffset = DiskFormat->Offset(pr.Sectors[0].ChunkIdx, 0, chunkOffset);
            std::unique_ptr<TDirectIoOpBase> op = AllocateOp<TPersistentBufferPartIoOp>();
            auto* partOp = static_cast<TPersistentBufferPartIoOp*>(op.get());
            partOp->SetCookie(batchEraseCookie);
            partOp->SetPartCookie(cookie);
            partOp->SetIsErase(true);
            partOp->PrepareWrite(TRope(zeroingData), diskOffset, pr.Sectors[0].ChunkIdx, chunkOffset);

            DirectUringOp(op);
        }
    }

    void TDDiskActor::FastErasePersistentBuffer(IEventHandle& queryEv, const TQueryCredentials& creds, const std::vector<std::tuple<ui64, ui32>>& erases, const TFastErase& fastErase) {
        Counters.Interface.ErasePersistentBuffer.Request(0);

        auto span = NWilson::TSpan(TWilson::DDiskTopLevel, std::move(queryEv.TraceId), "DDisk.FastErasePersistentBuffer",
                NWilson::EFlags::NONE, TActivationContext::ActorSystem());
        NPrivate::AddMessageWaitAttributes(span);
        span.Attribute("tablet_id", static_cast<i64>(creds.TabletId));

        const ui64 fastEraseCookie = NextCookie++;

        auto [inflightRecord, inserted] = PersistentBufferDiskOperationInflight.try_emplace(fastEraseCookie, TPersistentBufferDiskOperationInFlight{
            .Sender = queryEv.Sender,
            .Cookie = queryEv.Cookie,
            .Session = queryEv.InterconnectSession,
            .Span = std::move(span),
            .TabletId = creds.TabletId,
            .StartTs = HPNow(),
        });
        Y_ABORT_UNLESS(inserted);

        if (fastErase.OldChunkIdx != Max<ui32>()) {
            inflightRecord->second.Sectors.push_back({.ChunkIdx = fastErase.OldChunkIdx, .SectorIdx = fastErase.OldSectorIdx});
        }
        const ui64 cookie = NextCookie++;
        inflightRecord->second.OperationCookies.insert(cookie);
        inflightRecord->second.Erases[cookie] = erases;
        auto fastEraseData = TRcBuf::UninitializedPageAligned(SectorSize);
        memset(fastEraseData.GetDataMut(), 0, SectorSize);
        memcpy(fastEraseData.GetDataMut(), &fastErase.Header, sizeof(TPersistentBufferHeader));
        TRope headerRope(std::move(fastEraseData));
        ((TPersistentBufferHeader*)headerRope.Begin().ContiguousDataMut())->HeaderChecksum = CalculateChecksum(headerRope.Begin());

        inflightRecord->second.OccupiedSectors.emplace_back(TPersistentBufferSectorInfo{fastErase.ChunkIdx, fastErase.SectorIdx, 0, 0, 0});
        auto chunkOffset = fastErase.SectorIdx * SectorSize;
        auto diskOffset = DiskFormat->Offset(fastErase.ChunkIdx, 0, chunkOffset);
        std::unique_ptr<TDirectIoOpBase> op = AllocateOp<TPersistentBufferPartIoOp>();
        auto* partOp = static_cast<TPersistentBufferPartIoOp*>(op.get());
        partOp->SetCookie(fastEraseCookie);
        partOp->SetPartCookie(cookie);
        partOp->SetIsErase(true);
        partOp->PrepareWrite(std::move(headerRope), diskOffset, fastErase.ChunkIdx, chunkOffset);
        inflightRecord->second.Span.Event(
#if defined(__linux__)
            UringRouter ? "DirectUringOp" :
#endif
            "Send to pdisk");
        DirectUringOp(op);
    }

    void TDDiskActor::ClearPersistentBufferRecords(TPersistentBufferDiskOperationInFlight& inflight, ui64 partCookie) {
        auto erases = inflight.Erases.find(partCookie);
        Y_ABORT_UNLESS(erases != inflight.Erases.end());
        for (auto& e : erases->second) {
            auto lsn = std::get<0>(e);
            auto generation = std::get<1>(e);
            STLOG_I("TDDiskActor::ClearPersistentBufferRecords", (tabletId, inflight.TabletId), (lsn, lsn), (generation, generation));

            const auto it = PersistentBuffers.find({inflight.TabletId, generation});
            if (it == PersistentBuffers.end()) {
                continue;
            }
            TPersistentBuffer& buffer = it->second;
            const auto jt = buffer.Records.find(lsn);
            if (jt == buffer.Records.end()) {
                continue;
            }
            TPersistentBuffer::TRecord& pr = jt->second;
            SanitizePersistentBufferInMemoryCache(inflight.TabletId, generation, lsn, pr);

            PersistentBufferSpaceAllocator.Free(pr.Sectors);

            buffer.Size -= pr.Size;
            for (auto readCookie : pr.ReadInflight) {
                auto it = PersistentBufferDiskOperationInflight.find(readCookie);
                if (it != PersistentBufferDiskOperationInflight.end()) {
                    it->second.Status = NKikimrBlobStorage::NDDisk::TReplyStatus::MISSING_RECORD;
                    it->second.ErrorMessage = "Record is erased during read operation inflight";
                    ReplyReadPersistentBuffer(readCookie);
                    PersistentBufferDiskOperationInflight.erase(it);
                }
            }

            buffer.Records.erase(jt);
            if (buffer.Records.empty()) {
                PersistentBuffers.erase(it);
            }
        }
    }

    void TDDiskActor::Handle(TEvBatchErasePersistentBuffer::TPtr ev) {
        if (!CheckQuery(*ev, &Counters.Interface.ErasePersistentBuffer)) {
            return;
        }
        if (!PersistentBufferReady) {
            if (PendingPersistentBufferEvents.size() >= PersistentBufferFormat.MaxPendingEventsQueueSize) {
                STLOG_D("TDDiskActor::Handle(TEvBatchErasePersistentBuffer) pending queue overfill", (PendingPersistentBufferEvents.size(), PendingPersistentBufferEvents.size()));
                SendReply(*ev, std::make_unique<TEvErasePersistentBufferResult>(
                    NKikimrBlobStorage::NDDisk::TReplyStatus::OVERLOADED,
                    TStringBuilder() << "pending queue overfill, size:"
                        << PendingPersistentBufferEvents.size() << " limit: " << PersistentBufferFormat.MaxPendingEventsQueueSize));
                return;
            }
            STLOG_I("TDDiskActor::Handle(TEvBatchErasePersistentBuffer) pending",
                (PendingPersistentBufferEvents.size(), PendingPersistentBufferEvents.size()));
            PendingPersistentBufferEvents.emplace(ev, "WaitingPersistentBufferBatchErase");
            Counters.PersistentBuffer.PendingEventsQueueSize->Inc();
            return;
        }

        const auto& record = ev->Get()->Record;
        const TQueryCredentials creds(record.GetCredentials());

        std::vector<std::tuple<ui64, ui32>> erases;
        std::set<ui64> fastErases;

        for (auto& e : record.GetErases()) {
            auto lsn = e.GetLsn();
            auto generation = e.GetGeneration();
            const auto it = PersistentBuffers.find({creds.TabletId, generation});
            if (it != PersistentBuffers.end() && it->second.Records.find(lsn) != it->second.Records.end()) {
                erases.emplace_back(lsn, generation);
                if (PersistentBufferFormat.EnableFastErases) {
                    fastErases.insert(lsn);
                }
            }
        }

        if (erases.empty()) {
            SendReply(*ev, std::make_unique<TEvErasePersistentBufferResult>(
                NKikimrBlobStorage::NDDisk::TReplyStatus::OK, std::nullopt, GetPersistentBufferFreeSpace(), NormalizedOccupancy));
            return;
        }
        if (!PersistentBufferFormat.EnableFastErases) {
            ErasePersistentBuffer(*ev, creds, erases);
            return;
        }
        if (auto fastErase = PersistentBufferBarriersManager.Erase(creds.TabletId, fastErases, PersistentBufferSpaceAllocator); fastErase) {
            FastErasePersistentBuffer(*ev, creds, erases, fastErase.value());
        } else {
            ErasePersistentBuffer(*ev, creds, erases);
        }
    }

    void TDDiskActor::Handle(TEvErasePersistentBuffer::TPtr ev) {
        if (!CheckQuery(*ev, &Counters.Interface.ErasePersistentBuffer)) {
            return;
        }

        if (!PersistentBufferReady) {
            if (PendingPersistentBufferEvents.size() >= PersistentBufferFormat.MaxPendingEventsQueueSize) {
                STLOG_D("TDDiskActor::Handle(TEvErasePersistentBuffer) pending queue overfill", (PendingPersistentBufferEvents.size(), PendingPersistentBufferEvents.size()));
                SendReply(*ev, std::make_unique<TEvErasePersistentBufferResult>(
                    NKikimrBlobStorage::NDDisk::TReplyStatus::OVERLOADED,
                    TStringBuilder() << "pending queue overfill, size:"
                        << PendingPersistentBufferEvents.size() << " limit: " << PersistentBufferFormat.MaxPendingEventsQueueSize));
                return;
            }
            STLOG_I("TDDiskActor::Handle(TEvErasePersistentBuffer) pending",
                (PendingPersistentBufferEvents.size(), PendingPersistentBufferEvents.size()));
            PendingPersistentBufferEvents.emplace(ev, "WaitingPersistentBufferErase");
            Counters.PersistentBuffer.PendingEventsQueueSize->Inc();
            return;
        }

        const auto& record = ev->Get()->Record;
        const TQueryCredentials creds(record.GetCredentials());
        const ui64 lsn = record.GetLsn();

        std::vector<std::tuple<ui64, ui32>> erases;

        for (auto it = PersistentBuffers.lower_bound({creds.TabletId, 0}); it != PersistentBuffers.end() &&
                std::get<0>(it->first) == creds.TabletId; ++it) {
            const TPersistentBuffer& buffer = it->second;
            auto recordIt = buffer.Records.begin();
            while (recordIt != buffer.Records.end() && recordIt->first <= lsn) {
                erases.emplace_back(recordIt->first, std::get<1>(it->first));
                recordIt++;
            }
        }

        if (PersistentBufferSpaceAllocator.GetFreeSpace() < 2) {
            SendReply(*ev, std::make_unique<TEvErasePersistentBufferResult>(
                NKikimrBlobStorage::NDDisk::TReplyStatus::OVERFILL, "not enough free space to move barrier"));
            return;
        }
        if (!PersistentBufferBarriersManager.CanMoveBarrier(creds.TabletId, PersistentBufferFormat.MaxBarriersLimit)) {
            SendReply(*ev, std::make_unique<TEvErasePersistentBufferResult>(
                NKikimrBlobStorage::NDDisk::TReplyStatus::OVERFILL, "barrier can not be moved"));
            return;
        }

        BarrierErasePersistentBuffer(*ev, creds, erases, lsn);
    }

    void TDDiskActor::Handle(TEvGetPersistentBufferInfo::TPtr ev) {
        STLOG_D("TDDiskActor::Handle(TEvGetPersistentBufferInfo)",
            (Sender, ev->Sender), (cookie, ev->Cookie));
        auto reply = std::make_unique<TEvPersistentBufferInfo>();
        reply->StartedAt = StartedAt;
        reply->AllocatedChunks = PersistentBufferAllocatedChunks.size();
        reply->ChunkSize = DiskFormat->ChunkSize;
        reply->MaxChunks = PersistentBufferFormat.MaxChunks;
        reply->SectorSize = SectorSize;
        reply->FreeSectors = PersistentBufferSpaceAllocator.GetFreeSpace();
        reply->InMemoryCacheSize = PersistentBufferInMemoryCacheSize;
        reply->InMemoryCacheLimit = PersistentBufferFormat.MaxInMemoryCache;
        reply->DiskOperationsInflight = PersistentBufferDiskOperationInflight.size();
        reply->PendingEvents = PendingPersistentBufferEvents.size();
        reply->PerTabletStorageLimit = PersistentBufferFormat.PerTabletStorageLimit;

        auto fillOpStats = [&](const TString& name, const auto& opCounters) {
            TEvPersistentBufferInfo::TOpStats stats;
            stats.Name = name;

            if (opCounters.Requests && opCounters.ReplyOk && opCounters.ReplyErr) {
                const ui64 reqs = opCounters.Requests->Val();
                const ui64 done = opCounters.ReplyOk->Val() + opCounters.ReplyErr->Val();
                stats.RequestsInFlight = reqs > done ? reqs - done : 0;
            }

            auto histIt = PbStatsHistory.find(name);
            if (histIt == PbStatsHistory.end() || histIt->second.size() < 2) {
                reply->OpStats.push_back(std::move(stats));
                return;
            }
            const auto& dq = histIt->second;
            const TPbOpSnapshot& cur = dq.back();
            // Choose the oldest snapshot that is still within (cur.Timestamp - PbStatsWindow).
            // If we don't have a full window of history yet, fall back to the oldest snapshot.
            const TInstant target = cur.Timestamp - PbStatsWindow;
            const TPbOpSnapshot* base = &dq.front();
            for (const auto& s : dq) {
                if (s.Timestamp <= target) {
                    base = &s;
                } else {
                    break;
                }
            }
            const TDuration dt = cur.Timestamp - base->Timestamp;
            const double dtSec = dt.SecondsFloat();
            if (dtSec <= 0) {
                reply->OpStats.push_back(std::move(stats));
                return;
            }

            const ui64 dReq = cur.Requests >= base->Requests ? cur.Requests - base->Requests : 0;
            stats.Requests = dReq;
            stats.WindowSeconds = dtSec;

            if (!opCounters.ResponseTime || cur.BucketCounts.empty()) {
                reply->OpStats.push_back(std::move(stats));
                return;
            }
            auto histSnap = opCounters.ResponseTime->Snapshot();
            const ui32 nBuckets = histSnap->Count();
            std::vector<ui64> deltaBuckets(nBuckets, 0);
            ui64 total = 0;
            for (ui32 i = 0; i < nBuckets; ++i) {
                const ui64 curVal = i < cur.BucketCounts.size() ? cur.BucketCounts[i] : 0;
                const ui64 baseVal = i < base->BucketCounts.size() ? base->BucketCounts[i] : 0;
                deltaBuckets[i] = curVal >= baseVal ? curVal - baseVal : 0;
                total += deltaBuckets[i];
            }
            if (total > 0) {
                // Linear interpolation inside the matching bucket gives much
                // better precision than reporting the bucket upper bound.
                // Histogram bucket bounds are already in milliseconds.
                auto percentile = [&](double frac) -> double {
                    const double target = frac * static_cast<double>(total);
                    double acc = 0.0;
                    for (ui32 i = 0; i < nBuckets; ++i) {
                        const double bucketCount = static_cast<double>(deltaBuckets[i]);
                        const double next = acc + bucketCount;
                        if (next >= target && bucketCount > 0) {
                            const double lowerMs = i == 0 ? 0.0 : histSnap->UpperBound(i - 1);
                            const double upperMs = histSnap->UpperBound(i);
                            const double withinBucket = (target - acc) / bucketCount;
                            return lowerMs + withinBucket * (upperMs - lowerMs);
                        }
                        acc = next;
                    }
                    return histSnap->UpperBound(nBuckets - 1);
                };
                stats.LatencyP50Ms = percentile(0.5);
                stats.LatencyP99Ms = percentile(0.99);
                // Max: upper bound of the last non-empty bucket.
                for (ui32 i = 0; i < nBuckets; ++i) {
                    if (deltaBuckets[i] > 0) {
                        stats.LatencyMaxMs = histSnap->UpperBound(i);
                    }
                }
            }
            reply->OpStats.push_back(std::move(stats));
        };
#define DDISK_FILL_PB_OP_STATS(NAME) fillOpStats(#NAME, Counters.Interface.NAME);
        DDISK_FILL_PB_OP_STATS(WritePersistentBuffer)
        DDISK_FILL_PB_OP_STATS(ReadPersistentBuffer)
        DDISK_FILL_PB_OP_STATS(ErasePersistentBuffer)
#undef DDISK_FILL_PB_OP_STATS

        if (ev->Get()->DescribeTablets) {
            for (auto& [k, v] : PersistentBuffers) {
                reply->TabletInfos.emplace_back(std::get<0>(k), std::get<1>(k),
                    v.Records.begin()->first, v.Records.rbegin()->first,
                    v.Records.begin()->second.Timestamp, v.Records.rbegin()->second.Timestamp,
                    v.Records.size(), v.Size,
                    PersistentBufferBarriersManager.GetErasesCount(std::get<0>(k)));
            }
            reply->EraseBarriers = PersistentBufferBarriersManager.GetBarriers();
        }
        if (ev->Get()->DescribeFreeSpace) {
            reply->FreeSpace = PersistentBufferSpaceAllocator.DescribeFreeSpace();
        }
        Send(ev->Sender, std::move(reply), 0, ev->Cookie);
    }

    void TDDiskActor::Handle(TEvListPersistentBuffer::TPtr ev) {
        if (!CheckQuery(*ev, &Counters.Interface.ListPersistentBuffer)) {
            return;
        }

        if (!PersistentBufferReady) {
            if (PendingPersistentBufferEvents.size() >= PersistentBufferFormat.MaxPendingEventsQueueSize) {
                STLOG_D("TDDiskActor::Handle(TEvListPersistentBuffer) pending queue overfill", (PendingPersistentBufferEvents.size(), PendingPersistentBufferEvents.size()));
                SendReply(*ev, std::make_unique<TEvListPersistentBufferResult>(
                    NKikimrBlobStorage::NDDisk::TReplyStatus::OVERLOADED,
                    TStringBuilder() << "pending queue overfill, size:"
                        << PendingPersistentBufferEvents.size() << " limit: " << PersistentBufferFormat.MaxPendingEventsQueueSize));
                return;
            }
            STLOG_I("TDDiskActor::Handle(TEvListPersistentBuffer) pending",
                (PendingPersistentBufferEvents.size(), PendingPersistentBufferEvents.size()));
            PendingPersistentBufferEvents.emplace(ev, "WaitingPersistentBufferList");
            Counters.PersistentBuffer.PendingEventsQueueSize->Inc();
            return;
        }

        const auto& record = ev->Get()->Record;
        const TQueryCredentials creds(record.GetCredentials());

        Counters.Interface.ListPersistentBuffer.Request(0);

        auto reply = std::make_unique<TEvListPersistentBufferResult>(NKikimrBlobStorage::NDDisk::TReplyStatus::OK);
        auto& rr = reply->Record;
        rr.SetBarrierLsn(PersistentBufferBarriersManager.GetBarrier(creds.TabletId));
        for (auto it = PersistentBuffers.lower_bound({creds.TabletId, 0}); it != PersistentBuffers.end() &&
                std::get<0>(it->first) == creds.TabletId; ++it) {
            const TPersistentBuffer& buffer = it->second;
            for (const auto& [lsn, pr] : buffer.Records) {
                auto *pb = rr.AddRecords();
                auto *sel = pb->MutableSelector();
                sel->SetVChunkIndex(pr.VChunkIndex);
                sel->SetOffsetInBytes(pr.OffsetInBytes);
                sel->SetSize(pr.Size);
                pb->SetGeneration(std::get<1>(it->first));
                pb->SetLsn(lsn);
            }
        }

        Counters.Interface.ListPersistentBuffer.Reply(true, 0);
        SendReply(*ev, std::move(reply));
    }

    TString TDDiskActor::PersistentBufferToString() {
        TStringBuilder sb;
        sb << "PersistentBuffer size:" << PersistentBuffers.size() << "\n";
        for (auto [k, v] : PersistentBuffers) {
            sb << "  TabletId:" << std::get<0>(k) << "\n";
            for (auto [lsn, pr] : v.Records) {
                sb << "    Lsn:" << lsn << " Offset:" << pr.OffsetInBytes << " Size:" << pr.Size << " Sectors: ";
                for (auto sector : pr.Sectors) {
                    sb << " " << sector.ChunkIdx << ":" << sector.SectorIdx << " ";
                }
                sb  << "\n";
            }
        }
        return sb;
    }

    double TDDiskActor::GetPersistentBufferFreeSpace() {
        double freeSpace = PersistentBufferSpaceAllocator.GetFreeSpace();
        ui32 ownedChunks = PersistentBufferSpaceAllocator.OwnedChunks.size();
        freeSpace += (PersistentBufferFormat.MaxChunks - ownedChunks) * SectorInChunk;
        freeSpace /= (PersistentBufferFormat.MaxChunks * SectorInChunk);
        Y_ABORT_UNLESS(freeSpace >= 0 && freeSpace <= 1);
        return freeSpace;
    }

} // NKikimr::NDDisk
