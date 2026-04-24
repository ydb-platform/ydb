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

    void TDDiskActor::IssuePersistentBufferChunkAllocation() {
        Y_ABORT_UNLESS(IsPersistentBufferActor);
        if (!IssuePersistentBufferChunkAllocationInflight) {
            IssuePersistentBufferChunkAllocationInflight = true;
            auto ddiskActorId = MakeBlobStorageDDiskId(SelfId().NodeId(), BaseInfo.PDiskId, BaseInfo.VDiskSlotId);
            Send(ddiskActorId, new TEvPrivate::TEvIssuePersistentBufferChunkAllocation());
            STLOG(PRI_DEBUG, BS_DDISK, BSDD14, "TDDiskActor::ProcessPersistentBufferWrite empty space, request new chunk", (FreeSpace, PersistentBufferSpaceAllocator.GetFreeSpace()), (PersistentBufferSpaceAllocator, PersistentBufferSpaceAllocator.ToString()));
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
        if (!PersistentBufferReady) {
            StartRestorePersistentBuffer();
        } else {
            ProcessPersistentBufferQueue();
        }
    }

    ui64 CalculateChecksum(const TRope::TIterator begin, size_t numBytes) {
        XXH3_state_t state;
        XXH3_64bits_reset(&state);

        for (auto it = begin; numBytes && it.Valid(); it.AdvanceToNextContiguousBlock()) {
            const size_t n = Min(numBytes, it.ContiguousSize());
            XXH3_64bits_update(&state, it.ContiguousData(), n);
            numBytes -= n;
        }

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
            STLOG(PRI_DEBUG, BS_DDISK, BSDD12, "TDDiskActor::StartRestorePersistentBuffer ready");
            PersistentBufferReady = true;
            UpdateFreeSpaceInfo();
            return;
        }
        for (ui32 pos = 0; pos < PersistentBufferSpaceAllocator.OwnedChunks.size() && PersistentBufferRestoreChunksInflight < PersistentBufferFormat.MaxChunkRestoreInflight; pos++) {
            auto chunkIdx = PersistentBufferSpaceAllocator.OwnedChunks[pos];
            if (PersistentBufferAllocatedChunks.count(chunkIdx) > 0 || PersistentBufferRestoringChunks.count(chunkIdx) > 0) {
                continue;
            }
            STLOG(PRI_DEBUG, BS_DDISK, BSDD13, "TDDiskActor::StartRestorePersistentBuffer restoring chunk from DDisk", (ChunkIdx, chunkIdx));
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
        ui64 lsn, ui32 offsetInBytes, ui32 sizeInBytes, TRope&& payload, const std::vector<TPersistentBufferSectorInfo>& sectors) {
        auto headerData = TRcBuf::UninitializedPageAligned(SectorSize);
        TPersistentBufferHeader *header = (TPersistentBufferHeader*)headerData.GetDataMut();
        memset(header, 0, SectorSize);
        memcpy(header->Signature, TPersistentBufferHeader::PersistentBufferHeaderSignature, 16);
        header->Type = TPersistentBufferHeader::RECORD;
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
            loc.Checksum = CalculateChecksum(it, SectorSize);
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
                    header->HeaderChecksum = XXH3_64bits(headerData.GetDataMut(), SectorSize);
                    data = headerData;
                }
                payload.ExtractFront(partSize, &data);
                parts.emplace_back(sectors[first].ChunkIdx, sectors[first].SectorIdx * SectorSize, std::move(data));
                first = sectorIdx;
            }
        }
        return parts;
    }

    void TDDiskActor::ProcessPersistentBufferQueue() {
        if (PendingPersistentBufferEvents.empty() || !PersistentBufferReady) {
            return;
        }
        while (!PendingPersistentBufferEvents.empty()) {
            auto temp = PendingPersistentBufferEvents.front().Release();
            PendingPersistentBufferEvents.pop();
            auto size = PendingPersistentBufferEvents.size();
            Receive(temp);
            if (PendingPersistentBufferEvents.size() != size) {
                STLOG(PRI_DEBUG, BS_DDISK, BSDD11, "TDDiskActor::ProcessPersistentBufferQueue pending queue growth",
                    (PendingPersistentBufferEvents.size(), PendingPersistentBufferEvents.size()), (size, size));
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
            ui8 sector[SectorSize];
            auto sigPos = dataPos;
            sigPos.ExtractPlainDataAndAdvance(&sector, SectorSize);
            if (memcmp(&sector, TPersistentBufferHeader::PersistentBufferHeaderSignature, 16) == 0) {
                TPersistentBufferHeader* header = (TPersistentBufferHeader*)&sector;
                ui64 headerChecksum = header->HeaderChecksum;
                header->HeaderChecksum = 0;
                ui64 sectorChecksum = XXH3_64bits((char*)&sector, SectorSize);
                if (headerChecksum != sectorChecksum) {
                    STLOG(PRI_ERROR, BS_DDISK, BSDD11, "TDDiskActor::StartRestorePersistentBuffer header checksum failed", (TabletId, header->Record.TabletId), (VChunkIndex, header->Record.VChunkIndex), (Lsn, header->Record.Lsn));
                    continue;
                }
                if (header->Type == TPersistentBufferHeader::BARRIER) {
                    auto idx = header->Barrier.BarrierIdx;
                    if (idx >= PersistentBufferBarriers.size()) {
                        PersistentBufferBarriers.resize(idx + 1);
                    }
                    if (PersistentBufferBarriers[idx].Header.Barrier.BarrierLsn < header->Barrier.BarrierLsn) {
                        PersistentBufferBarriers[idx] = {chunkIdx, sectorIdx, *header};
                    }
                    continue;
                }
                auto& buffer = PersistentBuffers[{header->Record.TabletId, header->Record.Generation}];
                auto [it, inserted] = buffer.Records.try_emplace(header->Record.Lsn);
                if (!inserted) {
                    STLOG(PRI_ERROR, BS_DDISK, BSDD11, "TDDiskActor::StartRestorePersistentBuffer duplicated lsn for tablet in persistent buffer", (TabletId, header->Record.TabletId), (VChunkIndex, header->Record.VChunkIndex), (Lsn, header->Record.Lsn));
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
                PersistentBufferSectorsChecksum[chunkIdx][sectorIdx] = XXH3_64bits((char*)&sector, SectorSize);
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

            for (ui32 pos = 0; pos < PersistentBufferBarriers.size(); pos++) {
                auto& b = PersistentBufferBarriers[pos];
                for (ui32 FreeBarrierPosition = 0; FreeBarrierPosition < TPersistentBufferHeader::MaxBarriersPerHeader && b.Header.Barrier.Barriers[FreeBarrierPosition].TabletId > 0; FreeBarrierPosition++) {
                    auto& barrier = b.Header.Barrier.Barriers[FreeBarrierPosition];
                    auto it = PersistentBuffers.lower_bound({barrier.TabletId, 0});
                    if (it == PersistentBuffers.end() || std::get<0>(it->first) != barrier.TabletId) {
                        PersistentBufferBarrierHoles.push_back({pos, FreeBarrierPosition});
                    }
                    for (; it != PersistentBuffers.end() &&
                            std::get<0>(it->first) == barrier.TabletId;) {
                        TPersistentBuffer& buffer = it->second;
                        auto recordIt = buffer.Records.begin();
                        while (recordIt != buffer.Records.end() && recordIt->first <= barrier.Lsn) {
                            auto eraseIt = recordIt++;
                            buffer.Records.erase(eraseIt);
                        }
                        if (buffer.Records.empty()) {
                            auto eraseIt = it++;
                            PersistentBuffers.erase(eraseIt);
                        } else {
                            ++it;
                        }
                    }
                }
            }

            for (auto& [_, pb] : PersistentBuffers) {
                for (auto& [__, record] : pb.Records) {
                    PersistentBufferSpaceAllocator.MarkOccupied(record.Sectors);
                }
            }

            STLOG(PRI_DEBUG, BS_DDISK, BSDD16, "TDDiskActor::StartRestorePersistentBuffer ready");
            PersistentBufferReady = true;
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

    void TDDiskActor::Handle(TDDiskActor::TEvPrivate::TEvWritePersistentBufferPart::TPtr ev) {
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
        inflight.Span.Event("Part written");

        Y_ABORT_UNLESS(eraseCnt == 1);
        if (inflight.OperationCookies.empty()) {
            if (ev->Get()->IsErase) {
                Counters.Interface.ErasePersistentBuffer.Reply(!inflight.ErrorMessage, inflight.Size,
                    HPMilliSecondsFloat(HPNow() - inflight.StartTs));

                PersistentBufferSpaceAllocator.Free(inflight.Sectors);
                auto replyEv = std::make_unique<TEvErasePersistentBufferResult>(
                    inflight.Status, inflight.ErrorMessage, GetPersistentBufferFreeSpace(), NormalizedOccupancy);
                auto h = std::make_unique<IEventHandle>(inflight.Sender, SelfId(), replyEv.release(), 0, inflight.Cookie);
                if (inflight.Session) {
                    h->Rewrite(TEvInterconnect::EvForward, inflight.Session);
                }
                TActivationContext::Send(h.release());
            } else {
                Counters.Interface.WritePersistentBuffer.Reply(!inflight.ErrorMessage, inflight.Size,
                    HPMilliSecondsFloat(HPNow() - inflight.StartTs));
                if (!inflight.ErrorMessage) {
                    auto& buffer = PersistentBuffers[{inflight.TabletId, inflight.Generation}];
                    auto [it, inserted] = buffer.Records.try_emplace(inflight.Lsn);
                    TPersistentBuffer::TRecord& pr = it->second;
                    Y_ABORT_UNLESS(inflight.DataParts.size() == 1 && inflight.PartsCount == 1);
                    if (inserted) {
                        pr = {
                            .OffsetInBytes = inflight.OffsetInBytes,
                            .Size = (ui32)inflight.DataParts.begin()->second.size(),
                            .Sectors = std::move(inflight.Sectors),
                            .VChunkIndex = inflight.VChunkIdx,
                            .Timestamp = TInstant::Now()
                        };
                        buffer.Size += pr.Size;
                        pr.Data = std::move(inflight.DataParts.begin()->second);
                        PersistentBufferInMemoryCacheSize += pr.Size;
                        auto [_, inserted2] = PersistentBuffersInMemoryCacheUptime[pr.Timestamp].emplace(inflight.TabletId, inflight.Generation, inflight.Lsn);
                        Y_ABORT_UNLESS(inserted2);
                        SanitizePersistentBufferInMemoryCache();
                    } else {
                        Y_ABORT_UNLESS(pr.OffsetInBytes == inflight.OffsetInBytes);
                        Y_ABORT_UNLESS(pr.Size == inflight.DataParts.begin()->second.size());
                        Y_ABORT_UNLESS(inflight.DataParts.begin()->second == pr.Data);
                    }
                }
                auto replyEv = std::make_unique<TEvWritePersistentBufferResult>(
                    inflight.Status, inflight.ErrorMessage, GetPersistentBufferFreeSpace(), NormalizedOccupancy);
                auto h = std::make_unique<IEventHandle>(inflight.Sender, SelfId(), replyEv.release(), 0, inflight.Cookie);
                if (inflight.Session) {
                    h->Rewrite(TEvInterconnect::EvForward, inflight.Session);
                }
                TActivationContext::Send(h.release());
            }
            inflight.Span.End();
            PersistentBufferDiskOperationInflight.erase(itInflight);
        }
    }

    void TDDiskActor::ProcessPersistentBufferWrite(TEvWritePersistentBuffer::TPtr ev) {
        const auto& record = ev->Get()->Record;
        const TQueryCredentials creds(record.GetCredentials());
        const TBlockSelector selector(record.GetSelector());
        const ui64 lsn = record.GetLsn();
        ui32 sectorsCnt = selector.Size / SectorSize + 1;

        if (auto it = PersistentBuffers.find({creds.TabletId, creds.Generation});
            it != PersistentBuffers.end() && selector.Size + it->second.Size > PersistentBufferFormat.PerTabletStorageLimit) {
            STLOG(PRI_DEBUG, BS_DDISK, BSDD15, "TDDiskActor::ProcessPersistentBufferWrite tablet space occupation limit is reached",
                (TabletId, creds.TabletId), (Generation, creds.Generation), (TabletSpaceOccupied, it->second.Size),
                (WriteDataSize, selector.Size), (PerTabletStorageLimit, PersistentBufferFormat.PerTabletStorageLimit));
            SendReply(*ev, std::make_unique<TEvWritePersistentBufferResult>(
                NKikimrBlobStorage::NDDisk::TReplyStatus::OVERFILL,
                TStringBuilder() << "persistent buffer overfill "
                    << selector.Size << " bytes requested, current tablet space occupation " << it->second.Size << " bytes"));
            return;
        }

        const auto sectors = PersistentBufferSpaceAllocator.Occupy(sectorsCnt);
        if (sectors.size() == 0) {
            if (PersistentBufferSpaceAllocator.OwnedChunks.size() < PersistentBufferFormat.MaxChunks) {
                PendingPersistentBufferEvents.emplace(ev, "WaitingPersistentBufferWrite");
                IssuePersistentBufferChunkAllocation();
            } else {
                STLOG(PRI_DEBUG, BS_DDISK, BSDD15, "TDDiskActor::ProcessPersistentBufferWrite not enough space", (FreeSpace, PersistentBufferSpaceAllocator.GetFreeSpace() * SectorSize), (NeedSpace, sectorsCnt * SectorSize));
                SendReply(*ev, std::make_unique<TEvWritePersistentBufferResult>(
                    NKikimrBlobStorage::NDDisk::TReplyStatus::OVERFILL,
                    TStringBuilder() << "persistent buffer overfill "
                        << (sectorsCnt * SectorSize) << " bytes requested, free " << (PersistentBufferSpaceAllocator.GetFreeSpace() * SectorSize) << " bytes"));
            }
            return;
        }
        Y_ABORT_UNLESS(sectors.size() == sectorsCnt && sectorsCnt <= MaxSectorsPerBufferRecord + 1 && sectorsCnt > 1);

        auto span = std::move(NWilson::TSpan(TWilson::DDiskTopLevel, std::move(ev->TraceId), "DDisk.WritePersistentBuffer",
                NWilson::EFlags::NONE, TActivationContext::ActorSystem())
            .Attribute("tablet_id", static_cast<long>(creds.TabletId))
            .Attribute("vchunk_index", static_cast<long>(selector.VChunkIndex))
            .Attribute("offset_in_bytes", selector.OffsetInBytes)
            .Attribute("size", selector.Size)
            .Attribute("lsn", static_cast<long>(lsn)));
        Counters.Interface.WritePersistentBuffer.Request(selector.Size);

        const TWriteInstruction instr(record.GetInstruction());
        TRope payload;
        if (instr.PayloadId) {
            payload = ev->Get()->GetPayload(*instr.PayloadId);
        }

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
            .VChunkIdx = selector.VChunkIndex,
            .Lsn = lsn,
            .OffsetInBytes = selector.OffsetInBytes,
            .Size = selector.Size,
            .Sectors = std::move(sectors),
            .DataParts = {{0, std::move(payload)}},
            .PartsCount = 1,
            .StartTs = HPNow(),
        };

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
            inflightRecord.Span.Event(UringRouter ? "DirectUringOp" : "Send to pdisk");

            DirectUringOp(op);
        }
    }

    void TDDiskActor::Handle(TEvWritePersistentBuffer::TPtr ev) {
        if (!CheckQuery(*ev, &Counters.Interface.WritePersistentBuffer)) {
            return;
        }
        const auto& record = ev->Get()->Record;
        const TBlockSelector selector(record.GetSelector());
        if (selector.Size > MaxSectorsPerBufferRecord * SectorSize) {
            Counters.Interface.WritePersistentBuffer.Request(selector.Size);
            Counters.Interface.WritePersistentBuffer.Reply(false, selector.Size);
            SendReply(*ev, std::make_unique<TEvWritePersistentBufferResult>(
                NKikimrBlobStorage::NDDisk::TReplyStatus::INCORRECT_REQUEST,
                TStringBuilder() << "persistent buffer write limit "
                    << (MaxSectorsPerBufferRecord * SectorSize) << " bytes, received " << selector.Size << " bytes"));
            return;
        }
        if (!PersistentBufferReady) {
            PendingPersistentBufferEvents.emplace(ev, "WaitingPersistentBufferWrite");
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
            PendingPersistentBufferEvents.emplace(ev, "WaitingPersistentBufferRead");
            return;
        }

        const ui64 lsn = record.GetLsn();
        const ui32 generation = record.GetGeneration();
        TBlockSelector selector(record.GetSelector());

        Counters.Interface.ReadPersistentBuffer.Request(selector.Size);

        auto span = std::move(NWilson::TSpan(TWilson::DDiskTopLevel, std::move(ev->TraceId), "DDisk.ReadPersistentBuffer",
                NWilson::EFlags::NONE, TActivationContext::ActorSystem())
            .Attribute("tablet_id", static_cast<long>(creds.TabletId))
            .Attribute("vchunk_index", static_cast<long>(selector.VChunkIndex))
            .Attribute("offset_in_bytes", selector.OffsetInBytes)
            .Attribute("size", selector.Size)
            .Attribute("lsn", static_cast<long>(lsn)));

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
            .VChunkIdx = selector.VChunkIndex,
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

    void TDDiskActor::ReplyReadPersistentBuffer(TDDiskActor::TPersistentBuffer::TRecord& pr, NKikimrBlobStorage::NDDisk::TReplyStatus::E status, std::optional<TString> errorMessage) {
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
            auto& pb = PersistentBuffers.at({std::get<0>(*lsnIt), std::get<1>(*lsnIt)});
            auto& pr = pb.Records.at(std::get<2>(*lsnIt));
            Y_ABORT_UNLESS(pr.Data.size() == pr.Size);
            PersistentBufferInMemoryCacheSize -= pr.Size;
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
            auto icuIt = PersistentBuffersInMemoryCacheUptime.find(record.Timestamp);
            Y_ABORT_UNLESS(icuIt != PersistentBuffersInMemoryCacheUptime.end());
            auto count = icuIt->second.erase({tabletId, generation, lsn});
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
        STLOG(PRI_DEBUG, BS_DDISK, BSDD31, "TDDiskActor::BarrierErasePersistentBuffer", (tabletId, creds.TabletId), (lsn, lsn));
        auto span = std::move(NWilson::TSpan(TWilson::DDiskTopLevel, std::move(queryEv.TraceId), "DDisk.BarrierErasePersistentBuffer",
                NWilson::EFlags::NONE, TActivationContext::ActorSystem())
            .Attribute("tablet_id", static_cast<long>(creds.TabletId)));
        const ui64 barrierEraseCookie = NextCookie++;
        auto [inflightRecord, inserted] = PersistentBufferDiskOperationInflight.try_emplace(barrierEraseCookie, TPersistentBufferDiskOperationInFlight{
            .Sender = queryEv.Sender,
            .Cookie = queryEv.Cookie,
            .Session = queryEv.InterconnectSession,
            .Span = std::move(span),
            .StartTs = HPNow(),
        });
        Y_ABORT_UNLESS(inserted);

        for (auto& e : erases) {
            auto lsn = std::get<0>(e);
            auto generation = std::get<1>(e);
            STLOG(PRI_DEBUG, BS_DDISK, BSDD31, "TDDiskActor::ErasePersistentBuffer", (tabletId, creds.TabletId), (lsn, lsn), (generation, generation));
            const auto it = PersistentBuffers.find({creds.TabletId, generation});
            TPersistentBuffer& buffer = it->second;
            const auto jt = buffer.Records.find(lsn);
            TPersistentBuffer::TRecord& pr = jt->second;
            SanitizePersistentBufferInMemoryCache(creds.TabletId, generation, lsn, pr);
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

        auto it = PersistentBufferBarriersLocation.find(creds.TabletId);
        ui32 barrierIdx = 0;
        ui32 pos = 0;
        if (it == PersistentBufferBarriersLocation.end()) {
            if (!PersistentBufferBarrierHoles.empty()) {
                barrierIdx = std::get<0>(PersistentBufferBarrierHoles.back());
                pos = std::get<1>(PersistentBufferBarrierHoles.back());
                PersistentBufferBarrierHoles.pop_back();
                PersistentBufferBarriersLocation[creds.TabletId] = {barrierIdx, pos};
            } else {
                if (FreeBarrierPosition >= TPersistentBufferHeader::MaxBarriersPerHeader || PersistentBufferBarriers.empty()) {
                    FreeBarrierPosition = 0;

                    TPersistentBufferHeader header;
                    memset(&header, 0, SectorSize);
                    memcpy(header.Signature, TPersistentBufferHeader::PersistentBufferHeaderSignature, 16);
                    header.Type = TPersistentBufferHeader::BARRIER;
                    header.Barrier.BarrierLsn = 0;
                    header.Barrier.BarrierIdx = PersistentBufferBarriers.size();
                    PersistentBufferBarriers.push_back({Max<ui32>(), Max<ui32>(), std::move(header)});
                }
                barrierIdx = PersistentBufferBarriers.size() - 1;
                pos = FreeBarrierPosition;
                PersistentBufferBarriersLocation[creds.TabletId] = {barrierIdx, pos};
                FreeBarrierPosition++;
            }
        } else {
            barrierIdx = std::get<0>(it->second);
            pos = std::get<1>(it->second);
        }
        auto& barrier = PersistentBufferBarriers[barrierIdx];
        const auto sectors = PersistentBufferSpaceAllocator.Occupy(1);
        Y_ABORT_UNLESS(sectors.size() == 1);
        if (barrier.ChunkIdx != Max<ui32>()) {
            inflightRecord->second.Sectors.push_back({.ChunkIdx = barrier.ChunkIdx, .SectorIdx = barrier.SectorIdx});
        }
        barrier.ChunkIdx = sectors[0].ChunkIdx;
        barrier.SectorIdx = sectors[0].SectorIdx;
        barrier.Header.Barrier.Barriers[pos] = {creds.TabletId, lsn};
        barrier.Header.Barrier.BarrierLsn++;
        const ui64 cookie = NextCookie++;
        inflightRecord->second.OperationCookies.insert(cookie);
        auto headerData = TRcBuf::UninitializedPageAligned(SectorSize);
        barrier.Header.HeaderChecksum = 0;
        memcpy(headerData.GetDataMut(), &barrier.Header, sizeof(TPersistentBufferHeader));
        ((TPersistentBufferHeader*)headerData.GetDataMut())->HeaderChecksum = XXH3_64bits(headerData.GetDataMut(), SectorSize);

        auto chunkOffset = barrier.SectorIdx * SectorSize;
        auto diskOffset = DiskFormat->Offset(barrier.ChunkIdx, 0, chunkOffset);
        std::unique_ptr<TDirectIoOpBase> op = AllocateOp<TPersistentBufferPartIoOp>();
        auto* partOp = static_cast<TPersistentBufferPartIoOp*>(op.get());
        partOp->SetCookie(barrierEraseCookie);
        partOp->SetPartCookie(cookie);
        partOp->SetIsErase(true);
        partOp->PrepareWrite(TRope(headerData), diskOffset, barrier.ChunkIdx, chunkOffset);
        DirectUringOp(op);
    }

    void TDDiskActor::ErasePersistentBuffer(IEventHandle& queryEv, const TQueryCredentials& creds, const std::vector<std::tuple<ui64, ui32>>& erases) {
        Counters.Interface.ErasePersistentBuffer.Request(0);

        auto span = std::move(NWilson::TSpan(TWilson::DDiskTopLevel, std::move(queryEv.TraceId), "DDisk.BatchErasePersistentBuffer",
                NWilson::EFlags::NONE, TActivationContext::ActorSystem())
            .Attribute("tablet_id", static_cast<long>(creds.TabletId)));

        STLOG(PRI_DEBUG, BS_DDISK, BSDD31, "TDDiskActor::ErasePersistentBuffer", (tabletId, creds.TabletId));

        const ui64 batchEraseCookie = NextCookie++;

        auto [inflightRecord, inserted] = PersistentBufferDiskOperationInflight.try_emplace(batchEraseCookie, TPersistentBufferDiskOperationInFlight{
            .Sender = queryEv.Sender,
            .Cookie = queryEv.Cookie,
            .Session = queryEv.InterconnectSession,
            .Span = std::move(span),
            .StartTs = HPNow(),
        });
        Y_ABORT_UNLESS(inserted);

        // TODO: flush once
        for (auto& e : erases) {
            auto lsn = std::get<0>(e);
            auto generation = std::get<1>(e);
            STLOG(PRI_DEBUG, BS_DDISK, BSDD31, "TDDiskActor::ErasePersistentBuffer", (tabletId, creds.TabletId), (lsn, lsn), (generation, generation));

            const auto it = PersistentBuffers.find({creds.TabletId, generation});
            TPersistentBuffer& buffer = it->second;
            const auto jt = buffer.Records.find(lsn);
            TPersistentBuffer::TRecord& pr = jt->second;
            SanitizePersistentBufferInMemoryCache(creds.TabletId, generation, lsn, pr);

            for (auto i : pr.Sectors) {
                inflightRecord->second.Sectors.push_back(i);
            }

            const ui64 cookie = NextCookie++;
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

            DirectUringOp(op);
        }
    }

    void TDDiskActor::Handle(TEvBatchErasePersistentBuffer::TPtr ev) {
        if (!CheckQuery(*ev, &Counters.Interface.ErasePersistentBuffer)) {
            return;
        }
        if (!PersistentBufferReady) {
            PendingPersistentBufferEvents.emplace(ev, "WaitingPersistentBufferBatchErase");
            return;
        }

        const auto& record = ev->Get()->Record;
        const TQueryCredentials creds(record.GetCredentials());

        std::vector<std::tuple<ui64, ui32>> erases;
        for (auto& e : record.GetErases()) {
            auto lsn = e.GetLsn();
            auto generation = e.GetGeneration();
            const auto it = PersistentBuffers.find({creds.TabletId, generation});
            if (it != PersistentBuffers.end() && it->second.Records.find(lsn) != it->second.Records.end()) {
                erases.emplace_back(lsn, generation);
            }
        }

        if (erases.empty()) {
            SendReply(*ev, std::make_unique<TEvErasePersistentBufferResult>(
                NKikimrBlobStorage::NDDisk::TReplyStatus::OK, std::nullopt, GetPersistentBufferFreeSpace(), NormalizedOccupancy));
            return;
        }

        ErasePersistentBuffer(*ev, creds, erases);
    }

    void TDDiskActor::Handle(TEvErasePersistentBuffer::TPtr ev) {
        if (!CheckQuery(*ev, &Counters.Interface.ErasePersistentBuffer)) {
            return;
        }

        if (!PersistentBufferReady) {
            PendingPersistentBufferEvents.emplace(ev, "WaitingPersistentBufferErase");
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

        if (erases.empty()) {
            SendReply(*ev, std::make_unique<TEvErasePersistentBufferResult>(
                NKikimrBlobStorage::NDDisk::TReplyStatus::OK, std::nullopt, GetPersistentBufferFreeSpace(), NormalizedOccupancy));
            return;
        }
        if (erases.size() < 2 // Simple erase one header
            || PersistentBufferSpaceAllocator.GetFreeSpace() < 2 // Not enough space to write new barrier sector
            || (PersistentBufferBarrierHoles.empty()
                && PersistentBufferBarriersLocation.find(creds.TabletId) == PersistentBufferBarriersLocation.end()
                && FreeBarrierPosition >= TPersistentBufferHeader::MaxBarriersPerHeader // all barriers sectors are full
                && (PersistentBufferBarriers.size() >= PersistentBufferFormat.MaxBarriersLimit // max barrier sectors limit reached
            ))) { // not enough space to create new barriers sector
            ErasePersistentBuffer(*ev, creds, erases);
        } else {
            BarrierErasePersistentBuffer(*ev, creds, erases, lsn);
        }
    }

    void TDDiskActor::Handle(TEvGetPersistentBufferInfo::TPtr ev) {
        if (!PersistentBufferReady) {
            PendingPersistentBufferEvents.emplace(ev, "WaitingGetPersistentBufferInfo");
            return;
        }
        STLOG(PRI_DEBUG, BS_DDISK, BSDD40, "TDDiskActor::Handle(TEvGetPersistentBufferInfo)",
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

        if (ev->Get()->DescribeTablets) {
            for (auto& [k, v] : PersistentBuffers) {
                reply->TabletInfos.emplace_back(std::get<0>(k), std::get<1>(k),
                    v.Records.begin()->first, v.Records.rbegin()->first,
                    v.Records.begin()->second.Timestamp, v.Records.rbegin()->second.Timestamp,
                    v.Records.size(), v.Size);
            }
            for (auto& b : PersistentBufferBarriers) {
                for (auto& h : b.Header.Barrier.Barriers) {
                    if (h.TabletId != 0) {
                        reply->EraseBarriers.insert({h.TabletId, h.Lsn});
                    }
                }
            }
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
            PendingPersistentBufferEvents.emplace(ev, "WaitingPersistentBufferList");
            return;
        }

        const auto& record = ev->Get()->Record;
        const TQueryCredentials creds(record.GetCredentials());

        Counters.Interface.ListPersistentBuffer.Request(0);

        auto reply = std::make_unique<TEvListPersistentBufferResult>(NKikimrBlobStorage::NDDisk::TReplyStatus::OK);
        auto& rr = reply->Record;

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
