#include "ddisk_actor.h"
#include "direct_io_op.h"

#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_data.h>
#include <ydb/core/util/stlog.h>
#include <ydb/core/util/pb.h>

#define XXH_INLINE_ALL
#include <contrib/libs/xxhash/xxhash.h>

namespace NKikimr::NDDisk {

    void TDDiskActor::InitPersistentBuffer() {
        Y_ABORT_UNLESS(DiskFormat);
        SectorSize = DiskFormat->SectorSize;
        Y_ABORT_UNLESS(SectorSize >= sizeof(TPersistentBufferHeader));
        ChunkSize = DiskFormat->ChunkSize;
        Y_ABORT_UNLESS(ChunkSize % SectorSize == 0);
        SectorInChunk = ChunkSize / SectorSize;
        PersistentBufferSpaceAllocator = TPersistentBufferSpaceAllocator(SectorInChunk);
        UpdateFreeSpaceInfo();
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


            std::unique_ptr<TDirectIoOpBase> op = std::make_unique<TPersistentBufferPartIoOp>(SelfId(), Counters);
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
        header->TabletId = tabletId;
        header->Generation = generation;
        header->VChunkIndex = vchunkIndex;
        header->OffsetInBytes = offsetInBytes;
        header->Size = sizeInBytes;
        header->Lsn = lsn;

        for (ui32 i = 1; i < sectors.size(); ++i) {
            auto& loc = header->Locations[i - 1];
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
            Y_ABORT_UNLESS(PendingPersistentBufferEvents.size() == size);
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
                    STLOG(PRI_ERROR, BS_DDISK, BSDD11, "TDDiskActor::StartRestorePersistentBuffer header checksum failed", (TabletId, header->TabletId), (VChunkIndex, header->VChunkIndex), (Lsn, header->Lsn));
                    continue;
                }
                auto& buffer = PersistentBuffers[{header->TabletId, header->Generation, header->VChunkIndex}];
                auto [it, inserted] = buffer.Records.try_emplace(header->Lsn);
                if (!inserted) {
                    STLOG(PRI_ERROR, BS_DDISK, BSDD11, "TDDiskActor::StartRestorePersistentBuffer duplicated lsn for tablet in persistent buffer", (TabletId, header->TabletId), (VChunkIndex, header->VChunkIndex), (Lsn, header->Lsn));
                }
                TPersistentBuffer::TRecord& pr = it->second;
                pr = {
                    .OffsetInBytes = header->OffsetInBytes,
                    .Size = header->Size,
                    .PartsCount = 0,
                };
                ui32 sectorsCnt = header->Size / SectorSize;
                pr.Sectors.reserve(sectorsCnt + 1);
                pr.Sectors.push_back({
                    .ChunkIdx = chunkIdx,
                    .SectorIdx = sectorIdx,
                });
                for (ui32 i = 0; i < sectorsCnt; i++) {
                    pr.Sectors.push_back(header->Locations[i]);
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
            }
            std::erase_if(PersistentBuffers, [](const auto& pb) { return pb.second.Records.empty(); });
            for (auto& [_, pb] : PersistentBuffers) {
                for (auto& [__, record] : pb.Records) {
                    PersistentBufferSpaceAllocator.MarkOccupied(record.Sectors);
                }
            }
            PersistentBufferSectorsChecksum.clear();
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

        TRope data;
        auto it = PersistentBuffers.find({inflight.TabletId, inflight.Generation, inflight.VChunkIdx});
        if (it == PersistentBuffers.end()) {
            inflight.Status = NKikimrBlobStorage::NDDisk::TReplyStatus::MISSING_RECORD;
        } else {
            auto recordIt = it->second.Records.find(inflight.Lsn);
            if (recordIt == it->second.Records.end()) {
                inflight.Status = NKikimrBlobStorage::NDDisk::TReplyStatus::MISSING_RECORD;
            } else {
                auto& pr = recordIt->second;
                pr.DataParts.emplace(partCookie, std::move(ev->Get()->Data));
                if (inflight.OperationCookies.empty()) {
                    if (pr.PartsCount == 0 || pr.DataParts.size() != pr.PartsCount) {
                        inflight.Status = NKikimrBlobStorage::NDDisk::TReplyStatus::MISSING_RECORD;
                        pr.DataParts.clear();
                        pr.PartsCount = 0;
                    } else {
                        data = pr.JoinData(SectorSize);
                        PersistentBufferInMemoryCacheSize += pr.Size;
                        SanitizePersistentBufferInMemoryCache(pr);
                    }
                }
            }
        }

        if (inflight.OperationCookies.empty()) {
            ReplyReadPersistentBuffer(opCookie, std::move(data));
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
        Y_ABORT_UNLESS(eraseCnt == 1);
        if (inflight.OperationCookies.empty()) {
            if (ev->Get()->IsErase) {
                Counters.Interface.ErasePersistentBuffer.Reply(!inflight.ErrorMessage);

                auto replyEv = std::make_unique<TEvErasePersistentBufferResult>(
                    inflight.Status, inflight.ErrorMessage, GetPersistentBufferFreeSpace(), NormalizedOccupancy);
                auto h = std::make_unique<IEventHandle>(inflight.Sender, SelfId(), replyEv.release(), 0, inflight.Cookie);
                if (inflight.Session) {
                    h->Rewrite(TEvInterconnect::EvForward, inflight.Session);
                }
                TActivationContext::Send(h.release());
            } else {
                Counters.Interface.WritePersistentBuffer.Reply(!inflight.ErrorMessage);
                if (!inflight.ErrorMessage) {
                    auto& buffer = PersistentBuffers[{inflight.TabletId, inflight.Generation, inflight.VChunkIdx}];
                    auto [it, inserted] = buffer.Records.try_emplace(inflight.Lsn);
                    TPersistentBuffer::TRecord& pr = it->second;
                    if (inserted) {
                        pr = {
                            .OffsetInBytes = inflight.OffsetInBytes,
                            .Size = (ui32)inflight.Data.size(),
                            .Sectors = std::move(inflight.Sectors),
                            .PartsCount = 1,
                        };
                        pr.DataParts.emplace(0, std::move(inflight.Data));
                        PersistentBufferInMemoryCacheSize += pr.Size;
                    } else {
                        Y_ABORT_UNLESS(pr.OffsetInBytes == inflight.OffsetInBytes);
                        Y_ABORT_UNLESS(pr.Size == inflight.Data.size());
                        Y_ABORT_UNLESS(pr.PartsCount == 1);
                        Y_ABORT_UNLESS(pr.DataParts.begin()->second == inflight.Data);
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
        const auto sectors = PersistentBufferSpaceAllocator.Occupy(sectorsCnt);
        if (sectors.size() == 0) {
            if (PersistentBufferSpaceAllocator.OwnedChunks.size() < PersistentBufferFormat.MaxChunks) {
                STLOG(PRI_DEBUG, BS_DDISK, BSDD14, "TDDiskActor::ProcessPersistentBufferWrite empty space, request new chunk");
                PendingPersistentBufferEvents.emplace(ev, "WaitingPersistentBufferWrite");
                IssuePersistentBufferChunkAllocation();
            } else {
                STLOG(PRI_DEBUG, BS_DDISK, BSDD15, "TDDiskActor::ProcessPersistentBufferWrite not enough space", (FreeSpace, PersistentBufferSpaceAllocator.GetFreeSpace() * SectorSize), (NeedSpace, sectorsCnt * SectorSize));
                SendReply(*ev, std::make_unique<TEvWritePersistentBufferResult>(
                    NKikimrBlobStorage::NDDisk::TReplyStatus::BLOCKED,
                    TStringBuilder() << "persistent buffer overfill "
                        << (sectorsCnt * SectorSize) << " bytes requested, free " << (PersistentBufferSpaceAllocator.GetFreeSpace() * SectorSize) << " bytes"));
            }
            return;
        }
        Y_ABORT_UNLESS(sectors.size() == sectorsCnt && sectorsCnt <= MaxSectorsPerBufferRecord + 1 && sectorsCnt > 1);

        const TWriteInstruction instr(record.GetInstruction());
        TRope payload;
        if (instr.PayloadId) {
            payload = ev->Get()->GetPayload(*instr.PayloadId);
        }
        auto span = std::move(NWilson::TSpan(TWilson::DDiskTopLevel, std::move(ev->TraceId), "DDisk.WritePersistentBuffer",
                NWilson::EFlags::NONE, TActivationContext::ActorSystem())
            .Attribute("tablet_id", static_cast<long>(creds.TabletId))
            .Attribute("vchunk_index", static_cast<long>(selector.VChunkIndex))
            .Attribute("offset_in_bytes", selector.OffsetInBytes)
            .Attribute("size", selector.Size)
            .Attribute("lsn", static_cast<long>(lsn)));
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
            .VChunkIdx = selector.VChunkIndex,
            .Lsn = lsn,
            .OffsetInBytes = selector.OffsetInBytes,
            .Sectors = std::move(sectors),
            .Data = std::move(payload),
        };

        // TODO: make uring to call flush just once
        for(auto& [chunkIdx, offset, data] : parts) {
            const ui64 cookie = NextCookie++;
            inflightRecord.OperationCookies.insert(cookie);
            auto diskOffset = DiskFormat->Offset(chunkIdx, 0, offset);
            std::unique_ptr<TDirectIoOpBase> op = std::make_unique<TPersistentBufferPartIoOp>(SelfId(), Counters);
            auto* partOp = static_cast<TPersistentBufferPartIoOp*>(op.get());
            partOp->SetCookie(opCookie);
            partOp->SetPartCookie(cookie);
            partOp->PrepareWrite(std::move(data), diskOffset, chunkIdx, offset);
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
            Counters.Interface.WritePersistentBuffer.Reply(false);
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
        if (!CheckQuery(*ev, &Counters.Interface.ReadPersistentBuffer)) {
            return;
        }

        if (!PersistentBufferReady) {
            PendingPersistentBufferEvents.emplace(ev, "WaitingPersistentBufferRead");
            return;
        }

        const auto& record = ev->Get()->Record;
        const TQueryCredentials creds(record.GetCredentials());
        const TBlockSelector selector(record.GetSelector());
        const ui64 lsn = record.GetLsn();
        const ui32 generation = record.GetGeneration();

        Counters.Interface.ReadPersistentBuffer.Request();

        auto span = std::move(NWilson::TSpan(TWilson::DDiskTopLevel, std::move(ev->TraceId), "DDisk.ReadPersistentBuffer",
                NWilson::EFlags::NONE, TActivationContext::ActorSystem())
            .Attribute("tablet_id", static_cast<long>(creds.TabletId))
            .Attribute("vchunk_index", static_cast<long>(selector.VChunkIndex))
            .Attribute("offset_in_bytes", selector.OffsetInBytes)
            .Attribute("size", selector.Size)
            .Attribute("lsn", static_cast<long>(lsn)));

        auto it = PersistentBuffers.find({creds.TabletId, generation, selector.VChunkIndex});
        if (it == PersistentBuffers.end()) {
            Counters.Interface.ReadPersistentBuffer.Reply(false);
            span.End();
            SendReply(*ev, std::make_unique<TEvReadPersistentBufferResult>(
                NKikimrBlobStorage::NDDisk::TReplyStatus::MISSING_RECORD));
            return;
        }
        TPersistentBuffer& buffer = it->second;

        auto jt = buffer.Records.find(lsn);
        if (jt == buffer.Records.end()) {
            Counters.Interface.ReadPersistentBuffer.Reply(false);
            span.End();
            SendReply(*ev, std::make_unique<TEvReadPersistentBufferResult>(
                NKikimrBlobStorage::NDDisk::TReplyStatus::MISSING_RECORD));
            return;
        }
        TPersistentBuffer::TRecord& pr = jt->second;
        Y_ABORT_UNLESS(pr.OffsetInBytes == selector.OffsetInBytes);
        Y_ABORT_UNLESS(pr.Size == selector.Size);

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
        });
        Y_ABORT_UNLESS(inserted);

        if (!pr.DataParts.empty()) {
            Y_ABORT_UNLESS(pr.DataParts.size() == pr.PartsCount);
            ReplyReadPersistentBuffer(operationCookie, pr.JoinData(SectorSize));
        } else {
            Y_ABORT_UNLESS(pr.DataParts.empty() && pr.PartsCount == 0 && pr.Sectors.size() > 1);
            // Zero sector contains persistent buffer header, we skip it
            for (ui32 sectorIdx = 1, first = 1; sectorIdx <= pr.Sectors.size(); sectorIdx++) {
                if (sectorIdx == pr.Sectors.size()
                    || pr.Sectors[first].ChunkIdx != pr.Sectors[sectorIdx].ChunkIdx
                    || pr.Sectors[first].SectorIdx + sectorIdx - first != pr.Sectors[sectorIdx].SectorIdx) {
                    const ui64 cookie = NextCookie++;
                    inflightIt->second.OperationCookies.emplace(cookie);
                    std::unique_ptr<TDirectIoOpBase> op = std::make_unique<TPersistentBufferPartIoOp>(SelfId(), Counters);
                    auto* partOp = static_cast<TPersistentBufferPartIoOp*>(op.get());
                    partOp->SetCookie(operationCookie);
                    partOp->SetPartCookie(cookie);
                    auto dataOffset = pr.Sectors[first].SectorIdx * SectorSize;
                    auto size = (pr.Sectors[sectorIdx - 1].SectorIdx - pr.Sectors[first].SectorIdx + 1) * SectorSize;
                    auto offset = DiskFormat->Offset(pr.Sectors[first].ChunkIdx, 0, dataOffset);
                    op->PrepareRead(size, offset, pr.Sectors[first].ChunkIdx, dataOffset);
                    DirectUringOp(op);
                    pr.PartsCount++;
                    first = sectorIdx;
                }
            }
        }
    }

    void TDDiskActor::ReplyReadPersistentBuffer(ui64 operationCookie, TRope&& data) {
        auto inflightIt = PersistentBufferDiskOperationInflight.find(operationCookie);
        Y_ABORT_UNLESS(inflightIt != PersistentBufferDiskOperationInflight.end());
        auto& inflight = inflightIt->second;

        auto replyEv = std::make_unique<TEvReadPersistentBufferResult>(inflight.Status, inflight.ErrorMessage, data);
        auto h = std::make_unique<IEventHandle>(inflight.Sender, SelfId(), replyEv.release(), 0, inflight.Cookie);
        if (inflight.Session) {
            h->Rewrite(TEvInterconnect::EvForward, inflight.Session);
        }
        TActivationContext::Send(h.release());

        const bool ok = (inflight.Status == NKikimrBlobStorage::NDDisk::TReplyStatus::OK);
        Counters.Interface.ReadPersistentBuffer.Reply(ok, data.size());
        inflight.Span.End();
        PersistentBufferDiskOperationInflight.erase(inflightIt);
    }

    ui64 TDDiskActor::CalcPersistentBufferInMemoryCacheSize() {
        ui64 res = 0;
        for (auto& [_, v] : PersistentBuffers) {
            for (auto& [__, r] : v.Records) {
                if (r.PartsCount == r.DataParts.size()) {
                    for (auto& [___, d] : r.DataParts) {
                        res += d.size();
                    }
                }
            }
        }
        return res;
    }

    void TDDiskActor::SanitizePersistentBufferInMemoryCache(TPersistentBuffer::TRecord& record, bool force) {
        if ((force || PersistentBufferInMemoryCacheSize > PersistentBufferFormat.MaxInMemoryCache)
            && record.PartsCount > 0 && record.DataParts.size() == record.PartsCount) {
            Y_DEBUG_ABORT_UNLESS(PersistentBufferInMemoryCacheSize == CalcPersistentBufferInMemoryCacheSize());
            Y_ABORT_UNLESS(PersistentBufferInMemoryCacheSize >= record.Size);
            PersistentBufferInMemoryCacheSize -= record.Size;
            record.DataParts.clear();
            record.PartsCount = 0;
        }
    }

    TRope TDDiskActor::TPersistentBuffer::TRecord::JoinData(ui32 sectorSize) {
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

    void TDDiskActor::Handle(TEvBatchErasePersistentBuffer::TPtr ev) {
        if (!CheckQuery(*ev, &Counters.Interface.ErasePersistentBuffer)) {
            return;
        }
        if (!PersistentBufferReady) {
            PendingPersistentBufferEvents.emplace(ev, "WaitingPersistentBufferBatchErase");
            return;
        }

        const auto& record = ev->Get()->Record;

        Counters.Interface.ErasePersistentBuffer.Request();

        auto span = std::move(NWilson::TSpan(TWilson::DDiskTopLevel, std::move(ev->TraceId), "DDisk.BatchErasePersistentBuffer",
                NWilson::EFlags::NONE, TActivationContext::ActorSystem()));

        const TQueryCredentials creds(record.GetCredentials());
        for (auto& e : record.GetErases()) {
            const TBlockSelector selector(e.GetSelector());
            auto lsn = e.GetLsn();
            auto generation = e.GetGeneration();
            const auto it = PersistentBuffers.find({creds.TabletId, generation, selector.VChunkIndex});
            if (it == PersistentBuffers.end()
                || it->second.Records.find(lsn) == it->second.Records.end()) {
                Counters.Interface.ErasePersistentBuffer.Reply(false);
                span.End();
                SendReply(*ev, std::make_unique<TEvErasePersistentBufferResult>(
                    NKikimrBlobStorage::NDDisk::TReplyStatus::MISSING_RECORD));
                return;
            }
        }

        const ui64 batchEraseCookie = NextCookie++;

        auto [inflightRecord, inserted] = PersistentBufferDiskOperationInflight.try_emplace(batchEraseCookie, TPersistentBufferDiskOperationInFlight{
            .Sender = ev->Sender,
            .Cookie = ev->Cookie,
            .Session = ev->InterconnectSession,
            .Span = std::move(span)
        });
        Y_ABORT_UNLESS(inserted);

        // TODO: flush once
        for (auto& e : record.GetErases()) {
            const TBlockSelector selector(e.GetSelector());
            auto lsn = e.GetLsn();
            auto generation = e.GetGeneration();
            const auto it = PersistentBuffers.find({creds.TabletId, generation, selector.VChunkIndex});
            TPersistentBuffer& buffer = it->second;
            const auto jt = buffer.Records.find(lsn);
            TPersistentBuffer::TRecord& pr = jt->second;
            SanitizePersistentBufferInMemoryCache(pr, true);

            Y_ABORT_UNLESS(pr.OffsetInBytes == selector.OffsetInBytes);
            Y_ABORT_UNLESS(pr.Size == selector.Size);

            PersistentBufferSpaceAllocator.Free(pr.Sectors);

            const ui64 cookie = NextCookie++;
            inflightRecord->second.OperationCookies.insert(cookie);
            auto zeroingData = TRcBuf::UninitializedPageAligned(SectorSize);
            memset(zeroingData.GetDataMut(), 0, SectorSize);

            auto chunkOffset = pr.Sectors[0].SectorIdx * SectorSize;
            auto diskOffset = DiskFormat->Offset(pr.Sectors[0].ChunkIdx, 0, chunkOffset);
            std::unique_ptr<TDirectIoOpBase> op = std::make_unique<TPersistentBufferPartIoOp>(SelfId(), Counters);
            auto* partOp = static_cast<TPersistentBufferPartIoOp*>(op.get());
            partOp->SetCookie(batchEraseCookie);
            partOp->SetPartCookie(cookie);
            partOp->SetIsErase(true);
            partOp->PrepareWrite(TRope(zeroingData), diskOffset, pr.Sectors[0].ChunkIdx, chunkOffset);

            buffer.Records.erase(jt);
            if (buffer.Records.empty()) {
                PersistentBuffers.erase(it);
            }

            DirectUringOp(op);
        }
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
        const TBlockSelector selector(record.GetSelector());
        const ui64 lsn = record.GetLsn();
        const ui32 generation = record.GetGeneration();

        Counters.Interface.ErasePersistentBuffer.Request();

        auto span = std::move(NWilson::TSpan(TWilson::DDiskTopLevel, std::move(ev->TraceId), "DDisk.ErasePersistentBuffer",
                NWilson::EFlags::NONE, TActivationContext::ActorSystem())
            .Attribute("tablet_id", static_cast<long>(creds.TabletId))
            .Attribute("vchunk_index", static_cast<long>(selector.VChunkIndex))
            .Attribute("offset_in_bytes", selector.OffsetInBytes)
            .Attribute("size", selector.Size)
            .Attribute("lsn", static_cast<long>(lsn)));


        const ui64 eraseCookie = NextCookie++;

        const auto it = PersistentBuffers.find({creds.TabletId, generation, selector.VChunkIndex});
        if (it == PersistentBuffers.end()) {
            Counters.Interface.ErasePersistentBuffer.Reply(false);
            span.End();
            SendReply(*ev, std::make_unique<TEvErasePersistentBufferResult>(
                NKikimrBlobStorage::NDDisk::TReplyStatus::MISSING_RECORD));
            return;
        }
        TPersistentBuffer& buffer = it->second;

        const auto jt = buffer.Records.find(lsn);
        if (jt == buffer.Records.end()) {
            Counters.Interface.ErasePersistentBuffer.Reply(false);
            span.End();
            SendReply(*ev, std::make_unique<TEvErasePersistentBufferResult>(
                NKikimrBlobStorage::NDDisk::TReplyStatus::MISSING_RECORD));
            return;
        }
        TPersistentBuffer::TRecord& pr = jt->second;
        SanitizePersistentBufferInMemoryCache(pr, true);

        Y_ABORT_UNLESS(pr.OffsetInBytes == selector.OffsetInBytes);
        Y_ABORT_UNLESS(pr.Size == selector.Size);

        PersistentBufferSpaceAllocator.Free(pr.Sectors);

        const ui64 cookie = NextCookie++;
        auto zeroingData = TRcBuf::UninitializedPageAligned(SectorSize);
        memset(zeroingData.GetDataMut(), 0, SectorSize);

       auto [inflightRecord, inserted] = PersistentBufferDiskOperationInflight.try_emplace(eraseCookie, TPersistentBufferDiskOperationInFlight{
            .Sender = ev->Sender,
            .Cookie = ev->Cookie,
            .Session = ev->InterconnectSession,
            .Span = std::move(span)
        });
        Y_ABORT_UNLESS(inserted);

        inflightRecord->second.OperationCookies.insert(cookie);
        auto chunkOffset = pr.Sectors[0].SectorIdx * SectorSize;
        auto diskOffset = DiskFormat->Offset(pr.Sectors[0].ChunkIdx, 0, chunkOffset);
        std::unique_ptr<TDirectIoOpBase> op = std::make_unique<TPersistentBufferPartIoOp>(SelfId(), Counters);
        auto* partOp = static_cast<TPersistentBufferPartIoOp*>(op.get());
        partOp->SetCookie(eraseCookie);
        partOp->SetPartCookie(cookie);
        partOp->SetIsErase(true);
        partOp->PrepareWrite(TRope(zeroingData), diskOffset, pr.Sectors[0].ChunkIdx, chunkOffset);

        buffer.Records.erase(jt);
        if (buffer.Records.empty()) {
            PersistentBuffers.erase(it);
        }

        DirectUringOp(op);
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

        Counters.Interface.ListPersistentBuffer.Request();

        auto reply = std::make_unique<TEvListPersistentBufferResult>(NKikimrBlobStorage::NDDisk::TReplyStatus::OK);
        auto& rr = reply->Record;

        for (auto it = PersistentBuffers.lower_bound({creds.TabletId, 0, 0}); it != PersistentBuffers.end() &&
                std::get<0>(it->first) == creds.TabletId; ++it) {
            const TPersistentBuffer& buffer = it->second;
            for (const auto& [lsn, pr] : buffer.Records) {
                auto *pb = rr.AddRecords();
                auto *sel = pb->MutableSelector();
                sel->SetVChunkIndex(std::get<2>(it->first));
                sel->SetOffsetInBytes(pr.OffsetInBytes);
                sel->SetSize(pr.Size);
                pb->SetGeneration(std::get<1>(it->first));
                pb->SetLsn(lsn);
            }
        }

        Counters.Interface.ListPersistentBuffer.Reply(true, reply->CalculateSerializedSizeCached());
        SendReply(*ev, std::move(reply));
    }

    TString TDDiskActor::PersistentBufferToString() {
        TStringBuilder sb;
        sb << "PersistentBuffer size:" << PersistentBuffers.size() << "\n";
        for (auto [k,v] : PersistentBuffers) {
            sb << "  TabletId:" << std::get<0>(k) << " VChunk:" << std::get<1>(k) << "\n";
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
