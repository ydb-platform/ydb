#include "ddisk_actor.h"

#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_data.h>
#include <ydb/core/util/stlog.h>
#include <ydb/core/util/pb.h>

#define XXH_INLINE_ALL
#include <contrib/libs/xxhash/xxhash.h>

namespace NKikimr::NDDisk {

    void TDDiskActor::InitPersistentBuffer(NPDisk::TPersistentBufferFormatPtr&& format) {
        Y_ABORT_UNLESS(DiskFormat);
        SectorSize = DiskFormat->SectorSize;
        Y_ABORT_UNLESS(SectorSize >= sizeof(TPersistentBufferHeader));
        ChunkSize = DiskFormat->ChunkSize;
        Y_ABORT_UNLESS(ChunkSize % SectorSize == 0);
        SectorInChunk = ChunkSize / SectorSize;
        MaxChunks = format->MaxChunks;
        PersistentBufferInitChunks = format->InitChunks;
        MaxPersistentBufferInMemoryCache = format->MaxInMemoryCache;
        MaxPersistentBufferChunkRestoreInflight = format->MaxChunkRestoreInflight;
        PersistentBufferSpaceAllocator = TPersistentBufferSpaceAllocator(SectorInChunk);
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

    void TDDiskActor::StartRestorePersistentBuffer(ui32 pos) {
        if (PersistentBufferReady) {
            return;
        }
        if (PersistentBufferSpaceAllocator.OwnedChunks.size() < PersistentBufferInitChunks) {
            IssuePersistentBufferChunkAllocation();
            return;
        }

        if (PersistentBufferSpaceAllocator.OwnedChunks.size() == PersistentBufferAllocatedChunks.size()) {
            STLOG(PRI_DEBUG, BS_DDISK, BSDD12, "TDDiskActor::StartRestorePersistentBuffer ready");
            PersistentBufferReady = true;
            return;
        }
        while (pos < PersistentBufferSpaceAllocator.OwnedChunks.size() && PersistentBufferRestoreChunksInflight < MaxPersistentBufferChunkRestoreInflight) {
            auto chunkIdx = PersistentBufferSpaceAllocator.OwnedChunks[pos];
            STLOG(PRI_DEBUG, BS_DDISK, BSDD13, "TDDiskActor::StartRestorePersistentBuffer restoring chunk from DDisk", (ChunkIdx, chunkIdx));
            if (PersistentBufferAllocatedChunks.count(chunkIdx) > 0 || PersistentBufferRestoredChunks.count(chunkIdx) > 0) {
                pos++;
                continue;
            }
            const ui64 cookie = NextCookie++;
            PersistentBufferRestoreChunksInflight++;
            Send(BaseInfo.PDiskActorID, new NPDisk::TEvChunkReadRaw(
                PDiskParams->Owner,
                PDiskParams->OwnerRound,
                chunkIdx,
                0,
                ChunkSize), 0, cookie);
            ReadCallbacks.try_emplace(cookie, TPersistentBufferPendingRead{[this, chunkIdx, pos](NPDisk::TEvChunkReadRawResult& ev) {
                PersistentBufferRestoreChunksInflight--;
                Y_ABORT_UNLESS(ev.Data.size() == ChunkSize);
                for (ui32 sectorIdx = 0; sectorIdx < SectorInChunk; sectorIdx++) {
                    auto dataPos = ev.Data.Position(sectorIdx * SectorSize);
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
                        auto& buffer = PersistentBuffers[{header->TabletId, header->VChunkIndex}];
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
                PersistentBufferRestoredChunks.insert(chunkIdx);
                StartRestorePersistentBuffer(pos + 1);
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
            }});
            pos++;
        }

    }

    std::vector<std::tuple<ui32, ui32, TRope>> TDDiskActor::SlicePersistentBuffer(ui64 tabletId, ui64 vchunkIndex,
        ui64 lsn, ui32 offsetInBytes, ui32 sizeInBytes, TRope&& payload, const std::vector<TPersistentBufferSectorInfo>& sectors) {
        auto headerData = TRcBuf::Uninitialized(SectorSize);
        TPersistentBufferHeader *header = (TPersistentBufferHeader*)headerData.GetDataMut();
        memset(header, 0, SectorSize);
        memcpy(header->Signature, TPersistentBufferHeader::PersistentBufferHeaderSignature, 16);
        header->TabletId = tabletId;
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

    void TDDiskActor::ProcessPersistentBufferWrite(TEvWritePersistentBuffer::TPtr ev) {
        const auto& record = ev->Get()->Record;
        const TQueryCredentials creds(record.GetCredentials());
        const TBlockSelector selector(record.GetSelector());
        const ui64 lsn = record.GetLsn();
        ui32 sectorsCnt = selector.Size / SectorSize + 1;
        const auto sectors = PersistentBufferSpaceAllocator.Occupy(sectorsCnt);
        if (sectors.size() == 0) {
            if (PersistentBufferSpaceAllocator.OwnedChunks.size() < MaxChunks) {
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

        auto parts = SlicePersistentBuffer(creds.TabletId,
            selector.VChunkIndex, lsn, selector.OffsetInBytes, selector.Size, TRope(payload), sectors);

        auto& inflightRecord = PersistentBufferWriteInflight[{creds.TabletId, selector.VChunkIndex, lsn}];
        inflightRecord = {
            .Sender = ev->Sender,
            .Cookie = ev->Cookie,
            .Session = ev->InterconnectSession,
            .OffsetInBytes = selector.OffsetInBytes,
            .Size = selector.Size,
            .Sectors = std::move(sectors),
            .Data = std::move(payload),
            .Span = std::move(span),
        };

        for(auto& [chunkIdx, offset, data] : parts) {
            const ui64 cookie = NextCookie++;
            inflightRecord.WriteCookies.insert(cookie);

            Send(BaseInfo.PDiskActorID, new NPDisk::TEvChunkWriteRaw(
                PDiskParams->Owner,
                PDiskParams->OwnerRound,
                chunkIdx,
                offset,
                std::move(data)), 0, cookie);

            WriteCallbacks.try_emplace(cookie, [this, writeCookie = cookie, tabletId = creds.TabletId,
                vchunkIndex = selector.VChunkIndex, lsn](NPDisk::TEvChunkWriteRawResult& /*ev*/) {
                auto itInflight = PersistentBufferWriteInflight.find({tabletId, vchunkIndex, lsn});
                Y_ABORT_UNLESS(itInflight != PersistentBufferWriteInflight.end());
                auto& inflight = itInflight->second;
                auto eraseCnt = inflight.WriteCookies.erase(writeCookie);
                Y_ABORT_UNLESS(eraseCnt == 1);
                if (inflight.WriteCookies.empty()) {
                    Counters.Interface.WritePersistentBuffer.Reply(true);
                    inflight.Span.End();
                    auto& buffer = PersistentBuffers[{tabletId, vchunkIndex}];
                    auto [it, inserted] = buffer.Records.try_emplace(lsn);
                    TPersistentBuffer::TRecord& pr = it->second;
                    if (inserted) {
                        pr = {
                            .OffsetInBytes = inflight.OffsetInBytes,
                            .Size = inflight.Size,
                            .Sectors = std::move(inflight.Sectors),
                            .PartsCount = 1,
                        };
                        pr.DataParts.emplace(0, std::move(inflight.Data));
                        PersistentBufferInMemoryCacheSize += pr.Size;
                    } else {
                        Y_ABORT_UNLESS(pr.OffsetInBytes == inflight.OffsetInBytes);
                        Y_ABORT_UNLESS(pr.Size == inflight.Size);
                        Y_ABORT_UNLESS(pr.PartsCount == 1);
                        Y_ABORT_UNLESS(pr.DataParts.begin()->second == inflight.Data);
                    }
                    auto replyEv = std::make_unique<TEvWritePersistentBufferResult>(
                        NKikimrBlobStorage::NDDisk::TReplyStatus::OK, std::nullopt, GetPersistentBufferFreeSpace());
                    auto h = std::make_unique<IEventHandle>(inflight.Sender, SelfId(), replyEv.release(), 0, inflight.Cookie);
                    if (inflight.Session) {
                        h->Rewrite(TEvInterconnect::EvForward, inflight.Session);
                    }
                    TActivationContext::Send(h.release());
                    PersistentBufferWriteInflight.erase(itInflight);
                }
            });
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

        Counters.Interface.ReadPersistentBuffer.Request();

        auto span = std::move(NWilson::TSpan(TWilson::DDiskTopLevel, std::move(ev->TraceId), "DDisk.ReadPersistentBuffer",
                NWilson::EFlags::NONE, TActivationContext::ActorSystem())
            .Attribute("tablet_id", static_cast<long>(creds.TabletId))
            .Attribute("vchunk_index", static_cast<long>(selector.VChunkIndex))
            .Attribute("offset_in_bytes", selector.OffsetInBytes)
            .Attribute("size", selector.Size)
            .Attribute("lsn", static_cast<long>(lsn)));

        auto it = PersistentBuffers.find({creds.TabletId, selector.VChunkIndex});
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
        auto [_, inserted] = PersistentBufferDiskOperationInflight.try_emplace(operationCookie, TPersistentBufferDiskOperationInFlight{.Span = std::move(span)});
        Y_ABORT_UNLESS(inserted);

        GetPersistentBufferRecordData(pr, [this, selector, operationCookie, ev = std::move(ev)](TRope data) {
            Counters.Interface.ReadPersistentBuffer.Reply(true, selector.Size);
            auto it = PersistentBufferDiskOperationInflight.find(operationCookie);
            Y_ABORT_UNLESS(it != PersistentBufferDiskOperationInflight.end());
            it->second.Span.End();
            PersistentBufferDiskOperationInflight.erase(it);
            SendReply(*ev, std::make_unique<TEvReadPersistentBufferResult>(NKikimrBlobStorage::NDDisk::TReplyStatus::OK,
                std::nullopt, data));
        });
    }

    void TDDiskActor::GetPersistentBufferRecordData(TDDiskActor::TPersistentBuffer::TRecord& pr, std::function<void(TRope data)> callback) {
        if (!pr.DataParts.empty()) {
            Y_ABORT_UNLESS(pr.DataParts.size() == pr.PartsCount);
            callback(pr.JoinData(SectorSize));
        } else {
            Y_ABORT_UNLESS(pr.DataParts.empty() && pr.PartsCount == 0 && pr.Sectors.size() > 1);
            // Zero sector contains persistent buffer header, we skip it
            for (ui32 sectorIdx = 1, first = 1; sectorIdx <= pr.Sectors.size(); sectorIdx++) {
                if (sectorIdx == pr.Sectors.size()
                    || pr.Sectors[first].ChunkIdx != pr.Sectors[sectorIdx].ChunkIdx
                    || pr.Sectors[first].SectorIdx + sectorIdx - first != pr.Sectors[sectorIdx].SectorIdx) {
                    const ui64 cookie = NextCookie++;
                    Send(BaseInfo.PDiskActorID, new NPDisk::TEvChunkReadRaw(
                        PDiskParams->Owner,
                        PDiskParams->OwnerRound,
                        pr.Sectors[first].ChunkIdx,
                        pr.Sectors[first].SectorIdx * SectorSize,
                        (pr.Sectors[sectorIdx - 1].SectorIdx - pr.Sectors[first].SectorIdx + 1) * SectorSize), 0, cookie);
                    ReadCallbacks.try_emplace(cookie, TPersistentBufferPendingRead{[this, &pr, callback, partIdx = pr.PartsCount](NPDisk::TEvChunkReadRawResult& ev) {
                        pr.DataParts.emplace(partIdx, std::move(ev.Data));
                        if (pr.DataParts.size() == pr.PartsCount) {
                            PersistentBufferInMemoryCacheSize += pr.Size;
                            callback(pr.JoinData(SectorSize));
                            SanitizePersistentBufferInMemoryCache(pr);
                        }
                    }});
                    pr.PartsCount++;
                    first = sectorIdx;
                }
            }
        }
    }

    void TDDiskActor::SanitizePersistentBufferInMemoryCache(TPersistentBuffer::TRecord& record, bool force) {
        if ((force || PersistentBufferInMemoryCacheSize > MaxPersistentBufferInMemoryCache)
            && record.PartsCount > 0) {
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
            const auto it = PersistentBuffers.find({creds.TabletId, selector.VChunkIndex});
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
        for (auto& e : record.GetErases()) {
            const TBlockSelector selector(e.GetSelector());
            auto lsn = e.GetLsn();
            const auto it = PersistentBuffers.find({creds.TabletId, selector.VChunkIndex});
            TPersistentBuffer& buffer = it->second;
            const auto jt = buffer.Records.find(lsn);
            TPersistentBuffer::TRecord& pr = jt->second;
            SanitizePersistentBufferInMemoryCache(pr, true);

            Y_ABORT_UNLESS(pr.OffsetInBytes == selector.OffsetInBytes);
            Y_ABORT_UNLESS(pr.Size == selector.Size);

            PersistentBufferSpaceAllocator.Free(pr.Sectors);

            const ui64 cookie = NextCookie++;
            inflightRecord->second.OperationCookies.insert(cookie);
            auto zeroingData = TRcBuf::Uninitialized(SectorSize);
            *zeroingData.GetDataMut() = 0;

            Send(BaseInfo.PDiskActorID, new NPDisk::TEvChunkWriteRaw(
                PDiskParams->Owner,
                PDiskParams->OwnerRound,
                pr.Sectors[0].ChunkIdx,
                pr.Sectors[0].SectorIdx * SectorSize,
                std::move(zeroingData)), 0, cookie);

            buffer.Records.erase(jt);
            if (buffer.Records.empty()) {
                PersistentBuffers.erase(it);
            }

            WriteCallbacks.try_emplace(cookie, [this, batchEraseCookie, cookie](NPDisk::TEvChunkWriteRawResult& /*ev*/) {
                auto it = PersistentBufferDiskOperationInflight.find(batchEraseCookie);
                Y_ABORT_UNLESS(it != PersistentBufferDiskOperationInflight.end());
                it->second.OperationCookies.erase(cookie);
                if (it->second.OperationCookies.empty()) {
                    Counters.Interface.ErasePersistentBuffer.Reply(true);
                    it->second.Span.End();

                    auto replyEv = std::make_unique<TEvErasePersistentBufferResult>(
                        NKikimrBlobStorage::NDDisk::TReplyStatus::OK, std::nullopt, GetPersistentBufferFreeSpace());
                    auto h = std::make_unique<IEventHandle>(it->second.Sender, SelfId(), replyEv.release(), 0, it->second.Cookie);
                    if (it->second.Session) {
                        h->Rewrite(TEvInterconnect::EvForward, it->second.Session);
                    }
                    TActivationContext::Send(h.release());

                    PersistentBufferDiskOperationInflight.erase(it);
                }
            });
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

        Counters.Interface.ErasePersistentBuffer.Request();

        auto span = std::move(NWilson::TSpan(TWilson::DDiskTopLevel, std::move(ev->TraceId), "DDisk.ErasePersistentBuffer",
                NWilson::EFlags::NONE, TActivationContext::ActorSystem())
            .Attribute("tablet_id", static_cast<long>(creds.TabletId))
            .Attribute("vchunk_index", static_cast<long>(selector.VChunkIndex))
            .Attribute("offset_in_bytes", selector.OffsetInBytes)
            .Attribute("size", selector.Size)
            .Attribute("lsn", static_cast<long>(lsn)));


        const ui64 eraseCookie = NextCookie++;

        const auto it = PersistentBuffers.find({creds.TabletId, selector.VChunkIndex});
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
        auto zeroingData = TRcBuf::Uninitialized(SectorSize);
        *zeroingData.GetDataMut() = 0;

        Send(BaseInfo.PDiskActorID, new NPDisk::TEvChunkWriteRaw(
            PDiskParams->Owner,
            PDiskParams->OwnerRound,
            pr.Sectors[0].ChunkIdx,
            pr.Sectors[0].SectorIdx * SectorSize,
            std::move(zeroingData)), 0, cookie);

        buffer.Records.erase(jt);
        if (buffer.Records.empty()) {
            PersistentBuffers.erase(it);
        }

        auto [_, inserted] = PersistentBufferDiskOperationInflight.try_emplace(eraseCookie, TPersistentBufferDiskOperationInFlight{.Span = std::move(span)});
        Y_ABORT_UNLESS(inserted);

        WriteCallbacks.try_emplace(cookie, [this, eraseCookie, ev = std::move(ev)](NPDisk::TEvChunkWriteRawResult& /*ev*/) {
            auto it = PersistentBufferDiskOperationInflight.find(eraseCookie);
            Y_ABORT_UNLESS(it != PersistentBufferDiskOperationInflight.end());
            Counters.Interface.ErasePersistentBuffer.Reply(true);
            it->second.Span.End();
            SendReply(*ev, std::make_unique<TEvErasePersistentBufferResult>(
                NKikimrBlobStorage::NDDisk::TReplyStatus::OK, std::nullopt, GetPersistentBufferFreeSpace()));
            PersistentBufferDiskOperationInflight.erase(it);
        });
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

        for (auto it = PersistentBuffers.lower_bound({creds.TabletId, 0}); it != PersistentBuffers.end() &&
                std::get<0>(it->first) == creds.TabletId; ++it) {
            const TPersistentBuffer& buffer = it->second;
            for (const auto& [lsn, pr] : buffer.Records) {
                auto *pb = rr.AddRecords();
                auto *sel = pb->MutableSelector();
                sel->SetVChunkIndex(std::get<1>(it->first));
                sel->SetOffsetInBytes(pr.OffsetInBytes);
                sel->SetSize(pr.Size);
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
        freeSpace += (MaxChunks - ownedChunks) * SectorInChunk;
        freeSpace /= (MaxChunks * SectorInChunk);
        Y_ABORT_UNLESS(freeSpace >= 0 && freeSpace <= 1);
        return freeSpace;
    }

} // NKikimr::NDDisk
