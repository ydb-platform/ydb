#include "ddisk_actor.h"

#include <ydb/core/util/stlog.h>
#include <ydb/core/util/pb.h>

#define XXH_INLINE_ALL
#include <contrib/libs/xxhash/xxhash.h>

namespace NKikimr::NDDisk {

    void TDDiskActor::Handle(TEvPrivate::TEvHandlePersistentBufferEventForChunk::TPtr ev) {
        auto chunkIdx = ev->Get()->ChunkIndex;
        Y_ABORT_UNLESS(chunkIdx);
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

    ui64 CalculateChecksum(const char* begin, size_t numBytes) {
        XXH3_state_t state;
        XXH3_64bits_reset(&state);
        XXH3_64bits_update(&state, begin, numBytes);
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
                        ui64 sectorChecksum = CalculateChecksum((char*)&sector, SectorSize);
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
                        PersistentBufferSectorsChecksum[chunkIdx][sectorIdx] = CalculateChecksum(dataPos, SectorSize);
                    }
                }
                PersistentBufferRestoredChunks.insert(chunkIdx);
                StartRestorePersistentBuffer(pos + 1);
                if (PersistentBufferRestoreChunksInflight == 0) {
                    for (auto& [key, pb] : PersistentBuffers) {
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
            if (memcmp((*it).first, header->Signature, 16) == 0) {
                loc.HasSignatureCorrection = true;
                *payload.Position(SectorSize * (i - 1)).ContiguousDataMut() = 0;
            }
            loc.Checksum = CalculateChecksum(payload.Position(SectorSize * (i - 1)), SectorSize);
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
                    header->HeaderChecksum = CalculateChecksum(data.Begin(), SectorSize);
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
        Y_ABORT_UNLESS(sectors.size() == sectorsCnt && sectorsCnt <= MaxSectorsPerBufferRecord && sectorsCnt > 1);

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
                    auto replyEv = std::make_unique<TEvWritePersistentBufferResult>(NKikimrBlobStorage::NDDisk::TReplyStatus::OK);
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
        PersistentBufferReadInflight.try_emplace(ev->Cookie, TPersistentBufferToDiskReadInFlight{std::move(span)});
        GetPersistentBufferRecordData(pr, [this, selector, cookie = ev->Cookie, ev = ev.Release()](TRope data) {
            Counters.Interface.ReadPersistentBuffer.Reply(true, selector.Size);
            auto it = PersistentBufferReadInflight.find(cookie);
            Y_ABORT_UNLESS(it != PersistentBufferReadInflight.end());
            it->second.Span.End();
            PersistentBufferReadInflight.erase(it);
            SendReply(*ev, std::make_unique<TEvReadPersistentBufferResult>(NKikimrBlobStorage::NDDisk::TReplyStatus::OK,
                std::nullopt, data));
        });
    }

    void TDDiskActor::GetPersistentBufferRecordData(TDDiskActor::TPersistentBuffer::TRecord& pr, std::function<void(TRope data)> callback) {
        if (!pr.DataParts.empty()) {
            Y_ABORT_UNLESS(pr.DataParts.size() == pr.PartsCount);
            callback(pr.JoinData());
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
                            callback(pr.JoinData());
                            PersistentBufferInMemoryCacheSize += pr.Size;
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


    TRope TDDiskActor::TPersistentBuffer::TRecord::JoinData() {
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
                *payload.Position(SectorSize * (i - 1)).ContiguousDataMut() = TPersistentBufferHeader::PersistentBufferHeaderSignature[0];
            }
        }
        return DataParts.begin()->second;
    }

    void TDDiskActor::Handle(TEvFlushPersistentBuffer::TPtr ev) {
        if (!CheckQuery(*ev, &Counters.Interface.FlushPersistentBuffer)) {
            return;
        }

        if (!PersistentBufferReady) {
            PendingPersistentBufferEvents.emplace(ev, "WaitingPersistentBufferFlush");
            return;
        }

        const auto& record = ev->Get()->Record;
        const TQueryCredentials creds(record.GetCredentials());
        const TBlockSelector selector(record.GetSelector());
        const ui64 lsn = record.GetLsn();

        Counters.Interface.FlushPersistentBuffer.Request();

        auto span = std::move(NWilson::TSpan(TWilson::DDiskTopLevel, std::move(ev->TraceId), "DDisk.FlushPersistentBuffer",
                NWilson::EFlags::NONE, TActivationContext::ActorSystem())
            .Attribute("tablet_id", static_cast<long>(creds.TabletId))
            .Attribute("vchunk_index", static_cast<long>(selector.VChunkIndex))
            .Attribute("offset_in_bytes", selector.OffsetInBytes)
            .Attribute("size", selector.Size)
            .Attribute("lsn", static_cast<long>(lsn)));

        const auto it = PersistentBuffers.find({creds.TabletId, selector.VChunkIndex});
        if (it == PersistentBuffers.end()) {
            Counters.Interface.FlushPersistentBuffer.Reply(false);
            span.End();
            SendReply(*ev, std::make_unique<TEvFlushPersistentBufferResult>(
                NKikimrBlobStorage::NDDisk::TReplyStatus::MISSING_RECORD));
            return;
        }
        TPersistentBuffer& buffer = it->second;

        const auto jt = buffer.Records.find(lsn);
        if (jt == buffer.Records.end()) {
            Counters.Interface.FlushPersistentBuffer.Reply(false);
            span.End();
            SendReply(*ev, std::make_unique<TEvFlushPersistentBufferResult>(
                NKikimrBlobStorage::NDDisk::TReplyStatus::MISSING_RECORD));
            return;
        }
        TPersistentBuffer::TRecord& pr = jt->second;
        Y_ABORT_UNLESS(pr.OffsetInBytes == selector.OffsetInBytes);
        Y_ABORT_UNLESS(pr.Size == selector.Size);
        Y_DEBUG_ABORT_UNLESS(record.HasDDiskId());
        const ui64 cookie = NextWriteCookie++;
        WritesInFlight.emplace(cookie, TWriteInFlight{ev->Sender, ev->Cookie, ev->InterconnectSession,
            std::move(span), selector.Size});
        GetPersistentBufferRecordData(pr, [this, cookie, selector, creds, record, &pr](TRope data) {
            auto query = std::make_unique<TEvWrite>(TQueryCredentials(creds.TabletId, creds.Generation,
                record.GetDDiskInstanceGuid(), true), selector, TWriteInstruction(0));
            query->AddPayload(std::move(data));

            const auto& ddiskId = record.GetDDiskId();
            Send(MakeBlobStorageDDiskId(ddiskId.GetNodeId(), ddiskId.GetPDiskId(), ddiskId.GetDDiskSlotId()),
                query.release(), IEventHandle::FlagTrackDelivery, cookie, WritesInFlight[cookie].Span.GetTraceId());
            SanitizePersistentBufferInMemoryCache(pr);
        });
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

        buffer.Records.erase(jt);
        if (buffer.Records.empty()) {
            PersistentBuffers.erase(it);
        }
        Counters.Interface.ErasePersistentBuffer.Reply(true);
        span.End();
        SendReply(*ev, std::make_unique<TEvErasePersistentBufferResult>(NKikimrBlobStorage::NDDisk::TReplyStatus::OK));
    }

    void TDDiskActor::Handle(TEvWriteResult::TPtr ev) {
        HandleWriteInFlight(ev->Cookie, [&] {
            const auto& record = ev->Get()->Record;

            auto reply = std::make_unique<TEvFlushPersistentBufferResult>();
            auto& rr = reply->Record;

            rr.SetStatus(record.GetStatus());
            if (record.HasErrorReason()) {
                rr.SetErrorReason(record.GetErrorReason());
            }

            return reply;
        });
    }

    void TDDiskActor::Handle(TEvents::TEvUndelivered::TPtr ev) {
        if (ev->Get()->SourceType == TEv::EvWrite) {
            HandleWriteInFlight(ev->Cookie, [&] {
                return std::make_unique<TEvFlushPersistentBufferResult>(NKikimrBlobStorage::NDDisk::TReplyStatus::ERROR,
                    "write undelivered");
            });
        }
    }

    void TDDiskActor::HandleWriteInFlight(ui64 cookie, const std::function<std::unique_ptr<IEventBase>()>& factory) {
        if (const auto it = WritesInFlight.find(cookie); it != WritesInFlight.end()) {
            TWriteInFlight& wif = it->second;
            auto h = std::make_unique<IEventHandle>(wif.Sender, SelfId(), factory().release(), 0, wif.Cookie);
            if (wif.InterconnectionSessionId) {
                h->Rewrite(TEvInterconnect::EvForward, wif.InterconnectionSessionId);
            }
            wif.Span.End();
            if (auto *ptr = h->CastAsLocal<TEvFlushPersistentBufferResult>()) {
                const bool success = ptr->Record.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::OK;
                Counters.Interface.FlushPersistentBuffer.Reply(success, success ? wif.Size : 0);
            }
            TActivationContext::Send(h.release());
            WritesInFlight.erase(it);
        }
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

} // NKikimr::NDDisk
