#pragma once
#include "defs.h"

#include "blobstorage_pdisk.h"
#include "blobstorage_pdisk_completion.h"
#include "blobstorage_pdisk_crypto.h"
#include "blobstorage_pdisk_data.h"
#include "blobstorage_pdisk_drivemodel.h"
#include "blobstorage_pdisk_mon.h"
#include "blobstorage_pdisk_request_id.h"

#include <ydb/core/blobstorage/crypto/crypto.h>

#include <util/generic/deque.h>

namespace NKikimr {
namespace NPDisk {

class IBlockDevice;
class TBufferPool;
struct TBuffer;

////////////////////////////////////////////////////////////////////////////
// BufferedWriter
////////////////////////////////////////////////////////////////////////////
class TBufferedWriter {
protected:
    ui64 SectorSize;
    IBlockDevice &BlockDevice;
    TDiskFormat &Format;

    ui64 StartOffset;
    ui64 NextOffset;
    ui64 DirtyFrom;
    ui64 DirtyTo;

    ui8* CurrentSector;

    TBufferPool *Pool;
    TBuffer::TPtr CurrentBuffer;
    TActorSystem *ActorSystem;

    TReqId LastReqId;
    TDriveModel *DriveModel;

    void WriteBufferWithFlush(TReqId reqId, NWilson::TTraceId *traceId,
            TCompletionAction *flushAction, ui32 chunkIdx);
public:
    TBufferedWriter(ui64 sectorSize, IBlockDevice &blockDevice, TDiskFormat &format, TBufferPool *pool,
        TActorSystem *actorSystem, TDriveModel *driveModel);
    void SetupWithBuffer(ui64 startOffset, ui64 currentOffset, TBuffer *buffer, ui32 count, TReqId reqId);
    ui8* Seek(ui64 offset, ui32 count, ui32 reserve, TReqId reqId, NWilson::TTraceId *traceId, ui32 chunkIdx);
    ui8* Get() const;
    ui8* RawData() const;
    void Flush(TReqId reqId, NWilson::TTraceId *traceId, TCompletionAction *flushAction,
            ui32 chunkIdx);
    void MarkDirty();
    void Obliterate();
    ~TBufferedWriter();
};

struct TChunkIdxWithInfo {
    TChunkIdx Idx;
    TLogChunkInfo *Info;
};

struct TFirstUncommitted {
    ui32 ChunkIdx;
    ui32 SectorIdx;

    explicit TFirstUncommitted(ui32 chunkIdx, ui32 sectorIdx) : ChunkIdx(chunkIdx), SectorIdx(sectorIdx) {}
};

////////////////////////////////////////////////////////////////////////////
// TSectorWriter
////////////////////////////////////////////////////////////////////////////
template <bool IsLog, bool IsSysLog>
class TSectorWriter {
public:
    TPDiskMon &Mon;
    IBlockDevice &BlockDevice;
    TDiskFormat &Format;
    ui64 &Nonce;
    THolder<TBufferedWriter> BufferedWriter;
    ui64 CurrentPosition;
    ui32 SectorBytesFree;
    ui64 SectorIdx;
    ui64 FirstSectorIdx;
    ui64 EndSectorIdx;
    ui32 ChunkIdx;
    std::atomic<TFirstUncommitted> FirstUncommitted = TFirstUncommitted(0, 0);
    ui64 RecordBytesLeft;
    ui64 DataMagic;
    TDeque<TChunkIdxWithInfo> NextChunks;
    TLogChunkInfo *LogChunkInfo = nullptr;

    TPDiskHashCalculator Hash;
    TPDiskStreamCypher Cypher;
    TActorSystem *ActorSystem;
    ui32 PDiskId;
    TDriveModel *DriveModel;

    bool OnNewChunk;

    bool IsEmptySector() const {
        return (SectorBytesFree == Format.SectorPayloadSize());
    }

    TSectorWriter(TPDiskMon &mon, IBlockDevice &blockDevice, TDiskFormat &format, ui64 &nonce,
            const TKey &key, TBufferPool *pool, ui64 firstSectorIdx, ui64 endSectorIdx, ui64 dataMagic, ui32 chunkIdx,
            TLogChunkInfo *logChunkInfo, ui64 sectorIdx, TBuffer *buffer, TActorSystem *actorSystem, ui32 pDiskId,
            TDriveModel *driveModel, bool enableEncrytion)
        : Mon(mon)
        , BlockDevice(blockDevice)
        , Format(format)
        , Nonce(nonce)
        , CurrentPosition(0)
        , SectorBytesFree(format.SectorPayloadSize())
        , SectorIdx(sectorIdx)
        , FirstSectorIdx(firstSectorIdx)
        , EndSectorIdx(endSectorIdx)
        , ChunkIdx(chunkIdx)
        , RecordBytesLeft(0)
        , DataMagic(dataMagic)
        , LogChunkInfo(logChunkInfo)
        , Hash()
        , Cypher(enableEncrytion)
        , ActorSystem(actorSystem)
        , PDiskId(pDiskId)
        , DriveModel(driveModel)
        , OnNewChunk(true)
    {
        Y_ABORT_UNLESS(!LogChunkInfo || LogChunkInfo->ChunkIdx == ChunkIdx);
        BufferedWriter.Reset(new TBufferedWriter(Format.SectorSize, BlockDevice, Format, pool, actorSystem,
                    DriveModel));

        Cypher.SetKey(key);
        Cypher.StartMessage(Nonce);

        ui64 sectorOffset = Format.Offset(ChunkIdx, SectorIdx);
        if (buffer) {
            Y_ABORT_UNLESS(IsLog);
            Y_ABORT_UNLESS(!IsSysLog);
            ui64 startOffset = Format.Offset(ChunkIdx, SectorIdx);
            BufferedWriter->SetupWithBuffer(startOffset, sectorOffset, buffer, 1,
                    TReqId(TReqId::CreateTSectorWriterWithBuffer, 0));
        } else {
            BufferedWriter->Seek(sectorOffset, IsSysLog ? ReplicationFactor : 1,
                IsSysLog ? ReplicationFactor : 1,
                TReqId(TReqId::CreateTSectorWriterSeek, 0), nullptr, ChunkIdx);
        }

        if (SectorIdx == 0) {
            if (LogChunkInfo) {
                LogChunkInfo->FirstNonce = Nonce;
            }
        }

        if (ActorSystem) {
            LOG_DEBUG_S(*ActorSystem, NKikimrServices::BS_PDISK, SelfInfo() << " is created at "
                    << " chunkIdx# " << ChunkIdx << " sectorIdx# " << SectorIdx
                    << (buffer ? " WithBuffer" : " NoBuffer"));
        }
    }

    const TString SelfInfo() {
        TStringStream ss;
        ss << "PDiskId# " << PDiskId
            << " TSectorWriter<"
            << (IsLog ? "Log" : "!Log") << ","
            << (IsSysLog ? "SysLog" : "!SysLog")
            << "> ";
        return ss.Str();
    }

    void WriteNextChunkReference(TChunkIdx nextChunk, ui64 nextChunkNonce, TCompletionAction *action,
            TReqId reqId, NWilson::TTraceId *traceId) {

        if (ActorSystem) {
            LOG_INFO_S(*ActorSystem, NKikimrServices::BS_PDISK, SelfInfo()
                    << " WriteNextChunkReference, currentChunkIdx# " << ChunkIdx
                    << " nextChunkIdx# " << nextChunk << " Nonce# " << Nonce);
        }

        ui64 sectorOffset = Format.Offset(ChunkIdx, SectorIdx);
        ui8* sector = BufferedWriter->Seek(sectorOffset, ReplicationFactor, ReplicationFactor, reqId, traceId,
                ChunkIdx);

        memset(sector, 0, Format.SectorSize);
        auto *nextLogChunkReference = (TNextLogChunkReference3*)sector;
        nextLogChunkReference->Version = PDISK_DATA_VERSION_3;
        nextLogChunkReference->NextChunk = nextChunk;
        nextLogChunkReference->CreatedAt = TInstant::Now();
        // zero in typical case, non-zero only in case of log splicing
        nextLogChunkReference->NextChunkFirstNonce = nextChunkNonce;
        nextLogChunkReference->IsNotCompatible = 0;

        memcpy(sector + Format.SectorSize - CanarySize - sizeof(TDataSectorFooter), &Canary, CanarySize);
        Cypher.InplaceEncrypt(sector, Format.SectorSize - (ui32)sizeof(TDataSectorFooter));

        PrepareDataSectorFooter(sector, Format.MagicNextLogChunkReference, sectorOffset);

        for (ui32 replica = 1; replica < ReplicationFactor; ++replica) {
            memcpy(sector + Format.SectorSize * replica, sector, Format.SectorSize);
        }
        BufferedWriter->MarkDirty();
        SectorIdx += ReplicationFactor;

        if (!IsSysLog) {
            *Mon.BandwidthPLogChunkFooter += Format.SectorSize * ReplicationFactor;
            *Mon.BandwidthPLogChunkPadding += Format.ChunkSize - SectorIdx * Format.SectorSize;
        }

        BufferedWriter->Flush(reqId, traceId, action, ChunkIdx);
    }

    void SwitchToNewChunk(TReqId reqId, NWilson::TTraceId *traceId) {
        // Allocate next log chunk, write next log chunk pointer sectors, switch to that log chunk.
        Y_ABORT_UNLESS(IsLog);
        Y_ABORT_UNLESS(!NextChunks.empty());
        ui32 nextChunk = NextChunks.front().Idx;
        TLogChunkInfo *nextLogChunkInfo = NextChunks.front().Info;
        NextChunks.pop_front();

        WriteNextChunkReference(nextChunk, 0, nullptr, reqId, traceId);

        // Start working with new chunk
        OnNewChunk = true;
        ChunkIdx = nextChunk;
        LogChunkInfo = nextLogChunkInfo;
        LogChunkInfo->FirstNonce = Nonce;
        LogChunkInfo->DesiredPrevChunkLastNonce = Nonce - 1;
        SectorIdx = 0;
        FirstSectorIdx = 0;
        ui64 sectorOffset = Format.Offset(ChunkIdx, SectorIdx);

        ui32 seekCount = IsSysLog ?  ReplicationFactor : 1;
        BufferedWriter->Seek(sectorOffset, IsSysLog ? ReplicationFactor : 1, seekCount, reqId, traceId,
                ChunkIdx);
    }

    void Obliterate() {
        SectorBytesFree = Format.SectorPayloadSize();
        BufferedWriter->Obliterate();
    }

    void NextSector(const ui64 dataMagic, TReqId reqId, NWilson::TTraceId *traceId) {
        if (OnNewChunk) {
            OnNewChunk = false;
            if (ActorSystem) {
                LOG_DEBUG_S(*ActorSystem, NKikimrServices::BS_PDISK, SelfInfo()
                        << " NextSector on new chunk, currentChunkIdx# " << ChunkIdx
                        << " SectorIdx# " << SectorIdx << " Nonce# " << Nonce);
            }
        }
        ui32 reserve = 1;
        if (IsSysLog) {
            const ui64 sectorOffset = Format.Offset(ChunkIdx, SectorIdx);
            for (ui32 replica = 1; replica < ReplicationFactor; ++replica) {
                ui8 *sectorData = BufferedWriter->Get() + Format.SectorSize * replica;
                memcpy(sectorData, BufferedWriter->Get(), Format.SectorSize);
                // Check sector CRC
                const ui64 sectorHash = *(ui64*)(void*)(sectorData + Format.SectorSize - sizeof(ui64));
                Y_ABORT_UNLESS(Hash.CheckSectorHash(sectorOffset, dataMagic, sectorData, Format.SectorSize, sectorHash),
                        "Sector hash corruption detected!");
            }
            BufferedWriter->MarkDirty();
            reserve = ReplicationFactor;
            *Mon.BandwidthPSysLogErasure += Format.SectorSize * (ReplicationFactor - 1);
        }
        SectorIdx += (IsSysLog ? ReplicationFactor : 1);

        if (SectorIdx < EndSectorIdx) {
            ui64 sectorOffset = Format.Offset(ChunkIdx, SectorIdx);
            BufferedWriter->Seek(sectorOffset, IsSysLog ? ReplicationFactor : 1, reserve, reqId, traceId,
                    ChunkIdx);
            return;
        }

        if (IsSysLog) {
            SectorIdx = FirstSectorIdx;
            ui64 sectorOffset = Format.Offset(ChunkIdx, SectorIdx);
            BufferedWriter->Seek(sectorOffset, ReplicationFactor, reserve, reqId, traceId,
                    ChunkIdx);
            return;
        }
        BufferedWriter->Flush(reqId, traceId, nullptr, ChunkIdx);
        if (IsLog) {
            SwitchToNewChunk(reqId, traceId);
        }
    }

    bool OnFirstSectorInChunk() const {
        return SectorIdx == 0;
    }

    bool OnLastSectorInChunk() const {
        return SectorIdx + 1 == EndSectorIdx;
    }

    void PrepareDataSectorFooter(ui8 *sector, ui64 magic, ui64 sectorOffset) {
        if (IsLog && LogChunkInfo) {
            // Nonce is incremented in next lines, so save it now
            LogChunkInfo->LastNonce = Nonce;
        }

        TDataSectorFooter &sectorFooter = *(TDataSectorFooter*)(sector + Format.SectorSize - sizeof(TDataSectorFooter));
        sectorFooter.Version = PDISK_DATA_VERSION;
        sectorFooter.Nonce = Nonce;
        sectorFooter.Hash = Hash.HashSector(sectorOffset, magic, sector, Format.SectorSize);

        BufferedWriter->MarkDirty();
        ++Nonce;
        Cypher.StartMessage(Nonce);
    }

    void PrepareParitySectorFooter(ui8 *sector, ui64 magic, ui64 sectorOffset) {
        TParitySectorFooter &sectorFooter = *(TParitySectorFooter*)
            (sector + Format.SectorSize - sizeof(TParitySectorFooter));
        sectorFooter.Nonce = Nonce;
        sectorFooter.Hash = Hash.HashSector(sectorOffset, magic, sector, Format.SectorSize);
        if (!IsLog && ActorSystem) {
            LOG_TRACE_S(*ActorSystem, NKikimrServices::BS_PDISK, SelfInfo()
                    << " PrepareParitySectorFooter, sectorOffset#" << sectorOffset
                    << " Nonce# " << Nonce << " hash# " << sectorFooter.Hash);
        }
        BufferedWriter->MarkDirty();
        ++Nonce;
        Cypher.StartMessage(Nonce);
    }

    void WriteData(const void* data, ui64 size, TReqId reqId, NWilson::TTraceId *traceId) {
        ui8* source = (ui8*)data;
        while (size > 0) {
            ui64 partSize = Min(size, (ui64)SectorBytesFree);
            Write(source, partSize, reqId, traceId);
            source += partSize;
            size -= partSize;
        }
    }

    void WritePadding(ui64 size, TReqId reqId, NWilson::TTraceId *traceId) {
        while (size > 0) {
            ui64 partSize = Min(size, (ui64)SectorBytesFree);
            WriteZeroes(partSize, reqId, traceId);
            size -= partSize;
        }
    }

    void Write(const void* data, ui64 size, TReqId reqId, NWilson::TTraceId *traceId) {
        Y_ABORT_UNLESS(data != nullptr);
        Cypher.Encrypt(BufferedWriter->Get() + CurrentPosition, data, (ui32)size);
        FinalizeWrite(size, reqId, traceId);
    }

    void WriteZeroes(ui64 size, TReqId reqId, NWilson::TTraceId *traceId) {
        Cypher.EncryptZeroes(BufferedWriter->Get() + CurrentPosition, (ui32)size);
        FinalizeWrite(size, reqId, traceId);
    }

    void Flush(TReqId reqId, NWilson::TTraceId *traceId, TCompletionAction *flushAction) {
        if (!IsEmptySector()) {
            WriteZeroes(SectorBytesFree, reqId, traceId);
        }
        BufferedWriter->Flush(reqId, traceId, flushAction, ChunkIdx);
    }

    void TerminateLog(TReqId reqId, NWilson::TTraceId *traceId) {
        Y_ABORT_UNLESS(IsLog);
        if (SectorBytesFree == 0 || SectorBytesFree == Format.SectorPayloadSize()) {
            if (ActorSystem) {
                LOG_DEBUG_S(*ActorSystem, NKikimrServices::BS_PDISK, SelfInfo()
                        << " TerminateLog Sector is full or free "
                        << " SectorBytesFree# " << SectorBytesFree
                        << " chunkIdx# " << ChunkIdx << " sectorIdx# " << SectorIdx
                        << " sectorOffset# " << Format.Offset(ChunkIdx, SectorIdx)
                        << " Marker# BPD63");
            }
        } else if (SectorBytesFree <= sizeof(TFirstLogPageHeader)) {
            if (ActorSystem) {
                LOG_DEBUG_S(*ActorSystem, NKikimrServices::BS_PDISK, SelfInfo()
                        << " TerminateLog small SectorBytesFree# " << SectorBytesFree
                        << " chunkIdx# " << ChunkIdx << " sectorIdx# " << SectorIdx
                        << " sectorOffset# " << Format.Offset(ChunkIdx, SectorIdx)
                        << " Marker# BPD65");
            }
            TFirstLogPageHeader terminator(LogPageTerminator, 0, 0, 0, 0, 0);
            if (IsSysLog) {
                *Mon.BandwidthPSysLogPadding += SectorBytesFree;
            } else {
                *Mon.BandwidthPLogPadding += SectorBytesFree;
            }
            RecordBytesLeft += SectorBytesFree;
            Write(&terminator, SectorBytesFree, reqId, traceId);
        } else {
            if (ActorSystem) {
                LOG_DEBUG_S(*ActorSystem, NKikimrServices::BS_PDISK, SelfInfo()
                        << " TerminateLog large SectorBytesFree# " << SectorBytesFree
                        << " chunkIdx# " << ChunkIdx << " sectorIdx# " << SectorIdx
                        << " sectorOffset# " << Format.Offset(ChunkIdx, SectorIdx)
                        << " Marker# BPD66");
            }
            ui32 availableSize = SectorBytesFree - sizeof(TFirstLogPageHeader);
            TFirstLogPageHeader header(LogPageTerminator, availableSize, availableSize, 0, 0, 0);
            if (IsSysLog) {
                *Mon.BandwidthPSysLogPadding += SectorBytesFree;
            } else {
                *Mon.BandwidthPLogPadding += SectorBytesFree;
            }
            RecordBytesLeft += sizeof(TFirstLogPageHeader) + availableSize;
            Write(&header, sizeof(TFirstLogPageHeader), reqId, traceId);
            WriteZeroes(availableSize, reqId, traceId);
        }
    }

    void LogHeader(TOwner owner, TLogSignature signature, ui64 ownerLsn, ui64 dataSize, TReqId reqId,
            NWilson::TTraceId *traceId) {
        Y_ABORT_UNLESS(IsLog);
        Y_ABORT_UNLESS(SectorBytesFree >= sizeof(TFirstLogPageHeader));
        ui64 availableSize = SectorBytesFree - sizeof(TFirstLogPageHeader);
        bool isWhole = availableSize >= dataSize;
        bool isTornOffHeader = false;
        {
            ui64 sizeNeeded = sizeof(TFirstLogPageHeader) + dataSize;
            ui8 flags = LogPageFirst | (isWhole ? LogPageLast : 0);
            ui32 payloadSize = isWhole ? dataSize : availableSize;
            TFirstLogPageHeader header(flags, payloadSize, dataSize, owner, signature, ownerLsn);
            RecordBytesLeft = sizeNeeded;
            isTornOffHeader = (SectorBytesFree == sizeof(TFirstLogPageHeader) && !isWhole);
            if (ActorSystem) {
                LOG_DEBUG_S(*ActorSystem, NKikimrServices::BS_PDISK, SelfInfo()
                        << " LogPageHeader, chunkIdx# " << ChunkIdx << " sectorIdx# " << SectorIdx
                        << " nonce# " << Nonce << " Marker# BPD60");
            }
            Write(&header, sizeof(TFirstLogPageHeader), reqId, traceId);
        }
        if (isTornOffHeader) {
            bool isLast = RecordBytesLeft <= SectorBytesFree - sizeof(TLogPageHeader);
            ui8 flags = isLast ? LogPageLast : 0;
            ui32 payloadSize = isLast ? RecordBytesLeft : SectorBytesFree - sizeof(TLogPageHeader);
            TLogPageHeader header(flags, payloadSize);
            RecordBytesLeft += sizeof(TLogPageHeader);
            if (IsSysLog) {
                *Mon.BandwidthPSysLogRecordHeader += sizeof(TLogPageHeader);
            } else {
                *Mon.BandwidthPLogRecordHeader += sizeof(TLogPageHeader);
            }
            if (ActorSystem) {
                LOG_DEBUG_S(*ActorSystem, NKikimrServices::BS_PDISK, SelfInfo()
                        << " LogPageHeader, chunkIdx# " << ChunkIdx << " sectorIdx# " << SectorIdx
                        << " nonce# " << Nonce << " Marker# BPD61");
            }
            Write(&header, sizeof(TLogPageHeader), reqId, traceId);
        }
    }

    void LogDataPart(const void* data, ui64 size, TReqId reqId, NWilson::TTraceId *traceId) {
        Y_ABORT_UNLESS(IsLog);
        REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(data, size);
        Y_ABORT_UNLESS(data);
        Y_ABORT_UNLESS(size > 0);
        while (RecordBytesLeft > SectorBytesFree && size >= SectorBytesFree) {
            const ui64 bytesToWrite = SectorBytesFree;
            Write(data, bytesToWrite, reqId, traceId);
            size -= bytesToWrite;
            data = (ui8*)data + bytesToWrite;

            bool isLast = (RecordBytesLeft <= SectorBytesFree - sizeof(TLogPageHeader));
            ui8 flags = isLast ? LogPageLast : 0;
            ui32 payloadSize = isLast ? RecordBytesLeft : SectorBytesFree - sizeof(TLogPageHeader);
            TLogPageHeader header(flags, payloadSize);
            RecordBytesLeft += sizeof(TLogPageHeader);
            if (IsSysLog) {
                *Mon.BandwidthPSysLogRecordHeader += sizeof(TLogPageHeader);
            } else {
                *Mon.BandwidthPLogRecordHeader += sizeof(TLogPageHeader);
            }
            if (ActorSystem) {
                LOG_DEBUG_S(*ActorSystem, NKikimrServices::BS_PDISK, "PDiskId# " << PDiskId
                        << " Line# " << __LINE__ << " LogPageHeader writing"
                        << " chunkIdx# " << ChunkIdx << " sectorIdx# " << SectorIdx
                        << " Marker# BPD62");
            }
            Write(&header, sizeof(TLogPageHeader), reqId, traceId);
        }
        Write(data, size, reqId, traceId);
        if (RecordBytesLeft == 0 && SectorBytesFree > 0) {
            if (SectorBytesFree < sizeof(TFirstLogPageHeader)) {
                TerminateLog(reqId, traceId);
            }
        }
    }

protected:
    void FinalizeWrite(ui64 size, TReqId reqId, NWilson::TTraceId *traceId) {
        CurrentPosition += size;
        Y_ABORT_UNLESS(SectorBytesFree >= size);
        SectorBytesFree -= size;
        RecordBytesLeft -= size;
        if (size) {
            BufferedWriter->MarkDirty();
        }
        if (SectorBytesFree == 0) {
            Cypher.Encrypt(BufferedWriter->Get() + CurrentPosition, &Canary, CanarySize);
            ui64 sectorOffset = Format.Offset(ChunkIdx, SectorIdx);
            PrepareDataSectorFooter(BufferedWriter->Get(), DataMagic, sectorOffset);
            if (IsLog) {
                if (IsSysLog) {
                    *Mon.BandwidthPSysLogSectorFooter += sizeof(TDataSectorFooter);
                } else {
                    *Mon.BandwidthPLogSectorFooter += sizeof(TDataSectorFooter);
                }
            } else {
                *Mon.BandwidthPChunkSectorFooter += sizeof(TDataSectorFooter);
            }
            CurrentPosition = 0;
            NextSector(DataMagic, reqId, traceId);

            SectorBytesFree = Format.SectorPayloadSize();
            CurrentPosition = 0;
        }
    }
};

using TChunkWriter = TSectorWriter<false, false>;
using TLogWriter = TSectorWriter<true, false>;
using TSysLogWriter = TSectorWriter<true, true>;

} // NPDisk
} // NKikimr
