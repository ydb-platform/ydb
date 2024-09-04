#include "blobstorage_pdisk_blockdevice.h"
#include "blobstorage_pdisk_completion_impl.h"
#include "blobstorage_pdisk_data.h"
#include "blobstorage_pdisk_drivemodel.h"
#include "blobstorage_pdisk_impl.h"
#include "blobstorage_pdisk_sectorrestorator.h"
#include "blobstorage_pdisk_syslogreader.h"

namespace NKikimr {
namespace NPDisk {

class TCompletionSysLogRead: public TCompletionAction {
protected:
    TIntrusivePtr<TSysLogReader> Reader;
public:
    TCompletionSysLogRead(const TIntrusivePtr<TSysLogReader> &reader)
        : Reader(reader)
    {}

    void Exec(TActorSystem *actorSystem) override {
        Y_UNUSED(actorSystem);
        Reader->Exec();
        delete this;
    }

    void Release(TActorSystem *actorSystem) override {
        Y_UNUSED(actorSystem);
        delete this;
    }
};



////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Public interface
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

class TSysLogReadCompletionPart : public TCompletionPart {
    TAlignedData *CommonData;
    ui32 SizeToReadPart;
    ui32 Offset;
    TBuffer::TPtr DataPart;

public:
    TSysLogReadCompletionPart(TCumulativeCompletionHolder *cumulativeCompletionHolder, TAlignedData *data,
            TBuffer *buffer, ui32 sizeToReadPart, ui32 offset)
        : TCompletionPart(cumulativeCompletionHolder)
        , CommonData(data)
        , SizeToReadPart(sizeToReadPart)
        , Offset(offset)
        , DataPart(buffer)
    {}

    void Exec(TActorSystem *actorSystem) override {
        memcpy(CommonData->Get() + Offset, DataPart->Data(), SizeToReadPart);
        DataPart.Reset();
        TCompletionPart::Exec(actorSystem);
    }

    virtual ~TSysLogReadCompletionPart() {
    }
};

TSysLogReader::TSysLogReader(TPDisk *pDisk, TActorSystem *const actorSystem, const TActorId &replyTo, TReqId reqId)
    : PDisk(pDisk)
    , ActorSystem(actorSystem)
    , ReplyTo(replyTo)
    , ReqId(reqId)
    , Result(new TEvReadLogResult(NKikimrProto::ERROR, TLogPosition{0, 0}, TLogPosition::Invalid(),
                true, 0, nullptr, 0))
    , Cypher(pDisk->Cfg->EnableSectorEncryption)
    , SizeToRead(PDisk->Format.SysLogSectorCount * ReplicationFactor * PDisk->Format.SectorSize)
    , Data(SizeToRead)
{
    Cypher.SetKey(PDisk->Format.SysLogKey);
    AtomicIncrement(PDisk->InFlightLogRead);

    TDiskFormat &format = PDisk->Format;
    ui32 sectorSetCount = format.SysLogSectorCount;
    ui32 sectorCount = sectorSetCount * ReplicationFactor;
    BeginSectorIdx = format.FirstSysLogSectorIdx();
    EndSectorIdx = BeginSectorIdx + sectorCount;
    SectorSetInfo.resize(sectorSetCount);
    CriticalJunkCount = format.SysLogSectorsPerRecord() * 2;
}

TSysLogReader::~TSysLogReader() {
    AtomicDecrement(PDisk->InFlightLogRead);
}

void TSysLogReader::Start() {
    TDiskFormat &format = PDisk->Format;
    auto *finalCompletion = new TCompletionSysLogRead(this);
    finalCompletion->CostNs = PDisk->DriveModel.TimeForSizeNs(SizeToRead, 0, TDriveModel::EOperationType::OP_TYPE_READ);
    const ui32 bufferSize = PDisk->BufferPool->GetBufferSize();
    const ui32 partsToRead = (SizeToRead + bufferSize - 1) / bufferSize;
    Y_ABORT_UNLESS(partsToRead > 0);
    TVector<TCompletionAction *> completionParts;
    TVector<TBuffer *> bufferParts;
    completionParts.reserve(partsToRead);
    bufferParts.reserve(partsToRead);
    auto *cumulativeCompletion = new TCumulativeCompletionHolder();
    for (ui32 idx = 0; idx < partsToRead; ++idx) {
        const ui32 offset = idx * bufferSize;
        const ui32 sizeToReadPart = Min(bufferSize, SizeToRead - offset);
        bufferParts.push_back(PDisk->BufferPool->Pop());
        completionParts.push_back(new TSysLogReadCompletionPart(cumulativeCompletion, &Data,
                bufferParts[idx], sizeToReadPart, offset));
    }
    cumulativeCompletion->SetCompletionAction(finalCompletion);
    for (ui32 idx = 0; idx < partsToRead; ++idx) {
        const ui32 offset = idx * bufferSize;
        const ui32 sizeToReadPart = Min(bufferSize, SizeToRead - offset);
        PDisk->BlockDevice->PreadAsync(bufferParts[idx]->Data(), sizeToReadPart, BeginSectorIdx * format.SectorSize + offset,
                completionParts[idx], ReqId, {});
    }
}

void TSysLogReader::Exec() {
    if (IsReplied) {
        return;
    }
    RestoreSectorSets();
    FindLoopOffset();
    if (IsReplied) {
        return;
    }
    MarkConsistentRecords();
    FindTheBestRecord();
    if (IsReplied) {
        return;
    }
    MeasureJunkRegion();
    FindMaxNonce();

    VerboseCheck(JunkSetCount < CriticalJunkCount, "JunkSetCount exceeds CriticalJunkCount. Marker# BPS03");
    if (IsReplied) {
        return;
    }

    PrepareResult();
    Reply(); // Does nothing if IsReplied is set
    return;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Protected
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

void TSysLogReader::RestoreSectorSets() {
    for (ui32 sectorIdx = BeginSectorIdx; sectorIdx < EndSectorIdx; sectorIdx += ReplicationFactor) {
        TDiskFormat &format = PDisk->Format;
        ui8 *sectorSetData = Data.Get() + (sectorIdx - BeginSectorIdx) * format.SectorSize;
        ui32 setIdx = (sectorIdx - BeginSectorIdx) / ReplicationFactor;
        TSectorSetInfo &sectorSetInfo = SectorSetInfo[setIdx];
        sectorSetInfo.FirstSectorIdx = sectorIdx;

        const ui64 magic = format.MagicSysLogChunk;
        const bool isErasureEncode = format.IsErasureEncodeSysLog();
        TSectorRestorator restorator(true, LogErasureDataParts, isErasureEncode, format,
            PDisk->ActorSystem, PDisk->PDiskActor, PDisk->PDiskId, &PDisk->Mon, PDisk->BufferPool.Get());
        restorator.Restore(sectorSetData, sectorIdx * format.SectorSize, magic, 0, 0);

        if (!restorator.GoodSectorFlags) {
            continue;
        }

        // We want to read only 1 good sector (the last one)
        ui32 sectorsToSkip = restorator.LastGoodIdx;
        ui8* rawSector = sectorSetData + sectorsToSkip * format.SectorSize;
        TDataSectorFooter *sectorFooter = (TDataSectorFooter*)
            (rawSector + format.SectorSize - sizeof(TDataSectorFooter));

        sectorSetInfo.Nonce = sectorFooter->Nonce;
        sectorSetInfo.LastGoodSectorData = rawSector;
        sectorSetInfo.GoodSectorFlags = restorator.GoodSectorFlags;
        sectorSetInfo.IsIdeal = (restorator.LastBadIdx == (ui32)-1);

        // Decrypt data
        Cypher.StartMessage(sectorFooter->Nonce);
        Cypher.InplaceEncrypt(rawSector, format.SectorSize - ui32(sizeof(TDataSectorFooter)));
        PDisk->CheckLogCanary(rawSector, 0, sectorIdx + sectorsToSkip);

        TLogPageHeader *pageHeader = (TLogPageHeader*)rawSector;
        if (pageHeader->Flags & LogPageFirst) {
            sectorSetInfo.HasStart = true;
            TFirstLogPageHeader *firstPageHeader = (TFirstLogPageHeader*)rawSector;
            sectorSetInfo.FullPayloadSize = firstPageHeader->DataSize;
            sectorSetInfo.PayloadPartSize = firstPageHeader->Size;
            sectorSetInfo.PayloadSignature = firstPageHeader->LogRecordHeader.Signature;
            sectorSetInfo.PayloadLsn = firstPageHeader->LogRecordHeader.OwnerLsn;
            sectorSetInfo.Payload = rawSector + sizeof(TFirstLogPageHeader);
        } else {
            sectorSetInfo.Payload = rawSector + sizeof(TLogPageHeader);
        }
        if (pageHeader->Flags & LogPageLast) {
            sectorSetInfo.HasEnd = true;
            sectorSetInfo.PayloadPartSize = pageHeader->Size;
        }
        if (!sectorSetInfo.HasStart && !sectorSetInfo.HasEnd) {
            sectorSetInfo.HasMiddle = true;
            sectorSetInfo.PayloadPartSize = pageHeader->Size;
        }
    }
}

void TSysLogReader::FindLoopOffset() {
    ui32 loopOffset = 0;
    for (; loopOffset < SectorSetInfo.size(); ++loopOffset) {
        if (SectorSetInfo[loopOffset].HasStart) {
            break;
        }
    }
    VerboseCheck(loopOffset < SectorSetInfo.size(), "No SysLog sector has first log page. Marker# BPS07");
    if (IsReplied) {
        return;
    }
    VerboseCheck(SectorSetInfo[loopOffset].HasStart, "No SysLog sector has first log page. Marker# BPS01");
    if (IsReplied) {
        return;
    }
    bool hasAnotherStart = false;
    for (ui32 idx = loopOffset + 1; idx < SectorSetInfo.size(); ++idx) {
        if (SectorSetInfo[idx].HasStart) {
            hasAnotherStart = true;
        }
    }
    VerboseCheck(hasAnotherStart, "Only one SysLog sector has first log page. Marker# BPS02");
    if (IsReplied) {
        return;
    }
    LoopOffset = loopOffset;
}

// Loop around marking inconsistent records and nonce reversals
void TSysLogReader::MarkConsistentRecords() {
    bool hasFirst = false;
    ui64 fullSize = 0;
    ui64 actualSize = 0;
    ui64 firstIdx = 0;
    ui64 prevNonce = 0;
    // Loop around to step on the VeryFirst sector twice so that all inconsistencies are marked properly
    for (ui32 idx = 0; idx < SectorSetInfo.size() + 1; ++idx) {
        ui32 setIdx = (idx + LoopOffset) % SectorSetInfo.size();
        TSectorSetInfo &sectorSetInfo = SectorSetInfo[setIdx];
        if (!sectorSetInfo.LastGoodSectorData) {
            MarkInconsistentIncluding(firstIdx, setIdx);
            hasFirst = false;
            continue;
        }

        if (sectorSetInfo.Nonce <= prevNonce) {
            sectorSetInfo.IsNonceReversal = true;
        }

        if (sectorSetInfo.HasStart) {
            if (hasFirst) {
                MarkInconsistent(firstIdx, setIdx);
            }
            hasFirst = true;
            fullSize = sectorSetInfo.FullPayloadSize;
            actualSize = 0;
            firstIdx = setIdx;
            prevNonce = sectorSetInfo.Nonce;
        } else {
            if (sectorSetInfo.Nonce != prevNonce + 1) {
                if (hasFirst) {
                    MarkInconsistent(firstIdx, setIdx);
                }
                hasFirst = false;
                sectorSetInfo.IsConsistent = false;
                prevNonce = sectorSetInfo.Nonce;
                continue;
            }
            prevNonce = sectorSetInfo.Nonce;
        }
        if (!hasFirst) {
            sectorSetInfo.IsConsistent = false;
            continue;
        }
        actualSize += sectorSetInfo.PayloadPartSize;
        if (actualSize > fullSize) {
            MarkInconsistentIncluding(firstIdx, setIdx);
            hasFirst = false;
            continue;
        }
        if (sectorSetInfo.HasEnd) {
            if (actualSize != fullSize) {
                MarkInconsistentIncluding(firstIdx, setIdx);
                hasFirst = false;
                continue;
            }
            hasFirst = false;
        }
    }
}

void TSysLogReader::MarkInconsistent(ui32 beginSetIdx, ui32 endSetIdx) {
    for (ui32 idx = beginSetIdx; idx != endSetIdx; idx = (idx + 1) % SectorSetInfo.size()) {
        SectorSetInfo[idx].IsConsistent = false;
    }
}

void TSysLogReader::MarkInconsistentIncluding(ui32 beginSetIdx, ui32 endSetIdx) {
    for (ui32 idx = beginSetIdx; idx != endSetIdx; idx = (idx + 1) % SectorSetInfo.size()) {
        SectorSetInfo[idx].IsConsistent = false;
    }
    SectorSetInfo[endSetIdx].IsConsistent = false;
}

void TSysLogReader::FindTheBestRecord() {
    BestRecordFirstOffset = 0;
    BestRecordLastOffset = 0;
    BestNonce = 0;
    for (ui32 idx = 0; idx < SectorSetInfo.size(); ++idx) {
        ui32 setIdx = (idx + LoopOffset) % SectorSetInfo.size();
        TSectorSetInfo &info = SectorSetInfo[setIdx];
        if (info.IsConsistent && !info.IsNonceReversal && info.HasStart && info.Nonce > BestNonce) {
            BestNonce = info.Nonce;
            BestRecordFirstOffset = idx + LoopOffset;
        }
        if (info.IsConsistent && !info.IsNonceReversal && info.HasEnd && info.Nonce >= BestNonce) {
            BestRecordLastOffset = idx + LoopOffset;
        }
    }
    LOG_INFO_S(*PDisk->ActorSystem, NKikimrServices::BS_PDISK,
            "PDiskId# " << (ui32)PDisk->PDiskId << " SysLogReader BestRecordFirstOffset# " << BestRecordFirstOffset
            << " BestRecordLastOffset# " << BestRecordLastOffset << " BestNonce# " << BestNonce);
    VerboseCheck(BestNonce > 0, "No best record found! Marker# BPS06");
    // Can become replied at this point
}

void TSysLogReader::MeasureJunkRegion() {
    JunkBeginOffset = (BestRecordLastOffset + 1) % SectorSetInfo.size();
    JunkSetCount = 0;
    for (ui32 idx = 0; idx < SectorSetInfo.size(); ++idx) {
        ui32 setIdx = (idx + JunkBeginOffset) % SectorSetInfo.size();
        TSectorSetInfo &info = SectorSetInfo[setIdx];
        if (info.IsNonceReversal) {
            JunkSetCount = idx;
        }
        if (!info.IsConsistent) {
            JunkSetCount = idx + 1;
        }
    }
}

void TSysLogReader::FindMaxNonce() {
    MaxNonce = 0;
    for (ui32 setIdx = 0; setIdx < SectorSetInfo.size(); ++setIdx) {
        TSectorSetInfo &info = SectorSetInfo[setIdx];
        if (info.Nonce > MaxNonce) {
            MaxNonce = info.Nonce;
        }
    }
}

void TSysLogReader::PrepareResult() {
    TSectorSetInfo &info = SectorSetInfo[BestRecordFirstOffset % SectorSetInfo.size()];
    TRcBuf payload(TRcBuf::Uninitialized(info.FullPayloadSize));
    VerboseCheck(info.FullPayloadSize >= info.PayloadPartSize, "First payload part too large. Marker# BPS04");
    if (IsReplied) {
        return;
    }
    memcpy(payload.UnsafeGetDataMut(), info.Payload, info.PayloadPartSize);
    ui32 writePosition = info.PayloadPartSize;
    for (ui32 idx = 1; idx <= BestRecordLastOffset - BestRecordFirstOffset; ++idx) {
        ui32 setIdx = (idx + BestRecordFirstOffset) % SectorSetInfo.size();
        TSectorSetInfo &partInfo = SectorSetInfo[setIdx];
        void* destination = (ui8*)payload.data() + writePosition;
        if (info.FullPayloadSize < writePosition + partInfo.PayloadPartSize) {
            TStringStream str;
            str << "Payload part too large. info.FullPayloadSize# " << info.FullPayloadSize
                << " writePosition# " << writePosition
                << " partInfo.PayloadPartSize# " << partInfo.PayloadPartSize
                << " Marker# BPS05";
            VerboseCheck(false, str.Str().c_str());
        }
        if (IsReplied) {
            return;
        }
        memcpy(destination, partInfo.Payload, partInfo.PayloadPartSize);
        writePosition += partInfo.PayloadPartSize;
    }
    ui32 sectorIdx = JunkBeginOffset * ReplicationFactor + BeginSectorIdx;
    if (!IsReplied) {
        Result->Results.push_back(TLogRecord(info.PayloadSignature, payload, info.PayloadLsn));
        Result->NextPosition = PDisk->LogPosition(0, sectorIdx, 0);
        Result->Status = NKikimrProto::OK;
    }
}

void TSysLogReader::Reply() {
    if (!IsReplied) {
        LOG_DEBUG(*PDisk->ActorSystem, NKikimrServices::BS_PDISK, "PDiskId# %" PRIu32 " %s",
                (ui32)PDisk->PDiskId, Result->ToString().c_str());
        ActorSystem->Send(ReplyTo, Result.Release());
        IsReplied = true;
    }
}



bool TSysLogReader::VerboseCheck(bool condition, const char *desctiption) {
    if (!condition) {
        if (!IsReplied) {
            TStringStream str;
            str << desctiption << " ";
            DumpDebugInfo(str, true);
            Result->ErrorReason = str.Str();
            LOG_ERROR(*PDisk->ActorSystem, NKikimrServices::BS_PDISK, "PDiskId# %" PRIu32 " %s",
                (ui32)PDisk->PDiskId, Result->ToString().c_str());
            Reply();
        }
    }
    return condition;
}

void TSysLogReader::DumpDebugInfo(TStringStream &str, bool isSingleLine) {
    const char *nl = (isSingleLine ? "; " : "\n(B) ");
    str << "PDiskId# " << (ui32)PDisk->PDiskId;
    str << " SysLog";
    str << " BeginSectorIdx# " << BeginSectorIdx;
    str << " EndSectorIdx# " << EndSectorIdx;
    str << " LoopOffset# " << LoopOffset;
    str << " BestRecordFirstOffset# " << BestRecordFirstOffset;
    str << " BestRecordLastOffset# " << BestRecordLastOffset;
    str << " BestNonce# " << BestNonce;
    str << " MaxNonce# " << MaxNonce;
    str << " JunkBeginOffset# " << JunkBeginOffset;
    str << " JunkSetCount# " << JunkSetCount;
    str << " CriticalJunkCount# " << CriticalJunkCount;
    str << " SectorMap# " << nl;
    for (ui32 idx = 0; idx < SectorSetInfo.size(); ++idx) {
        TSectorSetInfo &sectorSetInfo = SectorSetInfo[idx];
        for (ui32 i = 0; i < ReplicationFactor; ++i) {
            str << ((sectorSetInfo.GoodSectorFlags & (1 << i)) ? "." : "X");
        }
        str << " ";
    }
    str << nl;
    str << " [SetIdx:X/Reversal/Inconsistent/.,Nonce,First/./Last,PayloadPartSize, FullPayloadSize,Lsn]" << nl;
    for (ui32 idx = 0; idx < SectorSetInfo.size(); ++idx) {
        TSectorSetInfo &sectorSetInfo = SectorSetInfo[idx];
        str << "[";
        str << idx << ":";
        if (!sectorSetInfo.IsConsistent) {
            str << "I";
        }
        if (sectorSetInfo.IsNonceReversal) {
            str << "R";
        }
        if (sectorSetInfo.IsConsistent && !sectorSetInfo.IsNonceReversal) {
            str << ".";
        }
        if (sectorSetInfo.LastGoodSectorData) {
            str << "," << sectorSetInfo.Nonce << ",";
            if (sectorSetInfo.HasStart) {
                str << "F";
            }
            if (sectorSetInfo.HasMiddle) {
                str << ".";
            }
            if (sectorSetInfo.HasEnd) {
                str << "L";
            }
            str << "," << sectorSetInfo.PayloadPartSize;
            if (sectorSetInfo.HasStart) {
                str << ", " << sectorSetInfo.FullPayloadSize << "," << sectorSetInfo.PayloadLsn;
            }
        } else {
            str << "X";
        }
        str << "]" << nl;
    }
}


} // NPDisk
} // NKikimr

