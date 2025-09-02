#include "blobstorage_pdisk_impl.h"

#include "blobstorage_pdisk_completion_impl.h"
#include "blobstorage_pdisk_logreader.h"
#include "blobstorage_pdisk_syslogreader.h"

#include <util/random/entropy.h>
#include <util/random/mersenne64.h>

namespace NKikimr::NPDisk {

class TLogFlushCompletionAction : public TCompletionAction {
    const ui32 EndChunkIdx;
    const ui32 EndSectorIdx;
    THolder<TLogWriter> &CommonLogger;
    TCompletionAction* CompletionLogWrite;
public:
    TLogFlushCompletionAction(ui32 endChunkIdx, ui32 endSectorIdx, THolder<TLogWriter> &commonLogger, TCompletionAction* completionLogWrite)
        : EndChunkIdx(endChunkIdx)
        , EndSectorIdx(endSectorIdx)
        , CommonLogger(commonLogger)
        , CompletionLogWrite(completionLogWrite)
    {
        TCompletionAction::ShouldBeExecutedInCompletionThread = false;
        Orbit = std::move(completionLogWrite->Orbit);
    }

    void SetUpCompletionLogWrite() {
        CompletionLogWrite->SubmitTime = SubmitTime;
        CompletionLogWrite->GetTime = GetTime;
        CompletionLogWrite->SetResult(Result);
        CompletionLogWrite->SetErrorReason(ErrorReason);
        CompletionLogWrite->Orbit = std::move(Orbit);
    }

    void Exec(TActorSystem *actorSystem) override {
        CommonLogger->FirstUncommitted = TFirstUncommitted(EndChunkIdx, EndSectorIdx);

        SetUpCompletionLogWrite();
        CompletionLogWrite->Exec(actorSystem);

        delete this;
    }

    void Release(TActorSystem *actorSystem) override {
        SetUpCompletionLogWrite();
        CompletionLogWrite->Release(actorSystem);
        delete this;
    }
};

void TPDisk::InitSysLogger() {
    ui64 writeSectorIdx = (ui64) ((InitialSysLogWritePosition + Format.SectorSize - 1) / Format.SectorSize);
    ui64 beginSectorIdx = (ui64)((FormatSectorSize * ReplicationFactor + Format.SectorSize - 1) /
        Format.SectorSize);
    ui64 endSectorIdx = beginSectorIdx + Format.SysLogSectorCount * ReplicationFactor;
    SysLogger.Reset(new TSysLogWriter(Mon, *BlockDevice.Get(), Format,
        SysLogRecord.Nonces.Value[NonceSysLog], Format.SysLogKey, BufferPool.Get(),
        beginSectorIdx, endSectorIdx, Format.MagicSysLogChunk, 0, nullptr, writeSectorIdx, nullptr, PCtx,
        &DriveModel, Cfg->EnableSectorEncryption));
}

bool TPDisk::InitCommonLogger() {
    TGuard<TMutex> guard(StateMutex);
    const ui32 chunkIdx = InitialLogPosition.ChunkIdx;
    ui64 sectorIdx = (InitialLogPosition.OffsetInChunk + Format.SectorSize - 1) / Format.SectorSize;

    TLogChunkInfo *info = &*std::find_if(LogChunks.begin(), LogChunks.end(), [=](const TLogChunkInfo& i) {
        return i.ChunkIdx == chunkIdx;
    });

    if (sectorIdx >= UsableSectorsPerLogChunk() && InitialTailBuffer) {
        InitialTailBuffer->Release(PCtx->ActorSystem);
        InitialTailBuffer = nullptr;
    }
    CommonLogger.Reset(new TLogWriter(Mon, *BlockDevice.Get(), Format,
            SysLogRecord.Nonces.Value[NonceLog], Format.LogKey, BufferPool.Get(), 0, UsableSectorsPerLogChunk(),
            Format.MagicLogChunk, chunkIdx, info, std::min(sectorIdx, UsableSectorsPerLogChunk()),
            InitialTailBuffer, PCtx, &DriveModel, Cfg->EnableSectorEncryption));
    InitialTailBuffer = nullptr;
    if (sectorIdx >= UsableSectorsPerLogChunk()) {
        if (!AllocateLogChunks(1, 0, OwnerSystem, 0, EOwnerGroupType::Static, true)) {
            return false;
        }
        CommonLogger->SwitchToNewChunk(TReqId(TReqId::InitCommonLoggerSwitchToNewChunk, 0), nullptr);

        // Log chunk can be collected as soon as no one needs it
        ChunkState[chunkIdx].CommitState = TChunkState::DATA_COMMITTED;
    }
    bool isOk = LogNonceJump(InitialPreviousNonce);
    return isOk;
}

void TPDisk::InitLogChunksInfo() {
    TGuard<TMutex> guard(StateMutex);
    for (auto it = LogChunks.begin(); it != LogChunks.end(); ++it) {
        for (ui32 owner = 0; owner < it->OwnerLsnRange.size(); ++owner) {
            if (OwnerData[owner].VDiskId != TVDiskID::InvalidId) {
                bool keep = true;
                if (SysLogFirstNoncesToKeep.FirstNonceToKeep[owner] > it->LastNonce) {
                    keep = false;
                } else if (it->OwnerLsnRange.size() > owner) {
                    if (OwnerData[owner].CurrentFirstLsnToKeep > it->OwnerLsnRange[owner].LastLsn) {
                        keep = false;
                    }
                }
                if (!keep && it->OwnerLsnRange.size() > owner && it->OwnerLsnRange[owner].IsPresent) {
                    TLogChunkInfo::TLsnRange &range = it->OwnerLsnRange[owner];
                    range.IsPresent = false;
                    Y_VERIFY_S(it->CurrentUserCount > 0, PCtx->PDiskLogPrefix);
                    it->CurrentUserCount--;
                    P_LOG(PRI_INFO, BPD01, "InitLogChunksInfo, chunk is dereferenced by owner",
                        (ChunkIdx, it->ChunkIdx),
                        (LsnRange, TString(TStringBuilder() << "[" << range.FirstLsn << ", " << range.LastLsn << "]")),
                        (PresentNonces, TString(TStringBuilder() << "[" << it->FirstNonce << ", " << it->LastNonce << "]")),
                        (OwnerId, ui32(owner)),
                        (CurrentFirstLsnToKeep, OwnerData[owner].CurrentFirstLsnToKeep),
                        (CurrentUserCount, it->CurrentUserCount));
                }
            }
        }
    }

    for (auto info : LogChunks) {
        P_LOG(PRI_INFO, BPD01, "InitLogChunksInfo state", (LogChunk, info));
    }

    PrintLogChunksInfo("startup");
}

void TPDisk::PrintLogChunksInfo(const TString& msg) {
    auto debugPrint = [&] () {
        TStringStream str;
        str << " [";
        for (auto it = LogChunks.begin(); it != LogChunks.end(); ++it) {
            str << "{";
            str << "chunkIdx# " << it->ChunkIdx;
            str << " users# " << it->CurrentUserCount;
            str << " endOfSplice# " << it->IsEndOfSplice;
            str << " isCommited# " << (ChunkState[it->ChunkIdx].CommitState == TChunkState::LOG_COMMITTED);

            for (ui32 owner = 0; owner < it->OwnerLsnRange.size(); ++owner) {
                auto &range = it->OwnerLsnRange[owner];
                if (range.IsPresent) {
                    str << " {";
                    str << "owner# " << owner;
                    str << " lsn# " << range.FirstLsn << "-" << range.LastLsn;
                    str << " firstLsnToKeep# " << OwnerData[owner].CurrentFirstLsnToKeep;
                    str << "},";
                }
            }
            str << "},";
        }
        str << "]";
        return str.Str();
    };

    P_LOG(PRI_DEBUG, BPD01, "PrintLogChunksInfo " << msg, (LogChunks, debugPrint()));
}

bool TPDisk::LogNonceJump(ui64 previousNonce) {
    bool isWhole = CommonLogger->SectorBytesFree >= sizeof(TNonceJumpLogPageHeader2);
    Y_VERIFY_S(isWhole, PCtx->PDiskLogPrefix);

    Y_VERIFY_S(CommonLogger->NextChunks.size() == 0, PCtx->PDiskLogPrefix);
    if (!PreallocateLogChunks(CommonLogger->SectorBytesFree, OwnerSystem, 0, EOwnerGroupType::Static, true)) {
        return false;
    }
    TVector<ui32> logChunksToCommit;
    if (CommonLogger->NextChunks.size()) {
        logChunksToCommit.push_back(CommonLogger->ChunkIdx);
    }

    TNonceJumpLogPageHeader2 nonceJump(LogPageNonceJump2, previousNonce, LastNonceJumpLogPageHeader2, LogChunks);

    P_LOG(PRI_INFO, BPD01, "LogNonceJump",
        (ChunkIdx, CommonLogger->ChunkIdx),
        (previousNonce, previousNonce),
        (Nonce, CommonLogger->Nonce),
        (NonceJump, nonceJump.ToString(false)));

    CommonLogger->RecordBytesLeft = sizeof(TNonceJumpLogPageHeader2);
    CommonLogger->Write(&nonceJump, sizeof(TNonceJumpLogPageHeader2), TReqId(TReqId::LogNonceJumpWriteHeader2, 0), {});
    CommonLogger->TerminateLog(TReqId(TReqId::LogNonceJumpTerminateLog, 0), {});
    OnNonceChange(NonceLog, TReqId(TReqId::NonceChangeForNonceJump, 0), {});
    auto write = MakeHolder<TCompletionLogWrite>(this, TVector<TLogWrite*>(), TVector<TLogWrite*>(),
            std::move(logChunksToCommit));

    ui32 curChunkIdx = CommonLogger->ChunkIdx;
    ui64 curSectorIdx = CommonLogger->SectorIdx;

    TLogFlushCompletionAction* flushCompletion = new TLogFlushCompletionAction(curChunkIdx, curSectorIdx, CommonLogger, write.Release());

    CommonLogger->Flush(TReqId(TReqId::LogNonceJumpFlush, 0), {}, flushCompletion);

    return true;
}

void TPDisk::GetStartingPoints(NPDisk::TOwner owner, TMap<TLogSignature, NPDisk::TLogRecord> &outStartingPoints) {
    TGuard<TMutex> guard(StateMutex);
    if (OwnerData[owner].VDiskId != TVDiskID::InvalidId) {
        outStartingPoints = OwnerData[owner].StartingPoints;
        //OwnerData[owner].StartingPoints.clear();
    } else {
        outStartingPoints.clear();
    }
}

void TPDisk::ReadSysLog(const TActorId &pDiskActor) {
    TIntrusivePtr<TSysLogReader> sysLogReader(new TSysLogReader(this, PCtx->ActorSystem, pDiskActor,
                TReqId(TReqId::ReadSysLog, 0)));
    sysLogReader->Start();
    return;
}

bool TPDisk::ProcessChunk0(const NPDisk::TEvReadLogResult &readLogResult, TString& errorReason) {
    TGuard<TMutex> guard(StateMutex);
    ui64 writePosition = 0;
    ui64 lastLsn = 0;
    TRcBuf lastSysLogRecord = ProcessReadSysLogResult(writePosition, lastLsn, readLogResult);
    if (lastSysLogRecord.size() == 0) {
        errorReason = TStringBuilder() << "Error while parsing sys log at booting state: lastSysLogRecord is empty,"
            << " lastSysLogRecord.Size()# 0 writePosition# " << writePosition
            << " lastLsn# " << lastLsn
            << " readLogResult# " << readLogResult.ToString();

        P_LOG(PRI_ERROR, BPD47, errorReason);
        return false;
    }
    ui64 remainingSize = lastSysLogRecord.size();
    if (remainingSize < sizeof(TSysLogRecord)) {
        errorReason = TStringBuilder() << "Error while parsing sys log at booting state: remainingSize# " << remainingSize
            << " < sizeof(TSysLogRecord)# " << sizeof(TSysLogRecord)
            << " writePosition# " << writePosition
            << " lastLsn# " << lastLsn
            << " readLogResult# " << readLogResult.ToString();

        P_LOG(PRI_ERROR, BPD48, errorReason);
        return false;
    }
    TSysLogRecord *sysLogRecord = (TSysLogRecord*)(lastSysLogRecord.data());

    if (sysLogRecord->Version < PDISK_SYS_LOG_RECORD_INCOMPATIBLE_VERSION_1000) {
        P_LOG(PRI_DEBUG, BPD49, "reading sys log", (SysLogRecord, sysLogRecord->ToString()));
    } else {
        errorReason = TStringBuilder() << "Error while parsing sys log at booting state: Incompatible SysLogRecord Version# "
                << sysLogRecord->Version;

        P_LOG(PRI_ERROR, BPD50, errorReason);
        return false;
    }
    P_LOG(PRI_NOTICE, BPD01, "SysLogRecord is read", (Record, sysLogRecord->ToString()));

    SysLogLsn = lastLsn + 1;

    // Parse VDiskOwners
    ui32 ownerCount = sizeof(sysLogRecord->OwnerVDisks) / sizeof(TVDiskID);
    Y_VERIFY_S(ownerCount <= 256, PCtx->PDiskLogPrefix);
    for (ui32 i = 0; i < ownerCount; ++i) {
        TVDiskID &id = sysLogRecord->OwnerVDisks[i];
        id.GroupGeneration = -1;  // Clear GroupGeneration in sys log record (for compatibility)
        OwnerData[i].VDiskId = id;
        OwnerData[i].Status = TOwnerData::VDISK_STATUS_HASNT_COME;
        ADD_RECORD_WITH_TIMESTAMP_TO_OPERATION_LOG(OwnerData[i].OperationLog, "Processing Chunk0, OwnerId# " << i);
        if (id != TVDiskID::InvalidId) {
            VDiskOwners[id] = TOwner(i);
            AtomicIncrement(TotalOwners);
        }
    }
    SysLogRecord = *sysLogRecord;
    SysLogRecord.Version = PDISK_SYS_LOG_RECORD_VERSION_8;

    // Set initial chunk owners
    // Use actual format info to set busy chunks mask
    ui32 chunkCount = (ui32)(Format.DiskSize / (ui64)Format.ChunkSize);
    Y_VERIFY_DEBUG_S(ChunkState.size() == 0, PCtx->PDiskLogPrefix);
    ChunkState = TVector<TChunkState>(chunkCount);
    Y_VERIFY_S(ChunkState.size() >= Format.SystemChunkCount, PCtx->PDiskLogPrefix);
    for (ui32 i = 0; i < Format.SystemChunkCount; ++i) {
        ChunkState[i].OwnerId = OwnerSystem;
    }

    DriveModel.SetTotalChunksCount(Format.DiskSizeChunks());

    // Parse chunk owners
    TChunkInfo* chunkOwners = (TChunkInfo*)(sysLogRecord + 1);

    // Make sure it is not out of bounds
    remainingSize -= sizeof(TSysLogRecord);
    ui64 expectedSize = chunkCount * sizeof(TChunkInfo);
    if (remainingSize < expectedSize) {
        errorReason = TStringBuilder() << " remainingSize# " << remainingSize
            << " < expectedSize# " << expectedSize
            << " writePosition# " << writePosition
            << " lastLsn# " << lastLsn
            << " readLogResult# " << readLogResult.ToString();

        P_LOG(PRI_ERROR, BPD51, "error while reading sys log", (ErrorReason, errorReason));
        return false;
    }

    // Checks are passed, so initialize position
    InitialSysLogWritePosition = writePosition;
    ShredGeneration = 0;
    for (ui32 i = 0; i < std::min(64u, chunkCount); ++i) {
        if (chunkOwners[i].IsGenerationBitSet()) {
            ShredGeneration |= (1ull<<i);
        }
    }
    int firstSetBitIdx = -1;
    for (ui32 i = 64; i < std::min(64u+8u, chunkCount); ++i) {
        if (chunkOwners[i].IsGenerationBitSet()) {
            firstSetBitIdx = i-64;
            break;
        }
    }
    bool isInconsistentShred = false;
    switch (firstSetBitIdx) {
        case (int)EShredStateDefault:
        case (int)EShredStateSendPreShredCompactVDisk:
        case (int)EShredStateSendShredVDisk:
        case (int)EShredStateFinished:
        case (int)EShredStateFailed:
            ShredState = (EShredState)firstSetBitIdx;
            break;
        default:
            isInconsistentShred = true;
            // The state is unexpected or unsupported yet, suppose it is 'just started'
            break;
    };
    bool isNonCurrentGenPresent = false;
    for (ui32 i = Format.SystemChunkCount; i < chunkCount; ++i) {
        TOwner owner = chunkOwners[i].OwnerId;
        ChunkState[i].OwnerId = owner;
        if (IsOwnerAllocated(owner)) {
            if (IsOwnerUser(owner)) {
                ChunkState[i].CommitState = TChunkState::DATA_COMMITTED;
                Mon.CommitedDataChunks->Inc();
                P_LOG(PRI_DEBUG, BPD01, "CommitedDataChunks is incremented",
                    (CommitedDataChunks, Mon.CommitedDataChunks->Val()),
                    (ChunkIdx, i),
                    (OwnerId, (ui32)owner));
            } else {
                ChunkState[i].CommitState = TChunkState::LOG_COMMITTED;
                if (TPDisk::IS_SHRED_ENABLED) {
                    ChunkState[i].IsDirty = true;
                }
            }
        } else {
            ChunkState[i].CommitState = TChunkState::FREE;
        }
        ChunkState[i].Nonce = chunkOwners[i].Nonce;
        if (TPDisk::IS_SHRED_ENABLED) {
            ChunkState[i].IsDirty = chunkOwners[i].IsDirty();
        }
        if (chunkOwners[i].IsCurrentShredGeneration()) {
          ChunkState[i].ShredGeneration = ShredGeneration;
        } else {
          ChunkState[i].ShredGeneration = 0;
          isNonCurrentGenPresent = true;
        }
    }
    if (ShredGeneration == 0) {
        // No shred ever started
    } else {
        // Shred was started somewhen
        if (isNonCurrentGenPresent || isInconsistentShred) {
            if (ShredState == EShredStateDefault) {
                // Shred is not over but is definitely started
                ShredState = EShredStateSendPreShredCompactVDisk;
            } else if (ShredState == EShredStateFinished) {
                // Shred is not over, inconsistency detected!
                ShredState = EShredStateSendShredVDisk;
            }
        }
    }

    // TODO: check for log/data chunk intersections while parsing common log, giving priority to syslog as chunks
    if (IsOwnerUser(ChunkState[SysLogRecord.LogHeadChunkIdx].OwnerId) &&
            ChunkState[SysLogRecord.LogHeadChunkIdx].CommitState == TChunkState::DATA_COMMITTED) {
        Mon.CommitedDataChunks->Dec();
        P_LOG(PRI_DEBUG, BPD01, "CommitedDataChunks is decremented",
            (CommitedDataChunks, Mon.CommitedDataChunks->Val()),
            (LogHeadChunkIdx, SysLogRecord.LogHeadChunkIdx),
            (PrevOwner, ChunkState[SysLogRecord.LogHeadChunkIdx].OwnerId));
    }

    // might come and go. But make sure each coming chunk goes away!
    P_LOG(PRI_DEBUG, BPD52, "Forcing log head owner to system",
            (ChunkIdx, SysLogRecord.LogHeadChunkIdx),
            (PrevOwner, (ui32)ChunkState[SysLogRecord.LogHeadChunkIdx].OwnerId));
    ChunkState[SysLogRecord.LogHeadChunkIdx].OwnerId = OwnerSystem;
    ChunkState[SysLogRecord.LogHeadChunkIdx].CommitState = TChunkState::DATA_COMMITTED;
    ChunkState[SysLogRecord.LogHeadChunkIdx].PreviousNonce = SysLogRecord.LogHeadChunkPreviousNonce;
    LoggedNonces = SysLogRecord.Nonces;

    // Parse first nonce to keep
    TSysLogFirstNoncesToKeep *firstNoncesToKeep = nullptr;
    if (sysLogRecord->Version == PDISK_SYS_LOG_RECORD_VERSION_2) {
        SysLogFirstNoncesToKeep.Clear();
    } else {
        firstNoncesToKeep = (TSysLogFirstNoncesToKeep*)(chunkOwners + chunkCount);
        // Make sure it is not out of bounds
        ui64 noneSize = (ui64)((char*)firstNoncesToKeep - (char*)sysLogRecord);
        if (lastSysLogRecord.size() == noneSize) {
            P_LOG(PRI_WARN, BPD53, "SysLogRecord size=noneSize", (SysLogRecordSize, lastSysLogRecord.size()), (NoneSize, noneSize));
            SysLogFirstNoncesToKeep.Clear();
        } else {
            ui64 minSize = noneSize + sizeof(TSysLogFirstNoncesToKeep);
            Y_VERIFY_S(lastSysLogRecord.size() >= minSize, PCtx->PDiskLogPrefix
                    << "SysLogRecord is too small, minSize# " << minSize << " size# " << lastSysLogRecord.size());
            memcpy(&SysLogFirstNoncesToKeep, firstNoncesToKeep, sizeof(TSysLogFirstNoncesToKeep));
        }
    }

    TChunkTrimInfo *trimStateEnd = nullptr;
    if (sysLogRecord->Version >= PDISK_SYS_LOG_RECORD_VERSION_4) {
        Y_VERIFY_S(firstNoncesToKeep, PCtx->PDiskLogPrefix);
        ui64 *trimInfoBytesPtr = (ui64*)(firstNoncesToKeep + 1);
        ui64 minSize = (ui64)((char*)(trimInfoBytesPtr + 1) - (char*)sysLogRecord);
        Y_VERIFY_S(lastSysLogRecord.size() >= minSize, PCtx->PDiskLogPrefix
                << "SysLogRecord is too small, minSize# " << minSize << " size# " << lastSysLogRecord.size());
        ui64 trimInfoBytes = ReadUnaligned<ui64>(trimInfoBytesPtr);
        TChunkTrimInfo *trimState = (TChunkTrimInfo*)(trimInfoBytesPtr + 1);
        trimStateEnd = trimState + trimInfoBytes / sizeof(TChunkTrimInfo);
        minSize = (ui64)((char*)trimStateEnd - (char*)sysLogRecord);
        Y_VERIFY_S(lastSysLogRecord.size() >= minSize, PCtx->PDiskLogPrefix
                << "SysLogRecord is too small, minSize# " << minSize << " size# " << lastSysLogRecord.size());
        Y_VERIFY_S(trimInfoBytes == 0 || trimInfoBytes == TChunkTrimInfo::SizeForChunkCount(chunkCount),
                PCtx->PDiskLogPrefix << "SysLogRecord's ChunkTrimInfo has size# " << trimInfoBytes
                << " different from expeceted #" << TChunkTrimInfo::SizeForChunkCount(chunkCount));
        for (ui32 i = 0; i < chunkCount; i++) {
            if (trimState[i / 8].IsChunkTrimmed(i % 8) && ChunkState[i].OwnerId == OwnerUnallocated) {
                ChunkState[i].OwnerId = OwnerUnallocatedTrimmed;
            }
        }
    }

    // Fill with default value to parse log form the start on old versions
    FirstLogChunkToParseCommits = SysLogRecord.LogHeadChunkIdx;

    ui32 *firstChunkEnd = nullptr;
    if (sysLogRecord->Version >= PDISK_SYS_LOG_RECORD_VERSION_6) {
        Y_VERIFY_S(trimStateEnd, PCtx->PDiskLogPrefix);
        ui32 *firstChunk = reinterpret_cast<ui32*>(trimStateEnd);
        firstChunkEnd = firstChunk + 1;
        ui64 minSize = (ui64)((char*)firstChunkEnd - (char*)sysLogRecord);
        Y_VERIFY_S(lastSysLogRecord.size() >= minSize, PCtx->PDiskLogPrefix
                << "SysLogRecord is too small, minSize# " << minSize << " size# " << lastSysLogRecord.size());
        FirstLogChunkToParseCommits = ReadUnaligned<ui32>(firstChunk);
    }

    bool suppressCompatibilityCheck = Cfg->FeatureFlags.GetSuppressCompatibilityCheck();

    char *compatibilityInfoEnd = nullptr;
    if (sysLogRecord->Version >= PDISK_SYS_LOG_RECORD_VERSION_7) {
        Y_VERIFY_S(firstChunkEnd, PCtx->PDiskLogPrefix);
        ui32 *protoSizePtr = reinterpret_cast<ui32*>(firstChunkEnd);
        ui32 *protoSizePtrEnd = protoSizePtr + 1;

        ui64 minSize = (ui64)((char*)protoSizePtrEnd - (char*)sysLogRecord);
        Y_VERIFY_S(lastSysLogRecord.size() >= minSize, PCtx->PDiskLogPrefix
                << "SysLogRecord is too small, minSize# " << minSize << " size# " << lastSysLogRecord.size());

        ui32 protoSize = ReadUnaligned<ui32>(protoSizePtr);
        Y_VERIFY_S(protoSize > 0, PCtx->PDiskLogPrefix);

        char *compatibilityInfo = reinterpret_cast<char*>(protoSizePtrEnd);
        compatibilityInfoEnd = compatibilityInfo + protoSize;

        minSize += protoSize;
        Y_VERIFY_S(lastSysLogRecord.size() >= minSize, PCtx->PDiskLogPrefix
                << "SysLogRecord is too small, minSize# " << minSize << " size# " << lastSysLogRecord.size());

        if (!suppressCompatibilityCheck) {
            auto storedCompatibilityInfo = NKikimrConfig::TStoredCompatibilityInfo();

            bool success = storedCompatibilityInfo.ParseFromArray(compatibilityInfo, protoSize);
            Y_VERIFY_S(success, PCtx->PDiskLogPrefix);

            bool isCompatible = CompatibilityInfo.CheckCompatibility(&storedCompatibilityInfo,
                    NKikimrConfig::TCompatibilityRule::PDisk, errorReason);

            if (!isCompatible) {
                P_LOG(PRI_ERROR, BPD01, "Incompatible version", (ErrorReason, errorReason));
                return false;
            }
        }
    } else if (!suppressCompatibilityCheck && sysLogRecord->Version != 0) {
        // Sys log is not empty, but it doesn't contain compatibility info record
        TString error;
        bool isCompatible = CompatibilityInfo.CheckCompatibility(nullptr,
                NKikimrConfig::TCompatibilityRule::PDisk, errorReason);

        if (!isCompatible) {
            P_LOG(PRI_ERROR, BPD01, "Stored compatibility info is absent, current version is incompatible with the default stored version of PDisk",
                (ErrorReason, errorReason));
            return false;
        }
    }

    char *ownersSizeInUnitsInfoEnd = nullptr;
    if (sysLogRecord->Version >= PDISK_SYS_LOG_RECORD_VERSION_8) {
        Y_VERIFY_S(compatibilityInfoEnd, PCtx->PDiskLogPrefix);
        ui32 *protoSizePtr = reinterpret_cast<ui32*>(compatibilityInfoEnd);
        ui32 *protoSizePtrEnd = protoSizePtr + 1;

        ui64 minSize = (ui64)((char*)protoSizePtrEnd - (char*)sysLogRecord);
        Y_VERIFY_S(lastSysLogRecord.size() >= minSize, PCtx->PDiskLogPrefix
                << "SysLogRecord is too small, minSize# " << minSize << " size# " << lastSysLogRecord.size());

        ui32 protoSize = ReadUnaligned<ui32>(protoSizePtr);

        char *ownersSizeInUnitsInfo = reinterpret_cast<char*>(protoSizePtrEnd);
        ownersSizeInUnitsInfoEnd = ownersSizeInUnitsInfo + protoSize;

        minSize += protoSize;
        Y_VERIFY_S(lastSysLogRecord.size() >= minSize, PCtx->PDiskLogPrefix
            << "SysLogRecord is too small, minSize# " << minSize << " size# " << lastSysLogRecord.size());

        for (ui32 i = 0; i < protoSize/2; ++i) {
            TOwner owner = ownersSizeInUnitsInfo[i*2 + 0];
            ui8 groupSizeInUnits = ownersSizeInUnitsInfo[i*2 + 1];
            static_assert(sizeof(TOwner) == 1);
            static_assert(sizeof(ui8) == 1);
            OwnerData[owner].GroupSizeInUnits = ui32(groupSizeInUnits);
        }
    }

    // needed for further parsing
    Y_UNUSED(ownersSizeInUnitsInfoEnd);

    PrintChunksDebugInfo();
    return true;
}

static void PrintCondensedChunksList(TStringStream& str, const std::vector<ui32>& chunks) {
    str << " [";
    bool first = true;
    std::optional<TChunkIdx> begin;
    for (size_t i = 0; i < chunks.size(); ++i) {
        if (!begin) {
            begin = chunks[i];
        }
        if (i + 1 < chunks.size() && chunks[i] + 1 == chunks[i + 1]) {
            continue;
        }
        str << (std::exchange(first, false) ? "" : " ");
        if (*begin == chunks[i]) {
            str << chunks[i];
        } else {
            str << *begin << "-" << chunks[i];
        }
        begin.reset();
    }
    str << "];";
}

void TPDisk::PrintChunksDebugInfo() {
    auto print = [&] () {
        std::map<TOwner, std::vector<ui32>> ownerToChunks;

        for (ui32 i = 0; i < ChunkState.size(); ++i) {
            const auto& state = ChunkState[i];
            ownerToChunks[state.OwnerId].push_back(i);
        }

        TStringStream str;
        str << "PrintChunksDebugInfo; ";
        for (auto& [owner, chunks] : ownerToChunks) {
            std::sort(chunks.begin(), chunks.end());
            str << " Owner# " << owner;
            PrintCondensedChunksList(str, chunks);
        }
        return str.Str();
    };

    P_LOG(PRI_DEBUG, BPD01, print());
}

TRcBuf TPDisk::ProcessReadSysLogResult(ui64 &outWritePosition, ui64 &outLsn,
        const NPDisk::TEvReadLogResult &readLogResult) {
    ui64 sectorIdx = (readLogResult.NextPosition.OffsetInChunk + Format.SectorSize - 1) / Format.SectorSize;
    ui64 firstSysLogSectorIdx = Format.FirstSysLogSectorIdx();
    ui64 sectorGroup = (sectorIdx - firstSysLogSectorIdx) / ReplicationFactor;

    outWritePosition = (firstSysLogSectorIdx + sectorGroup % Format.SysLogSectorCount * ReplicationFactor)
            * Format.SectorSize;
    Y_VERIFY_S(outWritePosition > 0, PCtx->PDiskLogPrefix);

    if (!readLogResult.Results.size()) {
        P_LOG(PRI_ERROR, BPD54, "ProcessReadSysLogResult Results.size() == 0");
        outLsn = 0;
        TRcBuf data;
        return data;
    }
    ui64 lastSysLogLsn = readLogResult.Results[0].Lsn;
    TRcBuf data = readLogResult.Results[0].Data;
    for (ui32 i = 1; i < readLogResult.Results.size(); ++i) {
        if (lastSysLogLsn < readLogResult.Results[i].Lsn) {
            lastSysLogLsn = readLogResult.Results[i].Lsn;
            data = readLogResult.Results[i].Data;
        }
    }
    outLsn = lastSysLogLsn;
    return data;
}

void TPDisk::ReadAndParseMainLog(const TActorId &pDiskActor) {
    TVector<TLogChunkItem> chunksToRead;
    TIntrusivePtr<TLogReaderBase> logReader(new TLogReader(true, this, PCtx->ActorSystem, pDiskActor, 0, TLogPosition{0, 0},
                EOwnerGroupType::Static, TLogPosition{0, 0}, (ui64)-1, SysLogRecord.LogHeadChunkPreviousNonce, 0, 0,
                TReqId(TReqId::ReadAndParseMainLog, 0), std::move(chunksToRead), 0, 0, TVDiskID::InvalidId));
    TVector<ui64> badOffsets;
    // Emits subrequests TCompletionLogReadPart which contains TIntrusivePtr to logReader
    logReader->Exec(0, badOffsets, PCtx->ActorSystem);
}

void TPDisk::ProcessLogReadQueue() {
    for (auto& req : JointLogReads) {
        req->Span.Event("PDisk.BeforeBlockDevice");
        switch (req->GetType()) {
        case ERequestType::RequestLogRead:
        {
            TLogRead &logRead = *static_cast<TLogRead*>(req);
            auto& ownerData = OwnerData[logRead.Owner];

            ownerData.Status = TOwnerData::VDISK_STATUS_READING_LOG;
            TLogPosition logStartPosition{0, 0};
            if (logRead.Owner < OwnerData.size() && ownerData.VDiskId != TVDiskID::InvalidId) {
                logStartPosition = ownerData.LogStartPosition;
            }
            TVector<TLogChunkItem> chunksToRead;
            bool isPrevDropped = false;
            for (auto it = LogChunks.begin(); it != LogChunks.end(); ++it) {
                if (it->OwnerLsnRange.size() > logRead.Owner && it->OwnerLsnRange[logRead.Owner].IsPresent) {
                    bool isPrevCut = (it->IsEndOfSplice && it != LogChunks.begin());
                    chunksToRead.emplace_back(it->ChunkIdx, isPrevDropped, isPrevCut);
                    isPrevDropped = false;
                } else {
                    isPrevDropped = true;
                }
            }
            ui64 firstLsnToKeep = 0;
            ui64 firstNonceToKeep = SysLogFirstNoncesToKeep.FirstNonceToKeep[logRead.Owner];
            if (ownerData.VDiskId != TVDiskID::InvalidId) {
                firstLsnToKeep = ownerData.CurrentFirstLsnToKeep;
                P_LOG(PRI_INFO, BPD01, "Going to read log for owner", (ownerId, logRead.Owner), (FirstLsnToKeep, firstLsnToKeep), (FirstNonceToKeep, firstNonceToKeep));
            }

            ui32 endLogChunkIdx;
            ui64 endLogSectorIdx;

            if (Cfg->ReadOnly) {
                endLogChunkIdx = LastInitialChunkIdx;
                endLogSectorIdx = LastInitialSectorIdx;
            } else {
                TOwnerData::TLogEndPosition &logEndPos = ownerData.LogEndPosition;
                if (logEndPos.ChunkIdx == 0 && logEndPos.SectorIdx == 0) {
                    TFirstUncommitted firstUncommitted = CommonLogger->FirstUncommitted.load();
                    endLogChunkIdx = firstUncommitted.ChunkIdx;
                    endLogSectorIdx = firstUncommitted.SectorIdx;
                } else {
                    endLogChunkIdx = logEndPos.ChunkIdx;
                    endLogSectorIdx = logEndPos.SectorIdx;
                }
            }

            ownerData.LogReader = new TLogReader(false,
                        this, PCtx->ActorSystem, logRead.Sender, logRead.Owner, logStartPosition,
                        logRead.OwnerGroupType,logRead.Position,
                        logRead.SizeLimit, 0, endLogChunkIdx, endLogSectorIdx, logRead.ReqId,
                        std::move(chunksToRead), firstLsnToKeep, firstNonceToKeep,
                        ownerData.VDiskId);
            TVector<ui64> badOffsets;
            ownerData.LogReader->Exec(0, badOffsets, PCtx->ActorSystem);
            break;
        }
        case ERequestType::RequestLogReadContinue:
        {
            TLogReadContinue *read = static_cast<TLogReadContinue*>(req);
            if (auto ptr = read->CompletionAction.lock()) {
                ptr->CostNs = DriveModel.TimeForSizeNs(read->Size, read->Offset / Format.ChunkSize, TDriveModel::OP_TYPE_READ);
                auto traceId = read->Span.GetTraceId();
                BlockDevice->PreadAsync(read->Data, read->Size, read->Offset, ptr.get(), read->ReqId, &traceId);
            }
            break;
        }
        case ERequestType::RequestLogSectorRestore:
        {
            TLogSectorRestore *restore = static_cast<TLogSectorRestore*>(req);
            BlockDevice->PwriteAsync(restore->Data, restore->Size, restore->Offset, restore->CompletionAction,
                    restore->ReqId, {});
            break;
        }
        case ERequestType::RequestLogReadResultProcess:
        {
            TLogReadResultProcess *result = static_cast<TLogReadResultProcess*>(req);
            ProcessReadLogResult(*result->ReadLogResult->Get(), result->Sender);
            break;
        }
        default:
            Y_ABORT();
            break;
        }
        delete req;
    }
    JointLogReads.clear();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// SysLog writing
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
void TPDisk::WriteSysLogRestorePoint(TCompletionAction *action, TReqId reqId, NWilson::TTraceId *traceId) {
    TGuard<TMutex> guard(StateMutex);
    LoggedNonces = SysLogRecord.Nonces;
    ui32 chunkCount = (ui32)(Format.DiskSize / (ui64)Format.ChunkSize);
    ui32 chunkOwnersSize = ui32(sizeof(TChunkInfo)) * chunkCount;
    // Must be ui64
    ui64 chunkIsTrimmedSize = TChunkTrimInfo::SizeForChunkCount(chunkCount);
    TVector<TChunkInfo> chunkOwners(chunkCount);
    TVector<TChunkTrimInfo> chunkIsTrimmed(TChunkTrimInfo::RecordsForChunkCount(chunkCount), TChunkTrimInfo(0));
    const ui32 chunkStateSize = ChunkState.size();
    for (ui32 i = 0; i < std::min(64u, chunkCount); ++i) {
        chunkOwners[i].SetGenerationBit(ShredGeneration & (1ull << i));
    }
    ui8 shredStateBits = (1ull << ShredState);
    for (ui32 i = 64; i < std::min(64u+8u, chunkCount); ++i) {
        chunkOwners[i].SetGenerationBit(shredStateBits & (1u << (i-64)));
    }
    for (ui32 i = 0; i < chunkCount; ++i) {
        TChunkInfo &info = chunkOwners[i];
        if (chunkStateSize > i
                && (ChunkState[i].CommitState == TChunkState::DATA_COMMITTED
                    || ChunkState[i].CommitState == TChunkState::DATA_COMMITTED_DELETE_IN_PROGRESS
                    || ChunkState[i].CommitState == TChunkState::DATA_COMMITTED_DELETE_ON_QUARANTINE)
                && IsOwnerUser(ChunkState[i].OwnerId)) {
            info.OwnerId = ChunkState[i].OwnerId;
            info.Nonce = ChunkState[i].Nonce;
        } else {
            if (chunkStateSize > i && ChunkState[i].OwnerId == OwnerUnallocatedTrimmed) {
                chunkIsTrimmed[i / 8].SetChunkTrimmed(i % 8);
            }
            // Write OwnerUnallocated for forward compatibility
            info.OwnerId = OwnerUnallocated;
            info.Nonce = 0;
        }
        if (chunkStateSize > i) {
            TChunkState &state = ChunkState[i];
            info.SetDirty(state.IsDirty, state.ShredGeneration == ShredGeneration);
        }
    }
    if (CommonLogger) {
        std::optional<TChunkIdx> firstChunk;
        for (auto rit = LogChunks.crbegin(); rit != LogChunks.crend(); ++rit) {
            if (rit->IsEndOfSplice) {
                firstChunk = rit->ChunkIdx;
                break;
            }
        }
        if (!firstChunk && !LogChunks.empty()) {
            firstChunk = LogChunks.front().ChunkIdx;
        }
        FirstLogChunkToParseCommits = firstChunk.value_or(SysLogRecord.LogHeadChunkIdx);
    }

    if (!SerializedCompatibilityInfo) {
        SerializedCompatibilityInfo.emplace(TString());
        auto stored = CompatibilityInfo.MakeStored(NKikimrConfig::TCompatibilityRule::PDisk);
        bool success = stored.SerializeToString(&*SerializedCompatibilityInfo);
        Y_VERIFY_S(success, PCtx->PDiskLogPrefix);
    }
    ui32 compatibilityInfoSize = SerializedCompatibilityInfo->size();
    std::vector<std::pair<TOwner, ui8>> ownersSizeInUnitsInfo(AtomicGet(TotalOwners));

    for (ui32 owner = 0; owner < OwnerData.size(); ++owner) {
        if (OwnerData[owner].VDiskId != TVDiskID::InvalidId) {
            ownersSizeInUnitsInfo.push_back(std::make_pair(owner, OwnerData[owner].GroupSizeInUnits));
        }
    }

    ui32 ownersSizeInUnitsInfoSize = sizeof(ownersSizeInUnitsInfo[0]) * ownersSizeInUnitsInfo.size();
    static_assert(sizeof(ownersSizeInUnitsInfo[0]) == 2);

    ui32 recordSize = sizeof(TSysLogRecord) + chunkOwnersSize + sizeof(TSysLogFirstNoncesToKeep)
        + sizeof(ui64) + chunkIsTrimmedSize + sizeof(ui32) + sizeof(ui32) + compatibilityInfoSize
        + sizeof(ownersSizeInUnitsInfoSize) + ownersSizeInUnitsInfoSize;
    ui64 beginSectorIdx = SysLogger->SectorIdx;
    *Mon.BandwidthPSysLogPayload += recordSize;
    *Mon.BandwidthPSysLogRecordHeader += sizeof(TFirstLogPageHeader);

    SysLogger->LogHeader(0, 0, SysLogLsn, recordSize, reqId, traceId);
    SysLogger->LogDataPart(&SysLogRecord, sizeof(TSysLogRecord), reqId, traceId);
    SysLogger->LogDataPart(&chunkOwners[0], chunkOwnersSize, reqId, traceId);
    SysLogger->LogDataPart(&SysLogFirstNoncesToKeep, sizeof(TSysLogFirstNoncesToKeep), reqId, traceId);
    SysLogger->LogDataPart(&chunkIsTrimmedSize, sizeof(chunkIsTrimmedSize), reqId, traceId);
    SysLogger->LogDataPart(&chunkIsTrimmed[0], chunkIsTrimmedSize, reqId, traceId);
    SysLogger->LogDataPart(&FirstLogChunkToParseCommits, sizeof(FirstLogChunkToParseCommits), reqId, traceId);
    SysLogger->LogDataPart(&compatibilityInfoSize, sizeof(compatibilityInfoSize), reqId, traceId);
    SysLogger->LogDataPart(SerializedCompatibilityInfo->data(), compatibilityInfoSize, reqId, traceId);
    SysLogger->LogDataPart(&ownersSizeInUnitsInfoSize, sizeof(ownersSizeInUnitsInfoSize), reqId, traceId);
    if (ownersSizeInUnitsInfoSize > 0) {
        SysLogger->LogDataPart(&ownersSizeInUnitsInfo[0], ownersSizeInUnitsInfoSize, reqId, traceId);
    }
    SysLogger->TerminateLog(reqId, traceId);
    SysLogger->Flush(reqId, traceId, action);

    ui64 endSectorIdx = SysLogger->SectorIdx;

    P_LOG(PRI_DEBUG, BPD69, "WriteSysLogRestorePoint",
            (FirstLogChunkToParseCommits, FirstLogChunkToParseCommits),
            (CommonLogger, TString(TStringBuilder() << (void*)CommonLogger.Get())),
            ("LogChunks.size()", LogChunks.size()),
            ("LogChunks.front().ChunkIdx", (LogChunks.empty() ? -1 : (i64)LogChunks.front().ChunkIdx)),
            (BeginSectorIdx, beginSectorIdx),
            (EndSectorIdx, endSectorIdx));
    ++SysLogLsn;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Common log writing
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
void TPDisk::ProcessLogWriteQueue() {
    while (JointLogWrites.size()) {
        TVector<TLogWrite*> logWrites;
        logWrites.reserve(JointLogWrites.size());
        TVector<TLogWrite*> commits;
        commits.reserve(JointLogWrites.size());
        size_t batchSizeBytes = 0;
        while (JointLogWrites.size()) {
            auto *log = static_cast<TLogWrite*>(JointLogWrites.front());
            JointLogWrites.pop();

            logWrites.push_back(log);
            batchSizeBytes += log->Data.Size();
            if (log->Signature.HasCommitRecord()) {
                commits.push_back(log);
            }
            if (UseNoopSchedulerCached && batchSizeBytes >= (size_t)ForsetiOpPieceSizeCached) {
                break;
            }
        }
        LWTRACK(PDiskProcessLogWriteQueue, UpdateCycleOrbit, PCtx->PDiskId, JointLogWrites.size(), logWrites.size(), commits.size());
        ProcessLogWriteBatch(std::move(logWrites), std::move(commits));
    }
}

void TPDisk::ProcessLogWriteBatch(TVector<TLogWrite*> logWrites, TVector<TLogWrite*> commits) {
    if (logWrites.empty()) {
        return;
    }

    for (TLogWrite *logCommit : commits) {
        TStringStream errorReason;
        NKikimrProto::EReplyStatus status = ValidateRequest(logCommit, errorReason);
        if (status == NKikimrProto::OK) {
            status = BeforeLoggingCommitRecord(*logCommit, errorReason);
        }
        if (status != NKikimrProto::OK) {
            PrepareLogError(logCommit, errorReason, status);
        }
    }
    NHPTimer::STime now = HPNow();
    NWilson::TTraceId traceId;
    size_t logOperationSizeBytes = 0;
    TVector<ui32> logChunksToCommit;
    for (TLogWrite *logWrite : logWrites) {
        Y_VERIFY_DEBUG_S(logWrite, PCtx->PDiskLogPrefix);
        Mon.LogQueueTime.Increment(logWrite->LifeDurationMs(now));
        logOperationSizeBytes += logWrite->Data.size();
        TStringStream errorReason;
        NKikimrProto::EReplyStatus status = ValidateRequest(logWrite, errorReason);
        if (status == NKikimrProto::OK) {
            logWrite->Span.Event("PDisk.BeforeBlockDevice");
            LogWrite(*logWrite, logChunksToCommit);
            logWrite->ScheduleTime = HPNow();
            if (auto logWriteTraceId = logWrite->Span.GetTraceId()) {
                traceId = std::move(logWriteTraceId);
            }
        } else {
            PrepareLogError(logWrite, errorReason, status);
        }
    }
    for (TLogWrite *logWrite : logWrites) {
        LWTRACK(PDiskLogWriteFlush, logWrite->Orbit, PCtx->PDiskId, logWrite->ReqId.Id, HPSecondsFloat(logWrite->CreationTime),
                double(logWrite->Cost) / 1000000.0, HPSecondsFloat(logWrite->Deadline),
                logWrite->Owner, logWrite->IsFast, logWrite->PriorityClass);
    }
    LWTRACK(PDiskProcessLogWriteBatch, UpdateCycleOrbit, PCtx->PDiskId, logWrites.size(), commits.size());
    TReqId reqId = logWrites.back()->ReqId;
    auto write = MakeHolder<TCompletionLogWrite>(
        this, std::move(logWrites), std::move(commits), std::move(logChunksToCommit));
    LogFlush(write.Get(), write->GetCommitedLogChunksPtr(), reqId, &traceId);
    Y_UNUSED(write.Release());

    // Check if we can TRIM some chunks that were deleted
    TryTrimChunk(false, 0);

    Mon.LogOperationSizeBytes.Increment(logOperationSizeBytes);
}

bool TPDisk::PreallocateLogChunks(ui64 headedRecordSize, TOwner owner, ui64 lsn, EOwnerGroupType ownerGroupType,
        bool isAllowedForSpaceRed) {
    ui32 additionalChunksNeeded = 0;
    ui32 additionalChunksContainingPayload = 0;
    if (CommonLogger->SectorBytesFree < headedRecordSize + sizeof(TFirstLogPageHeader)) {
        ui64 additionalDataSize = headedRecordSize + sizeof(TFirstLogPageHeader) -
            CommonLogger->SectorBytesFree;
        ui64 logPayloadPerSector = Format.SectorPayloadSize() - sizeof(TLogPageHeader);
        ui64 additionalPayloadSectors = (additionalDataSize + logPayloadPerSector - 1) / logPayloadPerSector;
        ui64 usableSectorsPerLogChunk = UsableSectorsPerLogChunk();
        ui64 sectorsUnusedPayload = usableSectorsPerLogChunk - CommonLogger->SectorIdx - 1;
        if (sectorsUnusedPayload <= additionalPayloadSectors) {
            ui64 extrachunkSectors = additionalPayloadSectors - sectorsUnusedPayload;
            ui64 chunkPayloadSectors = usableSectorsPerLogChunk;
            additionalChunksContainingPayload = (ui32)((extrachunkSectors + chunkPayloadSectors - 1) /
                chunkPayloadSectors);
            additionalChunksNeeded = (ui32)((extrachunkSectors + chunkPayloadSectors - 1) / chunkPayloadSectors);
        }
    }

    return AllocateLogChunks(
        additionalChunksNeeded, additionalChunksContainingPayload, owner, lsn, ownerGroupType, isAllowedForSpaceRed);
}

bool TPDisk::AllocateLogChunks(ui32 chunksNeeded, ui32 chunksContainingPayload, TOwner owner, ui64 lsn,
        EOwnerGroupType ownerGroupType, bool isAllowedForSpaceRed) {
    TGuard<TMutex> guard(StateMutex);
    TOwner keeperOwner = (ownerGroupType == EOwnerGroupType::Dynamic ? OwnerSystem : OwnerCommonStaticLog);

    // Check space and free it if needed
    using TColor = NKikimrBlobStorage::TPDiskSpaceColor;
    double occupancy;
    TColor::E color = Keeper.EstimateSpaceColor(keeperOwner, chunksNeeded, &occupancy);
    if (color >= TColor::RED && !isAllowedForSpaceRed) {
        return false;
    }
    if (color == TColor::BLACK) {
        return false;
    }

    if (IsOwnerUser(owner)) {
        Y_VERIFY_S(LogChunks.empty() || chunksNeeded > 0 || LogChunks.back().ChunkIdx == CommonLogger->ChunkIdx,
            PCtx->PDiskLogPrefix << "Chunk idx mismatch! back# " << LogChunks.back().ChunkIdx
            << " pre-back# " << (LogChunks.rbegin()->ChunkIdx == LogChunks.begin()->ChunkIdx ?
                0 : (++LogChunks.rbegin())->ChunkIdx)
            << " logger# " << CommonLogger->ChunkIdx);
        if (OwnerData[owner].VDiskId != TVDiskID::InvalidId) {
            LogChunks.back().RegisterLogSector<true>(owner, lsn);
        }
    }
    if (chunksNeeded == 0) {
        return true;
    }

    ui32 usableSectors = UsableSectorsPerLogChunk();
    ui64 noncesPerChunk = usableSectors + 1;
    ui64 sectorsToLast = usableSectors > CommonLogger->SectorIdx ?
        (usableSectors - CommonLogger->SectorIdx) : 1;
    ui64 lastNonce = CommonLogger->Nonce + sectorsToLast + noncesPerChunk * CommonLogger->NextChunks.size();

    TString errorReason;
    bool isDirtyMarked = false;
    for (ui32 i = 0; i < chunksNeeded; ++i) {
        ui32 chunkIdx = Keeper.PopOwnerFreeChunk(keeperOwner, errorReason);
        Y_VERIFY_S(chunkIdx, PCtx->PDiskLogPrefix << "errorReason# " << errorReason);
        Y_VERIFY_S(ChunkState[chunkIdx].OwnerId == OwnerUnallocated ||
                ChunkState[chunkIdx].OwnerId == OwnerUnallocatedTrimmed, PCtx->PDiskLogPrefix <<
                "Unexpected ownerId# " << ui32(ChunkState[chunkIdx].OwnerId));
        ChunkState[chunkIdx].CommitState = TChunkState::LOG_RESERVED;
        if (TPDisk::IS_SHRED_ENABLED && !ChunkState[chunkIdx].IsDirty) {
            ChunkState[chunkIdx].IsDirty = true;
            isDirtyMarked = true;
        }
        P_LOG(PRI_INFO, BPD01, "AllocateLogChunks for owner", (OwnerId, (ui32)owner), (Lsn, lsn), (ChunkIdx, chunkIdx),
            (LogChunksSize, LogChunks.size()));
        ChunkState[chunkIdx].OwnerId = OwnerSystem;
        ChunkState[chunkIdx].PreviousNonce = lastNonce + noncesPerChunk * (ui64)i;
        // Mark newly allocated log chunks as chunks containing this owners record
        LogChunks.push_back(TLogChunkInfo(chunkIdx, (ui32)OwnerData.size()));
        Mon.LogChunks->Inc();
        if (IsOwnerUser(owner) && i < chunksContainingPayload && OwnerData[owner].VDiskId != TVDiskID::InvalidId) {
            LogChunks.back().RegisterLogSector<true>(owner, lsn);
        }
        CommonLogger->NextChunks.push_back(TChunkIdxWithInfo{chunkIdx, &LogChunks.back()});
        AtomicSet(EstimatedLogChunkIdx, chunkIdx);
    }

    AskVDisksToCutLogs(OwnerSystem, false);
    if (isDirtyMarked) {
        WriteSysLogRestorePoint(nullptr, TReqId(TReqId::MarkDirtySysLog, 0), {});
    }
    return true;
}

void TPDisk::LogWrite(TLogWrite &evLog, TVector<ui32> &logChunksToCommit) {
    Y_VERIFY_DEBUG_S(!evLog.Result, PCtx->PDiskLogPrefix);
    OwnerData[evLog.Owner].Status = TOwnerData::VDISK_STATUS_LOGGED;

    bool isCommitRecord = evLog.Signature.HasCommitRecord();
    ui64 payloadSize = evLog.Data.size();
    *Mon.BandwidthPLogPayload += payloadSize;
    if (isCommitRecord) {
        ui64 commitSize = (sizeof(ui32) + sizeof(ui64)) * evLog.CommitRecord.CommitChunks.size() +
            sizeof(ui32) * evLog.CommitRecord.DeleteChunks.size() +
            (TPDisk::IS_SHRED_ENABLED ? sizeof(ui32) * evLog.CommitRecord.DirtyChunks.size() : 0) +
            sizeof(NPDisk::TCommitRecordFooter);
        payloadSize += commitSize;
        *Mon.BandwidthPLogCommit += commitSize;
    }

    ui64 headedRecordSize = payloadSize + sizeof(TFirstLogPageHeader);
    *Mon.BandwidthPLogRecordHeader += sizeof(TFirstLogPageHeader);
    bool isAllowedForSpaceRed = isCommitRecord && (evLog.CommitRecord.DeleteChunks.size() > 0);
    if (!PreallocateLogChunks(headedRecordSize, evLog.Owner, evLog.Lsn, evLog.OwnerGroupType, isAllowedForSpaceRed)) {
        // TODO: make sure that commit records that delete chunks are applied atomically even if this error occurs.
        TStringStream str;
        str << PCtx->PDiskLogPrefix << "Can't preallocate log chunks!"
            << " Marker# BPD70";
        P_LOG(PRI_ERROR, BPD70, str.Str());
        evLog.Result.Reset(new NPDisk::TEvLogResult(NKikimrProto::OUT_OF_SPACE,
            NotEnoughDiskSpaceStatusFlags(evLog.Owner, evLog.OwnerGroupType), str.Str(),
            Keeper.GetLogChunkCount()));
        Y_VERIFY_S(evLog.Result.Get(), PCtx->PDiskLogPrefix);
        evLog.Result->Results.push_back(NPDisk::TEvLogResult::TRecord(evLog.Lsn, evLog.Cookie));
        return;
    }
    if (!CommonLogger->NextChunks.empty()) {
        size_t sizeToCommit = CommonLogger->NextChunks.size() - 1;
        logChunksToCommit.reserve(logChunksToCommit.size() + 1 + sizeToCommit);
        logChunksToCommit.push_back(CommonLogger->ChunkIdx);
        for (size_t i = 0; i < sizeToCommit; ++i) {
            logChunksToCommit.push_back(CommonLogger->NextChunks[i].Idx);
        }
    }

    // Write to log
    auto evLogTraceId = evLog.Span.GetTraceId();
    CommonLogger->LogHeader(evLog.Owner, evLog.Signature, evLog.Lsn, payloadSize, evLog.ReqId, &evLogTraceId);
    OnNonceChange(NonceLog, evLog.ReqId, &evLogTraceId);
    if (evLog.Data.size()) {
        CommonLogger->LogDataPart(evLog.Data.data(), evLog.Data.size(), evLog.ReqId, &evLogTraceId);
    }
    if (isCommitRecord) {
        ui32 commitChunksCount = evLog.CommitRecord.CommitChunks.size();
        if (commitChunksCount) {
            CommonLogger->LogDataPart(evLog.CommitRecord.CommitChunks.data(), commitChunksCount * sizeof(ui32),
                    evLog.ReqId, &evLogTraceId);
            TVector<ui64> commitChunkNonces(commitChunksCount);
            for (ui32 idx = 0; idx < commitChunksCount; ++idx) {
                commitChunkNonces[idx] = ChunkState[evLog.CommitRecord.CommitChunks[idx]].Nonce;
            }
            CommonLogger->LogDataPart(&commitChunkNonces[0], sizeof(ui64) * commitChunksCount, evLog.ReqId, &evLogTraceId);
        }
        ui32 deleteChunksCount = evLog.CommitRecord.DeleteChunks.size();
        if (deleteChunksCount) {
            CommonLogger->LogDataPart(evLog.CommitRecord.DeleteChunks.data(), deleteChunksCount * sizeof(ui32),
                evLog.ReqId, &evLogTraceId);
        }
        if (TPDisk::IS_SHRED_ENABLED) {
            ui32 dirtyChunksCount = evLog.CommitRecord.DirtyChunks.size();
            if (dirtyChunksCount) {
                CommonLogger->LogDataPart(evLog.CommitRecord.DirtyChunks.data(), dirtyChunksCount * sizeof(ui32),
                    evLog.ReqId, &evLogTraceId);
            }
        }
        NPDisk::TCommitRecordFooter footer(evLog.Data.size(), evLog.CommitRecord.FirstLsnToKeep,
            evLog.CommitRecord.CommitChunks.size(), evLog.CommitRecord.DeleteChunks.size(),
            evLog.CommitRecord.DirtyChunks.size(),
            evLog.CommitRecord.IsStartingPoint);
        CommonLogger->LogDataPart(&footer, sizeof(footer), evLog.ReqId, &evLogTraceId);

        {
            TGuard<TMutex> guard(StateMutex);
            if (evLog.CommitRecord.IsStartingPoint) {
                TLogSignature unmasked = evLog.Signature.GetUnmasked();
                OwnerData[evLog.Owner].StartingPoints[unmasked] =
                    TLogRecord(unmasked, evLog.Data, evLog.Lsn);
            }
            if (evLog.CommitRecord.FirstLsnToKeep >= OwnerData[evLog.Owner].CurrentFirstLsnToKeep) {
                if (evLog.CommitRecord.FirstLsnToKeep > OwnerData[evLog.Owner].CurrentFirstLsnToKeep) {
                    OwnerData[evLog.Owner].CutLogAt = TInstant::Now();
                }
                OwnerData[evLog.Owner].CurrentFirstLsnToKeep = evLog.CommitRecord.FirstLsnToKeep;
            }
        }
    }
    Y_VERIFY_S(CommonLogger->NextChunks.empty(), PCtx->PDiskLogPrefix);

    evLog.Result.Reset(new NPDisk::TEvLogResult(NKikimrProto::OK,
        GetStatusFlags(OwnerSystem, evLog.OwnerGroupType), "", Keeper.GetLogChunkCount()));
    Y_VERIFY_S(evLog.Result.Get(), PCtx->PDiskLogPrefix);
    evLog.Result->Results.push_back(NPDisk::TEvLogResult::TRecord(evLog.Lsn, evLog.Cookie));
}

void TPDisk::LogFlush(TCompletionAction *action, TVector<ui32> *logChunksToCommit, TReqId reqId,
        NWilson::TTraceId *traceId) {

    if (!CommonLogger->IsEmptySector()) {
        size_t prevPreallocatedSize = CommonLogger->NextChunks.size();
        if (!PreallocateLogChunks(CommonLogger->SectorBytesFree, OwnerSystem, 0, EOwnerGroupType::Static, true)) {
            Y_ABORT_S(PCtx->PDiskLogPrefix << "Last chunk is over, how did you do that?!");
        }
        size_t nextPreallocatedSize = CommonLogger->NextChunks.size();
        if (nextPreallocatedSize != prevPreallocatedSize && logChunksToCommit) {
            if (prevPreallocatedSize == 0) {
                logChunksToCommit->push_back(CommonLogger->ChunkIdx);
            }
            size_t endIdx = nextPreallocatedSize - 1;
            for (size_t i = prevPreallocatedSize; i < endIdx; ++i) {
                logChunksToCommit->push_back(CommonLogger->NextChunks[i].Idx);
            }
        }
    }

    CommonLogger->TerminateLog(reqId, traceId);

    ui32 curChunkIdx = CommonLogger->ChunkIdx;
    ui32 curSectorIdx = CommonLogger->SectorIdx;

    TLogFlushCompletionAction* flushCompletion = new TLogFlushCompletionAction(curChunkIdx, curSectorIdx, CommonLogger, action);

    CommonLogger->Flush(reqId, traceId, flushCompletion);

    OnNonceChange(NonceLog, reqId, traceId);
}


////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Chunk commit log writing
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

NKikimrProto::EReplyStatus TPDisk::BeforeLoggingCommitRecord(const TLogWrite &logWrite, TStringStream& outErrorReason) {
    TGuard<TMutex> guard(StateMutex);
    for (ui32 i = 0; i < logWrite.CommitRecord.CommitChunks.size(); ++i) {
        if (!ValidateCommitChunk(logWrite.CommitRecord.CommitChunks[i], logWrite.Owner, outErrorReason)) {
            return NKikimrProto::ERROR;
        }
    }
    for (ui32 i = 0; i < logWrite.CommitRecord.DeleteChunks.size(); ++i) {
        if (!ValidateDeleteChunk(logWrite.CommitRecord.DeleteChunks[i], logWrite.Owner, outErrorReason)) {
            return NKikimrProto::ERROR;
        }
    }

    for (ui32 chunkIdx : logWrite.CommitRecord.CommitChunks) {
        if (ChunkState[chunkIdx].CommitState == TChunkState::DATA_RESERVED) {
            Mon.UncommitedDataChunks->Dec();
            Mon.CommitedDataChunks->Inc();
            P_LOG(PRI_INFO, BPD01, "Commit Data Chunk",
                (CommitedDataChunks, Mon.CommitedDataChunks->Val()),
                (ChunkIdx, chunkIdx),
                (OwnerId, (ui32)ChunkState[chunkIdx].OwnerId));
        }
        ++ChunkState[chunkIdx].CommitsInProgress;
    }
    bool isDirtyMarked = false;
    if (TPDisk::IS_SHRED_ENABLED) {
        bool isLogged = false;
        for (ui32 chunkIdx : logWrite.CommitRecord.DirtyChunks) {
            if (chunkIdx >= ChunkState.size()) {
                if (!isLogged) {
                    isLogged = true;
                    LOG_CRIT_S(*PCtx->ActorSystem, NKikimrServices::BS_PDISK_SHRED,
                        PCtx->PDiskLogPrefix << "Commit DirtyChunk contains invalid chunkIdx# " << chunkIdx
                        << " ShredGeneration# " << ShredGeneration);
                }
            } else {
                if (!ChunkState[chunkIdx].IsDirty) {
                    ChunkState[chunkIdx].IsDirty = true;
                    isDirtyMarked = true;
                    LOG_DEBUG_S(*PCtx->ActorSystem, NKikimrServices::BS_PDISK_SHRED,
                        PCtx->PDiskLogPrefix << "marked chunkIdx# " << chunkIdx << " as dirty"
                        << " chunk.ShredGeneration# " << ChunkState[chunkIdx].ShredGeneration
                        << " ShredGeneration# " << ShredGeneration);
                }
            }
        }
    }
    if (logWrite.CommitRecord.DeleteToDecommitted) {
        for (ui32 chunkIdx : logWrite.CommitRecord.DeleteChunks) {
            TChunkState& state = ChunkState[chunkIdx];
            if (TPDisk::IS_SHRED_ENABLED && !state.IsDirty) {
                state.IsDirty = true;
                isDirtyMarked = true;
            }
            switch (state.CommitState) {
            case TChunkState::DATA_RESERVED:
                state.CommitState = TChunkState::DATA_RESERVED_DECOMMIT_IN_PROGRESS;
                break;
            case TChunkState::DATA_COMMITTED:
                Mon.CommitedDataChunks->Dec();
                Mon.UncommitedDataChunks->Inc();
                state.CommitState = TChunkState::DATA_COMMITTED_DECOMMIT_IN_PROGRESS;
                break;
            default:
                Y_FAIL_S(PCtx->PDiskLogPrefix << "can't delete to decomitted chunkIdx# " << chunkIdx
                    << " request ownerId# " << logWrite.Owner
                    << " as it is in unexpected CommitState# " << state.ToString());
                break;
            }
        }
    } else {
        for (ui32 chunkIdx : logWrite.CommitRecord.DeleteChunks) {
            TChunkState& state = ChunkState[chunkIdx];
            if (TPDisk::IS_SHRED_ENABLED && !state.IsDirty) {
                state.IsDirty = true;
                isDirtyMarked = true;
            }
            if (state.HasAnyOperationsInProgress()) {
                switch (state.CommitState) {
                case TChunkState::DATA_RESERVED:
                    Mon.UncommitedDataChunks->Dec();
                    state.CommitState = TChunkState::DATA_RESERVED_DELETE_ON_QUARANTINE;
                    break;
                case TChunkState::DATA_COMMITTED:
                    Mon.CommitedDataChunks->Dec();
                    P_LOG(PRI_DEBUG, BPD10, "Remove commited data chunks",
                        (CommitedDataChunks, Mon.CommitedDataChunks->Val()),
                        (ChunkIdx, chunkIdx));
                    state.CommitState = TChunkState::DATA_COMMITTED_DELETE_ON_QUARANTINE;
                    break;
                default:
                    Y_FAIL_S(PCtx->PDiskLogPrefix << "can't delete chunkIdx# " << chunkIdx
                        << " request ownerId# " << logWrite.Owner
                        << " with operations in progress as it is in unexpected CommitState# " << state.ToString());
                    break;
                }
                QuarantineChunks.push_back(chunkIdx);
                P_LOG(PRI_INFO, BPD78, "Push chunk on QuarantineChunks because it has operations in flight",
                        (ChunkIdx, chunkIdx),
                        (OwnerId, logWrite.Owner),
                        (State, state.ToString()));
            } else if (state.CommitState == TChunkState::DATA_RESERVED) {
                Mon.UncommitedDataChunks->Dec();
                state.CommitState = TChunkState::DATA_RESERVED_DELETE_IN_PROGRESS;
            } else if (state.CommitState == TChunkState::DATA_COMMITTED) {
                Mon.CommitedDataChunks->Dec();
                state.CommitState = TChunkState::DATA_COMMITTED_DELETE_IN_PROGRESS;
            } else {
                Y_FAIL_S(PCtx->PDiskLogPrefix << "can't delete chunkIdx# " << chunkIdx
                    << " request ownerId# " << logWrite.Owner
                    << " as it is in unexpected CommitState# " << state.ToString());
            }
        }
    }
    if (isDirtyMarked) {
        WriteSysLogRestorePoint(nullptr, TReqId(TReqId::MarkDirtySysLog, 0), {});
    }

    return NKikimrProto::OK;
}

bool TPDisk::ValidateCommitChunk(ui32 chunkIdx, TOwner owner, TStringStream& outErrorReason) {
    TGuard<TMutex> guard(StateMutex);
    if (chunkIdx >= ChunkState.size()) {
        outErrorReason << PCtx->PDiskLogPrefix
            << "Can't commit chunkIdx# " << chunkIdx
            << " > total# " << ChunkState.size()
            << " ownerId# " << owner
            << " Marker# BPD74";
        P_LOG(PRI_ERROR, BPD01, outErrorReason.Str());
        return false;
    }
    if (ChunkState[chunkIdx].OwnerId != owner) {
        outErrorReason << PCtx->PDiskLogPrefix
            << "Can't commit chunkIdx# " << chunkIdx
            << ", ownerId# " << owner
            << " != real ownerId# " << ChunkState[chunkIdx].OwnerId
            << " Marker# BPD75";
        P_LOG(PRI_ERROR, BPD01, outErrorReason.Str());
        return false;
    }
    if (ChunkState[chunkIdx].CommitState != TChunkState::DATA_RESERVED
            && ChunkState[chunkIdx].CommitState != TChunkState::DATA_COMMITTED) {
        outErrorReason << PCtx->PDiskLogPrefix
            << "Can't commit chunkIdx# " << chunkIdx
            << " in CommitState# " << ChunkState[chunkIdx].CommitState
            << " ownerId# " << owner << " Marker# BPD83";
        P_LOG(PRI_ERROR, BPD01, outErrorReason.Str());
        return false;
    }
    return true;
}

// Called when commit record is successfully saved to the disk.
void TPDisk::CommitChunk(ui32 chunkIdx) {
    TGuard<TMutex> guard(StateMutex);
    TChunkState &state = ChunkState[chunkIdx];
    Y_VERIFY_S(state.CommitsInProgress > 0, PCtx->PDiskLogPrefix);
    --state.CommitsInProgress;

    switch (state.CommitState) {
    case TChunkState::DATA_RESERVED:
        [[fallthrough]];
    case TChunkState::DATA_COMMITTED:
        state.CommitState = TChunkState::DATA_COMMITTED;
        break;
    case TChunkState::DATA_RESERVED_DECOMMIT_IN_PROGRESS:
        [[fallthrough]];
    case TChunkState::DATA_COMMITTED_DECOMMIT_IN_PROGRESS:
        [[fallthrough]];
    case TChunkState::DATA_DECOMMITTED:
        [[fallthrough]];
    case TChunkState::DATA_ON_QUARANTINE:
        [[fallthrough]];
    case TChunkState::DATA_COMMITTED_DELETE_ON_QUARANTINE:
        [[fallthrough]];
    case TChunkState::DATA_RESERVED_DELETE_ON_QUARANTINE:
        // Do nothing
        break;
    default:
        Y_FAIL_S(PCtx->PDiskLogPrefix << "can't commit chunkIdx# " << chunkIdx
                << " as it is in unexpected CommitState# " << state.ToString());
        break;
    }
}

bool TPDisk::ValidateDeleteChunk(ui32 chunkIdx, TOwner owner, TStringStream& outErrorReason) {
    TGuard<TMutex> guard(StateMutex);
    if (chunkIdx >= ChunkState.size()) {
        outErrorReason << PCtx->PDiskLogPrefix
            << "Can't delete chunkIdx# " << (ui32)chunkIdx
            << " > total# " << (ui32)ChunkState.size()
            << " ownerId# " << (ui32)owner << "!"
            << " Marker# BPD76";
        P_LOG(PRI_ERROR, BPD01, outErrorReason.Str());
        return false;
    }
    if (ChunkState[chunkIdx].OwnerId != owner) {
        outErrorReason << PCtx->PDiskLogPrefix
            << "Can't delete chunkIdx# " << (ui32)chunkIdx
            << " ownerId# " << (ui32)owner
            << " != trueOwnerId# " << (ui32)ChunkState[chunkIdx].OwnerId << "!"
            << " Marker# BPD77";
        P_LOG(PRI_ERROR, BPD01, outErrorReason.Str());
        return false;
    }
    if (ChunkState[chunkIdx].CommitState != TChunkState::DATA_RESERVED
            && ChunkState[chunkIdx].CommitState != TChunkState::DATA_COMMITTED) {
        outErrorReason << PCtx->PDiskLogPrefix
            << "Can't delete chunkIdx# " << (ui32)chunkIdx
            << " in CommitState# " << ChunkState[chunkIdx].CommitState
            << " ownerId# " << (ui32)owner << " Marker# BPD82";
        P_LOG(PRI_ERROR, BPD01, outErrorReason.Str());
        return false;
    }
    P_LOG(PRI_INFO, BPD01, "Deletion of chunk is validated",
        (ChunkIdx, chunkIdx),
        (OwnerId, (ui32)owner));
    return true;
}

// Marks chunk deleted but does not move it to the free list.
// Called when commit record is successfully saved to the disk.
void TPDisk::DeleteChunk(ui32 chunkIdx, TOwner owner) {
    TGuard<TMutex> guard(StateMutex);
    TChunkState &state = ChunkState[chunkIdx];
    switch (state.CommitState) {
    // Chunk will be freed in TPDisk::ForceDeleteChunk() and may be released already
    case TChunkState::DATA_ON_QUARANTINE:
        break;
    case TChunkState::DATA_RESERVED_DELETE_IN_PROGRESS:
        [[fallthrough]];
    case TChunkState::DATA_COMMITTED_DELETE_IN_PROGRESS:
        Y_VERIFY_S(state.CommitsInProgress == 0, PCtx->PDiskLogPrefix
                << " chunkIdx# " << chunkIdx << " state# " << state.ToString());
        P_LOG(PRI_INFO, BPD01, "Chunk is deleted",
                (ChunkIdx, chunkIdx),
                (OldOwner, (ui32)state.OwnerId),
                (NewOwner, (ui32)OwnerUnallocated));
        Y_VERIFY_S(state.OwnerId == owner, PCtx->PDiskLogPrefix); // TODO DELETE
        state.OwnerId = OwnerUnallocated;
        state.CommitState = TChunkState::FREE;
        Keeper.PushFreeOwnerChunk(owner, chunkIdx);
        break;
    case TChunkState::DATA_COMMITTED_DELETE_ON_QUARANTINE:
        // Mark chunk as quarantine, so it will be released through default quarantine way
        Y_VERIFY_S(state.OwnerId == owner, PCtx->PDiskLogPrefix); // TODO DELETE
        state.CommitState = TChunkState::DATA_ON_QUARANTINE;
        break;
    case TChunkState::DATA_RESERVED_DELETE_ON_QUARANTINE:
        // Mark chunk as quarantine, so it will be released through default quarantine way
        Y_VERIFY_S(state.OwnerId == owner, PCtx->PDiskLogPrefix); // TODO DELETE
        state.CommitState = TChunkState::DATA_ON_QUARANTINE;
        break;
    case TChunkState::DATA_COMMITTED_DECOMMIT_IN_PROGRESS:
        [[fallthrough]];
    case TChunkState::DATA_RESERVED_DECOMMIT_IN_PROGRESS:
        state.CommitState = TChunkState::DATA_DECOMMITTED;
        break;

    default:
        Y_FAIL_S(PCtx->PDiskLogPrefix << "can't delete chunkIdx# " << chunkIdx
                << " requesting ownerId# " << owner
                << " as it is in unexpected CommitState# " << state.ToString());
    }
}

void TPDisk::OnLogCommitDone(TLogCommitDone &req) {
    TGuard<TMutex> guard(StateMutex);

    for (ui32 chunk : req.CommitedChunks) {
        CommitChunk(chunk);
    }
    for (ui32 chunk : req.DeletedChunks) {
        DeleteChunk(chunk, req.OwnerId);
    }

    // Decrement log chunk user counters and release unused log chunks
    TOwnerData &ownerData = OwnerData[req.OwnerId];
    ui64 currentFirstLsnToKeep = ownerData.CurrentFirstLsnToKeep;

    auto it = LogChunks.begin();
    bool isChunkReleased = false;
    if (req.Lsn <= ownerData.LastWrittenCommitLsn) {
        P_LOG(PRI_NOTICE, BPD01, "Got EvLog with lsn <= LastWrittenCommitLsn",
            (OwnerId, req.OwnerId),
            (VDiskId, ownerData.VDiskId.ToStringWOGeneration()),
            (Lsn, req.Lsn),
            (LastWrittenCommitLsn, ownerData.LastWrittenCommitLsn));
    }
    ownerData.LastWrittenCommitLsn = req.Lsn;
    while (it != LogChunks.end() && it->OwnerLsnRange.size() > req.OwnerId) {
        TLogChunkInfo::TLsnRange &range = it->OwnerLsnRange[req.OwnerId];
        if (range.IsPresent && range.LastLsn < currentFirstLsnToKeep) {
            //Y_VERIFY_S(range.FirstLsn != range.LastLsn, PCtx->PDiskLogPrefix);
            P_LOG(PRI_INFO, BPD27, "Log chunk is dereferenced by owner",
                (ChunkIdx, it->ChunkIdx),
                (LsnRange, TString(TStringBuilder() << "[" << range.FirstLsn << ", " << range.LastLsn << "]")),
                (ownerId, (ui32)req.OwnerId),
                (CurrentLsnToKeep, currentFirstLsnToKeep),
                (CausedbyLsn, req.Lsn),
                (PreviousCurrentUserCount, it->CurrentUserCount));
            range.IsPresent = false;
            Y_VERIFY_S(it->CurrentUserCount > 0, PCtx->PDiskLogPrefix);
            it->CurrentUserCount--;
            if (it->CurrentUserCount == 0) {
                isChunkReleased = true;
            }
        }
        ++it;
    }
    bool isContinueShredScheduled = false;
    if (isChunkReleased) {
        if (ShredIsWaitingForCutLog) {
            isContinueShredScheduled = true;
        }
        THolder<TCompletionEventSender> completion(isContinueShredScheduled ?
            new TCompletionEventSender(this, PCtx->PDiskActor, new NPDisk::TEvContinueShred()) :
            new TCompletionEventSender(this));
        if (ReleaseUnusedLogChunks(completion.Get())) {
            if (isContinueShredScheduled) {
                ShredIsWaitingForCutLog = 0;
                ContinueShredsInFlight++;
            }
            WriteSysLogRestorePoint(completion.Release(), req.ReqId, {}); // FIXME: wilson
        } else {
            isContinueShredScheduled = false;
        }
    }

    if (!isContinueShredScheduled) {
        if (ShredIsWaitingForCutLog) {
            ShredIsWaitingForCutLog = 0;
            if (ContinueShredsInFlight == 0) {
                ContinueShredsInFlight++;
                PCtx->ActorSystem->Send(new IEventHandle(
                    PCtx->PDiskActor, PCtx->PDiskActor, new TEvContinueShred(), 0, 0));
            }
        }
    }
    TryTrimChunk(false, 0);
}

void TPDisk::MarkChunksAsReleased(TReleaseChunks& req) {
    TGuard<TMutex> guard(StateMutex);

    for (const auto& chunkIdx : req.ChunksToRelease) {
        BlockDevice->EraseCacheRange(
            Format.Offset(chunkIdx, 0),
            Format.Offset(chunkIdx + 1, 0));
    }

    if (req.IsChunksFromLogSplice) {
        auto *releaseReq = ReqCreator.CreateFromArgs<TReleaseChunks>(std::move(req.ChunksToRelease));

        auto flushAction = MakeHolder<TCompletionEventSender>(this, THolder<TReleaseChunks>(releaseReq));

        ui64 nonce = req.GapStart->LastNonce;
        ui32 desiredSectorIdx = UsableSectorsPerLogChunk();
        ui32 dataChunkSizeSectors = Format.ChunkSize / Format.SectorSize;
        TLogWriter writer(Mon, *BlockDevice.Get(), Format, nonce, Format.LogKey, BufferPool.Get(), desiredSectorIdx,
                dataChunkSizeSectors, Format.MagicLogChunk, req.GapStart->ChunkIdx, nullptr, desiredSectorIdx,
                nullptr, PCtx, &DriveModel, Cfg->EnableSectorEncryption);

        Y_VERIFY_S(req.GapEnd->DesiredPrevChunkLastNonce, PCtx->PDiskLogPrefix
            << "Zero GapEnd->DesiredPrevChunkLastNonce, chunkInfo# " << *req.GapEnd);
        // +1 stands for -1 in logreader in old versions of pdisk
        ui64 expectedNonce = req.GapEnd->DesiredPrevChunkLastNonce + 1;

        req.GapEnd->IsEndOfSplice = true;
        writer.WriteNextChunkReference(req.GapEnd->ChunkIdx, expectedNonce, flushAction.Release(), {}, {});
        P_LOG(PRI_INFO, BPD81, "write nextChunkReference",
            (GapStartChunkIdx, req.GapStart->ChunkIdx), (GapEnd, *req.GapEnd));
    } else {
        for (const auto& chunkIdx : req.ChunksToRelease) {
            Keeper.PushFreeOwnerChunk(OwnerSystem, chunkIdx);
        }
        IsLogChunksReleaseInflight = false;

        TryTrimChunk(false, 0);
    }
}

// Schedules EvReadLogResult event for the system log
void TPDisk::InitiateReadSysLog(const TActorId &pDiskActor) {
    Y_VERIFY_S(PDiskThread.Running(), PCtx->PDiskLogPrefix << "expect PDiskThread to be running");
    Y_VERIFY_S(InitPhase == EInitPhase::Uninitialized, PCtx->PDiskLogPrefix
            << "expect InitPhase to be Uninitialized, but InitPhase# " << InitPhase.load());
    ui32 formatSectorsSize = FormatSectorSize * ReplicationFactor;
    THolder<TEvReadFormatResult> evReadFormatResult(new TEvReadFormatResult(formatSectorsSize, UseHugePages));
    ui8 *formatSectors = evReadFormatResult->FormatSectors.Get();
    BlockDevice->PreadAsync(formatSectors, formatSectorsSize, 0,
        new TCompletionEventSender(this, pDiskActor, evReadFormatResult.Release()), TReqId(TReqId::InitialFormatRead, 0), {});
    *Mon.PDiskState = NKikimrBlobStorage::TPDiskState::InitialFormatRead;
    *Mon.PDiskDetailedState = TPDiskMon::TPDisk::BootingFormatRead;
    InitPhase = EInitPhase::ReadingSysLog;
}

void TPDisk::ProcessReadLogResult(const NPDisk::TEvReadLogResult &evReadLogResult, const TActorId &pDiskActor) {
    TStringStream errStr;
    if (evReadLogResult.Status != NKikimrProto::OK) {
        P_LOG(PRI_ERROR, BPD01, "Error on log read",
            (evReadLogResult, evReadLogResult.ToString()),
            (InitPhase, InitPhase.load()));
        switch (InitPhase) {
            case EInitPhase::ReadingSysLog:
                *Mon.PDiskState = NKikimrBlobStorage::TPDiskState::InitialSysLogReadError;
                *Mon.PDiskBriefState = TPDiskMon::TPDisk::Error;
                *Mon.PDiskDetailedState = TPDiskMon::TPDisk::ErrorInitialSysLogRead;
                errStr << "Error in initial sys log read" << Endl;
                break;
            case EInitPhase::ReadingLog:
                *Mon.PDiskState = NKikimrBlobStorage::TPDiskState::InitialCommonLogReadError;
                *Mon.PDiskBriefState = TPDiskMon::TPDisk::Error;
                *Mon.PDiskDetailedState = TPDiskMon::TPDisk::ErrorInitialCommonLogRead;
                errStr << "Error in initial common log read" << Endl;
                break;
            default:
                break;

        }
        errStr << "evReadLogResult# " << evReadLogResult.ToString() << " Marker# BPD88";
        PCtx->ActorSystem->Send(pDiskActor, new TEvLogInitResult(false, errStr.Str()));
        return;
    }

    switch (InitPhase) {
        case EInitPhase::ReadingSysLog:
        {
            TString errorReason;
            bool success = ProcessChunk0(evReadLogResult, errorReason);

            if (InitialSysLogWritePosition == 0 || !success) {
                ErrorStr = errorReason;
                *Mon.PDiskState = NKikimrBlobStorage::TPDiskState::InitialSysLogParseError;
                *Mon.PDiskBriefState = TPDiskMon::TPDisk::Error;
                *Mon.PDiskDetailedState = TPDiskMon::TPDisk::ErrorInitialSysLogParse;
                PCtx->ActorSystem->Send(pDiskActor, new TEvLogInitResult(false,
                    errorReason));
                return;
            }
            // Parse the main log to obtain busy/free chunk lists
            *Mon.PDiskState = NKikimrBlobStorage::TPDiskState::InitialCommonLogRead;
            *Mon.PDiskDetailedState = TPDiskMon::TPDisk::BootingCommonLogRead;
            ReadAndParseMainLog(pDiskActor);
            InitPhase = EInitPhase::ReadingLog;
            return;
        }
        case EInitPhase::ReadingLog:
        {
            InitialLogPosition = evReadLogResult.NextPosition;
            if (InitialLogPosition == TLogPosition{0, 0}) {
                *Mon.PDiskState = NKikimrBlobStorage::TPDiskState::InitialCommonLogParseError;
                *Mon.PDiskBriefState = TPDiskMon::TPDisk::Error;
                *Mon.PDiskDetailedState = TPDiskMon::TPDisk::ErrorInitialCommonLogParse;
                PCtx->ActorSystem->Send(pDiskActor, new TEvLogInitResult(false,
                            "Error while parsing common log at booting state"));
                return;
            }

            LastInitialChunkIdx = evReadLogResult.LastGoodChunkIdx;
            LastInitialSectorIdx = evReadLogResult.LastGoodSectorIdx;

            // Initialize metadata.
            InitFormattedMetadata();
            // Prepare the FreeChunks list
            InitFreeChunks();
            // Actualize LogChunks counters according to OwnerData
            InitLogChunksInfo();

            {
                TGuard<TMutex> guard(StateMutex);

                // Check that there are no chunks for owners with no starting points
                TSet<TOwner> chunkOwners;
                for (size_t chunkIdx = 0; chunkIdx < ChunkState.size(); ++chunkIdx) {
                    TChunkState &state = ChunkState[chunkIdx];
                    if (IsOwnerUser(state.OwnerId)) {
                        chunkOwners.insert(state.OwnerId);
                    }
                }
                for (auto it = chunkOwners.begin(); it != chunkOwners.end(); ++it) {
                    TOwnerData &data = OwnerData[*it];
                    Y_VERIFY_S(data.VDiskId != TVDiskID::InvalidId, PCtx->PDiskLogPrefix);
                    if (data.StartingPoints.empty()) {
                        TStringStream str;
                        str << PCtx->PDiskLogPrefix
                            << "ownerId# " << (ui32)*it
                            << " Owns chunks, but has no starting points! ownedChunks# [";
                        for (size_t chunkIdx = 0; chunkIdx < ChunkState.size(); ++chunkIdx) {
                            TChunkState &state = ChunkState[chunkIdx];
                            if (state.OwnerId == *it) {
                                str << chunkIdx << ", ";
                            }
                        }
                        str << "]" << Endl;
                        Y_FAIL_S(str.Str());
                    }
                }

                // Set up UsedChunkCount for each owner
                TVector<ui32> usedForOwner;
                usedForOwner.resize(OwnerEndUser);
                for (ui32 ownerId = OwnerBeginUser; ownerId < OwnerEndUser; ++ownerId) {
                    usedForOwner[ownerId] = 0;
                }
                for (size_t chunkIdx = 0; chunkIdx < ChunkState.size(); ++chunkIdx) {
                    TChunkState &state = ChunkState[chunkIdx];
                    if (IsOwnerUser(state.OwnerId)) {
                        usedForOwner[state.OwnerId]++;
                    }
                }

                // Reset chunk trackers
                TKeeperParams params;
                params.TotalChunks = Format.DiskSizeChunks();
                params.ExpectedOwnerCount = Cfg->ExpectedSlotCount;
                params.SysLogSize = Format.SystemChunkCount; // sysLogSize = chunk 0 + additional SysLog chunks
                if (Format.IsDiskSmall() && Cfg->FeatureFlags.GetEnableSmallDiskOptimization()) {
                    params.SeparateCommonLog = false;
                } else {
                    params.SeparateCommonLog = true;
                }
                params.CommonLogSize = LogChunks.size();
                params.MaxCommonLogChunks = Cfg->MaxCommonLogChunks;
                params.SpaceColorBorder = Cfg->SpaceColorBorder;
                params.ChunkBaseLimit = Cfg->ChunkBaseLimit;
                for (ui32 ownerId = OwnerBeginUser; ownerId < OwnerEndUser; ++ownerId) {
                    if (OwnerData[ownerId].VDiskId != TVDiskID::InvalidId) {
                        params.OwnersInfo[ownerId] = {
                            .ChunksOwned = usedForOwner[ownerId],
                            .VDiskId = OwnerData[ownerId].VDiskId,
                            .Weight = Cfg->GetOwnerWeight(OwnerData[ownerId].GroupSizeInUnits),
                        };
                        if (OwnerData[ownerId].IsStaticGroupOwner()) {
                            params.HasStaticGroups = true;
                        }
                    }
                }

                TString errorReason;
                if (
                    !Keeper.Reset(params, TColorLimits::MakeLogLimits(), errorReason) &&
                    !Keeper.Reset(params, TColorLimits::MakeExtendedLogLimits(), errorReason)
                ) {
                    *Mon.PDiskState = NKikimrBlobStorage::TPDiskState::ChunkQuotaError;
                    *Mon.PDiskBriefState = TPDiskMon::TPDisk::Error;
                    *Mon.PDiskDetailedState = TPDiskMon::TPDisk::ErrorCalculatingChunkQuotas;
                    PCtx->ActorSystem->Send(pDiskActor, new TEvLogInitResult(false, errorReason));
                    return;
                }
            }

            // Increase Nonces to prevent collisions
            NPrivate::TMersenne64 randGen(Seed());
            do {
                for (ui32 i = 0; i < NonceCount; ++i) {
                    SysLogRecord.Nonces.Value[i] += ForceLogNonceDiff.Value[i] + 1 + randGen.GenRand() % ForceLogNonceDiff.Value[i];
                }
            } while (SysLogRecord.Nonces.Value[NonceLog] <= InitialPreviousNonce);
            InitSysLogger();

            InitPhase = EInitPhase::Initialized;

            if (!Cfg->ReadOnly) {
                // We don't need logger in ReadOnly mode.
                if (!InitCommonLogger()) {
                    // TODO: report red zone
                    *Mon.PDiskState = NKikimrBlobStorage::TPDiskState::CommonLoggerInitError;
                    *Mon.PDiskBriefState = TPDiskMon::TPDisk::Error;
                    *Mon.PDiskDetailedState = TPDiskMon::TPDisk::ErrorCommonLoggerInit;
                    PCtx->ActorSystem->Send(pDiskActor, new TEvLogInitResult(false, "Error in common logger init"));
                    return;
                }
            }

            // Now it's ok to write both logs and data.
            *Mon.PDiskState = NKikimrBlobStorage::TPDiskState::Normal;
            *Mon.PDiskBriefState = TPDiskMon::TPDisk::OK;
            *Mon.PDiskDetailedState = TPDiskMon::TPDisk::EverythingIsOk;

            if (Cfg->ReadOnly) {
                PCtx->ActorSystem->Send(pDiskActor, new TEvLogInitResult(true, "OK"));
            } else {
                auto completion = MakeHolder<TCompletionEventSender>(this, pDiskActor, new TEvLogInitResult(true, "OK"));
                ReleaseUnusedLogChunks(completion.Get());
                WriteSysLogRestorePoint(completion.Release(), TReqId(TReqId::AfterInitCommonLoggerSysLog, 0), {});
            }

            // Start reading metadata.
            ReadFormattedMetadataIfNeeded();

            // Output the fully initialized state for each owner and each chunk.
            P_LOG(PRI_NOTICE, BPD01, "PDisk have successfully started");
            P_LOG(PRI_INFO, BPD01, "", (StartupOwnerInfo, StartupOwnerInfo()));

            return;
        }
        default:
            Y_FAIL_S(PCtx->PDiskLogPrefix << "Unexpected InitPhase# " << InitPhase.load());
    }
}

}
