#include "blobstorage_pdisk_impl.h"

#include "blobstorage_pdisk_chunk_id_formatter.h"
#include "blobstorage_pdisk_completion_impl.h"
#include "blobstorage_pdisk_mon.h"
#include "blobstorage_pdisk_request_id.h"

#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/control/lib/dynamic_control_board_impl.h>
#include <ydb/core/control/lib/immediate_control_board_impl.h>
#include <ydb/core/protos/blobstorage.pb.h>
#include <ydb/core/blobstorage/crypto/secured_block.h>
#include <ydb/library/schlab/schine/job_kind.h>

#include <util/system/unaligned_mem.h>

constexpr size_t MAX_REQS_PER_CYCLE = 200; // 200 requests take ~0.2ms in EnqueueAll function

namespace NKikimr {
namespace NPDisk {

LWTRACE_USING(BLOBSTORAGE_PROVIDER);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Initialization
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

TPDisk::TPDisk(std::shared_ptr<TPDiskCtx> pCtx, const TIntrusivePtr<TPDiskConfig> cfg, const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters)
    : PCtx(std::move(pCtx))
    , Mon(counters, cfg->PDiskId, cfg.Get())
    , DriveModel(cfg->DriveModelSeekTimeNs,
            cfg->DriveModelSpeedBps,
            cfg->DriveModelBulkWrieBlockSize,
            cfg->DriveModelTrimSpeedBps,
            cfg->DriveModelSpeedBpsMin,
            cfg->DriveModelSpeedBpsMax,
            cfg->DeviceInFlight)
    , ReqCreator(PCtx, &Mon, &DriveModel, &EstimatedLogChunkIdx, cfg->SeparateHugePriorities)
    , ReorderingMs(cfg->ReorderingMs)
    , LogSeekCostLoop(2)
    , ExpectedDiskGuid(cfg->PDiskGuid)
    , PDiskCategory(cfg->PDiskCategory)
    , NextOwnerRound(cfg->StartOwnerRound)
    , OwnerData(OwnerCount)
    , Keeper(Mon, cfg)
    , CostLimitNs(cfg->CostLimitNs)
    , PDiskThread(*this)
    , BlockDevice(CreateRealBlockDevice(cfg->GetDevicePath(), Mon,
                    HPCyclesMs(ReorderingMs), DriveModel.SeekTimeNs(), cfg->DeviceInFlight,
                    TDeviceMode::LockFile | (cfg->UseSpdkNvmeDriver ? TDeviceMode::UseSpdk : 0),
                    cfg->MaxQueuedCompletionActions, cfg->CompletionThreadsCount, cfg->SectorMap,
                    cfg->BufferPoolBufferSizeBytes, this, cfg->ReadOnly, cfg->UseBytesFlightControl))
    , Cfg(cfg)
    , CreationTime(TInstant::Now())
    , ExpectedSlotCount(cfg->ExpectedSlotCount)
    , UseHugePages(cfg->UseSpdkNvmeDriver)
{
    SlowdownAddLatencyNs = TControlWrapper(0, 0, 100'000'000'000ll);
    EnableForsetiBinLog = TControlWrapper(0, 0, 1);
    ForsetiMinLogCostNsControl = TControlWrapper(ForsetiMinLogCostNs, 0, 100'000'000ull);
    ForsetiMilliBatchSize = TControlWrapper(PDiskCategory.IsSolidState() ? (128 * 1024) : (1 * 1024 * 1024),
            0, 64 * 1024 * 1024);
    ForsetiMaxLogBatchNs = TControlWrapper((PDiskCategory.IsSolidState() ? 50'000ll : 500'000ll), 0, 100'000'000ll);
    ForsetiMaxLogBatchNsCached = ForsetiMaxLogBatchNs;
    ForsetiOpPieceSizeSsd = TControlWrapper(64 * 1024, 1, Cfg->BufferPoolBufferSizeBytes);
    ForsetiOpPieceSizeRot = TControlWrapper(512 * 1024, 1, Cfg->BufferPoolBufferSizeBytes);
    ForsetiOpPieceSizeCached = PDiskCategory.IsSolidState() ?  ForsetiOpPieceSizeSsd : ForsetiOpPieceSizeRot;
    UseNoopSchedulerSSD = TControlWrapper(Cfg->UseNoopScheduler, 0, 1);
    UseNoopSchedulerHDD = TControlWrapper(Cfg->UseNoopScheduler, 0, 1);
    ChunkBaseLimitPerMille = TControlWrapper(0, 0, 130);  // 0 means ChunkBaseLimit isn't configured via ICB

    // Override PDiskConfig.SpaceColorBorder:
    // 0 - overriding disabled, respect PDiskConfig value
    // 1 - LightYellowMove
    // 2 - YellowStop
    SemiStrictSpaceIsolation = TControlWrapper(0, 0, 2);
    SemiStrictSpaceIsolationCached = 0;

    if (Cfg->SectorMap) {
        auto diskModeParams = Cfg->SectorMap->GetDiskModeParams();
        if (diskModeParams) {
            SectorMapFirstSectorReadRate = TControlWrapper(diskModeParams->FirstSectorReadRate, 0, 100000ull * 1024 * 1024);
            SectorMapLastSectorReadRate = TControlWrapper(diskModeParams->LastSectorReadRate, 0, 100000ull * 1024 * 1024);
            SectorMapFirstSectorWriteRate = TControlWrapper(diskModeParams->FirstSectorWriteRate, 0, 100000ull * 1024 * 1024);
            SectorMapLastSectorWriteRate = TControlWrapper(diskModeParams->LastSectorWriteRate, 0, 100000ull * 1024 * 1024);
            SectorMapSeekSleepMicroSeconds = TControlWrapper(diskModeParams->SeekSleepMicroSeconds, 0, 100ul * 1000 * 1000);
        }
        auto failureProbs = Cfg->SectorMap->GetFailureProbabilities();
        if (failureProbs) {
            SectorMapWriteErrorProbability = TControlWrapper(0, 0, 1000000000);
            SectorMapReadErrorProbability = TControlWrapper(0, 0, 1000000000);
            SectorMapSilentWriteFailProbability = TControlWrapper(0, 0, 1000000000);
            SectorMapReadReplayProbability = TControlWrapper(0, 0, 1000000000);
        }
    }

    AddCbs(OwnerSystem, GateLog, "Log", 2'000'000ull);
    ConfigureCbs(OwnerSystem, GateLog, 16);
    AddCbs(OwnerUnallocated, GateTrim, "Trim", 0ull);
    ConfigureCbs(OwnerUnallocated, GateTrim, 16);

    Format.Clear(Cfg->EnableFormatAndMetadataEncryption);
    *Mon.PDiskState = NKikimrBlobStorage::TPDiskState::Initial;
    *Mon.PDiskBriefState = TPDiskMon::TPDisk::Booting;
    ErrorStr = "PDisk is initializing now";

    for (ui32 i = 0; i < NonceCount; ++i) {
        SysLogRecord.Nonces.Value[i] = 1;
        LoggedNonces.Value[i] = 1;
        ForceLogNonceDiff.Value[i] = 1 << 20;
    }

    FastOperationsQueue.reserve(16 << 10);

    JointLogReads.reserve(16 << 10);
    JointChunkForgets.reserve(16 << 10);

    DebugInfoGenerator = [id = cfg->PDiskId, type = PDiskCategory]() {
        return TStringBuilder() << "PDisk DebugInfo# { Id# " << id << " Type# " << type.TypeStrLong() << " }";
    };
}

TString TPDisk::DynamicStateToString(bool isMultiline) {
    TStringStream str;
    const char *x = isMultiline ? "\n" : "";
    str << "{PDiskDynamicState" << x;
    str << " ChunkBeingTrimmed# " << AtomicGet(ChunkBeingTrimmed) << x;
    str << " TrimOffset# " << AtomicGet(TrimOffset) << x;
    str << " SysLogLsn# " << SysLogLsn << x;
    str << " NextOwnerRound# " << NextOwnerRound << x;
    str << " CostLimitNs# " << CostLimitNs << x;
    str << "}";
    return str.Str();
}

TCheckDiskFormatResult TPDisk::ReadChunk0Format(ui8* formatSectors, const NPDisk::TMainKey& mainKey) {
    TGuard<TMutex> guard(StateMutex);

    Format.SectorSize = FormatSectorSize;
    ui32 mainKeySize = mainKey.Keys.size();

    const bool allowObsoleteKey =
#ifdef DISABLE_PDISK_ENCRYPTION
        false;
#else
        true;
#endif

    for (ui32 k = 0; k < mainKeySize; ++k) {
        TPDiskStreamCypher cypher(Cfg->EnableFormatAndMetadataEncryption);
        cypher.SetKey(mainKey.Keys[k]);

        ui32 lastGoodIdx = (ui32)-1;
        bool isBad[ReplicationFactor];
        bool isBadPresent = false;
        for (ui32 i = 0; i < ReplicationFactor; ++i) {
            ui64 sectorOffset = i * FormatSectorSize;
            ui8* formatSector = formatSectors + sectorOffset;
            TDataSectorFooter *footer = (TDataSectorFooter*)
                (formatSector + FormatSectorSize - sizeof(TDataSectorFooter));

            cypher.StartMessage(footer->Nonce);
            alignas(16) TDiskFormat diskFormat;
            diskFormat.SetEncryptFormat(Cfg->EnableFormatAndMetadataEncryption);
            cypher.Encrypt(&diskFormat, formatSector, sizeof(TDiskFormat));

            isBad[i] = !diskFormat.IsHashOk(FormatSectorSize);
            if (!isBad[i]) {
                Format.UpgradeFrom(diskFormat);
                if (Format.IsErasureEncodeUserChunks()) {
                    P_LOG(PRI_ERROR, BPD80, "Read from disk Format has FormatFlagErasureEncodeUserChunks set,"
                            " but current version of PDisk can't work with it",
                            (Format, Format.ToString()));
                    Y_FAIL_S(PCtx->PDiskLogPrefix
                            << "Unable to run PDisk on disk with FormatFlagErasureEncodeUserChunks set");
                }
                if (Format.IsErasureEncodeUserLog()) {
                    P_LOG(PRI_ERROR, BPD801, "Read from disk Format has FormatFlagErasureEncodeUserLog set,"
                            " but current version of PDisk can't work with it",
                            (Format, Format.ToString()));
                    Y_FAIL_S(PCtx->PDiskLogPrefix
                            << "Unable to run PDisk on disk with FormatFlagErasureEncodeUserLog set");
                }
                lastGoodIdx = i;
                *Mon.TotalSpaceBytes = Format.DiskSize;
            } else {
                isBadPresent = true;
            }
        }
        if (lastGoodIdx < ReplicationFactor) {
            ui64 sectorOffset = lastGoodIdx * FormatSectorSize;
            ui8* formatSector = formatSectors + sectorOffset;
            if (k < mainKeySize - 1) { // obsolete key is used
                if (allowObsoleteKey) {
                    return TCheckDiskFormatResult(true, true);
                }
                continue;
            } else if (isBadPresent) {
                for (ui32 i = 0; i < ReplicationFactor; ++i) {
                    if (isBad[i]) {
                        TBuffer* buffer = BufferPool->Pop();
                        Y_VERIFY_S(FormatSectorSize <= buffer->Size(), PCtx->PDiskLogPrefix);
                        memcpy(buffer->Data(), formatSector, FormatSectorSize);
                        ui64 targetOffset = i * FormatSectorSize;
                        P_LOG(PRI_INFO, BPD46, "PWriteAsync for format restoration",
                                (fromOffset, targetOffset), (toOffset, targetOffset + FormatSectorSize));

                        REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(buffer->Data(), FormatSectorSize);
                        BlockDevice->PwriteAsync(buffer->Data(), FormatSectorSize, targetOffset, buffer,
                                TReqId(TReqId::RestoreFormatOnRead, 0), {});
                    }
                }
                //BlockDevice->FlushAsync(nullptr);
            }
            return TCheckDiskFormatResult(true, false);
        }
    }
    return TCheckDiskFormatResult(false, false);
}

bool TPDisk::IsFormatMagicValid(ui8 *magicData8, ui32 magicDataSize, const TMainKey& mainKey) {
    Y_VERIFY_S(magicDataSize % sizeof(ui64) == 0, PCtx->PDiskLogPrefix
            << "Magic data size# "<< magicDataSize << " must be a multiple of sizeof(ui64)");
    Y_VERIFY_S(magicDataSize >= FormatSectorSize, PCtx->PDiskLogPrefix
            << "Magic data size# "<< magicDataSize << " must greater or equals to FormatSectorSize# " << FormatSectorSize);
    ui64 magicOr = 0ull;
    ui64 isIncompleteFormatMagicPresent = true;
    ui64 *magicData64 = reinterpret_cast<ui64 *>(magicData8);
    for (ui32 i = 0; i < magicDataSize / sizeof(ui64); ++i) {
        magicOr |= magicData64[i];
        if (i < MagicIncompleteFormatSize / sizeof(ui64) && MagicIncompleteFormat != magicData64[i]) {
            isIncompleteFormatMagicPresent = false;
        }
    }
    if (magicOr == 0ull || isIncompleteFormatMagicPresent) {
        return true;
    }
    auto format = CheckMetadataFormatSector(
        magicData8,
        magicDataSize,
        mainKey,
        PCtx->PDiskLogPrefix,
        Cfg->EnableFormatAndMetadataEncryption);
    return format.has_value();
}

bool TPDisk::CheckGuid(TString *outReason) {
    const bool ok = Format.Guid == ExpectedDiskGuid || (Cfg->MetadataOnly && !ExpectedDiskGuid);
    if (!ok && outReason) {
        *outReason = TStringBuilder() << "expected# " << ExpectedDiskGuid << " on-disk# " << Format.Guid;
    }

    return ok;
}

bool TPDisk::CheckFormatComplete() {
    return !Format.IsFormatInProgress();
}

void TPDisk::InitFreeChunks() {
    TGuard<TMutex> guard(StateMutex);
    for (ui32 i = 0; i < ChunkState.size(); ++i) {
        if (ChunkState[i].OwnerId == OwnerUnallocated) {
            Keeper.InitialPushFree(i);
        } else if (ChunkState[i].OwnerId == OwnerUnallocatedTrimmed) {
            Keeper.InitialPushTrimmed(i);
        }
    }
    if (Cfg->FeatureFlags.GetTrimEntireDeviceOnStartup()) {
        TrimAllUntrimmedChunks();
    }
}

TString TPDisk::StartupOwnerInfo() {
    TStringStream str;
    str << "{";
    TGuard<TMutex> guard(StateMutex);
    TMap<TOwner, TDeque<ui32>> ownedChunks;
    for (ui32 chunkId = 0; chunkId < ChunkState.size(); ++chunkId) {
        ownedChunks[ChunkState[chunkId].OwnerId].push_back(chunkId);
    }
    for (ui32 owner = 0; owner < OwnerData.size(); ++owner) {
        TOwnerData &data = OwnerData[owner];
        if (data.VDiskId != TVDiskID::InvalidId) {
            str << "{OwnerId: " << (ui32)owner;
            str << " VDiskId: " << data.VDiskId.ToStringWOGeneration();
            str << " GroupSizeInUnits: " << data.GroupSizeInUnits;
            str << " ChunkWrites: " << data.InFlight->ChunkWrites.load();
            str << " ChunkReads: " << data.InFlight->ChunkReads.load();
            str << " LogWrites: " << data.InFlight->LogWrites.load();
            str << " LogReader: " << (bool)data.LogReader;
            str << " CurrentFirstLsnToKeep: " << data.CurrentFirstLsnToKeep;
            str << " FirstNonceToKeep: " << SysLogFirstNoncesToKeep.FirstNonceToKeep[owner];
            str << " StartingPoints: {";
            for (auto it = data.StartingPoints.begin(); it != data.StartingPoints.end(); ++it) {
                str << it->second.ToString();
            }
            str << "}";
            str << " Owned chunkIds: ";
            TChunkIdFormatter(str).PrintBracedChunksList(ownedChunks[owner]);
            str << "}";
        }
    }
    str << " PDisk system/log ChunkIds: ";
    TChunkIdFormatter(str).PrintBracedChunksList(ownedChunks[OwnerSystem]);
    str << " Free ChunkIds: ";
    TChunkIdFormatter(str).PrintBracedChunksList(ownedChunks[OwnerUnallocated]);
    return str.Str();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Destruction
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

TPDisk::~TPDisk() {
    TPDisk::Stop();
    Y_VERIFY_S(InputQueue.GetWaitingSize() == 0, PCtx->PDiskLogPrefix);
}

void TPDisk::Stop() {
    TGuard<TMutex> guard(StopMutex);
    if (!AtomicGet(IsStarted)) {
        // In case of destruction of a stopped pdisk there may be some requests in the queue scheduled by the actor
        while (InputQueue.GetWaitingSize() > 0) {
            TRequestBase::AbortDelete(InputQueue.Pop(), PCtx->ActorSystem);
        }
        return;
    }
    AtomicSet(IsStarted, false);
    PDiskThread.Stop();
    PDiskThread.Join();

    P_LOG(PRI_NOTICE, BPD01, "Shutdown", (OwnerInfo, StartupOwnerInfo()));

    BlockDevice->Stop();

    // BlockDevice is stopped, the data will NOT hit the disk.
    if (CommonLogger.Get()) {
        ObliterateCommonLogSectorSet();
    }
    // Drain Forseti
    while (!ForsetiScheduler.IsEmpty()) {
        ForsetiTimeNs += 1000000000ull;
        TIntrusivePtr<NSchLab::TJob> job = ForsetiScheduler.SelectJob(ForsetiTimeNs);
        if (job) {
            TRequestBase* req = static_cast<TRequestBase*>(job->Payload);
            ForsetiTimeNs += job->Cost + 1;
            ForsetiScheduler.CompleteJob(ForsetiTimeNs, job);
            TRequestBase::AbortDelete(req, PCtx->ActorSystem);
        }
    }

    for (TRequestBase* req : JointLogReads) {
        delete req;
    }
    JointLogReads.clear();

    for (; JointChunkReads.size(); JointChunkReads.pop()) {
        auto& req = JointChunkReads.front();
        Y_VERIFY_DEBUG_S(req->GetType() == ERequestType::RequestChunkReadPiece,
                PCtx->PDiskLogPrefix << "Unexpected request type# " << TypeName(*req));
        TRequestBase::AbortDelete(req.Get(), PCtx->ActorSystem);
    }

    for (; JointChunkWrites.size(); JointChunkWrites.pop()) {
        auto* req = JointChunkWrites.front();
        Y_VERIFY_DEBUG_S(req->GetType() == ERequestType::RequestChunkWritePiece,
                PCtx->PDiskLogPrefix << "Unexpected request type# " << TypeName(req));
        TRequestBase::AbortDelete(req, PCtx->ActorSystem);
    }

    for (; JointLogWrites.size(); JointLogWrites.pop()) {
        auto* req = JointLogWrites.front();
        TRequestBase::AbortDelete(req, PCtx->ActorSystem);
    }

    JointChunkForgets.clear();
    for (auto& req : FastOperationsQueue) {
        TRequestBase::AbortDelete(req.release(), PCtx->ActorSystem);
    }
    FastOperationsQueue.clear();
    for (TRequestBase* req : PausedQueue) {
        TRequestBase::AbortDelete(req, PCtx->ActorSystem);
    }
    PausedQueue.clear();

    while (InputQueue.GetWaitingSize() > 0) {
        TRequestBase::AbortDelete(InputQueue.Pop(), PCtx->ActorSystem);
    }

    DropAllMetadataRequests();

    if (InitialTailBuffer) {
        InitialTailBuffer->Exec(PCtx->ActorSystem);
        InitialTailBuffer = nullptr;
    }
}

void TPDisk::ObliterateCommonLogSectorSet() {
    CommonLogger->Obliterate();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Generic format-related calculations
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
ui32 TPDisk::SystemChunkSize(const TDiskFormat& format, ui32 userAccessibleChunkSizeBytes, ui32 sectorSizeBytes) const {
    ui32 usableSectorBytes = format.SectorPayloadSize();
    ui32 userSectors = (userAccessibleChunkSizeBytes + usableSectorBytes - 1) / usableSectorBytes;
    ui32 minChunkSize = userSectors * sectorSizeBytes;
    const ui32 chunkSizeAlignment = (2 << 20);
    ui32 alignedChunkSize = ((minChunkSize + chunkSizeAlignment - 1) / chunkSizeAlignment) * chunkSizeAlignment;
    return alignedChunkSize;
}

void ParsePayloadFromSectorOffset(const TDiskFormat& format, ui64 firstSector, ui64 lastSector, ui64 currentSector,
        ui64 *outPayloadBytes, ui64 *outPayloadOffset, const TString& logPrefix) {
    Y_VERIFY_S(firstSector <= currentSector && currentSector <= lastSector, logPrefix
            << firstSector << " <= " << currentSector << " <= " << lastSector);

    *outPayloadBytes = (lastSector + 1 - currentSector) * format.SectorPayloadSize();
    *outPayloadOffset = (currentSector - firstSector) * format.SectorPayloadSize();
}

bool ParseSectorOffset(const TDiskFormat& format, TActorSystem *actorSystem, ui32 pDiskId, ui64 offset, ui64 size,
        ui64 &outSectorIdx, ui64 &outLastSectorIdx, ui64 &outSectorOffset, const TString& logPrefix) {
    const ui64 chunkSizeUsableSectors = format.ChunkSize / format.SectorSize;
    const ui64 sectorPayloadSize = format.SectorPayloadSize();
    Y_VERIFY_S(sectorPayloadSize > 0, logPrefix);
    ui64 lastSectorIdx = (offset + size + sectorPayloadSize - 1) / sectorPayloadSize - 1;
    outLastSectorIdx = lastSectorIdx;

    ui64 sectorIdx = offset / sectorPayloadSize;
    outSectorIdx = sectorIdx;

    if (outSectorIdx >= chunkSizeUsableSectors || outLastSectorIdx >= chunkSizeUsableSectors) {
        TString reason;
        if (outSectorIdx >= chunkSizeUsableSectors) {
            reason =  "outSectorIdx >= chunkSizeUsableSectors";
        } else {
            reason = "outLastSectorIdx >= chunkSizeUsableSectors";
        }
        STLOGX(*actorSystem, PRI_ERROR, BS_PDISK, BPD01, reason, (PDiskId, pDiskId), (OutSectorIdx, outSectorIdx),
            (OutLastSectorIdx, outLastSectorIdx), (ChunkSizeUsableSectors, chunkSizeUsableSectors),
            (Offset, offset), (Size, size));
        return false;
    }
    outSectorOffset = offset - sectorIdx * sectorPayloadSize;
    return true;
}

ui64 TPDisk::UsableSectorsPerLogChunk() const {
    const ui64 sectorsPerLogChunk = Format.ChunkSize / Format.SectorSize;
    const ui64 nextChunkReferenceSectors = Format.IsErasureEncodeNextChunkReference() ? ReplicationFactor : 1;
    const ui64 maxBaseSectors = sectorsPerLogChunk - nextChunkReferenceSectors;
    return maxBaseSectors;
}

void TPDisk::CheckLogCanary(ui8* sectorData, ui32 chunkIdx, ui64 sectorIdx) const {
    if (CanarySize) {
        const ui64 readCanary = ReadUnaligned<ui64>(
                sectorData + Format.SectorSize - CanarySize - sizeof(TDataSectorFooter));
        if (readCanary != Canary) {
            TStringStream ss;
            ss << PCtx->PDiskLogPrefix << "Failed log canary at chunkIdx# " << chunkIdx
                << " sectorIdx# " << sectorIdx << " sectorOffset# " << Format.Offset(chunkIdx, sectorIdx)
                << " read canary# " << readCanary << " expected canary# " << Canary;
            P_LOG(PRI_ERROR, BPD01, ss.Str());
            Y_FAIL_S(ss.Str());
        }
    }
}

TLogPosition TPDisk::LogPosition(TChunkIdx chunkIdx, ui64 sectorIdx, ui64 offsetInSector) const {
    ui64 offsetBytes = sectorIdx * Format.SectorSize + offsetInSector;
    Y_VERIFY_S(offsetBytes <= Max<ui32>(), PCtx->PDiskLogPrefix);
    return {chunkIdx, static_cast<ui32>(offsetBytes)};
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Common operations
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// The goal of this function is to find the first row of unused log chunks and release them.
// There are two possible cases:
// 1. Row starts from the first log chunk, trivial case, just set first non-empty chunk as new head
// 2. Row is located between used log chunks
bool TPDisk::ReleaseUnusedLogChunks(TCompletionEventSender *completion) {
    TGuard<TMutex> guard(StateMutex);
    if (IsLogChunksReleaseInflight) {
        return false;
    }

    // Both gapStart end gapEnd point to non-empty log chunks around empty chunk region
    TMaybe<TLogChunkInfo> gapStart;
    TMaybe<TLogChunkInfo> gapEnd;
    auto it = LogChunks.begin();

    // Gap search requires whole LogChunks list traversal
    if (LogChunks.size() > 3) {
        while (it != LogChunks.end() && ChunkState[it->ChunkIdx].CommitState == TChunkState::LOG_COMMITTED
                && it->CurrentUserCount != 0) {
            gapStart = *it;
            ++it;
        }
    }

    PrintLogChunksInfo("before log cut");
    TVector<TChunkIdx> chunksToRelease;
    while (it != LogChunks.end() && it != std::prev(LogChunks.end())
            && ChunkState[it->ChunkIdx].CommitState == TChunkState::LOG_COMMITTED && it->CurrentUserCount == 0) {
        // Clear all info about a chunk, but do not add the chunk to a free list to prevent reuse
        // of the chunk before NextChunkReference is written to device
        const ui32 chunkIdx = it->ChunkIdx;
        chunksToRelease.push_back(chunkIdx);
        TChunkState &state = ChunkState[chunkIdx];
        P_LOG(PRI_INFO, BPD55, "chunk is released as unused",
                (ChunkIdx, chunkIdx),
                (OwnerId, ui32(state.OwnerId)),
                (NewOwnerId, ui32(OwnerUnallocated)));
        Y_VERIFY_S(state.OwnerId == OwnerSystem, PCtx->PDiskLogPrefix
                << "Unexpected ownerId# " << ui32(state.OwnerId));
        state.CommitState = TChunkState::FREE;
        state.OwnerId = OwnerUnallocated;
        Mon.LogChunks->Dec();

        auto curr = it;
        ++it;
        LogChunks.erase(curr);
    }
    if (it != LogChunks.end()) {
        if (gapStart) {
            if (!chunksToRelease.empty()) {
                it->IsEndOfSplice = true;
            }
        }
        gapEnd = *it;
    }

    // Nothing to release
    if (chunksToRelease.empty()) {
        return false;
    // Case 1: Chunks to be deleted located at the start of LogChunks list
    } else if (!gapStart && gapEnd) {
        IsLogChunksReleaseInflight = true;
        completion->Req = THolder<TRequestBase>(ReqCreator.CreateFromArgs<TReleaseChunks>(std::move(chunksToRelease)));
        SysLogRecord.LogHeadChunkIdx = gapEnd->ChunkIdx;
        SysLogRecord.LogHeadChunkPreviousNonce = ChunkState[gapEnd->ChunkIdx].PreviousNonce;
        PrintLogChunksInfo("cut tail log");
        return true;
    // Case 2: Chunks to be deleted located in the middle of LogChunksList
    } else if (gapStart && gapEnd) {
        IsLogChunksReleaseInflight = true;
        Mon.SplicedLogChunks->Add(chunksToRelease.size());
        completion->Req = THolder<TRequestBase>(ReqCreator.CreateFromArgs<TReleaseChunks>(*gapStart, *gapEnd, std::move(chunksToRelease)));
        PrintLogChunksInfo("log splice");
        return true;
    } else {
        TStringStream ss;
        ss << PCtx->PDiskLogPrefix;
        ss << "Impossible situation - we have non empty chunksToRelease vector and cannot release them";
        ss << " gapStart# ";
        if (gapStart) {
            ss << gapStart->ChunkIdx;
        } else {
            ss << "null";
        }
        ss << " gapEnd# ";
        if (gapEnd) {
            ss << gapEnd->ChunkIdx;
        } else {
            ss << "null";
        }
        Y_FAIL_S(ss.Str());
        return false;
    }
}

void TPDisk::OnNonceChange(ui32 idx, TReqId reqId, NWilson::TTraceId *traceId) {
    if (SysLogRecord.Nonces.Value[idx] - LoggedNonces.Value[idx] > ForceLogNonceDiff.Value[idx]) {
        WriteSysLogRestorePoint(nullptr, reqId, traceId);
    }
}

ui32 TPDisk::GetTotalChunks(ui32 ownerId, const EOwnerGroupType ownerGroupType) const {
    Y_UNUSED(ownerGroupType);
    // TODO(cthulhu): use ownerGroupType for logs
    return Keeper.GetOwnerHardLimit(ownerId);
}

ui32 TPDisk::GetFreeChunks(ui32 ownerId, const EOwnerGroupType ownerGroupType) const {
    Y_UNUSED(ownerGroupType);
    // TODO(cthulhu): use ownerGroupType for logs
    return Max<i64>(0, Keeper.GetOwnerFree(ownerId, false));
}

ui32 TPDisk::GetUsedChunks(ui32 ownerId, const EOwnerGroupType ownerGroupType) const {
    Y_UNUSED(ownerGroupType);
    // TODO(cthulhu): use ownerGroupType for logs
    ui32 ownedChunks = 0;
    {
        TGuard<TMutex> guard(StateMutex);
        size_t size = ChunkState.size();
        for (size_t idx = 0; idx < size; ++idx) {
            if (ChunkState[idx].OwnerId == ownerId) {
                ownedChunks++;
            }
        }
    }
    return ownedChunks;
}

ui32 TPDisk::GetNumActiveSlots() const {
    return Keeper.GetNumActiveSlots();
}

NPDisk::TStatusFlags TPDisk::GetStatusFlags(TOwner ownerId, const EOwnerGroupType ownerGroupType, double *occupancy) const {
    double occupancy_;
    NPDisk::TStatusFlags res;

    if (IsOwnerUser(ownerId)) {
        res = Keeper.GetSpaceStatusFlags(ownerId, &occupancy_);
    } else {
        TOwner keeperOwner = (ownerGroupType == EOwnerGroupType::Dynamic ? OwnerSystem : OwnerCommonStaticLog);
        res = Keeper.GetSpaceStatusFlags(keeperOwner, &occupancy_);
    }

    if (occupancy) {
        *occupancy = occupancy_;
    }

    return res;
}

NPDisk::TStatusFlags TPDisk::NotEnoughDiskSpaceStatusFlags(ui32 ownerId, const EOwnerGroupType ownerGroupType) const {
    return (NPDisk::TStatusFlags)(
            GetStatusFlags(ownerId, ownerGroupType) | ui32(NKikimrBlobStorage::StatusNotEnoughDiskSpaceForOperation));
}


////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Generic log writing
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Caused only by external TEvCutLog event for testing purposes
void TPDisk::SendCutLog(TAskForCutLog &request) {
    TGuard<TMutex> guard(StateMutex);
    if (request.Owner < OwnerData.size()) {
        const TOwnerData &data = OwnerData[request.Owner];
        if (data.VDiskId != TVDiskID::InvalidId) {
            AskVDisksToCutLogs(request.Owner, true);
        }
    }
}

ui32 TPDisk::AskVDisksToCutLogs(TOwner ownerFilter, bool doForce) {
    ui32 requestsSent = 0;
    TGuard<TMutex> guard(StateMutex);
    size_t logChunkCount = LogChunks.size();

    ui32 activeOwners = 0;
    for (ui32 owner = 0; owner < OwnerData.size(); ++owner) {
        const TOwnerData &data = OwnerData[owner];
        if (data.VDiskId != TVDiskID::InvalidId) {
            ++activeOwners;
        }
    }

    size_t cutThreshold = (size_t)Max(Cfg->MinLogChunksTotal,
        (ui64)(ui64(activeOwners) * Cfg->MaxLogChunksPerOwnerMultiplier + Cfg->MaxLogChunksPerOwnerDivisor - 1ull) /
            Cfg->MaxLogChunksPerOwnerDivisor);
    if (logChunkCount > cutThreshold * Cfg->WarningLogChunksMultiplier) {
        *Mon.TooMuchLogChunks = 1;
    } else {
        *Mon.TooMuchLogChunks = 0;
    }
    if (logChunkCount > cutThreshold || doForce) {
        struct TChunkCutInfoPerOwner {
            size_t ChunksToCut = 0;
            size_t FirstLogChunkNumber = 0;
            ui64 Lsn = 0;
        };

        TMap<TOwner, TChunkCutInfoPerOwner> cutLogInfoForOwner;

        {
            size_t logChunkNumber = 0;
            // LogChunks grows at the end() and cut from the begin()
            for (auto chunkIt = LogChunks.begin(); chunkIt != LogChunks.end(); ++chunkIt) {
                ++logChunkNumber;
                TVector<TLogChunkInfo::TLsnRange> &ownerLsnRange = chunkIt->OwnerLsnRange;
                for (ui32 chunkOwner = 0; chunkOwner < ownerLsnRange.size(); ++chunkOwner) {
                    if (ownerLsnRange[chunkOwner].IsPresent) {
                        auto &cutLogInfo = cutLogInfoForOwner[chunkOwner];

                        if (logChunkNumber <= logChunkCount - cutThreshold) {
                            cutLogInfo.Lsn = ownerLsnRange[chunkOwner].LastLsn;
                            cutLogInfo.ChunksToCut++;
                        } else if (ownerFilter != OwnerSystem || doForce) {
                            // Prevent cuts with lsn = 0.
                            if (cutLogInfo.Lsn == 0) {
                                cutLogInfo.Lsn = ownerLsnRange[chunkOwner].LastLsn;
                                cutLogInfo.ChunksToCut++;
                            }
                        }

                        if (cutLogInfo.FirstLogChunkNumber == 0) {
                            cutLogInfo.FirstLogChunkNumber = logChunkNumber;
                        }
                    }
                }
            }
        }
        TInstant now = TInstant::Now();
        if (ownerFilter == OwnerSystem) {
            for (auto it = cutLogInfoForOwner.begin(); it != cutLogInfoForOwner.end(); ++it) {
                TOwner chunkOwner = it->first;
                auto &cutLogInfo = it->second;

                if (cutLogInfo.Lsn == 0) {
                    // Prevent cuts with lsn = 0.
                    continue;
                }

                ui64 lsn = cutLogInfo.Lsn + 1;

                if (chunkOwner < OwnerEndUser) {
                    auto &data = OwnerData[chunkOwner];

                    if (data.CutLogId) {
                        auto ownedLogChunks = cutLogInfo.FirstLogChunkNumber ? logChunkCount - cutLogInfo.FirstLogChunkNumber : 0;

                        TOwnerRound chunkOwnerRound = data.OwnerRound;
                        THolder<NPDisk::TEvCutLog> cutLog(new NPDisk::TEvCutLog(chunkOwner, chunkOwnerRound, lsn,
                                    logChunkCount,
                                    cutLogInfo.FirstLogChunkNumber ? logChunkCount - cutLogInfo.FirstLogChunkNumber : 0,
                                    (InsaneLogChunks + cutThreshold) / 2, InsaneLogChunks));
                        P_LOG(PRI_DEBUG, BPD67, "send TEvCutLog",
                                (To, data.CutLogId.ToString()),
                                (OwnerId, (chunkOwner)),
                                (Event, cutLog->ToString()));
                        Y_VERIFY_S(cutLog->FreeUpToLsn, PCtx->PDiskLogPrefix << "Error! Should not ask to cut log at 0 lsn."
                                << " Send CutLog to# " << data.CutLogId.ToString().data()
                                << " ownerId#" << ui32(chunkOwner)
                                << " cutLog# " << cutLog->ToString());
                        PCtx->ActorSystem->Send(new IEventHandle(data.CutLogId, PCtx->PDiskActor, cutLog.Release(),
                                    IEventHandle::FlagTrackDelivery, 0));
                        data.AskedFreeUpToLsn = lsn;
                        data.AskedToCutLogAt = now;
                        data.AskedLogChunkToCut = cutLogInfo.ChunksToCut;
                        data.LogChunkCountBeforeCut = ownedLogChunks;
                        ++requestsSent;
                        // ADD_RECORD_WITH_TIMESTAMP_TO_OPERATION_LOG(data.OperationLog, "System owner asked to cut log, OwnerId# " << chunkOwner);
                    } else {
                        P_LOG(PRI_INFO, BPD14, "Can't send CutLog",
                            (OwnerId, ui32(chunkOwner)),
                            (VDiskId, data.VDiskId.ToStringWOGeneration()));
                    }
                }
            }
        } else if (ownerFilter < OwnerEndUser) {
            auto it = cutLogInfoForOwner.find(ownerFilter);
            if (it != cutLogInfoForOwner.end()) {
                auto &cutLogInfo = it->second;

                if (cutLogInfo.Lsn == 0) {
                    // Prevent cuts with lsn = 0.
                    return requestsSent;
                }

                ui64 lsn = cutLogInfo.Lsn + 1;

                auto &data = OwnerData[ownerFilter];

                TOwnerRound chunkOwnerRound = data.OwnerRound;
                if (data.CutLogId) {
                    auto ownedLogChunks = cutLogInfo.FirstLogChunkNumber ? logChunkCount - cutLogInfo.FirstLogChunkNumber : 0;

                    THolder<NPDisk::TEvCutLog> cutLog(new NPDisk::TEvCutLog(ownerFilter, chunkOwnerRound, lsn,
                                logChunkCount,
                                ownedLogChunks,
                                (InsaneLogChunks + cutThreshold) / 2, InsaneLogChunks));
                    P_LOG(PRI_DEBUG, BPD68, "Send CutLog",
                            (To, data.CutLogId.ToString()),
                            (OwnerId, ui32(ownerFilter)),
                            (Event, cutLog->ToString()));
                    if (!cutLog->FreeUpToLsn) {
                        TStringStream str;
                        str << "{";
                        size_t logChunkIdx = 0;
                        for (auto chunkIt = LogChunks.begin(); chunkIt != LogChunks.end(); ++chunkIt) {
                            ++logChunkIdx;
                            str << "logChunkIdx# " << logChunkIdx;
                            str << " " << chunkIt->ToString() << " ";
                        }
                        str << "}";
                        Y_VERIFY_S(cutLog->FreeUpToLsn, PCtx->PDiskLogPrefix << "Error! Should not ask to cut log at 0 lsn."
                                << " Send CutLog to# " << data.CutLogId.ToString().data()
                                << " ownerId#" << ui32(ownerFilter)
                                << " cutLog# " << cutLog->ToString()
                                << " LogChunks# " << str.Str());
                    }
                    PCtx->ActorSystem->Send(new IEventHandle(data.CutLogId, PCtx->PDiskActor, cutLog.Release(),
                                IEventHandle::FlagTrackDelivery, 0));
                    data.AskedFreeUpToLsn = lsn;
                    data.AskedToCutLogAt = now;
                    data.AskedLogChunkToCut = cutLogInfo.ChunksToCut;
                    data.LogChunkCountBeforeCut = ownedLogChunks;
                    // ADD_RECORD_WITH_TIMESTAMP_TO_OPERATION_LOG(data.OperationLog, "User owner asked to cut log, OwnerId# " << ownerFilter);
                } else {
                    P_LOG(PRI_INFO, BPD14, "Can't send CutLog",
                        (OwnerId, ui32(ownerFilter)),
                        (VDiskId, data.VDiskId.ToStringWOGeneration()));
                }
            }
        }
    }
    return requestsSent;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Chunk writing
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
void TPDisk::ChunkWritePiecePlain(TChunkWrite *evChunkWrite) {
    ui32 chunkIdx = evChunkWrite->ChunkIdx;
    Y_VERIFY_S(chunkIdx != 0, PCtx->PDiskLogPrefix);

    auto& comp = evChunkWrite->Completion;
    const ui32 count = evChunkWrite->PartsPtr->Size();
    const void* buff = nullptr;
    ui32 chunkOffset = evChunkWrite->Offset;
    ui64 size = evChunkWrite->TotalSize;
    ui64 newSize = 0;
    ui64 diskOffset = (ui64)chunkIdx * Format.ChunkSize + AlignDown<ui32>(chunkOffset, Format.SectorSize);

    P_LOG(PRI_INFO, BPD01, "Chunk write, buffer info",
        (bufferOffsetAligment, (intptr_t)(*evChunkWrite->PartsPtr)[0].first % 512),
        (count, count), (chunkOffset, chunkOffset), (size, size)
    );
    if (count == 1 // currently only one disk operation is allowed
            // linux require buffer to be aligned to 512 boundary
            && (intptr_t)(*evChunkWrite->PartsPtr)[0].first % 512 == 0
            // if chunkOffset is not aligned with sector size, then headroom is required to add
            && chunkOffset % Format.SectorSize == 0
            // if size is not aligned with sector size, then tailroom is required to add
            && size % Format.SectorSize == 0) {
        newSize = size;
    } else {
        newSize = comp->CompactBuffer(chunkOffset % Format.SectorSize, Format.SectorSize);

        auto log = [&] () {
            TStringStream str;
            str << "[";
            for (size_t i = 0; i < evChunkWrite->PartsPtr->Size(); ++i) {
                const auto& [ptr, size] = (*evChunkWrite->PartsPtr)[i];
                str << size << ",";
            }
            str << "]";
            return str.Str();
        };
        P_LOG(PRI_DEBUG, BPD01, "Chunk write, compact buffer",
            (count, count), (chunkOffset, chunkOffset),
            (bufAlign, (intptr_t)(*evChunkWrite->PartsPtr)[0].first % 512),
            (size, size), (newSize, newSize), (Sizes, log()));
        *Mon.WriteBufferCompactedBytes += size;
    }
    buff = comp->GetBuffer();
    P_LOG(PRI_DEBUG, BPD01, "Chunk write", (ChunkIdx, chunkIdx), (Encrypted, false), (format_ChunkSize, Format.ChunkSize),
        (aligndown, AlignDown<ui32>(chunkOffset, Format.SectorSize)),
        (chunkOffset, chunkOffset), (Size, size), (NewSize, newSize),
        (diskOffset, diskOffset), (count, count));

    auto traceId = evChunkWrite->Span.GetTraceId();
    evChunkWrite->Completion->Orbit = std::move(evChunkWrite->Orbit);
    BlockDevice->PwriteAsync(buff, newSize, diskOffset, comp.Release(), evChunkWrite->ReqId, &traceId);
    evChunkWrite->IsReplied = true;
}

bool TPDisk::ChunkWritePieceEncrypted(TChunkWrite *evChunkWrite, TChunkWriter& writer, ui32 bytesAvailable) {
    const ui32 count = evChunkWrite->PartsPtr->Size();
    for (ui32 partIdx = evChunkWrite->CurrentPart; partIdx < count; ++partIdx) {
        ui32 remainingPartSize = (*evChunkWrite->PartsPtr)[partIdx].second - evChunkWrite->CurrentPartOffset;
        auto traceId = evChunkWrite->Span.GetTraceId();
        if (bytesAvailable < remainingPartSize) {
            ui32 sizeToWrite = bytesAvailable;
            if (sizeToWrite > 0) {
                ui8 *data = (ui8*)(*evChunkWrite->PartsPtr)[partIdx].first;
                if (data) {
                    ui8 *source = data + evChunkWrite->CurrentPartOffset;
                    NSan::CheckMemIsInitialized(source, sizeToWrite);
                    writer.WriteData(source, sizeToWrite, evChunkWrite->ReqId, &traceId);
                    *Mon.BandwidthPChunkPayload += sizeToWrite;
                } else {
                    writer.WritePadding(sizeToWrite, evChunkWrite->ReqId, &traceId);
                    *Mon.BandwidthPChunkPadding += sizeToWrite;
                }
                evChunkWrite->RemainingSize -= sizeToWrite;
                evChunkWrite->BytesWritten += sizeToWrite;
            }
            evChunkWrite->CurrentPartOffset += sizeToWrite;
            evChunkWrite->CurrentPart = partIdx;
            return false;
        } else {
            Y_VERIFY_S(remainingPartSize, PCtx->PDiskLogPrefix);
            ui32 sizeToWrite = remainingPartSize;
            bytesAvailable -= remainingPartSize;
            ui8 *data = (ui8*)(*evChunkWrite->PartsPtr)[partIdx].first;
            if (data) {
                ui8 *source = data + evChunkWrite->CurrentPartOffset;
                writer.WriteData(source, sizeToWrite, evChunkWrite->ReqId, &traceId);
                *Mon.BandwidthPChunkPayload += sizeToWrite;
            } else {
                writer.WritePadding(sizeToWrite, evChunkWrite->ReqId, &traceId);
                *Mon.BandwidthPChunkPadding += sizeToWrite;
            }
            evChunkWrite->CurrentPartOffset = 0;
            evChunkWrite->RemainingSize -= sizeToWrite;
            evChunkWrite->BytesWritten += sizeToWrite;
        }
    }
    Y_VERIFY_S(evChunkWrite->RemainingSize == 0, PCtx->PDiskLogPrefix);

    ui32 chunkIdx = evChunkWrite->ChunkIdx;
    P_LOG(PRI_DEBUG, BPD79, "ChunkWrite",
            (ChunkIdx, chunkIdx),
            (OwnerId, (ui32)evChunkWrite->Owner),
            (SectorIdx, writer.SectorIdx));

    if (!writer.IsEmptySector()) {
        auto traceId = evChunkWrite->Span.GetTraceId();
        *Mon.BandwidthPChunkPadding += writer.SectorBytesFree;
        writer.WriteZeroes(writer.SectorBytesFree, evChunkWrite->ReqId, &traceId);
        P_LOG(PRI_INFO, BPD01, "Chunk was zero-padded after writing", (ChunkIdx, chunkIdx));
    }

    auto traceId = evChunkWrite->Span.GetTraceId();
    evChunkWrite->Completion->Orbit = std::move(evChunkWrite->Orbit);
    writer.Flush(evChunkWrite->ReqId, &traceId, evChunkWrite->Completion.Release());

    evChunkWrite->IsReplied = true;
    return true;
}

bool TPDisk::ChunkWritePiece(TChunkWrite *evChunkWrite, ui32 pieceShift, ui32 pieceSize) {
    if (evChunkWrite->IsReplied) {
        return true;
    }
    Y_VERIFY_S(evChunkWrite->BytesWritten == pieceShift, PCtx->PDiskLogPrefix);

    TGuard<TMutex> guard(StateMutex);
    Y_VERIFY_S(pieceShift % Format.SectorPayloadSize() == 0, PCtx->PDiskLogPrefix);
    Y_VERIFY_S(pieceSize % Format.SectorPayloadSize() == 0 || pieceShift + pieceSize == evChunkWrite->TotalSize,
        PCtx->PDiskLogPrefix << "pieceShift# " << pieceShift << " pieceSize# " << pieceSize
        << " evChunkWrite->TotalSize# " << evChunkWrite->TotalSize);

    ui64 desiredSectorIdx = 0;
    ui64 sectorOffset = 0;
    ui64 lastSectorIdx;
    if (!ParseSectorOffset(Format, PCtx->ActorSystem, PCtx->PDiskId, evChunkWrite->Offset + evChunkWrite->BytesWritten,
            evChunkWrite->TotalSize - evChunkWrite->BytesWritten, desiredSectorIdx, lastSectorIdx, sectorOffset,
            PCtx->PDiskLogPrefix)) {
        guard.Release();
        ui32 chunkIdx = evChunkWrite->ChunkIdx;
        Y_VERIFY_S(chunkIdx != 0, PCtx->PDiskLogPrefix);

        TString err = Sprintf("PDiskId# %" PRIu32 " Can't write chunk: incorrect offset/size offset# %" PRIu32
                " size# %" PRIu32 " chunkIdx# %" PRIu32 " ownerId# %" PRIu32, PCtx->PDiskId, (ui32)evChunkWrite->Offset,
                (ui32)evChunkWrite->TotalSize, (ui32)chunkIdx, (ui32)evChunkWrite->Owner);
        P_LOG(PRI_ERROR, BPD01, err);
        SendChunkWriteError(*evChunkWrite, err, NKikimrProto::ERROR);
        return true;
    }

    if (evChunkWrite->ChunkEncrypted) {
        ui32 chunkIdx = evChunkWrite->ChunkIdx;
        Y_VERIFY_S(chunkIdx != 0, PCtx->PDiskLogPrefix);

        TChunkState &state = ChunkState[chunkIdx];
        state.CurrentNonce = state.Nonce + (ui64)desiredSectorIdx;

        ui32 dataChunkSizeSectors = Format.ChunkSize / Format.SectorSize;
        TChunkWriter writer(Mon, *BlockDevice.Get(), Format, state.CurrentNonce, Format.ChunkKey, BufferPool.Get(),
                desiredSectorIdx, dataChunkSizeSectors, Format.MagicDataChunk, chunkIdx, nullptr, desiredSectorIdx,
                nullptr, PCtx, &DriveModel, Cfg->FeatureFlags.GetEnablePDiskDataEncryption());

        guard.Release();
        bool end = ChunkWritePieceEncrypted(evChunkWrite, writer, pieceSize);
        LWTRACK(PDiskChunkWriteLastPieceSendToDevice, evChunkWrite->Orbit, PCtx->PDiskId, evChunkWrite->Owner, chunkIdx, pieceShift, pieceSize);
        return end;
    } else {
        guard.Release();
        ChunkWritePiecePlain(evChunkWrite);
        return true;
    }
}

void TPDisk::SendChunkWriteError(TChunkWrite &chunkWrite, const TString &errorReason,
        NKikimrProto::EReplyStatus status) {
    Y_VERIFY_DEBUG_S(errorReason, PCtx->PDiskLogPrefix);
    Y_VERIFY_DEBUG_S(status != NKikimrProto::OK, PCtx->PDiskLogPrefix);
    P_LOG(PRI_ERROR, PBD23, errorReason);
    Y_VERIFY_S(!chunkWrite.IsReplied, PCtx->PDiskLogPrefix);
    NPDisk::TStatusFlags flags = status == NKikimrProto::OUT_OF_SPACE
        ? NotEnoughDiskSpaceStatusFlags(chunkWrite.Owner, chunkWrite.OwnerGroupType)
        : GetStatusFlags(chunkWrite.Owner, chunkWrite.OwnerGroupType);
    auto ev = std::make_unique<NPDisk::TEvChunkWriteResult>(status, chunkWrite.ChunkIdx, chunkWrite.Cookie, flags, errorReason);
    ev->Orbit = std::move(chunkWrite.Orbit);
    PCtx->ActorSystem->Send(chunkWrite.Sender, ev.release());
    Mon.GetWriteCounter(chunkWrite.PriorityClass)->CountResponse();
    chunkWrite.IsReplied = true;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Chunk reading
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

void TPDisk::SendChunkReadError(const TIntrusivePtr<TChunkRead>& read, TStringStream& error, NKikimrProto::EReplyStatus status) {
    error << " for ownerId# " << read->Owner << " can't read chunkIdx# " << read->ChunkIdx;
    Y_VERIFY_S(status != NKikimrProto::OK, PCtx->PDiskLogPrefix);
    P_LOG(PRI_ERROR, BPD01, "SendChunkReadError" + error.Str(), (ReqId, read->ReqId));

    THolder<NPDisk::TEvChunkReadResult> result = MakeHolder<NPDisk::TEvChunkReadResult>(status,
            read->ChunkIdx, read->Offset, read->Cookie, GetStatusFlags(read->Owner, read->OwnerGroupType), error.Str());
    PCtx->ActorSystem->Send(read->Sender, result.Release());
    read->IsReplied = true;
    Mon.GetReadCounter(read->PriorityClass)->CountResponse();
}

TPDisk::EChunkReadPieceResult TPDisk::ChunkReadPiece(TIntrusivePtr<TChunkRead> &read, ui64 pieceCurrentSector,
        ui64 pieceSizeLimit, NLWTrace::TOrbit&& orbit) {
    if (read->IsReplied) {
        return ReadPieceResultOk;
    }

    Y_VERIFY_S(pieceCurrentSector == read->CurrentSector,
        PCtx->PDiskLogPrefix << pieceCurrentSector << " != " << read->CurrentSector);
    ui64 sectorsCount = read->LastSector - read->FirstSector + 1;
    ui64 sectorsToRead = sectorsCount - read->CurrentSector;
    ui64 bytesToRead = sectorsToRead * Format.SectorSize;
    if (bytesToRead > pieceSizeLimit) {
        sectorsToRead = pieceSizeLimit / Format.SectorSize;
        bytesToRead = sectorsToRead * Format.SectorSize;
    }
    Y_VERIFY_S(bytesToRead == pieceSizeLimit,
        PCtx->PDiskLogPrefix << bytesToRead << " " << pieceSizeLimit);
    Y_VERIFY_S(sectorsToRead == pieceSizeLimit / Format.SectorSize,
        PCtx->PDiskLogPrefix << sectorsToRead << " " << pieceSizeLimit);
    Y_VERIFY_S(pieceSizeLimit % Format.SectorSize == 0,
        PCtx->PDiskLogPrefix << pieceSizeLimit);

    Y_VERIFY_S(sectorsToRead, PCtx->PDiskLogPrefix);

    ui64 firstSector;
    ui64 lastSector;
    ui64 sectorOffset;
    bool isOk = ParseSectorOffset(Format, PCtx->ActorSystem, PCtx->PDiskId,
            read->Offset, read->Size, firstSector, lastSector, sectorOffset, PCtx->PDiskLogPrefix);
    Y_VERIFY_S(isOk, PCtx->PDiskLogPrefix);

    ui64 currentSectorOffset = (ui64)read->CurrentSector * (ui64)Format.SectorSize;
    bool isTheFirstPart = read->CurrentSector == 0;
    bool isTheLastPart = read->FirstSector + read->CurrentSector + sectorsToRead > read->LastSector;

    size_t diskSize = sectorsToRead * Format.SectorSize;
    ui64 diskOffset = (ui64)read->ChunkIdx * Format.ChunkSize + AlignDown<ui32>(read->Offset, Format.SectorSize);
    P_LOG(PRI_DEBUG, BPD01, "Read chunk", (Encrypted, read->ChunkEncrypted), (ChunkIdx, read->ChunkIdx),
        (FirstSector, read->FirstSector), (Offset, read->Offset), (DiskSize, diskSize), (DiskOffset, diskOffset),
        (BytesToRead, bytesToRead), (PieceSizeLimit, pieceSizeLimit));

    if (!read->ChunkEncrypted) {
        Y_VERIFY(isTheFirstPart);
        Y_VERIFY_S(isTheLastPart, read->FirstSector << " + " << read->CurrentSector << " + " << sectorsToRead
                << " > " << read->LastSector);

        THolder<TCompletionChunkReadPart> completion(new TCompletionChunkReadPart(this, read, diskSize,
                    diskSize, 0, read->FinalCompletion, isTheLastPart));

        auto buf = read->FinalCompletion->GetCommonBuffer();
        Y_VERIFY_S(bytesToRead <= buf->SizeWithTail(), buf->SizeWithTail());
        ui8 *data = read->FinalCompletion->GetCommonBuffer()->RawDataPtr(0, diskSize);
        BlockDevice->PreadAsync(
            data,
            diskSize,
            diskOffset,
            completion.Release(),
            read->ReqId,
            {});
        return ReadPieceResultOk;
    }

    ui64 payloadBytesToRead;
    ui64 payloadOffset;
    ParsePayloadFromSectorOffset(Format, read->FirstSector, read->FirstSector + read->CurrentSector + sectorsToRead - 1,
            read->FirstSector + read->CurrentSector, &payloadBytesToRead, &payloadOffset, PCtx->PDiskLogPrefix);

    if (!isTheFirstPart) {
        payloadOffset -= sectorOffset;
    }

    //Adjust read size only if there is more than one piece
    if (isTheFirstPart && !isTheLastPart) {
        payloadBytesToRead -= sectorOffset;
    }
    if (!isTheFirstPart && isTheLastPart) {
        payloadBytesToRead += sectorOffset;
    }

    payloadBytesToRead = Min(payloadBytesToRead, read->RemainingSize);

    read->CurrentSector += sectorsToRead;
    read->RemainingSize -= payloadBytesToRead;
    AtomicAdd(InFlightChunkRead, (ui64)bytesToRead);

    if (isTheLastPart) {
        Y_VERIFY_S(read->RemainingSize == 0, PCtx->PDiskLogPrefix << read->RemainingSize << " " <<  payloadBytesToRead << " " <<  sectorsToRead);
    }

    ui64 footerTotalSize = sectorsToRead * sizeof(TDataSectorFooter);
    *Mon.BandwidthPChunkReadPayload += bytesToRead - footerTotalSize;
    *Mon.BandwidthPChunkReadSectorFooter += footerTotalSize;

    ui64 readOffset = Format.Offset(read->ChunkIdx, read->FirstSector, currentSectorOffset);
    // TODO: Get this from the drive
    THolder<TCompletionChunkReadPart> completion(new TCompletionChunkReadPart(this, read, bytesToRead,
                payloadBytesToRead, payloadOffset, read->FinalCompletion, isTheLastPart));
    completion->CostNs = DriveModel.TimeForSizeNs(bytesToRead, read->ChunkIdx, TDriveModel::OP_TYPE_READ);
    LWTRACK(PDiskChunkReadPiecesSendToDevice, orbit, PCtx->PDiskId);
    completion->Orbit = std::move(orbit);
    Y_VERIFY_S(bytesToRead <= completion->GetBuffer()->Size(), PCtx->PDiskLogPrefix);
    ui8 *data = completion->GetBuffer()->Data();
    BlockDevice->PreadAsync(data, bytesToRead, readOffset, completion.Release(),
            read->ReqId, {});
    // TODO: align the data on SectorSize, not PAGE_SIZE
    // TODO: use the BLKSSZGET ioctl to obtain a backing store's sector size
    return isTheLastPart ? ReadPieceResultOk : ReadPieceResultInProgress;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Chunk locking
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
TVector<TChunkIdx> TPDisk::LockChunksForOwner(TOwner owner, const ui32 count, TString &errorReason) {
    // chunkIdx = 0 is deprecated and will not be soon removed
    TGuard<TMutex> guard(StateMutex);

    const ui32 sharedFree = Keeper.GetFreeChunkCount() - 1;
    i64 ownerFree = Keeper.GetOwnerFree(owner, false);
    double occupancy;
    auto color = Keeper.EstimateSpaceColor(owner, count, &occupancy);

    auto makeError = [&](TString info) {
        guard.Release();
        TStringStream str;
        str << PCtx->PDiskLogPrefix
            << "Can't lock " << count << " chunks"
            << " for ownerId# " << owner
            << " sharedFree# " << sharedFree
            << " ownerFree# " << ownerFree
            << " estimatedColor after lock# " << NKikimrBlobStorage::TPDiskSpaceColor::E_Name(color)
            << " occupancy after lock# " << occupancy
            << " " << info
            << " Marker# BPD21";
        errorReason = str.Str();
        P_LOG(PRI_ERROR, BPD01, str.Str());
    };

    if (ownerFree < count) {
        makeError("Not enough free chunks");
        return {};
    }

    if (sharedFree <= count || color == NKikimrBlobStorage::TPDiskSpaceColor::BLACK) {
        makeError("");
        return {};
    }

    TVector<TChunkIdx> chunks = Keeper.PopOwnerFreeChunks(owner, count, errorReason);
    if (chunks.empty()) {
        makeError("PopOwnerFreeChunks failed");
        return {};
    }

    for (TChunkIdx chunkIdx : chunks) {
        TChunkState &state = ChunkState[chunkIdx];
        Y_VERIFY_S(state.OwnerId == OwnerUnallocated
                || state.OwnerId == OwnerUnallocatedTrimmed
                || state.CommitState == TChunkState::FREE,
            PCtx->PDiskLogPrefix << "chunkIdx# " << chunkIdx << " desired ownerId# " << owner
            << " state# " << state.ToString());
        P_LOG(PRI_INFO, BPD01, "locked chunk for owner",
                (ChunkIdx, chunkIdx),
                (OldOwnerId, ui32(state.OwnerId)),
                (NewOwnerId, ui32(owner)));
        state.OwnerId = owner;
        state.CommitState = TChunkState::LOCKED;
        Mon.LockedChunks->Inc();
    }
    OwnerLocks[owner] += chunks.size();
    return chunks;
}

std::unique_ptr<TEvChunkLockResult> TPDisk::ChunkLockFromQuota(TOwner owner, ui32 number) {
    TStringStream errorReason;
    TGuard<TMutex> guard(StateMutex);

    std::unique_ptr<NPDisk::TEvChunkLockResult> result;
    TString allocateError;
    TVector<TChunkIdx> chunks = LockChunksForOwner(owner, number, allocateError);
    errorReason << allocateError;

    if (chunks.empty()) {
        result.reset(new NPDisk::TEvChunkLockResult(NKikimrProto::OUT_OF_SPACE,
            {}, Keeper.GetFreeChunkCount(), errorReason.Str()));
    } else {
        result.reset(new NPDisk::TEvChunkLockResult(NKikimrProto::OK,
            std::move(chunks), Keeper.GetFreeChunkCount()));
    }

    guard.Release();
    return std::move(result);
}

std::unique_ptr<TEvChunkLockResult> TPDisk::ChunkLockFromQuota(TOwner owner, NKikimrBlobStorage::TPDiskSpaceColor::E color) {
    ui32 number = Keeper.ColorFlagLimit(owner, color);
    ui32 used = Keeper.GetOwnerUsed(owner);
    if (number <= used) {
        P_LOG(PRI_ERROR, BPD33, "Can't lock chunks by color since this space color flag is already raised",
            (Color, TPDiskSpaceColor_Name(color)));
        return std::unique_ptr<TEvChunkLockResult>(new NPDisk::TEvChunkLockResult(NKikimrProto::ERROR,
            {}, Keeper.GetFreeChunkCount(), "Space color flag is already raised"));
    } else {
        return std::move(ChunkLockFromQuota(owner, number - used));
    }
}

void TPDisk::ChunkLock(TChunkLock &evChunkLock) {
    std::unique_ptr<TEvChunkLockResult> result;

    TGuard<TMutex> guard(StateMutex);
    auto sendError = [&] (TChunkLock &evChunkLock, TString info, TString marker) {
        TStringStream str;
        str <<  "Can't lock " << evChunkLock.Count << " chunks";
        if (evChunkLock.Owner) {
            str << " for owner# " << evChunkLock.Owner;
        } else if (evChunkLock.ByVDiskId) {
            if (evChunkLock.IsGenerationSet) {
                str << " for vdisk# " << evChunkLock.VDiskId.ToString();
            } else {
                str << " for vdisk# " << evChunkLock.VDiskId.ToStringWOGeneration();
            }
        }
        if (!info.empty()) {
            str << ": " << info;
        }
        str << " Marker# " << marker;

        P_LOG(PRI_ERROR, BPD01, str.Str());
        guard.Release();
        PCtx->ActorSystem->Send(evChunkLock.Sender, new NPDisk::TEvChunkLockResult(
            NKikimrProto::ERROR, {}, Keeper.GetFreeChunkCount(), str.Str()));
    };

    if (evChunkLock.LockFrom == TEvChunkLock::ELockFrom::COUNT) {
        sendError(evChunkLock, "incorrect LockFrom in query", "BPD35");
        return;
    }

    TOwner owner;
    if (evChunkLock.LockFrom == TEvChunkLock::ELockFrom::LOG) {
        owner = OwnerSystem;
    } else {
        if (evChunkLock.ByVDiskId) {
            if (evChunkLock.VDiskId == TVDiskID::InvalidId) {
                sendError(evChunkLock, "invalid VDiskId", "BPD37");
            }
            auto it = VDiskOwners.begin();
            for (; it != VDiskOwners.end(); ++it) {
                if (evChunkLock.IsGenerationSet && it->first == evChunkLock.VDiskId) {
                    break;
                } else if (!evChunkLock.IsGenerationSet && it->first.SameExceptGeneration(evChunkLock.VDiskId)) {
                    break;
                }
            }
            if (it == VDiskOwners.end()) {
                sendError(evChunkLock, "didn't find owner by given VDisk Id", "BPD34");
                return;
            }
            owner = it->second;
        } else {
            owner = evChunkLock.Owner;
        }
    }

    if (evChunkLock.Count) {
        result.reset(ChunkLockFromQuota(owner, evChunkLock.Count).release());
    } else {
        result.reset(ChunkLockFromQuota(owner, evChunkLock.Color).release());
    }

    P_LOG(PRI_INFO, BPD01, "Locked chunks from owner", (ChunksCount, result->LockedChunks.size()), (OwnerId, (ui32)owner));

    guard.Release();
    PCtx->ActorSystem->Send(evChunkLock.Sender, new NPDisk::TEvChunkLockResult(NKikimrProto::OK, result->LockedChunks,
        Keeper.GetFreeChunkCount()));
}

void TPDisk::ChunkUnlock(TChunkUnlock &evChunkUnlock) {
    TGuard<TMutex> guard(StateMutex);
    TOwner owner;

    auto sendError = [&] (TChunkUnlock &evChunkUnlock, TString info, TString marker) {
        TStringStream str;
        str <<  "Can't unlock chunks";
        if (evChunkUnlock.Owner) {
            str << " for owner# " << evChunkUnlock.Owner;
        } else if (evChunkUnlock.ByVDiskId) {
            if (evChunkUnlock.IsGenerationSet) {
                str << " for vdisk# " << evChunkUnlock.VDiskId.ToString();
            } else {
                str << " for vdisk# " << evChunkUnlock.VDiskId.ToStringWOGeneration();
            }
        }
        if (!info.empty()) {
            str << ": " << info;
        }
        str << " Marker# " << marker;

        P_LOG(PRI_ERROR, BPD01, str.Str());
        guard.Release();
        PCtx->ActorSystem->Send(evChunkUnlock.Sender, new NPDisk::TEvChunkUnlockResult(NKikimrProto::ERROR, 0, str.Str()));
    };

    if (evChunkUnlock.LockFrom == TEvChunkLock::ELockFrom::LOG) {
        owner = OwnerSystem;
    } else {
        if (evChunkUnlock.ByVDiskId) {
            if (evChunkUnlock.VDiskId == TVDiskID::InvalidId) {
                sendError(evChunkUnlock, "invalid VDiskId", "BPD38");
                return;
            }
            auto it = VDiskOwners.begin();
            for (; it != VDiskOwners.end(); ++it) {
                if (evChunkUnlock.IsGenerationSet && it->first == evChunkUnlock.VDiskId) {
                    break;
                } else if (!evChunkUnlock.IsGenerationSet && it->first.SameExceptGeneration(evChunkUnlock.VDiskId)) {
                    break;
                }
            }
            if (it == VDiskOwners.end()) {
                sendError(evChunkUnlock, "didn't find owner by given VDisk Id", "BPD36");
                return;
            }
            owner = it->second;
        } else {
            owner = evChunkUnlock.Owner;
        }
    }

    ui32 lockedChunks = OwnerLocks[owner];
    ui32 unlockedChunks = 0;
    for (ui32 chunkIdx = 0; chunkIdx < ChunkState.size() && unlockedChunks < lockedChunks; ++chunkIdx) {
        if (ChunkState[chunkIdx].CommitState == TChunkState::LOCKED && ChunkState[chunkIdx].OwnerId == owner) {
            TChunkState &state = ChunkState[chunkIdx];
            state.OwnerId = OwnerUnallocated;
            state.CommitState = TChunkState::FREE;
            Mon.LockedChunks->Dec();
            Keeper.PushFreeOwnerChunk(owner, chunkIdx);
            ++unlockedChunks;
        }
    }

    OwnerLocks[owner] = 0;
    guard.Release();
    P_LOG(PRI_INFO, BPD01, "Unlocked chunks", (UnlockedChunksCount, unlockedChunks), (OwnerId, ui32(owner)));
    PCtx->ActorSystem->Send(evChunkUnlock.Sender, new NPDisk::TEvChunkUnlockResult(NKikimrProto::OK, unlockedChunks));
    return;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Chunk reservation
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

TVector<TChunkIdx> TPDisk::AllocateChunkForOwner(const TRequestBase *req, const ui32 count, TString &errorReason) {
    // chunkIdx = 0 is deprecated and will not be soon removed
    TGuard<TMutex> guard(StateMutex);
    Y_VERIFY_DEBUG_S(IsOwnerUser(req->Owner), PCtx->PDiskLogPrefix);

    const ui32 sharedFree = Keeper.GetFreeChunkCount() - 1;
    i64 ownerFree = Keeper.GetOwnerFree(req->Owner, false);
    double occupancy;
    auto color = Keeper.EstimateSpaceColor(req->Owner, count, &occupancy);

    auto makeError = [&](TString info) {
        guard.Release();
        TStringStream str;
        str << PCtx->PDiskLogPrefix
            << "Can't reserve " << count << " chunks"
            << " for ownerId# " << req->Owner
            << " sharedFree# " << sharedFree
            << " ownerFree# " << ownerFree
            << " estimatedColor after allocation# " << NKikimrBlobStorage::TPDiskSpaceColor::E_Name(color)
            << " occupancy after allocation# " << occupancy
            << " " << info
            << " Marker# BPD20";
        errorReason = str.Str();
        P_LOG(PRI_ERROR, BPD01, str.Str());
    };

    if (sharedFree <= count || color == NKikimrBlobStorage::TPDiskSpaceColor::BLACK) {
        makeError("");
        return {};
    }

    TVector<TChunkIdx> chunks = Keeper.PopOwnerFreeChunks(req->Owner, count, errorReason);
    if (chunks.empty()) {
        makeError("PopOwnerFreeChunks failed");
        return {};
    }

    const ui32 dataChunkSizeSectors = Format.ChunkSize / Format.SectorSize;
    for (TChunkIdx chunkIdx : chunks) {
        ui64 chunkNonce = SysLogRecord.Nonces.Value[NonceData];
        SysLogRecord.Nonces.Value[NonceData] += dataChunkSizeSectors;
        auto traceId = req->Span.GetTraceId();
        OnNonceChange(NonceData, req->ReqId, &traceId);
        // Remember who owns the sector, save chunk Nonce in order to be able to continue writing the chunk
        TChunkState &state = ChunkState[chunkIdx];
        Y_VERIFY_S(state.OwnerId == OwnerUnallocated
                || state.OwnerId == OwnerUnallocatedTrimmed
                || state.CommitState == TChunkState::FREE,
            PCtx->PDiskLogPrefix << "chunkIdx# " << chunkIdx << " desired ownerId# " << req->Owner
            << " state# " << state.ToString());
        state.Nonce = chunkNonce;
        state.CurrentNonce = chunkNonce;
        P_LOG(PRI_INFO, BPD01, "chunk is allocated",
                (ChunkIdx, chunkIdx),
                (OldOwnerId, state.OwnerId),
                (NewOwnerId, req->Owner));
        state.OwnerId = req->Owner;
        state.CommitState = TChunkState::DATA_RESERVED;
        Mon.UncommitedDataChunks->Inc();
    }
    return chunks;
}

void TPDisk::ChunkReserve(TChunkReserve &evChunkReserve) {
    TStringStream errorReason;
    TGuard<TMutex> guard(StateMutex);

    THolder<NPDisk::TEvChunkReserveResult> result;
    TString allocateError;
    TVector<TChunkIdx> chunks = AllocateChunkForOwner(&evChunkReserve, evChunkReserve.SizeChunks, allocateError);
    errorReason << allocateError;

    if (chunks.empty()) {
        result = MakeHolder<NPDisk::TEvChunkReserveResult>(NKikimrProto::OUT_OF_SPACE,
                NotEnoughDiskSpaceStatusFlags(evChunkReserve.Owner, evChunkReserve.OwnerGroupType),
                errorReason.Str());
    } else {
        result = MakeHolder<NPDisk::TEvChunkReserveResult>(NKikimrProto::OK, 0);
        result->ChunkIds = std::move(chunks);
        result->StatusFlags = GetStatusFlags(evChunkReserve.Owner, evChunkReserve.OwnerGroupType);
    }

    guard.Release();
    PCtx->ActorSystem->Send(evChunkReserve.Sender, result.Release(), 0, evChunkReserve.Cookie);
    Mon.ChunkReserve.CountResponse();

}
bool TPDisk::ValidateForgetChunk(ui32 chunkIdx, TOwner owner, TStringStream& outErrorReason) {
    TGuard<TMutex> guard(StateMutex);
    if (chunkIdx >= ChunkState.size()) {
        outErrorReason << PCtx->PDiskLogPrefix
            << "Can't forget chunkIdx# " << chunkIdx
            << " > total# " << ChunkState.size()
            << " ownerId# " << owner
            << " Marker# BPD89";
        P_LOG(PRI_ERROR, BPD89, outErrorReason.Str());
        return false;
    }
    if (ChunkState[chunkIdx].OwnerId != owner) {
        outErrorReason << PCtx->PDiskLogPrefix
            << "Can't forget chunkIdx# " << chunkIdx
            << ", ownerId# " << owner
            << " != real ownerId# " << ChunkState[chunkIdx].OwnerId
            << " Marker# BPD90";
        P_LOG(PRI_ERROR, BPD90, outErrorReason.Str());
        return false;
    }
    if (ChunkState[chunkIdx].CommitState != TChunkState::DATA_RESERVED_DECOMMIT_IN_PROGRESS
            && ChunkState[chunkIdx].CommitState != TChunkState::DATA_COMMITTED_DECOMMIT_IN_PROGRESS
            && ChunkState[chunkIdx].CommitState != TChunkState::DATA_DECOMMITTED) {
        outErrorReason << PCtx->PDiskLogPrefix
            << "Can't forget chunkIdx# " << chunkIdx
            << " in CommitState# " << ChunkState[chunkIdx].CommitState
            << " ownerId# " << owner << " Marker# BPD91";
        P_LOG(PRI_ERROR, BPD91, outErrorReason.Str());
        return false;
    }
    return true;
}

void TPDisk::ChunkForget(TChunkForget &evChunkForget) {
    TStringStream errorReason;
    TGuard<TMutex> guard(StateMutex);

    THolder<NPDisk::TEvChunkForgetResult> result;

    bool isOk = true;

    for (ui32 chunkIdx : evChunkForget.ForgetChunks) {
        if (!ValidateForgetChunk(chunkIdx, evChunkForget.Owner, errorReason)) {
            result = MakeHolder<NPDisk::TEvChunkForgetResult>(NKikimrProto::ERROR,
                    NotEnoughDiskSpaceStatusFlags(evChunkForget.Owner, evChunkForget.OwnerGroupType),
                    errorReason.Str());
            isOk = false;
            break;
        }
    }
    if (isOk) {
        for (ui32 chunkIdx : evChunkForget.ForgetChunks) {
            TChunkState& state = ChunkState[chunkIdx];
            if (state.HasAnyOperationsInProgress()) {
                switch (state.CommitState) {
                    case TChunkState::DATA_RESERVED_DECOMMIT_IN_PROGRESS:
                        Mon.UncommitedDataChunks->Dec();
                        state.CommitState = TChunkState::DATA_RESERVED_DELETE_ON_QUARANTINE;
                        QuarantineChunks.push_back(chunkIdx);
                        break;
                    case TChunkState::DATA_COMMITTED_DECOMMIT_IN_PROGRESS:
                        Mon.UncommitedDataChunks->Dec();
                        state.CommitState = TChunkState::DATA_COMMITTED_DELETE_ON_QUARANTINE;
                        QuarantineChunks.push_back(chunkIdx);
                        break;
                    case TChunkState::DATA_DECOMMITTED:
                        Mon.UncommitedDataChunks->Dec();
                        state.CommitState = TChunkState::DATA_ON_QUARANTINE;
                        QuarantineChunks.push_back(chunkIdx);
                        break;
                    default:
                        Y_FAIL_S(PCtx->PDiskLogPrefix
                                << "ChunkForget with in flight, ownerId# " << (ui32)evChunkForget.Owner
                                << " chunkIdx# " << chunkIdx << " unexpected commitState# " << state.CommitState);
                }
            } else {
                switch (state.CommitState) {
                    case TChunkState::DATA_RESERVED_DECOMMIT_IN_PROGRESS:
                        Mon.UncommitedDataChunks->Dec();
                        state.CommitState = TChunkState::DATA_RESERVED_DELETE_IN_PROGRESS;
                        break;
                    case TChunkState::DATA_COMMITTED_DECOMMIT_IN_PROGRESS:
                        Mon.UncommitedDataChunks->Dec();
                        state.CommitState = TChunkState::DATA_COMMITTED_DELETE_IN_PROGRESS;
                        break;
                    case TChunkState::DATA_DECOMMITTED:
                        Y_VERIFY_S(state.CommitsInProgress == 0, PCtx->PDiskLogPrefix
                                << "chunkIdx# " << chunkIdx << " state# " << state.ToString());
                        P_LOG(PRI_INFO, BPD01, "chunk was forgotten",
                                (ChunkIdx, chunkIdx),
                                (OldOwner, (ui32)state.OwnerId),
                                (NewOwner, (ui32)OwnerUnallocated));
                        Y_VERIFY_S(state.OwnerId == evChunkForget.Owner, PCtx->PDiskLogPrefix);
                        Mon.UncommitedDataChunks->Dec();
                        state.OwnerId = OwnerUnallocated;
                        state.CommitState = TChunkState::FREE;
                        Keeper.PushFreeOwnerChunk(evChunkForget.Owner, chunkIdx);
                        break;
                    default:
                        Y_FAIL_S(PCtx->PDiskLogPrefix
                                << "ChunkForget, ownerId# " << (ui32)evChunkForget.Owner
                                << " chunkIdx# " << chunkIdx << " unexpected commitState# " << state.CommitState);
                }
            }
        }
        result = MakeHolder<NPDisk::TEvChunkForgetResult>(NKikimrProto::OK, 0);
        result->StatusFlags = GetStatusFlags(evChunkForget.Owner, evChunkForget.OwnerGroupType);
    }

    guard.Release();
    PCtx->ActorSystem->Send(evChunkForget.Sender, result.Release());
    Mon.ChunkForget.CountResponse();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Report to Whiteboard
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

void TPDisk::WhiteboardReport(TWhiteboardReport &whiteboardReport) {
    TEvWhiteboardReportResult *reportResult = whiteboardReport.Response.Release();
    {
        TGuard<TMutex> guard(StateMutex);
        const ui64 totalSize = Format.DiskSize;
        const ui64 availableSize = (ui64)Format.ChunkSize * Keeper.GetFreeChunkCount();
        const ui32 numActiveSlots = GetNumActiveSlots();

        if (*Mon.PDiskBriefState != TPDiskMon::TPDisk::Error) {
            *Mon.FreeSpaceBytes = availableSize;
            *Mon.UsedSpaceBytes = totalSize - availableSize;
        } else {
            // If disk is in Error State, show its total and used space as 32KiB (format size)
            *Mon.FreeSpaceBytes = 0;
            *Mon.UsedSpaceBytes = 32_KB;
            *Mon.TotalSpaceBytes = 32_KB;
        }

        NKikimrWhiteboard::TPDiskStateInfo& pdiskState = reportResult->PDiskState->Record;
        pdiskState.SetPDiskId(PCtx->PDiskId);
        pdiskState.SetPath(Cfg->GetDevicePath());
        pdiskState.SetSerialNumber(Cfg->ExpectedSerial);
        const auto& state = static_cast<NKikimrBlobStorage::TPDiskState::E>(Mon.PDiskState->Val());
        pdiskState.SetState(state);

        // Only report size information when PDisk is not in error state
        if (*Mon.PDiskBriefState != TPDiskMon::TPDisk::Error) {
            pdiskState.SetAvailableSize(availableSize);
            pdiskState.SetTotalSize(totalSize);
            pdiskState.SetSystemSize(Format.ChunkSize * (Keeper.GetOwnerHardLimit(OwnerSystemLog) + Keeper.GetOwnerHardLimit(OwnerSystemReserve)));
            pdiskState.SetLogUsedSize(Format.ChunkSize * (Keeper.GetOwnerHardLimit(OwnerCommonStaticLog) - Keeper.GetOwnerFree(OwnerCommonStaticLog, {})));
            pdiskState.SetLogTotalSize(Format.ChunkSize * Keeper.GetOwnerHardLimit(OwnerCommonStaticLog));
        }
        pdiskState.SetNumActiveSlots(numActiveSlots);
        pdiskState.SetSlotSizeInUnits(Cfg->SlotSizeInUnits);
        if (ExpectedSlotCount) {
            pdiskState.SetExpectedSlotCount(ExpectedSlotCount);
        }

        *Mon.NumActiveSlots = numActiveSlots;
        *Mon.SlotSizeInUnits = Cfg->SlotSizeInUnits;
        *Mon.ExpectedSlotCount = ExpectedSlotCount;

        reportResult->DiskMetrics = MakeHolder<TEvBlobStorage::TEvControllerUpdateDiskStatus>();

        for (const auto& [vdiskId, owner] : VDiskOwners) {
            const TOwnerData &data = OwnerData[owner];
            // May be less than 0 if owner exceeded his quota
            i64 ownerAllocated = (i64)Keeper.GetOwnerUsed(owner) * Format.ChunkSize;
            i64 ownerFree = Max<i64>(0, Keeper.GetOwnerFree(owner, true) * Format.ChunkSize);

            reportResult->VDiskStateVect.emplace_back(data.WhiteboardProxyId, NKikimrWhiteboard::TVDiskStateInfo());
            auto& vdiskInfo = std::get<1>(reportResult->VDiskStateVect.back());
            vdiskInfo.SetAvailableSize(ownerFree);
            vdiskInfo.SetAllocatedSize(ownerAllocated);
            vdiskInfo.SetGroupSizeInUnits(data.GroupSizeInUnits);

            NKikimrBlobStorage::TVDiskMetrics* vdiskMetrics = reportResult->DiskMetrics->Record.AddVDisksMetrics();
            VDiskIDFromVDiskID(vdiskId, vdiskMetrics->MutableVDiskId());
            vdiskMetrics->MutableVDiskId()->ClearGroupGeneration();
            vdiskMetrics->SetAvailableSize(ownerFree);
            vdiskMetrics->SetAllocatedSize(ownerAllocated);
            double occupancy;
            NPDisk::TStatusFlags statusFlags = Keeper.GetSpaceStatusFlags(owner, &occupancy);
            NKikimrBlobStorage::TPDiskSpaceColor::E spaceColor = StatusFlagToSpaceColor(statusFlags);
            double vdiskSlotUsage = Keeper.GetVDiskSlotUsage(owner);
            double vdiskRawUsage = Keeper.GetVDiskRawUsage(owner);
            vdiskMetrics->SetStatusFlags(statusFlags);
            vdiskMetrics->SetNormalizedOccupancy(occupancy);
            vdiskMetrics->SetVDiskSlotUsage(vdiskSlotUsage);
            vdiskMetrics->SetVDiskRawUsage(vdiskRawUsage);
            vdiskMetrics->SetCapacityAlert(spaceColor);

            vdiskInfo.SetNormalizedOccupancy(occupancy);
            vdiskInfo.SetVDiskSlotUsage(vdiskSlotUsage);
            vdiskInfo.SetVDiskRawUsage(vdiskRawUsage);
            vdiskInfo.SetCapacityAlert(spaceColor);

            auto *vslotId = vdiskMetrics->MutableVSlotId();
            vslotId->SetNodeId(PCtx->ActorSystem->NodeId);
            vslotId->SetPDiskId(PCtx->PDiskId);
            vslotId->SetVSlotId(data.VDiskSlotId);
        }

        NKikimrBlobStorage::TPDiskMetrics& pDiskMetrics = *reportResult->DiskMetrics->Record.AddPDisksMetrics();
        pDiskMetrics.SetPDiskId(PCtx->PDiskId);
        pDiskMetrics.SetTotalSize(Format.DiskSize);
        pDiskMetrics.SetAvailableSize(availableSize);
        pDiskMetrics.SetMaxReadThroughput(DriveModel.Speed(TDriveModel::OP_TYPE_READ));
        pDiskMetrics.SetMaxWriteThroughput(DriveModel.Speed(TDriveModel::OP_TYPE_WRITE));
        pDiskMetrics.SetNonRealTimeMs(AtomicGet(NonRealTimeMs));
        pDiskMetrics.SetSlowDeviceMs(Max((ui64)AtomicGet(SlowDeviceMs), (ui64)*Mon.DeviceNonperformanceMs));
        pDiskMetrics.SetMaxIOPS(DriveModel.IOPS());

        i64 minSlotSize = Max<i64>();
        for (const auto& [_, owner] : VDiskOwners) {
            minSlotSize = Min(minSlotSize, Keeper.GetOwnerHardLimit(owner) / Keeper.GetOwnerWeight(owner) * Format.ChunkSize);
        }
        if (minSlotSize != Max<i64>()) {
            pDiskMetrics.SetEnforcedDynamicSlotSize(minSlotSize);
            pdiskState.SetEnforcedDynamicSlotSize(minSlotSize);
        }
        pDiskMetrics.SetState(state);
        pDiskMetrics.SetSlotSizeInUnits(Cfg->SlotSizeInUnits);
        if (ExpectedSlotCount) {
            pDiskMetrics.SetSlotCount(ExpectedSlotCount);
        }

        double pdiskUsage = Keeper.GetPDiskUsage();
        pDiskMetrics.SetPDiskUsage(pdiskUsage);
        pdiskState.SetPDiskUsage(pdiskUsage);
    }

    PCtx->ActorSystem->Send(whiteboardReport.Sender, reportResult);
    // Update VDisk's state Solomon metrics

    i64 atLeastOneVDiskNotLogged = 0;
    for (const TOwnerData& data : OwnerData) {
        if (data.VDiskId != TVDiskID::InvalidId && data.Status != TOwnerData::VDISK_STATUS_LOGGED) {
            atLeastOneVDiskNotLogged = 1;
            break;
        }
    }
    *Mon.AtLeastOneVDiskNotLogged = atLeastOneVDiskNotLogged;
    if (Cfg->SectorMap) {
        *Mon.SectorMapAllocatedBytes = Cfg->SectorMap->AllocatedBytes.load();
    }

}

void TPDisk::EventUndelivered(TUndelivered &req) {
    switch (req.Event->SourceType) {
    case TEvCutLog::EventType:
    {
        for (ui32 i = OwnerBeginUser; i < OwnerEndUser; ++i) {
            if (OwnerData[i].CutLogId == req.Sender) {
                P_LOG(PRI_CRIT, BPD24, "TEvCutLog was undelivered to vdisk",
                    (VDiskId, OwnerData[i].VDiskId.ToStringWOGeneration()));
                return;
            }

        }
        P_LOG(PRI_INFO, BPD25, "TEvCutLog was undelivered to unknown VDisk", (ActorID, req.Sender));
        return;
    }
    default:
        P_LOG(PRI_DEBUG, BPD26, "Event was undelivered to actor", (Event, req.Event->ToString()), (ActorID, req.Sender));
        return;
    }
}

void TPDisk::CommitLogChunks(TCommitLogChunks &req) {
    TGuard<TMutex> guard(StateMutex);
    for (auto it = req.CommitedLogChunks.begin(); it != req.CommitedLogChunks.end(); ++it) {
        Y_VERIFY_S(ChunkState[*it].OwnerId == OwnerSystem, PCtx->PDiskLogPrefix << "Unexpected chunkIdx# " << *it << " ownerId# "
                << (ui32)ChunkState[*it].OwnerId << " in CommitLogChunks");
        Y_VERIFY_DEBUG_S(ChunkState[*it].CommitState == TChunkState::LOG_RESERVED, PCtx->PDiskLogPrefix);
        ChunkState[*it].CommitState = TChunkState::LOG_COMMITTED;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// PDisk formatting
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

void TPDisk::WriteApplyFormatRecord(TDiskFormat format, const TKey &mainKey) {
    // Use temporal copy of format to restore from that copy because we can damage 'format' variable
    // by writing Magic in first bytes
    const TDiskFormat originalFormat = format;
    Format = format;
    Format.ChunkSize = FormatSectorSize * ReplicationFactor;
    Format.SectorSize = FormatSectorSize;

    {

        // Encrypt chunk0 format record using mainKey
        ui64 nonce = 1;
        bool encrypt = Cfg->EnableFormatAndMetadataEncryption;
        TSysLogWriter formatWriter(Mon, *BlockDevice.Get(), Format, nonce, mainKey, BufferPool.Get(),
                0, ReplicationFactor, Format.MagicFormatChunk, 0, nullptr, 0, nullptr, PCtx,
                &DriveModel, encrypt);

        if (format.IsFormatInProgress()) {
            // Fill first bytes with magic pattern
            ui64 *formatBegin = reinterpret_cast<ui64*>(&format);
            ui64 *formatMagicEnd = reinterpret_cast<ui64*>((ui8*)&format + MagicIncompleteFormatSize);
            Y_VERIFY_S((ui8*)formatMagicEnd - (ui8*)formatBegin <= (intptr_t)sizeof(format), PCtx->PDiskLogPrefix);
            Fill(formatBegin, formatMagicEnd, MagicIncompleteFormat);
        }
        formatWriter.Write(&format, sizeof(TDiskFormat), TReqId(TReqId::WriteApplyFormatRecordWrite, 0), {});
        TSignalEvent doneEvent;
        formatWriter.Flush(TReqId(TReqId::WriteApplyFormatRecordFlush, 0), {},
                new TCompletionSignal(&doneEvent));
        doneEvent.WaitI();
    }

    Format = originalFormat;
}


void TPDisk::WriteDiskFormat(ui64 diskSizeBytes, ui32 sectorSizeBytes, ui32 userAccessibleChunkSizeBytes,
        const ui64 &diskGuid, const TKey &chunkKey, const TKey &logKey, const TKey &sysLogKey, const TKey &mainKey,
        TString textMessage, const bool isErasureEncodeUserLog, const bool trimEntireDevice,
        std::optional<TRcBuf> metadata, bool plainDataChunks, std::optional<bool> forceRandomizeMagic) {
    TGuard<TMutex> guard(StateMutex);
    // Prepare format record
    alignas(16) TDiskFormat format;
    format.Clear(Cfg->EnableFormatAndMetadataEncryption);
    format.SetPlainDataChunks(plainDataChunks);
    format.DiskSize = diskSizeBytes;
    format.SectorSize = sectorSizeBytes;
    ui64 erasureFlags = FormatFlagErasureEncodeUserLog;
    format.FormatFlags = (format.FormatFlags & (~erasureFlags)) | (isErasureEncodeUserLog ? erasureFlags : 0);
    format.ChunkSize = SystemChunkSize(format, userAccessibleChunkSizeBytes, sectorSizeBytes);
    format.Guid = diskGuid;
    format.ChunkKey = chunkKey;
    format.LogKey = logKey;
    format.SysLogKey = sysLogKey;
    bool randomizeMagic = !Cfg->EnableFormatAndMetadataEncryption;
#ifdef DISABLE_PDISK_ENCRYPTION
    randomizeMagic = true;
#endif
    if (forceRandomizeMagic) {
        randomizeMagic = *forceRandomizeMagic;
    }
    format.InitMagic(randomizeMagic);
    memcpy(format.FormatText, textMessage.data(), Min(sizeof(format.FormatText) - 1, textMessage.size()));
    format.SysLogSectorCount = RecordsInSysLog * format.SysLogSectorsPerRecord();
    ui64 firstSectorIdx = format.FirstSysLogSectorIdx();
    ui64 endSectorIdx = firstSectorIdx + format.SysLogSectorCount * ReplicationFactor;
    format.SystemChunkCount = (endSectorIdx * format.SectorSize + format.ChunkSize - 1) / format.ChunkSize;
    // Check disk size
    {
        ui32 diskSizeChunks = format.DiskSizeChunks();
        if (diskSizeChunks <= (format.SystemChunkCount + 2)) {
            ythrow yexception() << "Incorrect disk parameters! Total chunks# " << diskSizeChunks
                << ", System chunks needed# " << format.SystemChunkCount << ", cant run with < 3 free chunks!"
                << " Debug format# " << format.ToString();
        }

        ChunkState = TVector<TChunkState>(diskSizeChunks);
        for (ui32 i = 0; i < format.SystemChunkCount; ++i) {
            ChunkState[i].OwnerId = OwnerSystem;
        }
        for (ui32 i = format.SystemChunkCount; i < diskSizeChunks; ++i) {
            ChunkState[i].OwnerId = OwnerUnallocated;
        }
    }
    // Trim the entire device
    if (trimEntireDevice && DriveModel.IsTrimSupported()) {
        P_LOG(PRI_NOTICE, BPD28, "Trim of the entire device started");
        NHPTimer::STime start = HPNow();
        TReqId reqId(TReqId::FormatTrim, AtomicIncrement(ReqCreator.LastReqId));
        BlockDevice->TrimSync(diskSizeBytes, 0);
        for (ui32 i = 0; i < ChunkState.size(); ++i) {
            if (ChunkState[i].OwnerId == OwnerUnallocated) {
                ChunkState[i].OwnerId = OwnerUnallocatedTrimmed;
            }
        }
        double trimDurationSec = HPSecondsFloat(HPNow() - start);
        P_LOG(PRI_NOTICE, BPD29, "Trim of the entire device done",
            (TrimDurationSec, trimDurationSec), (TrimSpeedMBps, diskSizeBytes / 1'000'000u / trimDurationSec));
    }

    // Write and apply format record with magic in first bytes
    format.SetFormatInProgress(true);
    format.SetHash();
    WriteApplyFormatRecord(format, mainKey);

    if (metadata) {
        // Prepare chunks for metadata, if needed.
        InitFormattedMetadata();
        WriteMetadataSync(std::move(*metadata), format);
    }

    // Prepare initial SysLogRecord
    memset(&SysLogRecord, 0, sizeof(SysLogRecord));
    SysLogRecord.LogHeadChunkIdx = format.SystemChunkCount;
    FirstLogChunkToParseCommits = format.SystemChunkCount;
    for (ui32 i = 0; i < NonceCount; ++i) {
        SysLogRecord.Nonces.Value[i] = 1;
        LoggedNonces.Value[i] = 1;
    }
    ui32 ownerCount = sizeof(SysLogRecord.OwnerVDisks) / sizeof(TVDiskID);
    for (ui32 i = 0; i < ownerCount; ++i) {
        SysLogRecord.OwnerVDisks[i] = TVDiskID::InvalidId;
    }

    // Fill the cyclic log with initial SysLogRecords
    SysLogger.Reset(new TSysLogWriter(Mon, *BlockDevice.Get(), Format, SysLogRecord.Nonces.Value[NonceSysLog],
                Format.SysLogKey, BufferPool.Get(), firstSectorIdx, endSectorIdx, Format.MagicSysLogChunk, 0,
                nullptr, firstSectorIdx, nullptr, PCtx, &DriveModel,
                Cfg->FeatureFlags.GetEnablePDiskDataEncryption()));

    bool isFull = false;
    while (!isFull) {
        ui64 sectorIdx = SysLogger->SectorIdx;
        TSignalEvent doneEvent;
        WriteSysLogRestorePoint(new TCompletionSignal(&doneEvent), TReqId(TReqId::FormatFillSysLog, 0), {});
        doneEvent.WaitI();
        isFull = SysLogger->SectorIdx < sectorIdx;
    }

    // Write and apply format record
    format.SetFormatInProgress(false);
    format.SetHash();
    WriteApplyFormatRecord(format, mainKey);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Owner initialization
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

void TPDisk::ReplyErrorYardInitResult(TYardInit &evYardInit, const TString &str, NKikimrProto::EReplyStatus status) {
    TStringStream error;
    error << PCtx->PDiskLogPrefix << "YardInit error for VDiskId# " << evYardInit.VDisk.ToStringWOGeneration()
        << " reason# " << str;
    P_LOG(PRI_ERROR, BPD01, error.Str());
    ui64 writeBlockSize = ForsetiOpPieceSizeCached;
    ui64 readBlockSize = ForsetiOpPieceSizeCached;
    bool isTinyDisk = (Format.DiskSize < NPDisk::TinyDiskSizeBoundary);
    PCtx->ActorSystem->Send(evYardInit.Sender, new NPDisk::TEvYardInitResult(status,
        DriveModel.SeekTimeNs() / 1000ull, DriveModel.Speed(TDriveModel::OP_TYPE_READ),
        DriveModel.Speed(TDriveModel::OP_TYPE_WRITE), readBlockSize, writeBlockSize,
        DriveModel.BulkWriteBlockSize(),
        Format.GetUserAccessibleChunkSize(), Format.GetAppendBlockSize(), OwnerSystem, 0,
        Cfg->SlotSizeInUnits,
        GetStatusFlags(OwnerSystem, evYardInit.OwnerGroupType), TVector<TChunkIdx>(),
        Cfg->RetrieveDeviceType(), isTinyDisk, Format.SectorSize, error.Str()));
    Mon.YardInit.CountResponse();
}

TOwner TPDisk::FindNextOwnerId() {
    const TOwner start = LastOwnerId;
    do {
        ++LastOwnerId;
        if (LastOwnerId == OwnerEndUser) {
            LastOwnerId = OwnerBeginUser;
        }
        if (LastOwnerId == start) {
            return 0;
        }
    } while (OwnerData[LastOwnerId].VDiskId != TVDiskID::InvalidId || OwnerData[LastOwnerId].OnQuarantine);

    Mon.OwnerIdsIssued->Inc();
    *Mon.LastOwnerId = LastOwnerId;
    return LastOwnerId;
}

bool TPDisk::YardInitForKnownVDisk(TYardInit &evYardInit, TOwner owner) {
    // Just register cut log id and reply with starting points.
    TVDiskID vDiskId = evYardInit.VDiskIdWOGeneration();

    TOwnerData &ownerData = OwnerData[owner];
    ADD_RECORD_WITH_TIMESTAMP_TO_OPERATION_LOG(ownerData.OperationLog, "YardInitForKnownVDisk, OwnerId# " << owner
            << ", evYardInit# " << evYardInit.ToString());

    if (Cfg->ReadOnly) {
        ownerData.LogEndPosition = TOwnerData::TLogEndPosition(LastInitialChunkIdx, LastInitialSectorIdx);
    } else {
        TFirstUncommitted firstUncommitted = CommonLogger->FirstUncommitted.load();
        ownerData.LogEndPosition = TOwnerData::TLogEndPosition(firstUncommitted.ChunkIdx, firstUncommitted.SectorIdx);
    }

    ownerData.OwnerRound = evYardInit.OwnerRound;
    TOwnerRound ownerRound = evYardInit.OwnerRound;
    TVector<TChunkIdx> ownedChunks;
    ownedChunks.reserve(ChunkState.size());
    for (TChunkIdx chunkId = 0; chunkId < ChunkState.size(); ++chunkId) {
        if (ChunkState[chunkId].OwnerId == owner) {
            ownedChunks.push_back(chunkId);
        }
    }
    ui64 writeBlockSize = ForsetiOpPieceSizeCached;
    ui64 readBlockSize = ForsetiOpPieceSizeCached;
    ui32 ownerWeight = Cfg->GetOwnerWeight(evYardInit.GroupSizeInUnits);
    bool isTinyDisk = (Format.DiskSize < NPDisk::TinyDiskSizeBoundary);

    THolder<NPDisk::TEvYardInitResult> result(new NPDisk::TEvYardInitResult(NKikimrProto::OK,
                DriveModel.SeekTimeNs() / 1000ull, DriveModel.Speed(TDriveModel::OP_TYPE_READ),
                DriveModel.Speed(TDriveModel::OP_TYPE_WRITE), readBlockSize, writeBlockSize,
                DriveModel.BulkWriteBlockSize(), Format.GetUserAccessibleChunkSize(), Format.GetAppendBlockSize(), owner,
                ownerRound, Cfg->SlotSizeInUnits,
                GetStatusFlags(OwnerSystem, evYardInit.OwnerGroupType), ownedChunks,
                Cfg->RetrieveDeviceType(), isTinyDisk, Format.SectorSize, ""));

    GetStartingPoints(owner, result->StartingPoints);
    result->DiskFormat = TDiskFormatPtr(new TDiskFormat(Format), +[](TDiskFormat* ptr) {
        delete ptr;
    });
    result->PersistentBufferFormat = NPDisk::TPersistentBufferFormatPtr(new NPDisk::TPersistentBufferFormat(), +[](NPDisk::TPersistentBufferFormat* ptr) {
        delete ptr;
    });
    if (evYardInit.GetDiskFd) {
        result->DiskFd = BlockDevice->DuplicateFd();
    }
    ownerData.VDiskId = vDiskId;
    ownerData.CutLogId = evYardInit.CutLogId;
    ownerData.WhiteboardProxyId = evYardInit.WhiteboardProxyId;
    ownerData.VDiskSlotId = evYardInit.SlotId;
    ownerData.LogRecordsConsequentlyRead = 0;
    ownerData.LastSeenLsn = 0;
    ownerData.HasAlreadyLoggedThisIncarnation = false;
    ownerData.HasReadTheWholeLog = false;
    ownerData.LogStartPosition = TLogPosition{0, 0};
    ownerData.Status = TOwnerData::VDISK_STATUS_SENT_INIT;
    ownerData.LastShredGeneration = 0;
    ownerData.ShredState = TOwnerData::VDISK_SHRED_STATE_NOT_REQUESTED;
    ownerData.GroupSizeInUnits = evYardInit.GroupSizeInUnits;

    Keeper.SetOwnerWeight(owner, ownerWeight);
    AddCbsSet(owner);

    P_LOG(PRI_NOTICE, BPD30, "Registered known VDisk",
            (VDisk, vDiskId),
            (OwnerId, owner),
            (OwnerRound, ownerRound),
            (GroupSizeInUnits, evYardInit.GroupSizeInUnits));

    PCtx->ActorSystem->Send(evYardInit.Sender, result.Release());
    Mon.YardInit.CountResponse();
    AskVDisksToCutLogs(owner, false);
    ProgressShredState();
    return true;
}

bool TPDisk::YardInitStart(TYardInit &evYardInit) {
    if (evYardInit.VDisk == TVDiskID::InvalidId) {
        ReplyErrorYardInitResult(evYardInit, "VDisk == InvalidId. Marker# BPD03");
        return false;
    }
    if (evYardInit.PDiskGuid != Format.Guid) {
        TStringStream str;
        str << "incorrect guid. User-provided# " << evYardInit.PDiskGuid
            << " on-disk# " << Format.Guid
            << " Marker# BPD04";
        ReplyErrorYardInitResult(evYardInit, str.Str());
        return false;
    }

    // Make sure owner round is always higher than the NextOwnerRound
    if (evYardInit.OwnerRound <= NextOwnerRound) {
        TStringStream str;
        str << "requested OwnerRound# " << evYardInit.OwnerRound
            << " <= minExpectedOwnerRound# " << NextOwnerRound
            << " OwnerRound seems to be older than the PDisk. Marker# BPD05";
        ReplyErrorYardInitResult(evYardInit, str.Str());
        return false;
    }

    TOwner owner;
    TVDiskID vDiskId = evYardInit.VDiskIdWOGeneration();

    TGuard<TMutex> guard(StateMutex);
    auto it = VDiskOwners.find(vDiskId);
    if (it != VDiskOwners.end()) {
        // Owner is already known, but use next ownerRound to decrease probability of errors
        owner = it->second;
    } else {
        owner = FindNextOwnerId();
        if (owner == 0) {
            ReplyErrorYardInitResult(evYardInit, "owner limit is reached. Marker# BPD07");
            return false;
        }
        // TODO REPLY ERROR
        TOwnerData &data = OwnerData[owner];
        Y_VERIFY_S(!data.HaveRequestsInFlight(), PCtx->PDiskLogPrefix << "owner# " << owner);
    }
    evYardInit.Owner = owner;

    TOwnerData &ownerData = OwnerData[owner];
    ui64 prevOwnerRound = ownerData.OwnerRound;
    if (prevOwnerRound >= evYardInit.OwnerRound) {
        guard.Release();
        TStringStream str;
        str << "requested OwnerRound# " << evYardInit.OwnerRound
            << " <= prevoiuslyUsedOwnerRound# " << prevOwnerRound
            << " OwnerRound may never decrease and can only be used once for YardInit. Marker# BPD13";
        ReplyErrorYardInitResult(evYardInit, str.Str());
        return false;
    }

    P_LOG(PRI_INFO, BPD56, "YardInitStart",
            (OwnerId, owner),
            (NewOwnerRound, evYardInit.OwnerRound),
            (ownerData.HaveRequestsInFlight(), ownerData.HaveRequestsInFlight()));
    // Update round and wait for all pending requests of old owner to finish
    ADD_RECORD_WITH_TIMESTAMP_TO_OPERATION_LOG(ownerData.OperationLog, "YardInitStart, OwnerId# "
            << owner << ", new OwnerRound# " << evYardInit.OwnerRound);
    ownerData.OwnerRound = evYardInit.OwnerRound;
    ownerData.GroupSizeInUnits = evYardInit.GroupSizeInUnits;
    return true;
}

void TPDisk::YardInitFinish(TYardInit &evYardInit) {
    TOwner owner = evYardInit.Owner;
    TOwnerRound ownerRound = evYardInit.OwnerRound;
    {
        TGuard<TMutex> guard(StateMutex);
        TVDiskID vDiskId = evYardInit.VDiskIdWOGeneration();

        auto it = VDiskOwners.find(vDiskId);
        if (it != VDiskOwners.end()) {
            YardInitForKnownVDisk(evYardInit, it->second);
            return;
        }

        if (Cfg->ReadOnly) {
            ReplyErrorYardInitResult(evYardInit, "PDisk is in ReadOnly mode. Marker# BPD47", NKikimrProto::CORRUPTED);
            return;
        }

        // Allocate quota for the owner
        Keeper.AddOwner(owner, vDiskId, Cfg->GetOwnerWeight(evYardInit.GroupSizeInUnits));

        TOwnerData& ownerData = OwnerData[owner];
        ownerData.Reset(false);
        // A new owner is created.

        AtomicIncrement(TotalOwners);
        ownerData.VDiskId = vDiskId;
        Y_VERIFY_S(SysLogFirstNoncesToKeep.FirstNonceToKeep[owner] <= SysLogRecord.Nonces.Value[NonceLog],
            PCtx->PDiskLogPrefix);
        SysLogFirstNoncesToKeep.FirstNonceToKeep[owner] = SysLogRecord.Nonces.Value[NonceLog];
        ownerData.CutLogId = evYardInit.CutLogId;
        ownerData.WhiteboardProxyId = evYardInit.WhiteboardProxyId;
        ownerData.VDiskSlotId = evYardInit.SlotId;
        ownerData.OwnerRound = evYardInit.OwnerRound;
        VDiskOwners[vDiskId] = owner;
        ownerData.GroupSizeInUnits = evYardInit.GroupSizeInUnits;
        ownerData.Status = TOwnerData::VDISK_STATUS_SENT_INIT;
        SysLogRecord.OwnerVDisks[owner] = vDiskId;
        ownerRound = ownerData.OwnerRound;
        ADD_RECORD_WITH_TIMESTAMP_TO_OPERATION_LOG(ownerData.OperationLog, "YardInitFinish, OwnerId# " << owner);

        AddCbsSet(owner);

        P_LOG(PRI_NOTICE, BPD02, "New owner is created",
                (ownerId, owner),
                (vDiskId, vDiskId.ToStringWOGeneration()),
                (FirstNonceToKeep, SysLogFirstNoncesToKeep.FirstNonceToKeep[owner]),
                (CutLogId, ownerData.CutLogId),
                (ownerRound, ownerData.OwnerRound));

        AskVDisksToCutLogs(OwnerSystem, false);
    }

    ui64 writeBlockSize = ForsetiOpPieceSizeCached;
    ui64 readBlockSize = ForsetiOpPieceSizeCached;
    bool isTinyDisk = (Format.DiskSize < NPDisk::TinyDiskSizeBoundary);

    THolder<NPDisk::TEvYardInitResult> result(new NPDisk::TEvYardInitResult(
        NKikimrProto::OK,
        DriveModel.SeekTimeNs() / 1000ull, DriveModel.Speed(TDriveModel::OP_TYPE_READ),
        DriveModel.Speed(TDriveModel::OP_TYPE_WRITE), readBlockSize, writeBlockSize,
        DriveModel.BulkWriteBlockSize(), Format.GetUserAccessibleChunkSize(), Format.GetAppendBlockSize(), owner, ownerRound,
        Cfg->SlotSizeInUnits,
        GetStatusFlags(OwnerSystem, evYardInit.OwnerGroupType) | ui32(NKikimrBlobStorage::StatusNewOwner), TVector<TChunkIdx>(),
        Cfg->RetrieveDeviceType(), isTinyDisk, Format.SectorSize, ""));

    GetStartingPoints(result->PDiskParams->Owner, result->StartingPoints);
    result->DiskFormat = TDiskFormatPtr(new TDiskFormat(Format), +[](TDiskFormat* ptr) {
        delete ptr;
    });
    result->PersistentBufferFormat = NPDisk::TPersistentBufferFormatPtr(new NPDisk::TPersistentBufferFormat(), +[](NPDisk::TPersistentBufferFormat* ptr) {
        delete ptr;
    });
    if (evYardInit.GetDiskFd) {
        result->DiskFd = BlockDevice->DuplicateFd();

    }
    WriteSysLogRestorePoint(new TCompletionEventSender(
        this, evYardInit.Sender, result.Release(), Mon.YardInit.Results), evYardInit.ReqId, {});

    if (ContinueShredsInFlight == 0) {
        ContinueShredsInFlight++;
        PCtx->ActorSystem->Send(new IEventHandle(PCtx->PDiskActor, PCtx->PDiskActor, new TEvContinueShred(), 0, 0));
    }
}

void TPDisk::YardResize(TYardResize &ev) {
    TStringStream errorReason;
    NKikimrProto::EReplyStatus errStatus = CheckOwnerAndRound(&ev, errorReason);
    if (errStatus != NKikimrProto::OK) {
        Mon.YardResize.CountResponse();
        PCtx->ActorSystem->Send(ev.Sender, new NPDisk::TEvYardResizeResult(errStatus, {}, errorReason.Str()));
        return;
    }

    {
        TGuard<TMutex> guard(StateMutex);
        OwnerData[ev.Owner].GroupSizeInUnits = ev.GroupSizeInUnits;
        Keeper.SetOwnerWeight(ev.Owner, Cfg->GetOwnerWeight(ev.GroupSizeInUnits));
    }

    auto result = std::make_unique<NPDisk::TEvYardResizeResult>(NKikimrProto::OK, GetStatusFlags(ev.Owner, ev.OwnerGroupType), TString());
    if (Cfg->ReadOnly) {
        Mon.YardResize.CountResponse();
        PCtx->ActorSystem->Send(ev.Sender, result.release());
    } else {
        WriteSysLogRestorePoint(new TCompletionEventSender(this, ev.Sender, result.release(), Mon.YardResize.Results),
            TReqId(TReqId::YardResize, 0), {});
    }
}

void TPDisk::ProcessChangeExpectedSlotCount(TChangeExpectedSlotCount& request) {
    TGuard<TMutex> guard(StateMutex);
    ExpectedSlotCount = request.ExpectedSlotCount;
    Cfg->ExpectedSlotCount = request.ExpectedSlotCount;
    Cfg->SlotSizeInUnits = request.SlotSizeInUnits;
    Keeper.SetExpectedOwnerCount(ExpectedSlotCount);
    for (TOwner owner = OwnerBeginUser; owner < OwnerEndUser; ++owner) {
        if (OwnerData[owner].VDiskId != TVDiskID::InvalidId) {
            Keeper.SetOwnerWeight(owner, Cfg->GetOwnerWeight(OwnerData[owner].GroupSizeInUnits));
        }
    }

    auto result = std::make_unique<NPDisk::TEvChangeExpectedSlotCountResult>(NKikimrProto::OK, TString());
    Mon.ChangeExpectedSlotCount.CountResponse();
    PCtx->ActorSystem->Send(request.Sender, result.release());
}

// Scheduler weight configuration

void TPDisk::ConfigureCbs(ui32 ownerId, EGate gate, ui64 weight) {
    NSchLab::TCbs *cbs = ForsetiScheduler.GetCbs(ownerId, gate);
    if (cbs) {
        cbs->Weight = weight;
    }
}

void TPDisk::SchedulerConfigure(const TPDiskSchedulerConfig& cfg, ui32 ownerId) {
    // TODO(cthulhu): Check OwnerRound here
    ui64 bytesTotalWeight = cfg.LogWeight + cfg.FreshWeight + cfg.CompWeight;
    ConfigureCbs(ownerId, GateLog, cfg.LogWeight * cfg.BytesSchedulerWeight);
    ConfigureCbs(ownerId, GateFresh, cfg.FreshWeight * cfg.BytesSchedulerWeight);
    ConfigureCbs(ownerId, GateComp, cfg.CompWeight * cfg.BytesSchedulerWeight);
    ConfigureCbs(ownerId, GateFastRead, cfg.FastReadWeight * bytesTotalWeight);
    ConfigureCbs(ownerId, GateOtherRead, cfg.OtherReadWeight * bytesTotalWeight);
    ConfigureCbs(ownerId, GateLoad, cfg.LoadWeight * bytesTotalWeight);
    ConfigureCbs(ownerId, GateHugeAsync, cfg.HugeWeight * bytesTotalWeight);
    ConfigureCbs(ownerId, GateHugeUser, cfg.HugeWeight * bytesTotalWeight);
    ConfigureCbs(ownerId, GateSyncLog, cfg.SyncLogWeight * bytesTotalWeight);
    ConfigureCbs(ownerId, GateLow, cfg.LowReadWeight);
    ForsetiScheduler.UpdateTotalWeight();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Free space check
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

void TPDisk::CheckSpace(TCheckSpace &evCheckSpace) {
    double occupancy;
    auto result = std::make_unique<NPDisk::TEvCheckSpaceResult>(NKikimrProto::OK,
                GetStatusFlags(evCheckSpace.Owner, evCheckSpace.OwnerGroupType, &occupancy),
                GetFreeChunks(evCheckSpace.Owner, evCheckSpace.OwnerGroupType),
                GetTotalChunks(evCheckSpace.Owner, evCheckSpace.OwnerGroupType),
                GetUsedChunks(evCheckSpace.Owner, evCheckSpace.OwnerGroupType),
                AtomicGet(TotalOwners),
                GetNumActiveSlots(),
                TString(),
                GetStatusFlags(OwnerSystem, evCheckSpace.OwnerGroupType));
    result->NormalizedOccupancy = occupancy;
    result->VDiskSlotUsage = Keeper.GetVDiskSlotUsage(evCheckSpace.Owner);
    result->VDiskRawUsage = Keeper.GetVDiskRawUsage(evCheckSpace.Owner);
    result->PDiskUsage = Keeper.GetPDiskUsage();
    PCtx->ActorSystem->Send(evCheckSpace.Sender, result.release());
    Mon.CheckSpace.CountResponse();
    return;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Owner self-destruction
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

void TPDisk::ForceDeleteChunk(TChunkIdx chunkIdx) {
    TGuard<TMutex> guard(StateMutex);
    TChunkState &state = ChunkState[chunkIdx];
    TOwner owner = state.OwnerId;
    Y_VERIFY_S(!state.HasAnyOperationsInProgress(), PCtx->PDiskLogPrefix
            << "ForceDeleteChunk, ownerId# " << owner
            << " chunkIdx# " << chunkIdx
            << " has operationsInProgress, state# " << state.ToString());

    switch (state.CommitState) {
    case TChunkState::DATA_ON_QUARANTINE:
        P_LOG(PRI_NOTICE, BPD01, "chunk owned by owner is released from quarantine and marked as free at ForceDeleteChunk",
                (ChunkIdx, chunkIdx),
                (OwnerId, (ui32)state.OwnerId));
        [[fallthrough]];
    case TChunkState::DATA_RESERVED:
        [[fallthrough]];
    case TChunkState::LOCKED:
        [[fallthrough]];
    case TChunkState::DATA_COMMITTED:
        state.OwnerId = OwnerUnallocated;
        state.CommitState = TChunkState::FREE;
        state.Nonce = 0;
        state.CurrentNonce = 0;
        Keeper.PushFreeOwnerChunk(owner, chunkIdx);
        break;
    case TChunkState::DATA_COMMITTED_DELETE_IN_PROGRESS:
        [[fallthrough]];
    case TChunkState::DATA_RESERVED_DELETE_IN_PROGRESS:
        // Chunk will be freed in TPDisk::DeleteChunk()
        break;
    default:
        Y_FAIL_S(PCtx->PDiskLogPrefix << "ForceDeleteChunk, ownerId# " << owner
                << " chunkIdx# " << chunkIdx << " unexpected commitState# " << state.CommitState);
        break;
    }
}

// Called before logging anything about the kill
void TPDisk::KillOwner(TOwner owner, TOwnerRound killOwnerRound, TCompletionEventSender *completionAction) {
    Y_UNUSED(killOwnerRound);
    {
        TGuard<TMutex> guard(StateMutex);
        bool pushedOwnerIntoQuarantine = false;
        for (ui32 i = 0; i < ChunkState.size(); ++i) {
            TChunkState &state = ChunkState[i];
            if (state.OwnerId == owner) {
                if (TPDisk::IS_SHRED_ENABLED) {
                    state.IsDirty = true;
                }
                if (state.CommitState == TChunkState::DATA_RESERVED
                        || state.CommitState == TChunkState::DATA_DECOMMITTED
                        || state.CommitState == TChunkState::DATA_RESERVED_DECOMMIT_IN_PROGRESS
                        || state.CommitState == TChunkState::DATA_COMMITTED_DECOMMIT_IN_PROGRESS) {
                    Mon.UncommitedDataChunks->Dec();
                } else if (state.CommitState == TChunkState::DATA_COMMITTED) {
                    Mon.CommitedDataChunks->Dec();
                    LOG_DEBUG(*PCtx->ActorSystem, NKikimrServices::BS_PDISK, "PDiskId# %" PRIu32
                            " Line# %" PRIu32 " --CommitedDataChunks# %" PRIi64 " chunkIdx# %" PRIu32 " Marker# BPD84",
                            (ui32)PCtx->PDiskId, (ui32)__LINE__, (i64)Mon.CommitedDataChunks->Val(), (ui32)i);
                } else if (state.CommitState == TChunkState::LOCKED) {
                    Mon.LockedChunks->Dec();
                }
                if (state.CommitState == TChunkState::DATA_ON_QUARANTINE) {
                    if (!pushedOwnerIntoQuarantine) {
                        pushedOwnerIntoQuarantine = true;
                        ADD_RECORD_WITH_TIMESTAMP_TO_OPERATION_LOG(OwnerData[owner].OperationLog, "KillOwner(), Add owner to quarantine, "
                                << "CommitState# DATA_ON_QUARANTINE, OwnerId# " << owner);
                        QuarantineOwners.push_back(owner);
                        P_LOG(PRI_NOTICE, BPD01, "push owner into quarantine as there is a chunk in DATA_ON_QUARANTINE",
                            (OwnerId, (ui32)owner));
                    }
                } else if (state.HasAnyOperationsInProgress()
                        || state.CommitState == TChunkState::DATA_RESERVED_DELETE_IN_PROGRESS
                        || state.CommitState == TChunkState::DATA_COMMITTED_DELETE_IN_PROGRESS
                        || state.CommitState == TChunkState::DATA_RESERVED_DELETE_ON_QUARANTINE
                        || state.CommitState == TChunkState::DATA_COMMITTED_DELETE_ON_QUARANTINE
                        || state.CommitState == TChunkState::DATA_DECOMMITTED
                        || state.CommitState == TChunkState::DATA_RESERVED_DECOMMIT_IN_PROGRESS
                        || state.CommitState == TChunkState::DATA_COMMITTED_DECOMMIT_IN_PROGRESS
                        ) {
                    if (state.CommitState == TChunkState::DATA_RESERVED
                            || state.CommitState == TChunkState::DATA_COMMITTED
                            || state.CommitState == TChunkState::DATA_DECOMMITTED) {
                        state.CommitState = TChunkState::DATA_ON_QUARANTINE;
                    } else  if (state.CommitState == TChunkState::DATA_COMMITTED_DELETE_IN_PROGRESS) {
                        state.CommitState = TChunkState::DATA_RESERVED_DELETE_IN_PROGRESS;
                    } else  if (state.CommitState == TChunkState::DATA_COMMITTED_DELETE_ON_QUARANTINE) {
                        state.CommitState = TChunkState::DATA_RESERVED_DELETE_ON_QUARANTINE;
                    } else if (state.CommitState == TChunkState::DATA_RESERVED_DECOMMIT_IN_PROGRESS
                            || state.CommitState == TChunkState::DATA_COMMITTED_DECOMMIT_IN_PROGRESS) {
                        state.CommitState = TChunkState::DATA_RESERVED_DELETE_ON_QUARANTINE;
                        QuarantineChunks.push_back(i);
                    }

                    if (state.CommitState != TChunkState::DATA_RESERVED_DELETE_ON_QUARANTINE
                            && state.CommitState != TChunkState::DATA_RESERVED_DELETE_IN_PROGRESS) {
                        QuarantineChunks.push_back(i);
                    }

                    if (!pushedOwnerIntoQuarantine) {
                        pushedOwnerIntoQuarantine = true;
                        ADD_RECORD_WITH_TIMESTAMP_TO_OPERATION_LOG(OwnerData[owner].OperationLog, "KillOwner(), Add owner to quarantine, OwnerId# " << owner);
                        QuarantineOwners.push_back(owner);
                        P_LOG(PRI_NOTICE, BPD01, "Push owner into quarantine", (OwnerId, (ui32)owner));
                    }
                } else {
                    ForceDeleteChunk(i);
                }
            }
        }
        if (!pushedOwnerIntoQuarantine && OwnerData[owner].HaveRequestsInFlight()) {
            pushedOwnerIntoQuarantine = true;
            ADD_RECORD_WITH_TIMESTAMP_TO_OPERATION_LOG(OwnerData[owner].OperationLog, "KillOwner(), Add owner to quarantine, "
                    << "HaveRequestsInFlight, OwnerId# " << owner);
            QuarantineOwners.push_back(owner);
            P_LOG(PRI_NOTICE, BPD01, "Push owner into quarantine as there are requests in flight", (OwnerId, (ui32)owner));
        }
        if (!pushedOwnerIntoQuarantine) {
            ADD_RECORD_WITH_TIMESTAMP_TO_OPERATION_LOG(OwnerData[owner].OperationLog, "KillOwner(), Remove owner without quarantine, OwnerId# " << owner);
            Keeper.RemoveOwner(owner);
            P_LOG(PRI_NOTICE, BPD01, "removed owner from chunks Keeper", (OwnerId, (ui32)owner));
        }

        TryTrimChunk(false, 0);
        bool readingLog = OwnerData[owner].ReadingLog();
        ui64 lastSeenLsn = 0;
        auto it = LogChunks.begin();
        while (it != LogChunks.end()) {
            if (it->OwnerLsnRange.size() > owner && it->OwnerLsnRange[owner].IsPresent) {
                lastSeenLsn = Max(it->OwnerLsnRange[owner].LastLsn, lastSeenLsn);

                if (!readingLog) {
                    Y_VERIFY_S(it->CurrentUserCount > 0, PCtx->PDiskLogPrefix);
                    it->CurrentUserCount--;
                    it->OwnerLsnRange[owner].IsPresent = false;
                    it->OwnerLsnRange[owner].FirstLsn = 0;
                    it->OwnerLsnRange[owner].LastLsn = 0;
                }
            }
            ++it;
        }

        bool isProgressShredStateNeeded = false;
        if (OwnerData[owner].ShredState == TOwnerData::VDISK_SHRED_STATE_COMPACT_REQUESTED ||
                OwnerData[owner].ShredState == TOwnerData::VDISK_SHRED_STATE_SHRED_REQUESTED) {
            isProgressShredStateNeeded = true;
        }

        ReleaseUnusedLogChunks(completionAction);

        TVDiskID vDiskId = SysLogRecord.OwnerVDisks[owner];
        vDiskId.GroupGeneration = -1;  // Since it might be non-zero.
                                      // TODO(cthulhu): Replace with VERIFY.
        SysLogRecord.OwnerVDisks[owner] = TVDiskID::InvalidId;

        Y_VERIFY_S(AtomicGet(TotalOwners) > 0, PCtx->PDiskLogPrefix);
        AtomicDecrement(TotalOwners);

        TOwnerRound ownerRound = OwnerData[owner].OwnerRound;
        OwnerData[owner].Reset(pushedOwnerIntoQuarantine);
        OwnerData[owner].OwnerRound = ownerRound;
        VDiskOwners.erase(vDiskId);

        if (isProgressShredStateNeeded) {
            ProgressShredState();
        }

        P_LOG(PRI_NOTICE, BPD12, "KillOwner", (ownerId, owner), (ownerRound, ownerRound),
            (VDiskId, vDiskId.ToStringWOGeneration()), (lastSeenLsn, lastSeenLsn));
    }

    WriteSysLogRestorePoint(completionAction, TReqId(TReqId::KillOwnerSysLog, 0), {});
}

void TPDisk::Harakiri(THarakiri &evHarakiri) {
    KillOwner(evHarakiri.Owner, evHarakiri.OwnerRound,
            new TCompletionEventSender(this, evHarakiri.Sender,
                new NPDisk::TEvHarakiriResult(NKikimrProto::OK,
                    GetStatusFlags(evHarakiri.Owner, evHarakiri.OwnerGroupType), TString()),
                Mon.Harakiri.Results));
}

void TPDisk::Slay(TSlay &evSlay) {
    {
        TGuard<TMutex> guard(StateMutex);
        TVDiskID vDiskId = evSlay.VDiskId;
        vDiskId.GroupGeneration = -1;
        auto it = VDiskOwners.find(vDiskId);

        for (auto& pendingInit : PendingYardInits) {
            if (vDiskId == pendingInit->VDiskIdWOGeneration()) {
                TStringStream str;
                str << PCtx->PDiskLogPrefix << "Can't slay VDiskId# " << evSlay.VDiskId
                    << " as it has pending YardInit Marker# BPD48";
                P_LOG(PRI_ERROR, BPD48, str.Str());
                THolder<NPDisk::TEvSlayResult> result(new NPDisk::TEvSlayResult(
                    NKikimrProto::NOTREADY,
                    GetStatusFlags(evSlay.Owner, evSlay.OwnerGroupType), evSlay.VDiskId, evSlay.SlayOwnerRound,
                    evSlay.PDiskId, evSlay.VSlotId, str.Str()));
                PCtx->ActorSystem->Send(evSlay.Sender, result.Release());
                Mon.YardSlay.CountResponse();
                return;
            }
        }

        if (it == VDiskOwners.end()) {
            TStringStream str;
            str << PCtx->PDiskLogPrefix << "Can't slay VDiskId# " << evSlay.VDiskId;
            str << " as it is not created yet or is already slain"
                << " Marker# BPD31";
            P_LOG(PRI_ERROR, BPD31, str.Str());
            THolder<NPDisk::TEvSlayResult> result(new NPDisk::TEvSlayResult(
                NKikimrProto::ALREADY,
                GetStatusFlags(evSlay.Owner, evSlay.OwnerGroupType), evSlay.VDiskId, evSlay.SlayOwnerRound,
                evSlay.PDiskId, evSlay.VSlotId, str.Str()));
            PCtx->ActorSystem->Send(evSlay.Sender, result.Release());
            Mon.YardSlay.CountResponse();
            return;
        }
        TOwner owner = it->second;
        TOwnerRound ownerRound = OwnerData[owner].OwnerRound;
        if (evSlay.SlayOwnerRound <= ownerRound) {
            TStringStream str;
            str << PCtx->PDiskLogPrefix << "Can't slay VDiskId# " << evSlay.VDiskId;
            str << " as SlayOwnerRound# " << evSlay.SlayOwnerRound << " <= ownerRound# " << ownerRound
                << " Marker# BPD32";
            P_LOG(PRI_ERROR, BPD32, str.Str());
            THolder<NPDisk::TEvSlayResult> result(new NPDisk::TEvSlayResult(
                NKikimrProto::RACE,
                GetStatusFlags(evSlay.Owner, evSlay.OwnerGroupType), evSlay.VDiskId, evSlay.SlayOwnerRound,
                evSlay.PDiskId, evSlay.VSlotId, str.Str()));
            PCtx->ActorSystem->Send(evSlay.Sender, result.Release());
            Mon.YardSlay.CountResponse();
            return;
        }

        KillOwner(owner, evSlay.SlayOwnerRound,
                new TCompletionEventSender(this, evSlay.Sender,
                    new NPDisk::TEvSlayResult(NKikimrProto::OK,
                        GetStatusFlags(evSlay.Owner, evSlay.OwnerGroupType),
                        evSlay.VDiskId, evSlay.SlayOwnerRound,
                        evSlay.PDiskId, evSlay.VSlotId, ""), Mon.YardSlay.Results));
    }
    return;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Update process
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

void TPDisk::ProcessChunkWriteQueue() {
    auto start = HPNow();

    size_t initialSize = JointChunkWrites.size();
    size_t processed = 0;
    size_t processedBytes = 0;
    double processedCostMs = 0;
    while (JointChunkWrites.size()) {
        TRequestBase *req = JointChunkWrites.front();
        JointChunkWrites.pop();

        req->Span.Event("PDisk.BeforeBlockDevice");

        Y_VERIFY_S(req->GetType() == ERequestType::RequestChunkWritePiece, PCtx->PDiskLogPrefix
            << "Unexpected request type# " << ui64(req->GetType())
            << " TypeName# " << TypeName(*req) << " in JointChunkWrites");
        TChunkWritePiece *piece = static_cast<TChunkWritePiece*>(req);
        processed++;
        processedBytes += piece->PieceSize;
        processedCostMs += piece->GetCostMs();

        P_LOG(PRI_DEBUG, BPD01, "ChunkWritePiece",
            (ChunkIdx, piece->ChunkWrite->ChunkIdx),
            (Offset, piece->PieceShift),
            (Size, piece->PieceSize)
        );
        bool lastPart = ChunkWritePiece(piece->ChunkWrite.Get(), piece->PieceShift, piece->PieceSize);
        if (lastPart) {
            Mon.IncrementQueueTime(piece->ChunkWrite->PriorityClass, piece->ChunkWrite->LifeDurationMs(HPNow()));
        }
        delete piece;
        // prevent the thread from being stuck for long
        if (UseNoopSchedulerCached && processed >= Cfg->SchedulerCfg.MaxChunkWritesPerCycle
            && HPMilliSecondsFloat(HPNow() - start) > Cfg->SchedulerCfg.MaxChunkWritesDurationPerCycleMs) {
            break;
        }
    }
    LWTRACK(PDiskProcessChunkWriteQueue, UpdateCycleOrbit, PCtx->PDiskId, initialSize, processed, processedBytes, processedCostMs);
}

void TPDisk::ProcessChunkReadQueue() {
    auto start = HPNow();
    // Size (bytes) of elementary sectors block, it is useless to read/write less than that blockSize
    ui64 bufferSize;
    with_lock(StateMutex) {
        bufferSize = BufferPool->GetBufferSize() / Format.SectorSize * Format.SectorSize;
    }

    size_t initialSize = JointChunkReads.size();
    size_t processed = 0;
    size_t processedBytes = 0;
    double processedCostMs = 0;
    while (JointChunkReads.size()) {
        auto req = std::move(JointChunkReads.front());
        JointChunkReads.pop();
        req->Span.Event("PDisk.BeforeBlockDevice");

        Y_VERIFY_S(req->GetType() == ERequestType::RequestChunkReadPiece, PCtx->PDiskLogPrefix
                << "Unexpected request type# " << ui64(req->GetType()) << " in JointChunkReads");
        TChunkReadPiece *piece = static_cast<TChunkReadPiece*>(req.Get());
        processed++;
        processedBytes += piece->PieceSizeLimit;
        processedCostMs += piece->GetCostMs();
        Y_VERIFY_S(!piece->SelfPointer, PCtx->PDiskLogPrefix);
        TIntrusivePtr<TChunkRead> &read = piece->ChunkRead;
        TReqId reqId = read->ReqId;
        ui32 chunkIdx = read->ChunkIdx;
        ui8 priorityClass = read->PriorityClass;
        NHPTimer::STime creationTime = read->CreationTime;
        Y_VERIFY_S(!read->IsReplied, PCtx->PDiskLogPrefix << "read's reqId# " << read->ReqId);
        P_LOG(PRI_DEBUG, BPD36, "Performing TChunkReadPiece", (ReqId, reqId), (chunkIdx, chunkIdx),
            (PieceCurrentSector, piece->PieceCurrentSector),
            (PieceSizeLimit, piece->PieceSizeLimit),
            (IsTheLastPiece, piece->IsTheLastPiece),
            (BufferSize, bufferSize)
        );

        ui64 currentLimit = Min(bufferSize, piece->PieceSizeLimit);
        Y_VERIFY(!read->ChunkEncrypted || piece->PieceSizeLimit <= bufferSize);
        EChunkReadPieceResult result = ChunkReadPiece(read, piece->PieceCurrentSector, piece->PieceSizeLimit,
            std::move(piece->Orbit));
        bool isComplete = (result != ReadPieceResultInProgress);
        Y_VERIFY_S(isComplete || currentLimit >= piece->PieceSizeLimit, PCtx->PDiskLogPrefix
                << isComplete << " " << currentLimit << " " << piece->PieceSizeLimit);
        piece->OnSuccessfulDestroy(PCtx->ActorSystem);
        if (isComplete) {
            //
            // WARNING: Don't access "read" after this point.
            // Don't add code before the warning!
            //
            Mon.IncrementQueueTime(priorityClass, HPMilliSeconds(HPNow() - creationTime));
            P_LOG(PRI_DEBUG, BPD37, "enqueued all TChunkReadPiece", (ReqId, reqId), (chunkIdx, chunkIdx));
        }

        ++processed;
        // prevent the thread from being stuck for long
        if (UseNoopSchedulerCached && processed >= Cfg->SchedulerCfg.MaxChunkReadsPerCycle
            && HPMilliSecondsFloat(HPNow() - start) > Cfg->SchedulerCfg.MaxChunkReadsDurationPerCycleMs) {
            break;
        }
    }
    LWTRACK(PDiskProcessChunkReadQueue, UpdateCycleOrbit, PCtx->PDiskId, initialSize, processed, processedBytes, processedCostMs);
}

void TPDisk::TrimAllUntrimmedChunks() {
    TGuard<TMutex> g(StateMutex);
    if (!DriveModel.IsTrimSupported()) {
        return;
    }

    while (ui32 idx = Keeper.PopUntrimmedFreeChunk()) {
        BlockDevice->TrimSync(Format.ChunkSize, idx * Format.ChunkSize);
        Y_VERIFY_S(ChunkState[idx].OwnerId == OwnerUnallocated || ChunkState[idx].OwnerId == OwnerUnallocatedTrimmed,
                PCtx->PDiskLogPrefix << "Unexpected ownerId# " << ui32(ChunkState[idx].OwnerId));
        ChunkState[idx].OwnerId = OwnerUnallocatedTrimmed;
        Keeper.PushTrimmedFreeChunk(idx);
    }
}

void TPDisk::ProcessChunkTrimQueue() {
    Y_VERIFY_S(JointChunkTrims.size() <= 1, PCtx->PDiskLogPrefix);
    for (auto it = JointChunkTrims.begin(); it != JointChunkTrims.end(); ++it) {
        TChunkTrim *trim = (*it);
        trim->Span.Event("PDisk.BeforeBlockDevice");
        ui64 chunkOffset = Format.ChunkSize * ui64(trim->ChunkIdx);
        ui64 offset = chunkOffset + trim->Offset;
        ui64 trimSize = trim->Size;
        if (trim->Deadline < HPNow()) {
            // If deadline occurs, than we want to maximize throughput, so trim entire chunk
            trimSize = Format.ChunkSize - trim->Offset;
        }
        auto completion = MakeHolder<TChunkTrimCompletion>(this, trim->CreationTime, trimSize, trim->ReqId);
        completion->CostNs = DriveModel.TrimTimeForSizeNs(trimSize);
        BlockDevice->TrimAsync(trimSize, offset, completion.Release(), trim->ReqId);
        delete trim;
    }
    JointChunkTrims.clear();
}

void TPDisk::ClearQuarantineChunks() {
    if (QuarantineChunks.empty() && QuarantineOwners.empty()) {
        return;
    }

    bool isKillInFlight = (
        (Mon.YardSlay.Results->Val() != Mon.YardSlay.Requests->Val())
        && (Mon.Harakiri.Results->Val() != Mon.Harakiri.Requests->Val()));

    if (isKillInFlight) {
        return;
    }

    TGuard<TMutex> guard(StateMutex);
    {
        const auto it = std::partition(QuarantineChunks.begin(), QuarantineChunks.end(), [&] (TChunkIdx i) {
            return ChunkState[i].HasAnyOperationsInProgress()
                || ChunkState[i].CommitState == TChunkState::DATA_COMMITTED_DELETE_ON_QUARANTINE
                || ChunkState[i].CommitState == TChunkState::DATA_RESERVED_DELETE_ON_QUARANTINE;
        });
        for (auto delIt = it; delIt != QuarantineChunks.end(); ++delIt) {
            ForceDeleteChunk(*delIt);
        }
        QuarantineChunks.erase(it, QuarantineChunks.end());
        *Mon.QuarantineChunks = QuarantineChunks.size();
    }

    bool haveChunksToRelease = false;

    {
        const auto it = std::partition(QuarantineOwners.begin(), QuarantineOwners.end(), [&] (TOwner i) {
            return Keeper.GetOwnerUsed(i) || OwnerData[i].HaveRequestsInFlight();
        });
        for (auto delIt = it; delIt != QuarantineOwners.end(); ++delIt) {
            TOwner owner = *delIt;
            ADD_RECORD_WITH_TIMESTAMP_TO_OPERATION_LOG(OwnerData[owner].OperationLog, "Remove owner from quarantine, OwnerId# " << owner);
            TOwnerRound ownerRound = OwnerData[owner].OwnerRound;
            OwnerData[owner].Reset(false);
            OwnerData[owner].OwnerRound = ownerRound;
            Keeper.RemoveOwner(owner);

            ui64 lastSeenLsn = 0;
            auto it = LogChunks.begin();
            while (it != LogChunks.end()) {
                if (it->OwnerLsnRange.size() > owner && it->OwnerLsnRange[owner].IsPresent) {
                    Y_VERIFY_S(it->CurrentUserCount > 0, PCtx->PDiskLogPrefix);
                    ui32 userCount = --it->CurrentUserCount;
                    it->OwnerLsnRange[owner].IsPresent = false;
                    it->OwnerLsnRange[owner].FirstLsn = 0;
                    lastSeenLsn = Max(it->OwnerLsnRange[owner].LastLsn, lastSeenLsn);
                    it->OwnerLsnRange[owner].LastLsn = 0;

                    if (userCount == 0) {
                        haveChunksToRelease = true;
                    }
                }
                ++it;
            }

            P_LOG(PRI_NOTICE, BPD01, "removed owner from chunks Keeper through QuarantineOwners" << (haveChunksToRelease ? " along with log chunks" : ""),
                (OwnerId, (ui32)owner), (LastSeenLsn, lastSeenLsn));
        }
        QuarantineOwners.erase(it, QuarantineOwners.end());
        *Mon.QuarantineOwners = QuarantineOwners.size();

        ProgressShredState();
    }

    if (haveChunksToRelease) {
        THolder<TCompletionEventSender> completion(new TCompletionEventSender(this));
        if (ReleaseUnusedLogChunks(completion.Get())) {
            WriteSysLogRestorePoint(completion.Release(), TReqId(TReqId::KillOwnerSysLog, 0), {});
        }
    }
}

// Should be called to initiate TRIM (on chunk delete or prev trim done)
void TPDisk::TryTrimChunk(bool prevDone, ui64 trimmedSize) {
    TGuard<TMutex> g(StateMutex);
    TrimOffset += trimmedSize;
    if (!DriveModel.IsTrimSupported()) {
        return;
    }

    if (prevDone) {
        TrimInFly = false;
    } else if (TrimInFly) {
        return; // Three is already one request in fly (max-in-fly is one)
    }

    if (!ChunkBeingTrimmed) { // There was no previous TRIMs
        ChunkBeingTrimmed = Keeper.PopUntrimmedFreeChunk();
        if (ChunkBeingTrimmed) {
            Y_VERIFY_S(ChunkState[ChunkBeingTrimmed].OwnerId == OwnerUnallocated
                    || ChunkState[ChunkBeingTrimmed].OwnerId == OwnerUnallocatedTrimmed, PCtx->PDiskLogPrefix
                    << "Unexpected ownerId# " << ui32(ChunkState[ChunkBeingTrimmed].OwnerId));
        }
        TrimOffset = 0;
    } else if (TrimOffset >= Format.ChunkSize) { // Previous chunk entirely trimmed
        Y_VERIFY_S(ChunkState[ChunkBeingTrimmed].OwnerId == OwnerUnallocated
                || ChunkState[ChunkBeingTrimmed].OwnerId == OwnerUnallocatedTrimmed, PCtx->PDiskLogPrefix
                << "Unexpected ownerId# " << ui32(ChunkState[ChunkBeingTrimmed].OwnerId));
        ChunkState[ChunkBeingTrimmed].OwnerId = OwnerUnallocatedTrimmed;
        Keeper.PushTrimmedFreeChunk(ChunkBeingTrimmed);
        ChunkBeingTrimmed = Keeper.PopUntrimmedFreeChunk();
        if (ChunkBeingTrimmed) {
            Y_VERIFY_S(ChunkState[ChunkBeingTrimmed].OwnerId == OwnerUnallocated
                    || ChunkState[ChunkBeingTrimmed].OwnerId == OwnerUnallocatedTrimmed, PCtx->PDiskLogPrefix
                    << "Unexpected ownerId# " << ui32(ChunkState[ChunkBeingTrimmed].OwnerId));
        }
        TrimOffset = 0;
    }

    if (ChunkBeingTrimmed) { // Initiate trim of next part of chunk
        const ui64 trimStep = (Keeper.GetTrimmedFreeChunkCount() > 100 ? 2 << 20 : 32 << 20);
        ui64 trimSize = Min<ui64>(Format.ChunkSize - TrimOffset, trimStep);
        TChunkTrim* trim = ReqCreator.CreateChunkTrim(ChunkBeingTrimmed, TrimOffset, trimSize);
        InputRequest(trim);
        TrimInFly = true;
    }
}

void TPDisk::ProcessFastOperationsQueue() {
    for (auto& req : FastOperationsQueue) {
        switch (req->GetType()) {
            case ERequestType::RequestYardInit: {
                std::unique_ptr<TYardInit> init{static_cast<TYardInit*>(req.release())};
                if (YardInitStart(*init)) {
                    PendingYardInits.emplace(std::move(init));
                }
                break;
            }
            case ERequestType::RequestCheckSpace:
                CheckSpace(static_cast<TCheckSpace&>(*req));
                break;
            case ERequestType::RequestHarakiri:
                Harakiri(static_cast<THarakiri&>(*req));
                break;
            case ERequestType::RequestYardSlay:
                Slay(static_cast<TSlay&>(*req));
                break;
            case ERequestType::RequestChunkReserve:
                ChunkReserve(static_cast<TChunkReserve&>(*req));
                break;
            case ERequestType::RequestChunkLock:
                ChunkLock(static_cast<TChunkLock&>(*req));
                break;
            case ERequestType::RequestChunkUnlock:
                ChunkUnlock(static_cast<TChunkUnlock&>(*req));
                break;
            case ERequestType::RequestAskForCutLog:
                SendCutLog(static_cast<TAskForCutLog&>(*req));
                break;
            case ERequestType::RequestConfigureScheduler: {
                const auto& cfgReq = static_cast<TConfigureScheduler&>(*req);
                SchedulerConfigure(cfgReq.SchedulerCfg, cfgReq.OwnerId);
                break;
            }
            case ERequestType::RequestWhiteboartReport:
                WhiteboardReport(static_cast<TWhiteboardReport&>(*req));
                break;
            case ERequestType::RequestHttpInfo:
                HttpInfo(static_cast<THttpInfo&>(*req));
                break;
            case ERequestType::RequestUndelivered:
                EventUndelivered(static_cast<TUndelivered&>(*req));
                break;
            case ERequestType::RequestCommitLogChunks:
                CommitLogChunks(static_cast<TCommitLogChunks&>(*req));
                break;
            case ERequestType::RequestLogCommitDone:
                OnLogCommitDone(static_cast<TLogCommitDone&>(*req));
                break;
            case ERequestType::RequestTryTrimChunk:
                TryTrimChunk(true, static_cast<TTryTrimChunk&>(*req).TrimSize);
                break;
            case ERequestType::RequestReleaseChunks:
                MarkChunksAsReleased(static_cast<TReleaseChunks&>(*req));
                break;
            case ERequestType::RequestReadMetadata:
                ProcessReadMetadata(std::move(req));
                break;
            case ERequestType::RequestInitialReadMetadataResult:
                ProcessInitialReadMetadataResult(static_cast<TInitialReadMetadataResult&>(*req));
                break;
            case ERequestType::RequestWriteMetadata:
                ProcessWriteMetadata(std::move(req));
                break;
            case ERequestType::RequestWriteMetadataResult:
                ProcessWriteMetadataResult(static_cast<TWriteMetadataResult&>(*req));
                break;
            case ERequestType::RequestPushUnformattedMetadataSector:
                ProcessPushUnformattedMetadataSector(static_cast<TPushUnformattedMetadataSector&>(*req));
                break;
            case ERequestType::RequestContinueReadMetadata:
                static_cast<TContinueReadMetadata&>(*req).Execute(PCtx->ActorSystem);
                break;
            case ERequestType::RequestShredPDisk:
                ProcessShredPDisk(static_cast<TShredPDisk&>(*req));
                break;
            case ERequestType::RequestPreShredCompactVDiskResult:
                ProcessPreShredCompactVDiskResult(static_cast<TPreShredCompactVDiskResult&>(*req));
                break;
            case ERequestType::RequestShredVDiskResult:
                ProcessShredVDiskResult(static_cast<TShredVDiskResult&>(*req));
                break;
            case ERequestType::RequestChunkShredResult:
                ProcessChunkShredResult(static_cast<TChunkShredResult&>(*req));
                break;
            case ERequestType::RequestContinueShred:
                ProcessContinueShred(static_cast<TContinueShred&>(*req));
                break;
            case ERequestType::RequestYardResize:
                YardResize(static_cast<TYardResize&>(*req));
                break;
            case ERequestType::RequestChangeExpectedSlotCount:
                ProcessChangeExpectedSlotCount(static_cast<TChangeExpectedSlotCount&>(*req));
                break;
            default:
                Y_FAIL_S(PCtx->PDiskLogPrefix << "Unexpected request type# " << TypeName(*req));
                break;
        }
    }
    FastOperationsQueue.clear();
}

void TPDisk::ProcessChunkForgetQueue() {
    if (JointChunkForgets.empty())
        return;

    for (auto& req : JointChunkForgets) {
        ChunkForget(*req);
    }

    JointChunkForgets.clear();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Drive info and write cache
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

void TPDisk::OnDriveStartup() {
    if (Cfg->UseSpdkNvmeDriver) {
        return;
    }
    TStringStream str;
    str << " OnDriveStartup ";

    DriveData = BlockDevice->GetDriveData();

    bool isChanged = false;
    switch (Cfg->WriteCacheSwitch) {
        case NKikimrBlobStorage::TPDiskConfig::Enable:
            if (DriveData.IsWriteCacheValid && !DriveData.IsWriteCacheEnabled) {
                BlockDevice->SetWriteCache(true);
                isChanged = true;
                str << " Attempted enabling the disabled Write Cache.";
            } else {
                str << " Write cache is not disabled, no need to enable it.";
            }
            break;
        case NKikimrBlobStorage::TPDiskConfig::ForceEnable:
            BlockDevice->SetWriteCache(true);
            isChanged = true;
            str << " Attempted force-enabling the Write Cache.";
            break;
        case NKikimrBlobStorage::TPDiskConfig::Disable:
            if (DriveData.IsWriteCacheValid && DriveData.IsWriteCacheEnabled) {
                BlockDevice->SetWriteCache(false);
                isChanged = true;
            } else {
                str << " Write cache is not enabled, no need to disable it.";
            }
            break;
        case NKikimrBlobStorage::TPDiskConfig::ForceDisable:
            BlockDevice->SetWriteCache(false);
            isChanged = true;
            str << "Attempted force-disabling the Write Cache.";
            break;
        default:
            break;
    }
    if (isChanged) {
        if (Cfg->GetDriveDataSwitch == NKikimrBlobStorage::TPDiskConfig::Enable ||
                Cfg->GetDriveDataSwitch == NKikimrBlobStorage::TPDiskConfig::ForceEnable) {
            DriveData = BlockDevice->GetDriveData();
        }
    }

    *Mon.DeviceWriteCacheIsValid = (DriveData.IsWriteCacheValid ? 1 : 0);
    *Mon.DeviceWriteCacheIsEnabled = (DriveData.IsWriteCacheEnabled ? 1 : 0);

    P_LOG(PRI_NOTICE, BPD38, str.Str(), (Path, Cfg->Path.Quote()));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Internal interface
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

bool TPDisk::Initialize() {

#define REGISTER_LOCAL_CONTROL(control) \
    dcb->RegisterLocalControl(control, \
            TStringBuilder() << "PDisk_" << PCtx->PDiskId << "_" << #control)

    if (!IsStarted) {
        if (PCtx->ActorSystem && PCtx->ActorSystem->AppData<TAppData>() && PCtx->ActorSystem->AppData<TAppData>()->Icb) {
            auto& dcb = PCtx->ActorSystem->AppData<TAppData>()->Dcb;
            auto& icb = PCtx->ActorSystem->AppData<TAppData>()->Icb;

            REGISTER_LOCAL_CONTROL(SlowdownAddLatencyNs);
            REGISTER_LOCAL_CONTROL(EnableForsetiBinLog);
            REGISTER_LOCAL_CONTROL(ForsetiMinLogCostNsControl);
            REGISTER_LOCAL_CONTROL(ForsetiMilliBatchSize);
            REGISTER_LOCAL_CONTROL(ForsetiMaxLogBatchNs);
            REGISTER_LOCAL_CONTROL(ForsetiOpPieceSizeSsd);
            REGISTER_LOCAL_CONTROL(ForsetiOpPieceSizeRot);
            TControlBoard::RegisterSharedControl(UseNoopSchedulerHDD, icb->PDiskControls.UseNoopSchedulerHDD);
            TControlBoard::RegisterSharedControl(UseNoopSchedulerSSD, icb->PDiskControls.UseNoopSchedulerSSD);
            REGISTER_LOCAL_CONTROL(ChunkBaseLimitPerMille);
            TControlBoard::RegisterSharedControl(SemiStrictSpaceIsolation, icb->PDiskControls.SemiStrictSpaceIsolation);

            if (Cfg->SectorMap) {
                auto diskModeParams = Cfg->SectorMap->GetDiskModeParams();
                if (diskModeParams) {
                    REGISTER_LOCAL_CONTROL(SectorMapFirstSectorReadRate);
                    REGISTER_LOCAL_CONTROL(SectorMapLastSectorReadRate);
                    REGISTER_LOCAL_CONTROL(SectorMapFirstSectorWriteRate);
                    REGISTER_LOCAL_CONTROL(SectorMapLastSectorWriteRate);
                    REGISTER_LOCAL_CONTROL(SectorMapSeekSleepMicroSeconds);

                    LastSectorReadRateControlName = TStringBuilder() << "PDisk_" << PCtx->PDiskId << "_SectorMapLastSectorReadRate";
                    LastSectorWriteRateControlName = TStringBuilder() << "PDisk_" << PCtx->PDiskId << "_SectorMapLastSectorWriteRate";
                }
                if (Cfg->SectorMap->GetFailureProbabilities()) {
                    REGISTER_LOCAL_CONTROL(SectorMapWriteErrorProbability);
                    REGISTER_LOCAL_CONTROL(SectorMapReadErrorProbability);
                    REGISTER_LOCAL_CONTROL(SectorMapSilentWriteFailProbability);
                    REGISTER_LOCAL_CONTROL(SectorMapReadReplayProbability);
                }
            }
        }

        Y_VERIFY_S(BlockDevice, PCtx->PDiskLogPrefix);
        BlockDevice->Initialize(PCtx);
        IsStarted = true;

        BufferPool = THolder<TBufferPool>(CreateBufferPool(Cfg->BufferPoolBufferSizeBytes, Cfg->BufferPoolBufferCount,
                UseHugePages, {Mon.DeviceBufferPoolFailedAllocations, PCtx->ActorSystem, PCtx->PDiskId}));

        P_LOG(PRI_INFO, BPD01, "PDisk initialized", (Cfg, Cfg->ToString()), (DriveModel, DriveModel.ToString()));
    }

    if (PDiskThread.Running()) {
        return true;
    }
    PDiskThread.Start();

    if (!BlockDevice->IsGood()) {
        *Mon.PDiskState = NKikimrBlobStorage::TPDiskState::OpenFileError;
        *Mon.PDiskBriefState = TPDiskMon::TPDisk::Error;
        TStringStream errStr;
        errStr << "Can't open file " << Cfg->GetDevicePath().Quote() << ": ";
        if (!Cfg->GetDevicePath() && Cfg->ExpectedSerial) {
            errStr << "no device with such serial was found";
            *Mon.PDiskDetailedState = TPDiskMon::TPDisk::ErrorNoDeviceWithSuchSerial;
        } else if (BlockDevice->GetLastErrno() == ENOENT) {
            errStr << "no such file.";
            *Mon.PDiskDetailedState = TPDiskMon::TPDisk::ErrorOpenNonexistentFile;
        } else if (BlockDevice->GetLastErrno() == EACCES) {
            errStr << "not enough rights.";
            *Mon.PDiskDetailedState = TPDiskMon::TPDisk::ErrorOpenFileWithoutPermissions;
        } else {
            errStr << "unknown reason, errno# " << BlockDevice->GetLastErrno() << ".";
            *Mon.PDiskDetailedState = TPDiskMon::TPDisk::ErrorOpenFileUnknown;
        }
        ErrorStr = errStr.Str();
        P_LOG(PRI_CRIT, BPD39," BlockDevice initialization error", (Details, ErrorStr));
        return false;
    }

    OnDriveStartup();

    if (!Cfg->CheckSerial(DriveData.SerialNumber)) {
        BlockDevice->Stop();

        TStringStream str;
        str << "serial number mismatch, expected# " << Cfg->ExpectedSerial.Quote()
            << " but got# " << DriveData.SerialNumber.Quote();
        P_LOG(PRI_WARN, BPD01, str.Str());
        ErrorStr = str.Str();

        *Mon.PDiskState = NKikimrBlobStorage::TPDiskState::OpenFileError;
        *Mon.PDiskBriefState = TPDiskMon::TPDisk::Error;
        *Mon.PDiskDetailedState = TPDiskMon::TPDisk::ErrorDeviceSerialMismatch;

        *Mon.SerialNumberMismatched = 1;
        return false;
    } else {
        *Mon.SerialNumberMismatched = 0;
    }

    if (Format.IsPlainDataChunks()) {
        P_LOG(PRI_NOTICE, BPD93, "PDisk starts with PlainDataChunks parameter enabled",
            (Format.IsPlainDataChunks(), Format.IsPlainDataChunks()), (Cfg->PlainDataChunks, Cfg->PlainDataChunks)
        );
    }
    if (Format.IsPlainDataChunks() ^ Cfg->PlainDataChunks) {
        P_LOG(PRI_WARN, BPD92, "PDisk's PlainDataChunks parameters mismatch, flag from Format will be used",
            (Format.IsPlainDataChunks(), Format.IsPlainDataChunks()), (Cfg->PlainDataChunks, Cfg->PlainDataChunks)
        );
    }

    return true;
#undef REGISTER_LOCAL_CONTROL
}

//////////////////////////////////// Forseti scheduler Gates ///////////////////////////////////////////////////////////
//    BytesW = Log + Fresh + Comp
//
//    Priority Class                  Queue       Weight
//----------------------------------------------------------------------------------------------------------------------
//                                    Log         LogWeight * BytesSchedulerWeight
//
//    case NPriWrite::HullFresh:      Fresh       FreshWeight * BytesSchedulerWeight
//
//    case NPriRead::HullComp:
//    case NPriWrite::HullComp:       Comp        CompWeight * BytesSchedulerWeight
//
//
//    case NPriRead::HullOnlineRt:    FastRead    FastReadWeight * BytesTotalWeight
//
//    case NPriRead::HullOnlineOther: OtherRead   OtherReadWeight * BytesTotalWeight
//
//    case NPriRead::HullLoad:        Load        LoadWeight * BytesTotalWeight
//
//
//    case NPriWrite::HullHuge:       Huge        HugeWeight * BytesTotalWeight
//
//    case NPriRead::SyncLog:
//    case NPriWrite::SyncLog:        SyncLog     SyncLogWeight * BytesTotalWeight
//
//    case NPriRead::HullLow:         LowRead     LowReadWeight
//
//                                    Trim
//                                    FastOperation
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

NKikimrProto::EReplyStatus TPDisk::ValidateRequest(TLogWrite *logWrite, TStringStream& outErr) {
    if (logWrite->Signature.HasCommitRecord() && logWrite->CommitRecord.FirstLsnToKeep > logWrite->Lsn) {
        outErr << " invalid commit record, CommitRecord.FirstLsnToKeep# " << logWrite->CommitRecord.FirstLsnToKeep
            << " > Lsn# " << logWrite->Lsn << " Marker# BPD73";
        return NKikimrProto::ERROR;
    }
    if (logWrite->Result && logWrite->Result->Status != NKikimrProto::OK) {
        return NKikimrProto::ERROR;
    }

    return NKikimrProto::OK;
}

void TPDisk::PrepareLogError(TLogWrite *logWrite, TStringStream& err, NKikimrProto::EReplyStatus status) {
    Y_VERIFY_DEBUG_S(status != NKikimrProto::OK, PCtx->PDiskLogPrefix);
    if (logWrite->Result && logWrite->Result->Status != NKikimrProto::OK) {
        return;
    }

    err << " error in TLogWrite for ownerId# " << logWrite->Owner << " ownerRound# " << logWrite->OwnerRound
        << " lsn# " << logWrite->Lsn;
    P_LOG(PRI_ERROR, BPD01, err.Str());

    logWrite->Result.Reset(new NPDisk::TEvLogResult(status,
        GetStatusFlags(logWrite->Owner, logWrite->OwnerGroupType), err.Str(),
        Keeper.GetLogChunkCount()));
    logWrite->Result->Results.push_back(NPDisk::TEvLogResult::TRecord(logWrite->Lsn, logWrite->Cookie));
}

NKikimrProto::EReplyStatus TPDisk::CheckOwnerAndRound(TRequestBase* req, TStringStream& err) {
    const auto& ownerData = OwnerData[req->Owner];

    if (!IsOwnerUser(req->Owner)) {
        if (req->Owner == OwnerUnallocated && req->OwnerRound == 0) {
            return NKikimrProto::OK;
        }
        err << "  ownerId# " << req->Owner << " < Begin# " << (ui32)OwnerBeginUser
            << " or >= End# " << (ui32)OwnerEndUser << " Marker# BPD72";
        return NKikimrProto::ERROR;
    } else if (ownerData.VDiskId == TVDiskID::InvalidId) {
        err << " request from unregistered ownerId# " << req->Owner;
        return NKikimrProto::INVALID_OWNER;
    } else if (ownerData.OwnerRound != req->OwnerRound) {
        err << " ownerId# " << req->Owner
            << " invalid OwnerRound, got# " << req->OwnerRound
            << " expected# " << ownerData.OwnerRound;
        return NKikimrProto::INVALID_ROUND;
    }

    return NKikimrProto::OK;
}

// Checks request validity, responds to and deletes non-valid requests
// Fills some important fields in request
// Must be called under StateMutex
// Returns is request valid and should be processed further
bool TPDisk::PreprocessRequest(TRequestBase *request) {
    TStringStream err;
    err << PCtx->PDiskLogPrefix;

    // Advisory check, further code may ignore results
    NKikimrProto::EReplyStatus errStatus = CheckOwnerAndRound(request, err);

    P_LOG(PRI_DEBUG, BPD01, "PreprocessRequest", (RequestType, TypeName(*request)), (OwnerId, request->Owner),
            (OwnerRound, request->OwnerRound), (errStatus, errStatus));

    auto readableAndWritable = [&](TChunkState& state) {
        return state.CommitState == TChunkState::DATA_RESERVED
            || state.CommitState == TChunkState::DATA_COMMITTED
            || state.CommitState == TChunkState::DATA_DECOMMITTED
            || state.CommitState == TChunkState::DATA_RESERVED_DECOMMIT_IN_PROGRESS
            || state.CommitState == TChunkState::DATA_COMMITTED_DECOMMIT_IN_PROGRESS;
    };

    switch (request->GetType()) {
        case ERequestType::RequestLogRead:
        {
            TLogRead &evLog = *static_cast<TLogRead*>(request);
            TOwnerData &ownerData = OwnerData[evLog.Owner];
            if (ownerData.HasReadTheWholeLog) {
                err << "Can't read log for ownerId# " << evLog.Owner
                    << " ownerRound# " << evLog.OwnerRound << ", owner has already read the log!";
                errStatus = NKikimrProto::ERROR; // -> NOT OK
            }

            if (errStatus != NKikimrProto::OK) {
                P_LOG(PRI_ERROR, BPD01, err.Str());
                auto result = MakeHolder<NPDisk::TEvReadLogResult>(errStatus, evLog.Position, evLog.Position, true,
                        GetStatusFlags(evLog.Owner, evLog.OwnerGroupType), err.Str(), evLog.Owner);
                PCtx->ActorSystem->Send(evLog.Sender, result.Release());
                Mon.LogRead.CountResponse();
                delete request;
                return false;
            }
            evLog.SetOwnerGroupType(ownerData.IsStaticGroupOwner());
            request->JobKind = NSchLab::JobKindRead;
            break;
        }
        case ERequestType::RequestLogReadContinue:
            request->JobKind = NSchLab::JobKindRead;
            break;
        case ERequestType::RequestLogSectorRestore:
            break;
        case ERequestType::RequestLogReadResultProcess:
            break;
        case ERequestType::RequestChunkRead:
        {
            Mon.QueueRequests->Dec();
            TIntrusivePtr<TChunkRead> read = std::move(static_cast<TChunkRead*>(request)->SelfPointer);
            *Mon.QueueBytes -= read->Size;

            if (errStatus != NKikimrProto::OK) {
                err << "ReqId# " << read->ReqId.Id;
                SendChunkReadError(read, err, errStatus);
                return false;
            }
            if (read->ChunkIdx >= ChunkState.size()) {
                err << "chunkIdx is too large (total# " << ChunkState.size() << ")";
                SendChunkReadError(read, err, NKikimrProto::ERROR);
                return false;
            }
            TChunkState &state = ChunkState[read->ChunkIdx];
            if (!IsOwnerUser(state.OwnerId)) {
                err << "chunk owned by the system";
                SendChunkReadError(read, err, NKikimrProto::ERROR);
                return false;
            }
            if (state.OwnerId != read->Owner) {
                err << "chunk's real ownerId# " << state.OwnerId;
                SendChunkReadError(read, err, NKikimrProto::ERROR);
                return false;
            }
            if (!readableAndWritable(state)) {
                err << "chunk has unexpected CommitState# " << state.CommitState;
                SendChunkReadError(read, err, NKikimrProto::ERROR);
                return false;
            }
            ui64 offset = 0;
            if (!ParseSectorOffset(Format, PCtx->ActorSystem, PCtx->PDiskId, read->Offset, read->Size,
                        read->FirstSector, read->LastSector, offset, PCtx->PDiskLogPrefix)) {
                err << "invalid size# " << read->Size << " and offset# " << read->Offset;
                SendChunkReadError(read, err, NKikimrProto::ERROR);
                return false;
            }
            TOwnerData &ownerData = OwnerData[request->Owner];
            read->SetOwnerGroupType(ownerData.IsStaticGroupOwner());
            ownerData.ReadThroughput.Increment(read->Size, PCtx->ActorSystem->Timestamp());
            request->JobKind = NSchLab::JobKindRead;
            read->ChunkEncrypted = !Format.IsPlainDataChunks();
            if (!read->ChunkEncrypted) {
                read->FirstSector = read->Offset / Format.SectorSize;
                read->LastSector = (read->Offset + read->Size + Format.SectorSize - 1) / Format.SectorSize - 1;
            }

            Y_VERIFY_S(read->FinalCompletion == nullptr, PCtx->PDiskLogPrefix);


            ++state.OperationsInProgress;
            ++ownerData.InFlight->ChunkReads;
            auto onDestroy = [&, inFlight = ownerData.InFlight]() {
                --state.OperationsInProgress;
                --inFlight->ChunkReads;
            };
            read->FinalCompletion = new TCompletionChunkRead(this, read, std::move(onDestroy), state.Nonce, PCtx->ActorSystem->GetRcBufAllocator());

            static_cast<TChunkRead*>(request)->SelfPointer = std::move(read);

            return true;
        }
        case ERequestType::RequestChunkWrite:
        {

            TChunkWrite &ev = *static_cast<TChunkWrite*>(request);
            TOwnerData &ownerData = OwnerData[ev.Owner];
            Mon.QueueRequests->Dec();
            const ui32 size = ev.PartsPtr ? ev.PartsPtr->ByteSize() : 0;
            *Mon.QueueBytes -= size;
            if (errStatus != NKikimrProto::OK) {
                SendChunkWriteError(ev, err.Str(), errStatus);
                delete request;
                return false;
            }
            if (!ev.PartsPtr) {
                err << Sprintf("Can't write chunkIdx# %" PRIu32 " with null PartsPtr for ownerId# %"
                        PRIu32, (ui32)ev.ChunkIdx, (ui32)ev.Owner);
                SendChunkWriteError(ev, err.Str(), NKikimrProto::ERROR);
                delete request;
                return false;
            }
            if (ev.Offset % Format.GetAppendBlockSize() != 0) {
                err << Sprintf("Can't write chunkIdx# %" PRIu32 " with not aligned offset# %" PRIu32 " ownerId# %"
                        PRIu32, ev.ChunkIdx, ev.Offset, (ui32)ev.Owner);
                SendChunkWriteError(ev, err.Str(), NKikimrProto::ERROR);
                delete request;
                return false;
            }
            if (ev.ChunkIdx > ChunkState.size()) {
                err << Sprintf("Can't write chunk: chunkIdx# %" PRIu32
                        " is too large (total# %" PRIu32 ") ownerId# %" PRIu32,
                        (ui32)ev.ChunkIdx, (ui32)ChunkState.size(), (ui32)ev.Owner);
                SendChunkWriteError(ev, err.Str(), NKikimrProto::ERROR);
                delete request;
                return false;
            }
            if (ev.ChunkIdx == 0) {
                TString allocError;
                TVector<TChunkIdx> chunks = AllocateChunkForOwner(request, 1, allocError);
                 if (chunks.empty()) {
                     err << allocError;
                     SendChunkWriteError(ev, err.Str(), NKikimrProto::OUT_OF_SPACE);
                     delete request;
                     return false;
                 } else {
                     Y_VERIFY_DEBUG_S(chunks.size() == 1, PCtx->PDiskLogPrefix);
                     ev.ChunkIdx = chunks.front();
                 }
            }
            TChunkState &state = ChunkState[ev.ChunkIdx];
            if (state.OwnerId != ev.Owner) {
                err << "Can't write chunkIdx# " << ev.ChunkIdx
                    << " chunk is owner by another owner."
                    << " chunk's owner# " << state.OwnerId
                    << " request's owner# " << ev.Owner;
                SendChunkWriteError(ev, err.Str(), NKikimrProto::ERROR);
                delete request;
                return false;
            }
            if (!IsOwnerUser(state.OwnerId)) {
                err << "Can't write chunkIdx# " << ev.ChunkIdx
                    << " destination chunk is owned by the system! ownerId# " << ev.Owner;
                SendChunkWriteError(ev, err.Str(), NKikimrProto::ERROR);
                delete request;
                return false;
            }
            if (!readableAndWritable(state)) {
                err << "Can't write chunkIdx# " << ev.ChunkIdx
                    << " destination chunk has CommitState# " << state.CommitState
                    << " ownerId# " << ev.Owner;
                SendChunkWriteError(ev, err.Str(), NKikimrProto::ERROR);
                delete request;
                return false;
            }

            ev.SetOwnerGroupType(ownerData.IsStaticGroupOwner());
            ownerData.WriteThroughput.Increment(ev.TotalSize, PCtx->ActorSystem->Timestamp());
            request->JobKind = NSchLab::JobKindWrite;
            ev.ChunkEncrypted = !Format.IsPlainDataChunks();

            auto result = std::make_unique<TEvChunkWriteResult>(NKikimrProto::OK, ev.ChunkIdx, ev.Cookie,
                        GetStatusFlags(ev.Owner, ev.OwnerGroupType), TString());

            ++state.OperationsInProgress;
            ++ownerData.InFlight->ChunkWrites;
            auto onDestroy = [&, inFlight = ownerData.InFlight]() {
                --state.OperationsInProgress;
                --inFlight->ChunkWrites;
            };
            ev.Completion = MakeHolder<TCompletionChunkWrite>(ev.Sender, result.release(), &Mon, PCtx->PDiskId,
                    ev.CreationTime, ev.TotalSize, ev.PriorityClass, std::move(onDestroy), ev.ReqId,
                    ev.Span.CreateChild(TWilson::PDiskBasic, "PDisk.CompletionChunkWrite"));
            ev.Completion->Parts = ev.PartsPtr;

            return true;
        }
        case ERequestType::RequestChunkReadRaw: {
            auto& ev = *static_cast<TChunkReadRaw*>(request);

            auto errorPrefix = [&] {
                return std::ref(err
                    << "Can't raw-read Owner# " << (int)ev.Owner
                    << " ChunkIdx# " << ev.ChunkIdx
                    << " Offset# " << ev.Offset
                    << " Size# " << ev.Size
                    << ": ");
            };

            if (ev.Offset % Format.SectorSize) {
                errorPrefix() << "unaligned offset";
            } else if (ev.Size % Format.SectorSize) {
                errorPrefix() << "unaligned size";
            } else if (!ev.Size) {
                errorPrefix() << "zero size";
            } else if (!ev.ChunkIdx || ChunkState.size() <= ev.ChunkIdx) {
                errorPrefix() << "incorrect chunk index";
            } else if (TChunkState& state = ChunkState[ev.ChunkIdx]; state.OwnerId != ev.Owner) {
                errorPrefix() << "chunk isn't owned";
            } else if (!IsOwnerUser(state.OwnerId)) {
                errorPrefix() << "chunk is owned by system";
            } else if (!readableAndWritable(state)) {
                errorPrefix() << "incorrect commit state";
            } else {
                NWilson::TTraceId traceId = ev.Span.GetTraceId();
                auto completion = std::make_unique<TCompletionChunkReadRaw>(ev.Size, ev.Sender, ev.Cookie, std::move(ev.Span));
                const ui64 diskOffset = Format.Offset(ev.ChunkIdx, 0, ev.Offset);
                void *buffer = completion->GetBuffer();
                BlockDevice->PreadAsync(buffer, ev.Size, diskOffset, completion.release(), ev.ReqId, &traceId);
                delete request;
                return false;
            }

            PCtx->ActorSystem->Send(ev.Sender, new TEvChunkReadRawResult(NKikimrProto::ERROR, err.Str()), ev.Cookie);
            delete request;
            return false;
        }
        case ERequestType::RequestChunkWriteRaw: {
            auto& ev = *static_cast<TChunkWriteRaw*>(request);

            auto errorPrefix = [&] {
                return std::ref(err
                    << "Can't raw-write Owner# " << (int)ev.Owner
                    << " ChunkIdx# " << ev.ChunkIdx
                    << " Offset# " << ev.Offset
                    << " Size# " << ev.Data.size()
                    << ": ");
            };

            if (ev.Offset % Format.SectorSize) {
                errorPrefix() << "unaligned offset";
            } else if (ev.Data.size() % Format.SectorSize) {
                errorPrefix() << "unaligned size";
            } else if (!ev.Data.size()) {
                errorPrefix() << "zero size";
            } else if (!ev.ChunkIdx || ChunkState.size() <= ev.ChunkIdx) {
                errorPrefix() << "incorrect chunk index";
            } else if (TChunkState& state = ChunkState[ev.ChunkIdx]; state.OwnerId != ev.Owner) {
                errorPrefix() << "chunk isn't owned";
            } else if (!IsOwnerUser(state.OwnerId)) {
                errorPrefix() << "chunk is owned by system";
            } else if (!readableAndWritable(state)) {
                errorPrefix() << "incorrect commit state";
            } else {
                TRcBuf buffer;
                auto iter = ev.Data.Begin();
                if (iter.ContiguousSize() == ev.Data.size() && reinterpret_cast<uintptr_t>(iter.ContiguousData()) % 4096 == 0) {
                    buffer = iter.GetChunk();
                } else {
                    buffer = TRcBuf::UninitializedPageAligned(ev.Data.size());
                    iter.ExtractPlainDataAndAdvance(buffer.GetDataMut(), buffer.size());
                }

                const auto span = buffer.GetContiguousSpan();
                NWilson::TTraceId traceId = ev.Span.GetTraceId();
                const ui64 diskOffset = Format.Offset(ev.ChunkIdx, 0, ev.Offset);

                auto completion = std::make_unique<TCompletionChunkWriteRaw>(std::move(buffer), ev.Sender, ev.Cookie, std::move(ev.Span));
                BlockDevice->PwriteAsync(span.data(), span.size(), diskOffset, completion.release(), ev.ReqId, &traceId);
                delete request;
                return false;
            }

            PCtx->ActorSystem->Send(ev.Sender, new TEvChunkWriteRawResult(NKikimrProto::ERROR, err.Str()), ev.Cookie);
            delete request;
            return false;
        }
        case ERequestType::RequestChunkTrim:
            request->JobKind = NSchLab::JobKindWrite;
            break;
        case ERequestType::RequestLogWrite:
        {
            auto *log = static_cast<TLogWrite*>(request);
            Mon.QueueRequests->Dec();
            *Mon.QueueBytes -= log->Data.size();
            log->InputTime = HPNow();
            request->JobKind = NSchLab::JobKindWrite;

            TOwnerData &ownerData = OwnerData[log->Owner];
            ++ownerData.InFlight->LogWrites;
            log->SetOnDestroy([inFlight = ownerData.InFlight]() {
                --inFlight->LogWrites;
            });

            if (errStatus == NKikimrProto::OK) {
                errStatus = ValidateRequest(log, err);
            }
            if (errStatus != NKikimrProto::OK) {
                log->SetOwnerGroupType(true);
                PrepareLogError(log, err, errStatus);
                break; // Reply even for invalid TLogWrite should be sent from completion thread, so allow request to pass further
            }

            log->SetOwnerGroupType(ownerData.IsStaticGroupOwner());
            ownerData.SetLastSeenLsn(log->Lsn);
            ownerData.WriteThroughput.Increment(log->Data.size(), PCtx->ActorSystem->Timestamp());
            break;
        }
        case ERequestType::RequestChunkForget:
        {
            auto *forget = static_cast<TChunkForget*>(request);
            TOwnerData &ownerData = OwnerData[forget->Owner];
            forget->SetOwnerGroupType(ownerData.IsStaticGroupOwner());
            forget->JobKind = NSchLab::JobKindWrite;
            break;
        }
        case ERequestType::RequestYardInit:
            break;
        case ERequestType::RequestCheckSpace:
        {
            TCheckSpace &ev = *static_cast<TCheckSpace*>(request);
            if (errStatus != NKikimrProto::OK) {
                P_LOG(PRI_ERROR, BPD01, err.Str());
                THolder<NPDisk::TEvCheckSpaceResult> result(new NPDisk::TEvCheckSpaceResult(errStatus,
                            GetStatusFlags(ev.Owner, ev.OwnerGroupType), 0, 0, 0, 0, 0u, err.Str()));
                PCtx->ActorSystem->Send(ev.Sender, result.Release());
                Mon.CheckSpace.CountResponse();
                delete request;
                return false;
            }
            ev.SetOwnerGroupType(OwnerData[ev.Owner].IsStaticGroupOwner());
            break;
        }
        case ERequestType::RequestHarakiri:
        {
            THarakiri &ev = *static_cast<THarakiri*>(request);
            if (errStatus != NKikimrProto::OK) {
                THolder<NPDisk::TEvHarakiriResult> result(new NPDisk::TEvHarakiriResult(errStatus,
                            GetStatusFlags(ev.Owner, ev.OwnerGroupType), err.Str()));
                PCtx->ActorSystem->Send(ev.Sender, result.Release());
                Mon.Harakiri.CountResponse();
                delete request;
                return false;
            }
            ev.SetOwnerGroupType(OwnerData[ev.Owner].IsStaticGroupOwner());
            break;
        }
        case ERequestType::RequestYardSlay:
        {
            TSlay &ev = *static_cast<TSlay*>(request);
            if (ev.VDiskId == TVDiskID::InvalidId) {
                P_LOG(PRI_ERROR, BPD01, err.Str());
                THolder<NPDisk::TEvSlayResult> result(new NPDisk::TEvSlayResult(NKikimrProto::ERROR,
                            GetStatusFlags(ev.Owner, ev.OwnerGroupType),
                            ev.VDiskId, ev.SlayOwnerRound, ev.PDiskId, ev.VSlotId, err.Str()));
                PCtx->ActorSystem->Send(ev.Sender, result.Release());
                Mon.YardSlay.CountResponse();
                delete request;
                return false;
            }
            break;
        }
        case ERequestType::RequestChunkReserve:
        {
            TChunkReserve &ev = *static_cast<TChunkReserve*>(request);

            if (errStatus != NKikimrProto::OK) {
                P_LOG(PRI_ERROR, BPD01, err.Str());
                THolder<NPDisk::TEvChunkReserveResult> result(new NPDisk::TEvChunkReserveResult(errStatus,
                            GetStatusFlags(ev.Owner, ev.OwnerGroupType), err.Str()));
                PCtx->ActorSystem->Send(ev.Sender, result.Release(), 0, ev.Cookie);
                Mon.ChunkReserve.CountResponse();
                delete request;
                return false;
            }

            ev.SetOwnerGroupType(OwnerData[ev.Owner].IsStaticGroupOwner());
            break;
        }
        case ERequestType::RequestChunkLock:
            break;
        case ERequestType::RequestChunkUnlock:
            break;
        case ERequestType::RequestYardControl:
        {
            TYardControl &ev = *static_cast<TYardControl*>(request);
            if (ev.Action == NPDisk::TEvYardControl::ActionPause) {
                PCtx->ActorSystem->Send(ev.Sender, new NPDisk::TEvYardControlResult(NKikimrProto::OK, ev.Cookie, TString()));
                Mon.YardControl.CountResponse();
                IsQueuePaused = true;
                delete request;
                return false; // OK
            }
            err << " Can't process yard control Action " << (ui32)ev.Action;
            P_LOG(PRI_ERROR, BPD01, err.Str());
            PCtx->ActorSystem->Send(ev.Sender, new NPDisk::TEvYardControlResult(NKikimrProto::ERROR, ev.Cookie, err.Str()));
            Mon.YardControl.CountResponse();
            delete request;
            return false;
        }
        case ERequestType::RequestAskForCutLog:
        case ERequestType::RequestConfigureScheduler:
        case ERequestType::RequestWhiteboartReport:
        case ERequestType::RequestHttpInfo:
        case ERequestType::RequestUndelivered:
        case ERequestType::RequestCommitLogChunks:
        case ERequestType::RequestLogCommitDone:
        case ERequestType::RequestTryTrimChunk:
        case ERequestType::RequestReleaseChunks:
        case ERequestType::RequestInitialReadMetadataResult:
        case ERequestType::RequestWriteMetadataResult:
        case ERequestType::RequestPushUnformattedMetadataSector:
        case ERequestType::RequestReadMetadata:
        case ERequestType::RequestWriteMetadata:
        case ERequestType::RequestContinueReadMetadata:
        case ERequestType::RequestShredPDisk:
        case ERequestType::RequestPreShredCompactVDiskResult:
        case ERequestType::RequestShredVDiskResult:
        case ERequestType::RequestChunkShredResult:
        case ERequestType::RequestContinueShred:
        case ERequestType::RequestYardResize:
        case ERequestType::RequestChangeExpectedSlotCount:
            break;
        case ERequestType::RequestStopDevice:
            BlockDevice->Stop();
            delete request;
            return false;
        case ERequestType::RequestChunkReadPiece:
        case ERequestType::RequestChunkWritePiece:
        case ERequestType::RequestNop:
            Y_ABORT_S(PCtx->PDiskLogPrefix);
    }
    return true;
}

void TPDisk::PushRequestToScheduler(TRequestBase *request) {
    if (request->GateId == GateFastOperation) {
        FastOperationsQueue.push_back(std::unique_ptr<TRequestBase>(request));
        LOG_DEBUG(*PCtx->ActorSystem, NKikimrServices::BS_PDISK, "PDiskId# %" PRIu32 " ReqId# %" PRIu64
                " PushRequestToScheduler Push to FastOperationsQueue.size# %" PRIu64,
                (ui32)PCtx->PDiskId, (ui64)request->ReqId.Id, (ui64)FastOperationsQueue.size());
        return;
    }

    if (request->GetType() == ERequestType::RequestChunkWrite) {
        TIntrusivePtr<TChunkWrite> whole(static_cast<TChunkWrite*>(request));

        const ui32 jobSizeLimit = whole->ChunkEncrypted
            ? ui64(ForsetiOpPieceSizeCached) * Format.SectorPayloadSize() / Format.SectorSize
            : whole->TotalSize;
        const ui32 jobCount = (whole->TotalSize + jobSizeLimit - 1) / jobSizeLimit;

        ui32 remainingSize = whole->TotalSize;
        for (ui32 idx = 0; idx < jobCount; ++idx) {
            auto span = request->Span.CreateChild(TWilson::PDiskDetailed, "PDisk.ChunkWritePiece", NWilson::EFlags::AUTO_END);
            span.Attribute("small_job_idx", idx)
                .Attribute("is_last_piece", idx == jobCount - 1);
            ui32 jobSize = Min(remainingSize, jobSizeLimit);
            TChunkWritePiece *piece = new TChunkWritePiece(whole, idx * jobSizeLimit, jobSize, std::move(span));
            piece->GateId = whole->GateId;
            piece->EstimateCost(DriveModel);
            AddJobToScheduler(piece, request->JobKind);
            remainingSize -= jobSize;
        }
        Y_VERIFY_S(remainingSize == 0, PCtx->PDiskLogPrefix << "remainingSize# " << remainingSize);
    } else if (request->GetType() == ERequestType::RequestChunkRead) {
        TIntrusivePtr<TChunkRead> read = std::move(static_cast<TChunkRead*>(request)->SelfPointer);
        ui32 totalSectors = read->LastSector - read->FirstSector + 1;

        Y_VERIFY_DEBUG_S(ForsetiOpPieceSizeCached % Format.SectorSize == 0, PCtx->PDiskLogPrefix);
        const ui32 jobSizeLimit = read->ChunkEncrypted
            ? ForsetiOpPieceSizeCached / Format.SectorSize
            : totalSectors;
        const ui32 jobCount = (totalSectors + jobSizeLimit - 1) / jobSizeLimit;
        Y_VERIFY(read->ChunkEncrypted || jobCount == 1);
        for (ui32 idx = 0; idx < jobCount; ++idx) {
            bool isLast = idx == jobCount - 1;

            ui32 jobSize = Min(totalSectors, jobSizeLimit);
            auto piece = new TChunkReadPiece(read, idx * jobSizeLimit, jobSize * Format.SectorSize, isLast);
            piece->GateId = read->GateId;
            read->Orbit.Fork(piece->Orbit);
            P_LOG(PRI_INFO, BPD01, "PDiskChunkReadPieceAddToScheduler", (idx, idx), (jobSizeLimit, jobSizeLimit),
                (jobSize, jobSize), (totalSectors, totalSectors), (FistSector, read->FirstSector), (LastSector, read->LastSector));
            LWTRACK(PDiskChunkReadPieceAddToScheduler, piece->Orbit, PCtx->PDiskId, idx, idx * jobSizeLimit * Format.SectorSize,
                    jobSizeLimit * Format.SectorSize);
            piece->EstimateCost(DriveModel);
            piece->SelfPointer = piece;
            AddJobToScheduler(piece, request->JobKind);
            totalSectors -= jobSize;
        }
        Y_VERIFY_S(totalSectors == 0, PCtx->PDiskLogPrefix << "totalSectors# " << totalSectors);
    } else {
        AddJobToScheduler(request, request->JobKind);
    }
}

void TPDisk::AddJobToScheduler(TRequestBase *request, NSchLab::EJobKind jobKind) {
    if (UseNoopSchedulerCached) {
        RouteRequest(request);
        LWTRACK(PDiskAddToNoopScheduler, request->Orbit, PCtx->PDiskId, request->ReqId.Id, HPSecondsFloat(request->CreationTime),
                request->Owner, request->IsFast, request->PriorityClass);
        return;
    }

    // Forseti part
    NSchLab::TCbs *cbs = ForsetiScheduler.GetCbs(request->Owner, request->GateId);
    if (!cbs) {
        if (request->Owner == OwnerUnallocated && request->OwnerRound == 0) {
            // it's ok
        } else {
            P_LOG(PRI_ERROR, BPD44, "PushRequestToScheduler Can't push to Forseti! Trying system log gate",
                        (ReqId, request->ReqId),
                        (Cost, request->Cost),
                        (JobKind, (ui64)request->JobKind),
                        (ownerId, request->Owner),
                        (GateId, (ui64)request->GateId));
            Mon.ForsetiCbsNotFound->Inc();
        }
        ui8 originalGateId = request->GateId;
        request->GateId = GateLog;
        cbs = ForsetiScheduler.GetCbs(OwnerSystem, request->GateId);
        if (!cbs) {
            TStringStream str;
            str << PCtx->PDiskLogPrefix
                << "ReqId# " <<  request->ReqId
                << " Cost# " << request->Cost
                << " JobKind# " << (ui64)request->JobKind
                << " ownerId# " << request->Owner
                << " GateId# " << (ui64)request->GateId
                << " originalGateId# " << (ui64)originalGateId
                << " PushRequestToScheduler Can't push to Forseti! Request may get lost."
                << " Marker# BPD45";
            Y_FAIL_S(str.Str());
        }
    }

    if (request->GetType() == ERequestType::RequestLogWrite) {
        TIntrusivePtr<NSchLab::TJob> job = cbs->PeekTailJob();
        // try to glue new log write to the previuos write which is already inside the scheduler
        if (job && job->Cost < ForsetiMaxLogBatchNsCached
                && static_cast<TRequestBase *>(job->Payload)->GetType() == ERequestType::RequestLogWrite) {
            TLogWrite &batch = *static_cast<TLogWrite*>(job->Payload);

            request->Span.Event("PDisk.InScheduler.InLogWriteBatch", {
                {"Batch.ReqId", static_cast<i64>(batch.ReqId.Id)}
            });
            batch.AddToBatch(static_cast<TLogWrite*>(request));
            ui64 prevCost = job->Cost;
            job->Cost += request->Cost;

            ForsetiTimeNs++;
            ForsetiScheduler.OnJobCostChange(cbs, job, ForsetiTimeNs, prevCost);

            P_LOG(PRI_DEBUG, BPD01, "LogWrite add to batch in scheduler", (prevCost , prevCost), (reqCost, request->Cost));
            return;
        }
    }
    LWTRACK(PDiskAddToScheduler, request->Orbit, PCtx->PDiskId, request->ReqId.Id, HPSecondsFloat(request->CreationTime),
            request->Owner, request->IsFast, request->PriorityClass);
    request->Span.Event("PDisk.InScheduler");
    TIntrusivePtr<NSchLab::TJob> job = ForsetiScheduler.CreateJob();
    job->Payload = request;
    job->Cost = request->Cost;
    job->JobKind = jobKind;
    ForsetiTimeNs++;
    ForsetiScheduler.AddJob(cbs, job, request->Owner, request->GateId, ForsetiTimeNs);
    P_LOG(PRI_DEBUG, BPD84, "Add job to scheduler", (type, TypeName(*request)) , (reqCost, request->Cost),
        (ForsetiTimeNs, ForsetiTimeNs) , (owner, request->Owner) , (gateId, (ui64)request->GateId) , (jobKind, (ui64)jobKind));
}

void TPDisk::RouteRequest(TRequestBase *request) {
    LOG_DEBUG(*PCtx->ActorSystem, NKikimrServices::BS_PDISK, "PDiskId# %" PRIu32 " ReqId# %" PRIu64 " Type# %" PRIu64
            " RouteRequest",
            (ui32)PCtx->PDiskId, (ui64)request->ReqId.Id, (ui64)request->GetType());

    LWTRACK(PDiskRouteRequest, request->Orbit, PCtx->PDiskId, request->ReqId.Id, HPSecondsFloat(request->CreationTime),
            request->Owner, request->IsFast, request->PriorityClass);

    switch (request->GetType()) {
        case ERequestType::RequestLogRead:
            [[fallthrough]];
        case ERequestType::RequestLogReadContinue:
            [[fallthrough]];
        case ERequestType::RequestLogReadResultProcess:
            [[fallthrough]];
        case ERequestType::RequestLogSectorRestore:
            JointLogReads.emplace_back(request);
            break;
        case ERequestType::RequestChunkReadPiece:
        {
            TChunkReadPiece *piece = static_cast<TChunkReadPiece*>(request);
            JointChunkReads.emplace(piece->SelfPointer.Get());
            piece->SelfPointer.Reset();
            // FIXME(cthulhu): Unreserve() for TChunkReadPiece is called while processing to avoid requeueing issues
            break;
        }
        case ERequestType::RequestChunkWritePiece:
            JointChunkWrites.push(request);
            break;
        case ERequestType::RequestChunkTrim:
        {
            TChunkTrim *trim = static_cast<TChunkTrim*>(request);
            JointChunkTrims.push_back(trim);
            break;
        }
        case ERequestType::RequestLogWrite:
        {
            TLogWrite *log = static_cast<TLogWrite*>(request);
            while (log) {
                TLogWrite *batch = log->PopFromBatch();

                JointLogWrites.push(log);
                log = batch;
            }
            break;
        }
        case ERequestType::RequestChunkForget:
        {
            TChunkForget *forget = static_cast<TChunkForget*>(request);
            JointChunkForgets.push_back(std::unique_ptr<TChunkForget>(forget));
            break;
        }
        default:
            FastOperationsQueue.push_back(std::unique_ptr<TRequestBase>(request));
            LOG_DEBUG(*PCtx->ActorSystem, NKikimrServices::BS_PDISK, "PDiskId# %" PRIu32 " ReqId# %" PRIu64
                    " PushRequestToScheduler Push to FastOperationsQueue.size# %" PRIu64,
                    (ui32)PCtx->PDiskId, (ui64)request->ReqId.Id, (ui64)FastOperationsQueue.size());
            break;
    }
}

void TPDisk::ProcessPausedQueue() {
    while (!PausedQueue.empty()) {
        if (PausedQueue.front()->GetType() == ERequestType::RequestYardControl) {
            TYardControl *ev = static_cast<TYardControl*>(PausedQueue.front());
            PausedQueue.pop_front();

            if (ev->Action == NPDisk::TEvYardControl::ActionPause) {
                if (PreprocessRequest(ev)) {
                    Y_ABORT_S(PCtx->PDiskLogPrefix);
                }
                break;
            }
            if (ev->Action == NPDisk::TEvYardControl::ActionResume) {
                PCtx->ActorSystem->Send(ev->Sender, new NPDisk::TEvYardControlResult(NKikimrProto::OK, ev->Cookie,
                            TString()));
                Mon.YardControl.CountResponse();
                delete ev;
                continue;
            }
            if (ev->Action == NPDisk::TEvYardControl::ActionStep) {
                PCtx->ActorSystem->Send(ev->Sender, new NPDisk::TEvYardControlResult(NKikimrProto::OK, ev->Cookie,
                            TString()));
                Mon.YardControl.CountResponse();
                delete ev;
                IsQueueStep = true;
                IsQueuePaused = true;
                continue;
            }
        } else {
            TRequestBase *ev = PausedQueue.front();
            PausedQueue.pop_front();
            if (PreprocessRequest(ev)) {
                PushRequestToScheduler(ev);
            }
        }
    }
}

void TPDisk::ProcessYardInitSet() {
    for (ui32 owner = 0; owner < OwnerData.size(); ++owner) {
        TOwnerData &data = OwnerData[owner];
        if (data.LogReader) {
            TGuard<TMutex> guard(StateMutex);
            if (data.LogReader && data.LogReader->GetIsReplied()) {
                data.LogReader = nullptr;
            }
        }
    }

    if (!PendingYardInits.empty()) {
        TGuard<TMutex> guard(StateMutex);
        // Process pending queue
        for (auto it = PendingYardInits.begin(); it != PendingYardInits.end();) {
            if (!OwnerData[(*it)->Owner].HaveRequestsInFlight()) {
                YardInitFinish(**it);
                it = PendingYardInits.erase(it);
            } else {
                ++it;
            }
        }
    }
    *Mon.PendingYardInits = PendingYardInits.size();
}

void TPDisk::EnqueueAll() {
    auto start = TMonotonic::Now();

    TGuard<TMutex> guard(StateMutex);
    size_t initialQueueSize = InputQueue.GetWaitingSize();
    size_t processedReqs = 0;
    size_t pushedToSchedulerReqs = 0;


    while (InputQueue.GetWaitingSize() > 0) {
        TRequestBase* request = InputQueue.Pop();

        if (Cfg->ReadOnly && HandleReadOnlyIfWrite(request)) {
            LOG_DEBUG(*PCtx->ActorSystem, NKikimrServices::BS_PDISK, "PDiskId# %" PRIu32 " ReqId# %" PRIu64
                " got write request in ReadOnly mode type# %" PRIu64,
                (ui32)PCtx->PDiskId, (ui64)request->ReqId.Id, (ui32)request->GetType());

            delete request;
            return;
        }

        P_LOG(PRI_TRACE, BPD83, "EnqueueAll, pop from InputQueue", (requestType, TypeName(*request)), (alreadyProcessedReqs, processedReqs));
        AtomicSub(InputQueueCost, request->Cost);
        if (IsQueuePaused) {
            if (IsQueueStep) {
                IsQueueStep = false;
                PausedQueue.push_back(request);
                IsQueuePaused = false;
                ProcessPausedQueue();
            }
            if (request->GetType() == ERequestType::RequestYardControl) {
                TYardControl &evControl = *static_cast<TYardControl*>(request);
                switch (evControl.Action) {
                    case NPDisk::TEvYardControl::ActionPause:
                        PausedQueue.push_back(request);
                        break;
                    case NPDisk::TEvYardControl::ActionStep:
                        PausedQueue.push_back(request);
                        PCtx->ActorSystem->Send(evControl.Sender, new NPDisk::TEvYardControlResult(
                            NKikimrProto::OK, evControl.Cookie, TString()));
                        Mon.YardControl.CountResponse();
                        break;
                    case NPDisk::TEvYardControl::ActionResume:
                    {
                        PCtx->ActorSystem->Send(evControl.Sender, new NPDisk::TEvYardControlResult(
                            NKikimrProto::OK, evControl.Cookie, TString()));
                        Mon.YardControl.CountResponse();
                        delete request;

                        IsQueuePaused = false;
                        ProcessPausedQueue();
                        break;
                    }
                    default:
                        break;
                }
            } else {
                PausedQueue.push_back(request);
            }
        } else {
            if (PreprocessRequest(request)) {
                PushRequestToScheduler(request);
                ++pushedToSchedulerReqs;
            }
        }
        ++processedReqs;
        if (processedReqs >= MAX_REQS_PER_CYCLE) {
            break;
        }
    }

    double spentTimeMs = (TMonotonic::Now() - start).MillisecondsFloat();
    LWTRACK(PDiskEnqueueAllDetails, UpdateCycleOrbit, PCtx->PDiskId, initialQueueSize, processedReqs, pushedToSchedulerReqs, spentTimeMs);
}

void TPDisk::GetJobsFromForsetti() {
    // Prepare
    UpdateMinLogCostNs();
    ui64 milliBatchSize = ForsetiMilliBatchSize;
    // Schedule
    ui64 nowCycles = HPNow();
    ui64 prevForsetiTimeNs = ForsetiTimeNs;
    ui64 realDuration = HPNanoSeconds(nowCycles - ForsetiRealTimeCycles);
    ui64 virtualDuration = ForsetiTimeNs - ForsetiPrevTimeNs;
    ui64 timeCorrection = 0;

    if (virtualDuration < realDuration) {
        // Correct virtual time to catch up with real time.
        timeCorrection = realDuration - virtualDuration;
        // Fix time correction in case we have had seeks before writing log.
        timeCorrection = LogSeekCostLoop.TryPop(timeCorrection);
        // Apply correction.
        ForsetiTimeNs += timeCorrection;
    }

    // Millibatch size in terms of virtual time
    ui64 maxDuration = DriveModel.TimeForSizeNs(milliBatchSize, TDriveModel::OP_TYPE_AVG);
    ui64 virtualDeadline = ForsetiTimeNs + maxDuration;

    // Save time at with millibatch starts
    ForsetiPrevTimeNs = ForsetiTimeNs;

    // Select and complete jobs for the whole millibatch
    ui64 totalLogCost = 0;
    ui64 totalNonLogCost = 0;
    ui64 totalLogReqs = 0;
    ui64 totalNonLogReqs = 0;

    while (ForsetiTimeNs <= virtualDeadline) {
        ForsetiTimeNs++;
        TIntrusivePtr<NSchLab::TJob> job = ForsetiScheduler.SelectJob(ForsetiTimeNs);
        if (job) {
            TRequestBase* req = static_cast<TRequestBase*>(job->Payload);

            // Slowdown requests (ACHTUNG!)
            if (const ui64 addLatencyNs = SlowdownAddLatencyNs) {
                while (true) {
                    i64 now = HPNow();
                    if (!req || req->CreationTime <= 0 || req->CreationTime >= now) {
                        break;
                    }
                    ui64 duration = HPNanoSeconds(now - req->CreationTime);
                    if (addLatencyNs <= duration) {
                        break;
                    }
                    ui64 toWaitNs = addLatencyNs - duration;
                    const ui64 OneSecondNs = 1000000000ll;
                    if (toWaitNs < OneSecondNs) {
                        NanoSleep(toWaitNs);
                        break;
                    }
                    NanoSleep(OneSecondNs);
                }
            }

            RouteRequest(req);
            ForsetiTimeNs += job->Cost;
            if (req->GetType() == ERequestType::RequestLogWrite) {
                totalLogCost += job->Cost;
                totalLogReqs++;
            } else {
                totalNonLogCost += job->Cost;
                totalNonLogReqs++;
            }
            ForsetiTimeNs++;
            ForsetiScheduler.CompleteJob(ForsetiTimeNs, job);
            P_LOG(PRI_TRACE, BPD01, "forsetti CompleteJob", (requestType, TypeName(*req)), (ForsetiTimeNs, ForsetiTimeNs));
        } else {
            break;
        }
    }
    ui64 totalCost = totalLogCost + totalNonLogCost;
    LWTRACK(PDiskForsetiCycle, UpdateCycleOrbit, PCtx->PDiskId, nowCycles, prevForsetiTimeNs, ForsetiPrevTimeNs, timeCorrection,
            realDuration, virtualDuration, ForsetiTimeNs, totalCost, virtualDeadline);
    LWTRACK(PDiskMilliBatchSize, UpdateCycleOrbit, PCtx->PDiskId, totalLogCost, totalNonLogCost, totalLogReqs, totalNonLogReqs);
    ForsetiRealTimeCycles = nowCycles;
    P_LOG(PRI_TRACE, BPD82, "got requests from forsetti", (totalLogReqs, totalLogReqs), (totalChunkReqs, totalNonLogReqs));
}

void TPDisk::Update() {
    Mon.UpdateDurationTracker.UpdateStarted();
    LWTRACK(PDiskUpdateStarted, UpdateCycleOrbit, PCtx->PDiskId);

    {
        TGuard<TMutex> guard(StateMutex);

        ForsetiMaxLogBatchNsCached = ForsetiMaxLogBatchNs;
        ForsetiOpPieceSizeCached = PDiskCategory.IsSolidState() ? ForsetiOpPieceSizeSsd : ForsetiOpPieceSizeRot;
        ForsetiOpPieceSizeCached = Min<i64>(ForsetiOpPieceSizeCached, Cfg->BufferPoolBufferSizeBytes);
        ForsetiOpPieceSizeCached = AlignDown<i64>(ForsetiOpPieceSizeCached, Format.SectorSize);

        auto prev = UseNoopSchedulerCached;
        UseNoopSchedulerCached = PDiskCategory.IsSolidState() ? UseNoopSchedulerSSD : UseNoopSchedulerHDD;
        // if we are going to start using noop scheduler then drain Forseti scheduler
        if (!prev && UseNoopSchedulerCached) {
            while (!ForsetiScheduler.IsEmpty()) {
                GetJobsFromForsetti();
            }
        }

        using TColor = NKikimrBlobStorage::TPDiskSpaceColor;
        if (i64 currentIsolation = SemiStrictSpaceIsolation; currentIsolation != SemiStrictSpaceIsolationCached) {
            TColor::E colorBorder = GetColorBorderIcb();
            Keeper.SetColorBorder(colorBorder);
            SemiStrictSpaceIsolationCached = currentIsolation;
        }

        // Switch the scheduler when possible
        ForsetiScheduler.SetIsBinLogEnabled(EnableForsetiBinLog);

        // Make input queue empty
        EnqueueAll();
    }

    // Make token injection to correct drive model underestimations and avoid disk underutilization

    Mon.UpdateDurationTracker.SchedulingStart();

    // Schedule using Forseti Scheduler
    if (!UseNoopSchedulerCached) {
        GetJobsFromForsetti();
    }

    // Processing
    bool isNonLogWorkloadPresent = !JointChunkWrites.empty() || !FastOperationsQueue.empty() ||
            !JointChunkReads.empty() || !JointLogReads.empty() || !JointChunkTrims.empty() || !JointChunkForgets.empty();
    bool isLogWorkloadPresent = !JointLogWrites.empty();
    bool isNothingToDo = true;
    if (isLogWorkloadPresent || isNonLogWorkloadPresent) {
        isNothingToDo = false;
    }
    ++UpdateIdx;

    // We are doing a 2-tact work cycle here:
    //
    // Chunk Log | Log Chunk | Chunk Log | Log Log | Log Chunk | Chunk Chunk | Chunk Log
    // case CL     case LC     case CL     case LL   case LC     case CC       case CL

    bool isLogSeekExpected = false;
    ETact tact = LastTact;
    if (isLogWorkloadPresent) {
        if (isNonLogWorkloadPresent) {
            // LC follows CL, LL
            // CL follows LC, CC
            if (LastTact == ETact::TactCl || LastTact == ETact::TactLl) {
                tact = ETact::TactLc;
            } else {
                tact = ETact::TactCl;
                isLogSeekExpected = true;
            }
        } else {
            // LL
            tact = ETact::TactLl;
            if (LastTact == ETact::TactCc || LastTact == ETact::TactLc) {
                isLogSeekExpected = true;
            }
        }
    } else {
        if (isNonLogWorkloadPresent) {
            // CC
            tact = ETact::TactCc;
        }
        // else nothing at all
    }

    if (!isNothingToDo) {
        ui64 logSeekCostNs = 0;
        if (isLogSeekExpected) {
            logSeekCostNs += DriveModel.SeekTimeNs();
        }
        LogSeekCostLoop.Push(logSeekCostNs);
    }

    Mon.UpdateDurationTracker.ProcessingStart();

    ClearQuarantineChunks();

    if (UseNoopSchedulerCached) {
        ProcessLogWriteQueue();
        ProcessChunkReadQueue();
        ProcessChunkWriteQueue();

        ProcessFastOperationsQueue();
        ProcessLogReadQueue();
        ProcessChunkTrimQueue();
    } else {
        if (tact == ETact::TactLc) {
            ProcessLogWriteQueue();
        }
        ProcessChunkWriteQueue();
        ProcessFastOperationsQueue();
        ProcessChunkReadQueue();
        ProcessLogReadQueue();
        ProcessChunkTrimQueue();
        if (tact != ETact::TactLc) {
            ProcessLogWriteQueue();
        }
    }
    ProcessChunkForgetQueue();
    LastTact = tact;

    ProcessYardInitSet();

    Mon.UpdateDurationTracker.WaitingStart(isNothingToDo);
    LWTRACK(PDiskStartWaiting, UpdateCycleOrbit, PCtx->PDiskId);

    if (Cfg->SectorMap) {
        auto diskModeParams = Cfg->SectorMap->GetDiskModeParams();
        if (diskModeParams) {
            TAtomic prevValue;

            diskModeParams->FirstSectorReadRate.store(SectorMapFirstSectorReadRate);
            if (SectorMapFirstSectorReadRate < SectorMapLastSectorReadRate) {
                PCtx->ActorSystem->AppData<TAppData>()->Dcb->SetValue(LastSectorReadRateControlName, SectorMapFirstSectorReadRate, prevValue);
                diskModeParams->LastSectorReadRate.store(SectorMapFirstSectorReadRate);
            } else {
                diskModeParams->LastSectorReadRate.store(SectorMapLastSectorReadRate);
            }

            diskModeParams->FirstSectorWriteRate.store(SectorMapFirstSectorWriteRate);
            if (SectorMapFirstSectorWriteRate < SectorMapLastSectorWriteRate) {
                PCtx->ActorSystem->AppData<TAppData>()->Dcb->SetValue(LastSectorWriteRateControlName, SectorMapFirstSectorWriteRate, prevValue);
                diskModeParams->LastSectorWriteRate.store(SectorMapFirstSectorWriteRate);
            } else {
                diskModeParams->LastSectorWriteRate.store(SectorMapLastSectorWriteRate);
            }

            diskModeParams->SeekSleepMicroSeconds.store(SectorMapSeekSleepMicroSeconds);
        }
        auto failureProbs = Cfg->SectorMap->GetFailureProbabilities();
        if (failureProbs) {
            failureProbs->WriteErrorProbability.store(SectorMapWriteErrorProbability / 1000000000.0);
            failureProbs->ReadErrorProbability.store(SectorMapReadErrorProbability / 1000000000.0);
            failureProbs->SilentWriteFailProbability.store(SectorMapSilentWriteFailProbability / 1000000000.0);
            failureProbs->ReadReplayProbability.store(SectorMapReadReplayProbability / 1000000000.0);

            auto& sectorMap = *Cfg->SectorMap;
            *Mon.EmulatedWriteErrors = sectorMap.EmulatedWriteErrors.load(std::memory_order_relaxed);
            *Mon.EmulatedReadErrors = sectorMap.EmulatedReadErrors.load(std::memory_order_relaxed);
            *Mon.EmulatedSilentWriteFails = sectorMap.EmulatedSilentWriteFails.load(std::memory_order_relaxed);
            *Mon.EmulatedReadReplays = sectorMap.EmulatedReadReplays.load(std::memory_order_relaxed);
        }
    }

    // Wait for something to do
    if (isNothingToDo && InputQueue.GetWaitingSize() == 0 && ForsetiScheduler.IsEmpty()) {
        // use deadline to be able to wakeup in situation of pdisk destruction
        InputQueue.ProducedWait(TDuration::MilliSeconds(10));
    }

    auto entireUpdateMs = Mon.UpdateDurationTracker.UpdateEnded();
    LWTRACK(PDiskUpdateEnded, UpdateCycleOrbit, PCtx->PDiskId, entireUpdateMs );
    UpdateCycleOrbit.Reset();
    *Mon.PDiskThreadCPU = ThreadCPUTime();
}

void TPDisk::UpdateMinLogCostNs() {
    ui64 cost = ForsetiMinLogCostNsControl;
    if (cost != ForsetiMinLogCostNs) {
        ForsetiMinLogCostNs = cost;
        for (ui32 ownerId = 0; ownerId < OwnerCount; ++ownerId) {
            NSchLab::TCbs *cbs = ForsetiScheduler.GetCbs(ownerId, GateLog);
            if (cbs) {
                cbs->MaxBudget = ForsetiMinLogCostNs;
            }
        }
    }
}

// Handles write requests (only in read-only mode). Returns true, if request is a write request.
bool TPDisk::HandleReadOnlyIfWrite(TRequestBase *request) {
    const TActorId& sender = request->Sender;
    TString errorReason = "PDisk is in read-only mode";

    switch (request->GetType()) {
        // Reads and other operations that can be processed in read-only mode.
        case ERequestType::RequestLogRead:
        case ERequestType::RequestLogReadContinue:
        case ERequestType::RequestLogReadResultProcess:
        case ERequestType::RequestLogSectorRestore:
        case ERequestType::RequestChunkRead:
        case ERequestType::RequestChunkReadPiece:
        case ERequestType::RequestYardInit:
        case ERequestType::RequestCheckSpace:
        case ERequestType::RequestYardControl:
        case ERequestType::RequestWhiteboartReport:
        case ERequestType::RequestHttpInfo:
        case ERequestType::RequestStopDevice:
        case ERequestType::RequestReadMetadata:
        case ERequestType::RequestInitialReadMetadataResult:
        case ERequestType::RequestUndelivered:
        case ERequestType::RequestNop:
        case ERequestType::RequestConfigureScheduler:
        case ERequestType::RequestPushUnformattedMetadataSector:
        case ERequestType::RequestContinueReadMetadata:
        case ERequestType::RequestYardResize:
        case ERequestType::RequestChangeExpectedSlotCount:
        case ERequestType::RequestChunkReadRaw:
            return false;

        // Can't be processed in read-only mode.
        case ERequestType::RequestLogWrite: {
            TLogWrite &req = *static_cast<TLogWrite*>(request);
            NPDisk::TEvLogResult* result = new NPDisk::TEvLogResult(NKikimrProto::CORRUPTED, 0, errorReason, 0);
            result->Results.push_back(NPDisk::TEvLogResult::TRecord(req.Lsn, req.Cookie));
            PCtx->ActorSystem->Send(sender, result);
            req.Replied = true;
            return true;
        }
        case ERequestType::RequestChunkWrite: {
            TChunkWrite &req = *static_cast<TChunkWrite*>(request);
            SendChunkWriteError(req, errorReason, NKikimrProto::CORRUPTED);
            return true;
        }
        case ERequestType::RequestChunkReserve:
            PCtx->ActorSystem->Send(sender, new NPDisk::TEvChunkReserveResult(NKikimrProto::CORRUPTED, 0, errorReason), 0, request->Cookie);
            return true;
        case ERequestType::RequestChunkLock:
            PCtx->ActorSystem->Send(sender, new NPDisk::TEvChunkLockResult(NKikimrProto::CORRUPTED, {}, 0, errorReason));
            return true;
        case ERequestType::RequestChunkUnlock:
            PCtx->ActorSystem->Send(sender, new NPDisk::TEvChunkUnlockResult(NKikimrProto::CORRUPTED, 0, errorReason));
            return true;
        case ERequestType::RequestChunkForget:
            PCtx->ActorSystem->Send(sender, new NPDisk::TEvChunkForgetResult(NKikimrProto::CORRUPTED, 0, errorReason));
            return true;
        case ERequestType::RequestHarakiri:
            PCtx->ActorSystem->Send(sender, new NPDisk::TEvHarakiriResult(NKikimrProto::CORRUPTED, 0, errorReason));
            return true;
        case ERequestType::RequestYardSlay: {
            TSlay &req = *static_cast<TSlay*>(request);
            // We send NOTREADY, since BSController can't handle CORRUPTED or ERROR.
            // If for some reason the disk will become *not* read-only, the request will be retried and VDisk will be slain.
            // If not, we will be retrying the request until the disk is replaced during maintenance.
            PCtx->ActorSystem->Send(sender, new NPDisk::TEvSlayResult(NKikimrProto::NOTREADY, 0,
                        req.VDiskId, req.SlayOwnerRound, req.PDiskId, req.VSlotId, errorReason));
            return true;
        }

        case ERequestType::RequestChunkWriteRaw:
            PCtx->ActorSystem->Send(sender, new NPDisk::TEvChunkWriteRawResult(NKikimrProto::CORRUPTED, errorReason), 0,
                request->Cookie);
            return true;

        case ERequestType::RequestWriteMetadata:
        case ERequestType::RequestWriteMetadataResult:
        case ERequestType::RequestTryTrimChunk:
        case ERequestType::RequestReleaseChunks:
        case ERequestType::RequestChunkWritePiece:
        case ERequestType::RequestChunkTrim:
        case ERequestType::RequestAskForCutLog:
        case ERequestType::RequestCommitLogChunks:
        case ERequestType::RequestLogCommitDone:
        case ERequestType::RequestShredPDisk:
        case ERequestType::RequestPreShredCompactVDiskResult:
        case ERequestType::RequestShredVDiskResult:
        case ERequestType::RequestChunkShredResult:
        case ERequestType::RequestContinueShred:
            // These requests don't require response.
            return true;
    }
}

void TPDisk::AddCbs(ui32 ownerId, EGate gate, const char *gateName, ui64 minBudget) {
    if (!ForsetiScheduler.GetCbs(ownerId, gate)) {
        NSchLab::TCbs cbs;
        cbs.CbsName = Sprintf("Owner_%" PRIu32 "_%s", (ui32)ownerId, gateName);
        cbs.MaxBudget = minBudget;
        ForsetiTimeNs++;
        ForsetiScheduler.AddCbs(ownerId, gate, cbs, ForsetiTimeNs);
    }
}

void TPDisk::AddCbsSet(ui32 ownerId) {
    AddCbs(ownerId, GateLog, "Log", ForsetiMinLogCostNs);
    AddCbs(ownerId, GateFresh, "Fresh", 0ull);
    AddCbs(ownerId, GateComp, "Comp", 0ull);
    AddCbs(ownerId, GateFastRead, "FastRead", 0ull);
    AddCbs(ownerId, GateOtherRead, "OtherRead", 0ull);
    AddCbs(ownerId, GateLoad, "Load", 0ull);
    AddCbs(ownerId, GateHugeAsync, "HugeAsync", 0ull);
    AddCbs(ownerId, GateHugeUser, "HugeUser", 0ull);
    AddCbs(ownerId, GateSyncLog, "SyncLog", 0ull);
    AddCbs(ownerId, GateLow, "LowRead", 0ull);

    SchedulerConfigure(Cfg->SchedulerCfg, ownerId);
}

TChunkIdx TPDisk::GetUnshreddedFreeChunk() {
    // Find a free unshredded chunk
    for (TFreeChunks* freeChunks : {&Keeper.UntrimmedFreeChunks, &Keeper.TrimmedFreeChunks}) {
        for (auto it = freeChunks->begin(); it != freeChunks->end(); ++it) {
            TChunkIdx chunkIdx = *it;
            TChunkState& state = ChunkState[chunkIdx];
            // Look for free chunks that haven't been shredded in this generation
            if (state.CommitState == TChunkState::FREE && state.IsDirty && state.ShredGeneration < ShredGeneration) {
                // Found an unshredded free chunk
                TChunkIdx unshreddedChunkIdx = freeChunks->PopAt(it);
                Y_VERIFY_S(unshreddedChunkIdx == chunkIdx, PCtx->PDiskLogPrefix);
                // Mark it as being shredded and update its generation
                LOG_DEBUG_S(*PCtx->ActorSystem, NKikimrServices::BS_PDISK_SHRED,
                    "PDisk# " << PCtx->PDiskId
                    << " found unshredded free chunk# " << chunkIdx
                    << " ShredGeneration# " << ShredGeneration);
                return unshreddedChunkIdx;
            }
        }
    }
    return 0;
}

void TPDisk::ProgressShredState() {
    TGuard<TMutex> guard(StateMutex);
    LOG_TRACE_S(*PCtx->ActorSystem, NKikimrServices::BS_PDISK_SHRED,
        "ProgressShredState at PDisk# " << PCtx->PDiskId
        << " ShredGeneration# " << ShredGeneration
        << " ShredState# " << (ui32)ShredState);
    if (ShredState == EShredStateFinished || ShredState == EShredStateDefault || ShredState == EShredStateFailed) {
        // It's a terminal state
        return;
    }
    if (ShredState == EShredStateSendPreShredCompactVDisk) {
        ui32 finishedCount = 0;
        for (ui32 ownerId = 0; ownerId < OwnerData.size(); ++ownerId) {
            TOwnerData &data = OwnerData[ownerId];
            if (data.VDiskId != TVDiskID::InvalidId) {
                if (data.Status == TOwnerData::VDISK_STATUS_DEFAULT || data.Status == TOwnerData::VDISK_STATUS_HASNT_COME) {
                    LOG_DEBUG_S(*PCtx->ActorSystem, NKikimrServices::BS_PDISK_SHRED,
                        "PDisk# " << PCtx->PDiskId
                        << " can't send compact request to VDisk# " << data.VDiskId
                        << " ownerId# " << ownerId
                        << " as owner.Status# " << data.Status);
                    data.ShredState = TOwnerData::VDISK_SHRED_STATE_NOT_REQUESTED;
                } else {
                    if (data.LastShredGeneration < ShredGeneration) {
                        std::vector<TChunkIdx> chunksToShred;
                        THolder<TEvPreShredCompactVDisk> compactRequest(new TEvPreShredCompactVDisk(ShredGeneration));
                        LOG_DEBUG_S(*PCtx->ActorSystem, NKikimrServices::BS_PDISK_SHRED,
                            "PDisk# " << PCtx->PDiskId
                            << " sends compact request to VDisk# " << data.VDiskId
                            << " ownerId# " << ownerId
                            << " request# " << compactRequest->ToString());
                        PCtx->ActorSystem->Send(new IEventHandle(data.CutLogId, PCtx->PDiskActor, compactRequest.Release()));
                        data.LastShredGeneration = ShredGeneration;
                        data.ShredState = TOwnerData::VDISK_SHRED_STATE_COMPACT_REQUESTED;
                    }
                }
                if (data.ShredState != TOwnerData::VDISK_SHRED_STATE_COMPACT_FINISHED) {
                    LOG_DEBUG_S(*PCtx->ActorSystem, NKikimrServices::BS_PDISK_SHRED,
                        "PDisk# " << PCtx->PDiskId
                        << " ShredGeneration# " << ShredGeneration
                        << " is waiting for ownerId# " << ownerId
                        << " before finishing pre-shred compact"
                        << " VDiskId# " << data.VDiskId
                        << " VDiskStatus# " << data.GetStringStatus()
                        << " ShredState# \"" << data.GetStringShredState() << "\"");
                    return;
                }
                ++finishedCount;
            }
        }
        LOG_DEBUG_S(*PCtx->ActorSystem, NKikimrServices::BS_PDISK_SHRED,
            "PDisk# " << PCtx->PDiskId
            << " has finished all pre-shred compact VDisk requests"
            << " ShredGeneration# " << ShredGeneration
            << " finishedCount# " << finishedCount);
        // All preparations are done, no junk chunks can be unmarked,
        // Update chunk states and start shredding the empty space
        for (TChunkIdx chunkIdx = 0; chunkIdx < ChunkState.size(); ++chunkIdx) {
            TChunkState& state = ChunkState[chunkIdx];
            // Update shred generation for all the clean chunks
            if (!state.IsDirty) {
                state.ShredGeneration = ShredGeneration;
            }
        }
        ShredState = EShredStateSendShredVDisk;
        WriteSysLogRestorePoint(nullptr, TReqId(TReqId::MarkDirtySysLog, 0), {});
    }
    if (ShredState == EShredStateSendShredVDisk) {
        // Shred free space while possible
        if (ChunkBeingShredded == 0) {
            ChunkBeingShredded = GetUnshreddedFreeChunk();
        }
        if (ChunkBeingShredded != 0) {
            // Continue shredding the free chunk
            while (true) {
                if (ChunkBeingShreddedInFlight >= 2) {
                    // We have enough in-flight requests, don't start a new one
                    return;
                }
                if (ChunkBeingShreddedNextSectorIdx * Format.SectorSize >= Format.ChunkSize) {
                    ++ChunkBeingShreddedIteration;
                    ChunkBeingShreddedNextSectorIdx = 0;
                }
                if (ChunkBeingShreddedIteration >= 2) {
                    // We have enough iterations, don't start a new one, just wait for the in-flight requests to finish
                    if (ChunkBeingShreddedInFlight > 0) {
                        return;
                    }
                    // Done shredding the chunk, mark it clean and push it back to the free chunks
                    LOG_DEBUG_S(*PCtx->ActorSystem, NKikimrServices::BS_PDISK_SHRED, "PDisk# " << PCtx->PDiskId
                        << " is done shredding chunk ChunkBeingShredded# " << ChunkBeingShredded);
                    TChunkState &state = ChunkState[ChunkBeingShredded];
                    state.OperationsInProgress--;
                    state.IsDirty = false;
                    state.ShredGeneration = ShredGeneration;
                    Y_VERIFY_S(ChunkState[ChunkBeingShredded].OperationsInProgress == 0, PCtx->PDiskLogPrefix);
                    Keeper.UntrimmedFreeChunks.PushFront(ChunkBeingShredded);
                    ChunkBeingShredded = GetUnshreddedFreeChunk();
                    ChunkBeingShreddedIteration = 0;
                    ChunkBeingShreddedNextSectorIdx = 0;
                    // Write a syslog entry to mark the progress
                    WriteSysLogRestorePoint(nullptr, TReqId(TReqId::MarkDirtySysLog, 0), {});
                }
                if (ChunkBeingShredded) {
                    if (ChunkBeingShreddedIteration == 0 && ChunkBeingShreddedNextSectorIdx == 0) {
                        Y_VERIFY_S(ChunkState[ChunkBeingShredded].OperationsInProgress == 0, PCtx->PDiskLogPrefix);
                        ChunkState[ChunkBeingShredded].OperationsInProgress++;
                    }
                    // Continue shredding the chunk: send a write request to the device using the iteration-specific pattern
                    THolder<TAlignedData>& payload = ShredPayload[ChunkBeingShreddedIteration];
                    if (payload == nullptr) {
                        payload = MakeHolder<TAlignedData>(Format.RoundUpToSectorSize(2097152));
                        ui8* data = payload->Get();
                        memset(data, ChunkBeingShreddedIteration == 0 ? 0x55 : 0xaa, payload->Size());
                    }
                    ui64 size = std::min((ui64)Format.ChunkSize - ChunkBeingShreddedNextSectorIdx * Format.SectorSize, (ui64)payload->Size());
                    ui64 offset = Format.Offset(ChunkBeingShredded, ChunkBeingShreddedNextSectorIdx);
                    ui64 reqIdx = ShredReqIdx++;
                    TCompletionAction *completionAction = new TChunkShredCompletion(this, ChunkBeingShredded, ChunkBeingShreddedNextSectorIdx, size, TReqId(TReqId::ChunkShred, reqIdx));
                    ++ChunkBeingShreddedInFlight;
                    ChunkBeingShreddedNextSectorIdx += size / Format.SectorSize;
                    Mon.ChunkShred.CountRequest(size);
                    BlockDevice->PwriteAsync(payload->Get(), size, offset, completionAction,
                        TReqId(TReqId::ChunkShred, reqIdx), {});
                    return;
                }
                break;
            }
        }

        // If there are no free chunks unshredded, we should ask a vdisk to shred its free space
        ui32 shreddedFreeChunks = Keeper.GetFreeChunkCount();
        ui32 finishedCount = 0;
        for (ui32 ownerId = 0; ownerId < OwnerData.size(); ++ownerId) {
            TOwnerData &data = OwnerData[ownerId];
            if (data.VDiskId != TVDiskID::InvalidId) {
                if (data.Status == TOwnerData::VDISK_STATUS_DEFAULT || data.Status == TOwnerData::VDISK_STATUS_HASNT_COME) {
                    data.ShredState = TOwnerData::VDISK_SHRED_STATE_COMPACT_FINISHED;
                } else if (data.ShredState != TOwnerData::VDISK_SHRED_STATE_SHRED_REQUESTED
                        && data.ShredState != TOwnerData::VDISK_SHRED_STATE_SHRED_FINISHED) {
                    std::vector<TChunkIdx> chunksToShred;
                    chunksToShred.reserve(shreddedFreeChunks/2);
                    for (TChunkIdx chunkIdx = 0; chunkIdx < ChunkState.size(); ++chunkIdx) {
                        TChunkState& state = ChunkState[chunkIdx];
                        // We need to shred only chunks that got dirty before the current shred generation
                        if (state.OwnerId == ownerId && state.IsDirty && state.ShredGeneration < ShredGeneration) {
                            chunksToShred.push_back(chunkIdx);
                            if (chunksToShred.size() >= shreddedFreeChunks/2) {
                                break;
                            }
                        }
                    }
                    if (chunksToShred.size() > 0) {
                            THolder<TEvShredVDisk> shredRequest(new TEvShredVDisk(ShredGeneration, chunksToShred));
                            LOG_DEBUG_S(*PCtx->ActorSystem, NKikimrServices::BS_PDISK_SHRED,
                                "PDisk# " << PCtx->PDiskId
                            << " sends shred request to VDisk# " << data.VDiskId
                            << " ownerId# " << ownerId
                            << " request# " << shredRequest->ToString());
                        PCtx->ActorSystem->Send(new IEventHandle(data.CutLogId, PCtx->PDiskActor, shredRequest.Release()));
                        data.ShredState = TOwnerData::VDISK_SHRED_STATE_SHRED_REQUESTED;
                        data.LastShredGeneration = ShredGeneration;
                    } else {
                        data.ShredState = TOwnerData::VDISK_SHRED_STATE_SHRED_FINISHED;
                        data.LastShredGeneration = ShredGeneration;
                    }
                }
                if (data.ShredState != TOwnerData::VDISK_SHRED_STATE_SHRED_FINISHED) {
                    LOG_DEBUG_S(*PCtx->ActorSystem, NKikimrServices::BS_PDISK_SHRED,
                        "PDisk# " << PCtx->PDiskId
                        << " ShredGeneration# " << ShredGeneration
                        << " is waiting for ownerId# " << ownerId
                        << " VDiskId# " << data.VDiskId
                        << " ShredState# \"" << data.GetStringShredState()
                        << "\" before finishing shred");
                    return;
                }
                ++finishedCount;
            }
        }
        // Check if there are log chunks that require shredding
        bool isLogDirty = false;
        for (const TLogChunkInfo &info : LogChunks) {
            TChunkState &state = ChunkState[info.ChunkIdx];
            if (state.IsDirty && state.ShredGeneration < ShredGeneration) {
                isLogDirty = true;
            }
        }
        if (isLogDirty) {
            bool isLogPaddingNeeded = false;
            TChunkState &state = ChunkState[LogChunks.back().ChunkIdx];
            if (state.IsDirty && state.ShredGeneration < ShredGeneration) {
                isLogPaddingNeeded = true;
            }
            if (isLogPaddingNeeded) {
                while (ShredLogPaddingInFlight < 2) {
                    TRcBuf data = TRcBuf::Uninitialized(2<<20);
                    memset(data.GetDataMut(), 0, data.Size());
                    TEvLog evLog(OwnerUnallocated, 0, {}, data, {}, 0);
                    double burstMs;
                    TLogWrite* request = ReqCreator.CreateLogWrite(evLog, PCtx->PDiskActor, burstMs, {});
                    request->Orbit = std::move(evLog.Orbit);
                    InputRequest(request);
                    ++ShredLogPaddingInFlight;

                    LOG_TRACE_S(*PCtx->ActorSystem, NKikimrServices::BS_PDISK_SHRED,
                        "PDisk# " << PCtx->PDiskId
                        << " delivered itself a TEvLog to pad the common log"
                        << " ShredGeneration# " << ShredGeneration
                        << " ShredState# " << (ui32)ShredState);
                }
                LOG_TRACE_S(*PCtx->ActorSystem, NKikimrServices::BS_PDISK_SHRED,
                    "PDisk# " << PCtx->PDiskId
                    << " is waiting for ShredLogPaddingInFlight# " << ShredLogPaddingInFlight.load()
                    << " ShredGeneration# " << ShredGeneration
                    << " ShredState# " << (ui32)ShredState);
                return;
            }

            LOG_TRACE_S(*PCtx->ActorSystem, NKikimrServices::BS_PDISK_SHRED,
                "PDisk# " << PCtx->PDiskId
                << " Needs vdisks to cut their logs "
                << " ShredGeneration# " << ShredGeneration
                << " ShredState# " << (ui32)ShredState);
            ui32 requestsSent = AskVDisksToCutLogs(OwnerSystem, true);
            if (requestsSent == 0) {
                LOG_WARN_S(*PCtx->ActorSystem, NKikimrServices::BS_PDISK_SHRED,
                    "PDisk# " << PCtx->PDiskId
                    << " needs VDisks to cut log, but could not AskVDisksToCutLogs, 0 requests sent"
                    << " ShredGeneration# " << ShredGeneration
                    << " ShredState# " << (ui32)ShredState);
                // Send/schedule a request to retry
                THolder<TCompletionEventSender> completion(new TCompletionEventSender(this, PCtx->PDiskActor, new NPDisk::TEvContinueShred()));
                if (ReleaseUnusedLogChunks(completion.Get())) {
                    ContinueShredsInFlight++;
                    WriteSysLogRestorePoint(completion.Release(), TReqId(TReqId::ShredPDisk, 0), {});
                } else {
                    // No unused chunks released, try to continue
                    if (ContinueShredsInFlight == 0) {
                        ContinueShredsInFlight++;
                        PCtx->ActorSystem->Send(new IEventHandle(PCtx->PDiskActor, PCtx->PDiskActor, new TEvContinueShred(), 0, 0));
                    }
                }
                return;
            } else {
                if (!ShredIsWaitingForCutLog) {
                    LOG_DEBUG_S(*PCtx->ActorSystem, NKikimrServices::BS_PDISK_SHRED,
                        "PDisk# " << PCtx->PDiskId
                        << " Is now waiting for VDisks to cut their log, requestsSent# " << requestsSent
                        << " ShredGeneration# " << ShredGeneration);
                    ShredIsWaitingForCutLog = 1;
                }
                return;
            }
        }
        // Looks good, but there still can be chunks that need to be shredded still int transition between states.
        // For example, log chunks are removed from the log chunk list on log cut but added to free chunk list on log cut
        // write operation completion. So, walk through the whole chunk list and check.
        for (ui32 chunkIdx = Format.SystemChunkCount; chunkIdx < ChunkState.size(); ++chunkIdx) {
            TChunkState &state = ChunkState[chunkIdx];
            if (state.IsDirty && state.ShredGeneration < ShredGeneration) {
                if (ContinueShredsInFlight) {
                    // There are continue shreds in flight, so we don't need to schedule a new one.
                    // Just wait for it to arrive.
                    LOG_DEBUG_S(*PCtx->ActorSystem, NKikimrServices::BS_PDISK_SHRED,
                        "PDisk# " << PCtx->PDiskId
                        << " found a dirtyInTransition chunkIdx# " << chunkIdx
                        << " state# " << state.ToString()
                        << ", there are already ContinueShredsInFlight# " << ContinueShredsInFlight.load()
                        << " so just wait for it to arrive. "
                        << " ShredGeneration# " << ShredGeneration);
                    return;
                } else {
                    LOG_DEBUG_S(*PCtx->ActorSystem, NKikimrServices::BS_PDISK_SHRED,
                        "PDisk# " << PCtx->PDiskId
                        << " found a dirtyInTransition chunkIdx# " << chunkIdx
                        << " state# " << state.ToString()
                        << ", scheduling ContinueShred. "
                        << " ShredGeneration# " << ShredGeneration);
                    ContinueShredsInFlight++;
                    PCtx->ActorSystem->Schedule(TDuration::MilliSeconds(50),
                            new IEventHandle(PCtx->PDiskActor, PCtx->PDiskActor, new TEvContinueShred(), 0, 0));
                    return;
                }
            }
        }
        ShredIsWaitingForCutLog = 0;


        LOG_DEBUG_S(*PCtx->ActorSystem, NKikimrServices::BS_PDISK_SHRED,
            "PDisk# " << PCtx->PDiskId
            << " has finished all shred requests"
            << " ShredGeneration# " << ShredGeneration
            << " finishedCount# " << finishedCount);
        ShredState = EShredStateFinished;
        // TODO: send result to the requester after actual shred is done
        LOG_NOTICE_S(*PCtx->ActorSystem, NKikimrServices::BS_PDISK_SHRED,
            "Shred request is finished at PDisk# " << PCtx->PDiskId
            << " ShredGeneration# " << ShredGeneration);
        for (auto& [requester, cookie] : ShredRequesters) {
            PCtx->ActorSystem->Send(new IEventHandle(requester, PCtx->PDiskActor, new TEvShredPDiskResult(
                NKikimrProto::OK, ShredGeneration, ""), 0, cookie));
        }
        ShredRequesters.clear();
    }
}

void TPDisk::ProcessShredPDisk(TShredPDisk& request) {
    if (NPDisk::TPDisk::IS_SHRED_ENABLED) {
        LOG_NOTICE_S(*PCtx->ActorSystem, NKikimrServices::BS_PDISK_SHRED,
            "ProcessShredPDisk at PDisk# " << PCtx->PDiskId
            << " ShredGeneration# " << ShredGeneration
            << " request# " << request.ToString());
    } else {
        LOG_CRIT_S(*PCtx->ActorSystem, NKikimrServices::BS_PDISK_SHRED,
            "ProcessShredPDisk with IS_SHRED_ENABLED# false at PDisk# " << PCtx->PDiskId
            << " ShredGeneration# " << ShredGeneration
            << " request# " << request.ToString());
    }
    if (!PCtx->ActorSystem) {
        return;
    }
    TGuard<TMutex> guard(StateMutex);
    if (request.ShredGeneration < ShredGeneration) {
        guard.Release();
        PCtx->ActorSystem->Send(new IEventHandle(request.Sender, PCtx->PDiskActor, new TEvShredPDiskResult(
            NKikimrProto::RACE, request.ShredGeneration, "A shred request with a higher generation is already in progress"),
            0, request.Cookie));
        return;
    }
    if (request.ShredGeneration == ShredGeneration) {
        if (ShredState != EShredStateFailed) {
            if (ShredState == EShredStateFinished) {
                PCtx->ActorSystem->Send(new IEventHandle(request.Sender, PCtx->PDiskActor, new TEvShredPDiskResult(
                    NKikimrProto::OK, request.ShredGeneration, "A shred request with this generation is already complete"), 0,
                    request.Cookie));
            }
            // Do nothing, since we already have a shred request with the same generation.
            // Just add the sender to the list of requesters.
            LOG_NOTICE_S(*PCtx->ActorSystem, NKikimrServices::BS_PDISK_SHRED,
                "Registered one more shred requester at PDisk# " << PCtx->PDiskId
                << " ShredGeneration# " << ShredGeneration
                << " request# " << request.ToString());
            ShredRequesters.emplace_back(request.Sender, request.Cookie);
            return;
        }
        LOG_NOTICE_S(*PCtx->ActorSystem, NKikimrServices::BS_PDISK_SHRED,
            "Retrying a failed shred at PDisk# " << PCtx->PDiskId
            << " ShredGeneration# " << ShredGeneration
            << " request# " << request.ToString());
    }
    // ShredGeneration > request.ShredGeneration
    if (ShredRequesters.size() > 0) {
        for (auto& [requester, cookie] : ShredRequesters) {
            PCtx->ActorSystem->Send(new IEventHandle(requester, PCtx->PDiskActor, new TEvShredPDiskResult(
                NKikimrProto::RACE, request.ShredGeneration, "A shred request with a higher generation is received"), 0,
                cookie));
        }
        ShredRequesters.clear();
    }
    ShredGeneration = request.ShredGeneration;
    ShredRequesters.emplace_back(request.Sender, request.Cookie);
    ShredState = EShredStateSendPreShredCompactVDisk;
    for (ui32 owner = 0; owner < OwnerData.size(); ++owner) {
        OwnerData[owner].ShredState = TOwnerData::VDISK_SHRED_STATE_NOT_REQUESTED;
        OwnerData[owner].LastShredGeneration = 0;
    }
    ProgressShredState();
}

void TPDisk::ProcessPreShredCompactVDiskResult(TPreShredCompactVDiskResult& request) {
    LOG_DEBUG_S(*PCtx->ActorSystem, NKikimrServices::BS_PDISK_SHRED,
        "ProcessPreShredCompactVDiskResult at PDisk# " << PCtx->PDiskId
        << " ShredGeneration# " << ShredGeneration
        << " request# " << request.ToString());
    TGuard<TMutex> guard(StateMutex);
    if (request.ShredGeneration != ShredGeneration) {
        // Ignore old results
        LOG_WARN_S(*PCtx->ActorSystem, NKikimrServices::BS_PDISK_SHRED,
            "Old PreShredCompactVDiskResult is ignored at PDisk# " << PCtx->PDiskId
            << " ShredGeneration# " << ShredGeneration
            << " for PreShredCompactVDiskResult generation# " << request.ShredGeneration
            << " owner# " << request.Owner
            << " ownerRound# " << request.OwnerRound);
        return;
    }
    TStringStream err;
    NKikimrProto::EReplyStatus errStatus = CheckOwnerAndRound(&request, err);
    if (errStatus != NKikimrProto::OK) {
        LOG_ERROR_S(*PCtx->ActorSystem, NKikimrServices::BS_PDISK_SHRED,
            "Incorrect PreShredCompactVDiskResult is received at PDisk# " << PCtx->PDiskId
            << " ShredGeneration# " << ShredGeneration
            << " owner# " << request.Owner
            << " ownerRound# " << request.OwnerRound
            << " " << err.Str());
        return;
    }
    if (OwnerData[request.Owner].ShredState != TOwnerData::VDISK_SHRED_STATE_COMPACT_REQUESTED) {
        // Ignore incorrect state results
        LOG_ERROR_S(*PCtx->ActorSystem, NKikimrServices::BS_PDISK_SHRED,
            "Unexpected PreShredCompactVDiskResult is received at PDisk# " << PCtx->PDiskId
            << " ShredGeneration# " << ShredGeneration
            << " for PreShredCompactVDiskResult generation# " << request.ShredGeneration
            << " owner# " << request.Owner
            << " ownerRound# " << request.OwnerRound
            << " ownerShredState# " << OwnerData[request.Owner].ShredState);
        return;
    }
    if (request.Status != NKikimrProto::OK) {
        ShredState = EShredStateFailed;
        for (auto& [requester, cookie] : ShredRequesters) {
            TStringStream str;
            str << "Shred request failed at PDisk# " << PCtx->PDiskId
                << " for shredGeneration# " << request.ShredGeneration
                << " because owner# " << request.Owner
                << " ownerRound# " << request.OwnerRound
                << " replied with PreShredCompactVDiskResult status# " << request.Status
                << " and ErrorReason# " << request.ErrorReason;
            LOG_ERROR_S(*PCtx->ActorSystem, NKikimrServices::BS_PDISK_SHRED, str.Str());
            PCtx->ActorSystem->Send(new IEventHandle(requester, PCtx->PDiskActor, new TEvShredPDiskResult(
                NKikimrProto::ERROR, request.ShredGeneration, str.Str()), 0, cookie));
        }
        ShredRequesters.clear();
        return;
    }
    OwnerData[request.Owner].ShredState = TOwnerData::VDISK_SHRED_STATE_COMPACT_FINISHED;
    ProgressShredState();
}

void TPDisk::ProcessShredVDiskResult(TShredVDiskResult& request) {
    LOG_DEBUG_S(*PCtx->ActorSystem, NKikimrServices::BS_PDISK_SHRED,
        "ProcessShredVDiskResult at PDisk# " << PCtx->PDiskId
        << " ShredGeneration# " << ShredGeneration
        << " request# " << request.ToString());
    TGuard<TMutex> guard(StateMutex);
    if (request.ShredGeneration != ShredGeneration) {
        // Ignore old results
        LOG_WARN_S(*PCtx->ActorSystem, NKikimrServices::BS_PDISK_SHRED,
            "Old ShredVDiskResult is ignored at PDisk# " << PCtx->PDiskId
            << " ShredGeneration# " << ShredGeneration
            << " for shredGeneration# " << request.ShredGeneration
            << " owner# " << request.Owner
            << " ownerRound# " << request.OwnerRound);
        return;
    }
    TStringStream err;
    NKikimrProto::EReplyStatus errStatus = CheckOwnerAndRound(&request, err);
    if (errStatus != NKikimrProto::OK) {
        LOG_ERROR_S(*PCtx->ActorSystem, NKikimrServices::BS_PDISK_SHRED,
            "Incorrect ShredVDiskResult is received at PDisk# " << PCtx->PDiskId
            << " ShredGeneration# " << ShredGeneration
            << " owner# " << request.Owner
            << " ownerRound# " << request.OwnerRound
            << " " << err.Str());
        return;
    }
    if (OwnerData[request.Owner].ShredState != TOwnerData::VDISK_SHRED_STATE_SHRED_REQUESTED) {
        // Ignore incorrect state results
        LOG_ERROR_S(*PCtx->ActorSystem, NKikimrServices::BS_PDISK_SHRED,
            "Unexpected ShredVDiskResult is received at PDisk# " << PCtx->PDiskId
            << " ShredGeneration# " << ShredGeneration
            << " for ShredVDiskResult generation# " << request.ShredGeneration
            << " owner# " << request.Owner
            << " ownerRound# " << request.OwnerRound
            << " ownerShredState# " << OwnerData[request.Owner].ShredState);
        return;
    }
    if (request.Status != NKikimrProto::OK) {
        ShredState = EShredStateFailed;
        for (auto& [requester, cookie] : ShredRequesters) {
            TStringStream str;
            str << "Shred request failed at PDisk# " << PCtx->PDiskId
                << " for shredGeneration# " << request.ShredGeneration
                << " because owner# " << request.Owner
                << " ownerRound# " << request.OwnerRound
                << " replied with status# " << request.Status
                << " and ErrorReason# " << request.ErrorReason;
            LOG_ERROR_S(*PCtx->ActorSystem, NKikimrServices::BS_PDISK_SHRED, str.Str());
            PCtx->ActorSystem->Send(new IEventHandle(requester, PCtx->PDiskActor, new TEvShredPDiskResult(
                NKikimrProto::ERROR, request.ShredGeneration, str.Str()), 0, cookie));
        }
        ShredRequesters.clear();
        return;
    }
    OwnerData[request.Owner].ShredState = TOwnerData::VDISK_SHRED_STATE_COMPACT_FINISHED;
    ProgressShredState();
}

void TPDisk::ProcessChunkShredResult(TChunkShredResult& request) {
    LOG_TRACE_S(*PCtx->ActorSystem, NKikimrServices::BS_PDISK_SHRED,
        "ProcessChunkShredResult at PDisk# " << PCtx->PDiskId
        << " ShredGeneration# " << ShredGeneration
        << " request# " << request.ToString());
    Y_VERIFY_S(ChunkBeingShreddedInFlight > 0, PCtx->PDiskLogPrefix);
    --ChunkBeingShreddedInFlight;
    ProgressShredState();
}

void TPDisk::ProcessContinueShred(TContinueShred& request) {
    if (ContinueShredsInFlight > 0) {
        if (ContinueShredsInFlight > 1) {
          ContinueShredsInFlight--;
          return;
        }
        ContinueShredsInFlight--;
    } else {
        LOG_CRIT_S(*PCtx->ActorSystem, NKikimrServices::BS_PDISK_SHRED,
            "ProcessContinueShred at PDisk# " << PCtx->PDiskId
            << " ContinueShredInFlight miscalculation, ShredGeneration# " << ShredGeneration
            << " ContinueShredsInFlight# " << ContinueShredsInFlight.load()
            << " request# " << request.ToString());
    }
    LOG_TRACE_S(*PCtx->ActorSystem, NKikimrServices::BS_PDISK_SHRED,
        "ProcessContinueShred at PDisk# " << PCtx->PDiskId
        << " ShredGeneration# " << ShredGeneration
        << " ContinueShredsInFlight# " << ContinueShredsInFlight.load()
        << " request# " << request.ToString());
    ProgressShredState();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// External interface
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
void TPDisk::Wakeup() {
    InputQueue.WakeUp();
}

// Pushes request to the InputQueue; almost thread-safe
void TPDisk::InputRequest(TRequestBase* request) {
    bool isTrim = request->PriorityClass == NPriInternal::Trim;
    if (request->GateId != GateFastOperation) {
        ui64 timeout = 0;
        if (isTrim) {
            TGuard<TMutex> g(StateMutex);
            timeout = HPCyclesNs(50000000ull + 14000000000ull * Keeper.GetTrimmedFreeChunkCount() / Format.DiskSizeChunks());
        }
        LWPROBE(PDiskInputRequestTimeout, PCtx->PDiskId, request->ReqId.Id, HPMilliSecondsFloat(timeout));
        request->Deadline = HPNow() + timeout;
    }
    ui64 qla = InputQueue.GetWaitingSize();
    Mon.InputQLA.Increment(qla);
    ui64 qca = AtomicAdd(InputQueueCost, request->Cost);
    Mon.InputQCA.Increment(qca / 1000000ull);
    LWTRACK(PDiskInputRequest, request->Orbit, PCtx->PDiskId, request->ReqId.Id, HPSecondsFloat(request->CreationTime),
            double(request->Cost) / 1000000.0, qla, double(qca) / 1000000.0,
            HPSecondsFloat(request->Deadline),
            request->Owner, request->IsFast, request->PriorityClass, InputQueue.GetWaitingSize());

    LOG_DEBUG(*PCtx->ActorSystem, NKikimrServices::BS_PDISK, "PDiskId# %" PRIu32 " ReqId# %" PRIu64
            " InputRequest InputQueue.Push priortiyClass# %" PRIu64 " creationTime# %f",
            (ui32)PCtx->PDiskId, (ui64)request->ReqId.Id, (ui64)request->PriorityClass,
            HPSecondsFloat(request->CreationTime));

    InputQueue.Push(request);
}

} // NPDisk
} // NKikimr
