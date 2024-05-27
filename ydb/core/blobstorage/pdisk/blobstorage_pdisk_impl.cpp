#include "blobstorage_pdisk_impl.h"

#include "blobstorage_pdisk_chunk_id_formatter.h"
#include "blobstorage_pdisk_completion_impl.h"
#include "blobstorage_pdisk_mon.h"
#include "blobstorage_pdisk_request_id.h"

#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/control/immediate_control_board_impl.h>
#include <ydb/core/protos/blobstorage.pb.h>
#include <ydb/core/blobstorage/crypto/secured_block.h>
#include <ydb/library/schlab/schine/job_kind.h>

#include <util/system/unaligned_mem.h>

namespace NKikimr {
namespace NPDisk {

LWTRACE_USING(BLOBSTORAGE_PROVIDER);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Initialization
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

TPDisk::TPDisk(const TIntrusivePtr<TPDiskConfig> cfg, const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters)
    : PDiskId(cfg->PDiskId)
    , Mon(counters, PDiskId, cfg.Get())
    , DriveModel(cfg->DriveModelSeekTimeNs,
            cfg->DriveModelSpeedBps,
            cfg->DriveModelBulkWrieBlockSize,
            cfg->DriveModelTrimSpeedBps,
            cfg->DriveModelSpeedBpsMin,
            cfg->DriveModelSpeedBpsMax,
            cfg->DeviceInFlight)
    , ReqCreator(PDiskId, &Mon, &DriveModel, &EstimatedLogChunkIdx)
    , ReorderingMs(cfg->ReorderingMs)
    , LogSeekCostLoop(2)
    , ActorSystem(nullptr)
    , ExpectedDiskGuid(cfg->PDiskGuid)
    , PDiskCategory(cfg->PDiskCategory)
    , NextOwnerRound(cfg->StartOwnerRound)
    , OwnerData(OwnerCount)
    , Keeper(Mon, cfg)
    , CostLimitNs(cfg->CostLimitNs)
    , PDiskThread(*this)
    , BlockDevice(CreateRealBlockDevice(cfg->GetDevicePath(), cfg->PDiskId, Mon,
                    HPCyclesMs(ReorderingMs), DriveModel.SeekTimeNs(), cfg->DeviceInFlight,
                    TDeviceMode::LockFile | (cfg->UseSpdkNvmeDriver ? TDeviceMode::UseSpdk : 0),
                    cfg->MaxQueuedCompletionActions, cfg->SectorMap, this))
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
    ForsetiOpPieceSizeSsd = TControlWrapper(64 * 1024, 1, 64 * 1024 * 1024);
    ForsetiOpPieceSizeRot = TControlWrapper(2 * 1024 * 1024, 1, 64 * 1024 * 1024);
    ForsetiOpPieceSizeCached = PDiskCategory.IsSolidState() ?  ForsetiOpPieceSizeSsd : ForsetiOpPieceSizeRot;

    if (Cfg->SectorMap) {
        auto diskModeParams = Cfg->SectorMap->GetDiskModeParams();
        if (diskModeParams) {
            SectorMapFirstSectorReadRate = TControlWrapper(diskModeParams->FirstSectorReadRate, 0, 100000ull * 1024 * 1024);
            SectorMapLastSectorReadRate = TControlWrapper(diskModeParams->LastSectorReadRate, 0, 100000ull * 1024 * 1024);
            SectorMapFirstSectorWriteRate = TControlWrapper(diskModeParams->FirstSectorWriteRate, 0, 100000ull * 1024 * 1024);
            SectorMapLastSectorWriteRate = TControlWrapper(diskModeParams->LastSectorWriteRate, 0, 100000ull * 1024 * 1024);
            SectorMapSeekSleepMicroSeconds = TControlWrapper(diskModeParams->SeekSleepMicroSeconds, 0, 100ul * 1000 * 1000);
        }
    }

    AddCbs(OwnerSystem, GateLog, "Log", 2'000'000ull);
    ConfigureCbs(OwnerSystem, GateLog, 16);
    AddCbs(OwnerUnallocated, GateTrim, "Trim", 0ull);
    ConfigureCbs(OwnerUnallocated, GateTrim, 16);

    Format.Clear();
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
    JointChunkReads.reserve(16 << 10);
    JointChunkWrites.reserve(16 << 10);
    JointLogWrites.reserve(16 << 10);
    JointCommits.reserve(16 << 10);
    JointChunkForgets.reserve(16 << 10);

    DebugInfoGenerator = [id = PDiskId, type = PDiskCategory]() {
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

    for (ui32 k = 0; k < mainKeySize; ++k) {
        TPDiskStreamCypher cypher(true); // Format record is always encrypted
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
            cypher.Encrypt(&diskFormat, formatSector, sizeof(TDiskFormat));

            isBad[i] = !diskFormat.IsHashOk(FormatSectorSize);
            if (!isBad[i]) {
                Format.UpgradeFrom(diskFormat);
                if (Format.IsErasureEncodeUserChunks()) {
                    LOG_ERROR_S(*ActorSystem, NKikimrServices::BS_PDISK, "PDiskId# " << PDiskId
                            << " Read from disk Format has FormatFlagErasureEncodeUserChunks set, "
                            << " but current version of PDisk can't work with it"
                            << " Format# " << Format.ToString()
                            << " Marker# BPD80");
                    Y_FAIL_S("PDiskId# " << PDiskId
                            << "Unable to run PDisk on disk with FormatFlagErasureEncodeUserChunks set");
                }
                if (Format.IsErasureEncodeUserLog()) {
                    LOG_ERROR_S(*ActorSystem, NKikimrServices::BS_PDISK, "PDiskId# " << PDiskId
                            << " Read from disk Format has FormatFlagErasureEncodeUserLog set, "
                            << " but current version of PDisk can't work with it"
                            << " Format# " << Format.ToString()
                            << " Marker# BPD801");
                    Y_FAIL_S("PDiskId# " << PDiskId
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
                return TCheckDiskFormatResult(true, true);
            } else if (isBadPresent) {
                for (ui32 i = 0; i < ReplicationFactor; ++i) {
                    if (isBad[i]) {
                        TBuffer* buffer = BufferPool->Pop();
                        Y_ABORT_UNLESS(FormatSectorSize <= buffer->Size());
                        memcpy(buffer->Data(), formatSector, FormatSectorSize);
                        ui64 targetOffset = i * FormatSectorSize;
                        LOG_INFO_S(*ActorSystem, NKikimrServices::BS_PDISK, "PDiskId# " << (ui32)PDiskId
                                << " PWriteAsync offset# " << targetOffset
                                << " to# " << (targetOffset + FormatSectorSize)
                                << " for format restoration"
                                << " Marker# BPD46");

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

bool TPDisk::IsFormatMagicValid(ui8 *magicData8, ui32 magicDataSize) {
    Y_VERIFY_S(magicDataSize % sizeof(ui64) == 0, "Magic data size# "<< magicDataSize
            << " must be a multiple of sizeof(ui64)");
    Y_VERIFY_S(magicDataSize >= FormatSectorSize, "Magic data size# "<< magicDataSize
            << " must greater or equals to FormatSectorSize# " << FormatSectorSize);
    ui64 magicOr = 0ull;
    ui64 isIncompleteFormatMagicPresent = true;
    ui64 *magicData64 = reinterpret_cast<ui64 *>(magicData8);
    for (ui32 i = 0; i < magicDataSize / sizeof(ui64); ++i) {
        magicOr |= magicData64[i];
        if (i < MagicIncompleteFormatSize / sizeof(ui64) && MagicIncompleteFormat != magicData64[i]) {
            isIncompleteFormatMagicPresent = false;
        }
    }
    return magicOr == 0ull || isIncompleteFormatMagicPresent;
}

bool TPDisk::CheckGuid(TString *outReason) {
    const bool ok = Format.Guid == ExpectedDiskGuid;
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
            str << " VDiskId: " << data.VDiskId.ToString();
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
    Y_ABORT_UNLESS(InputQueue.GetWaitingSize() == 0);
}

void TPDisk::Stop() {
    TGuard<TMutex> guard(StopMutex);
    if (!AtomicGet(IsStarted)) {
        // In case of destruction of a stopped pdisk there may be some requests in the queue scheduled by the actor
        while (InputQueue.GetWaitingSize() > 0) {
            TRequestBase::AbortDelete(InputQueue.Pop(), ActorSystem);
        }
        return;
    }
    AtomicSet(IsStarted, false);
    PDiskThread.Stop();
    PDiskThread.Join();

    if (ActorSystem) {
        LOG_NOTICE_S(*ActorSystem, NKikimrServices::BS_PDISK, "PDiskId# " << PDiskId
                << " shutdown owner info# " << StartupOwnerInfo());
    }

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
            TRequestBase::AbortDelete(req, ActorSystem);
        }
    }

    for (TRequestBase* req : JointLogReads) {
        delete req;
    }
    JointLogReads.clear();
    for (auto& req : JointChunkReads) {
        TRequestBase::AbortDelete(req.Get(), ActorSystem);
    }
    JointChunkReads.clear();
    for (TRequestBase* req : JointChunkWrites) {
        switch (req->GetType()) {
            case ERequestType::RequestChunkWrite:
            {
                TChunkWrite *write = static_cast<TChunkWrite*>(req);
                if (write->IsTotallyEnqueued()) {
                    delete write;
                }
                break;
            }
            case ERequestType::RequestChunkWritePiece:
                delete req;
                break;
            default:
                Y_FAIL_S("Unexpected request type# " << ui64(req->GetType()) << " in JointChunkWrites");
        }
    }
    JointChunkWrites.clear();
    for (TLogWrite* req : JointLogWrites) {
        delete req;
    }
    JointLogWrites.clear();
    JointCommits.clear();
    JointChunkForgets.clear();
    for (const auto& req : FastOperationsQueue) {
        TRequestBase::AbortDelete(req.get(), ActorSystem);
    }
    FastOperationsQueue.clear();
    for (TRequestBase* req : PausedQueue) {
        TRequestBase::AbortDelete(req, ActorSystem);
    }
    PausedQueue.clear();

    while (InputQueue.GetWaitingSize() > 0) {
        TRequestBase::AbortDelete(InputQueue.Pop(), ActorSystem);
    }
    if (InitialTailBuffer) {
        InitialTailBuffer->Exec(ActorSystem);
        InitialTailBuffer = nullptr;
    }
}

void TPDisk::ObliterateCommonLogSectorSet() {
    CommonLogger->Obliterate();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Generic format-related calculations
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

ui32 TPDisk::GetUserAccessibleChunkSize() const {
    return Format.GetUserAccessibleChunkSize();
}

ui32 TPDisk::GetChunkAppendBlockSize() const {
    return Format.SectorPayloadSize();
}

ui32 TPDisk::SystemChunkSize(const TDiskFormat& format, ui32 userAccessibleChunkSizeBytes, ui32 sectorSizeBytes) const {
    ui32 usableSectorBytes = format.SectorPayloadSize();
    ui32 userSectors = (userAccessibleChunkSizeBytes + usableSectorBytes - 1) / usableSectorBytes;
    ui32 minChunkSize = userSectors * sectorSizeBytes;
    const ui32 chunkSizeAlignment = (2 << 20);
    ui32 alignedChunkSize = ((minChunkSize + chunkSizeAlignment - 1) / chunkSizeAlignment) * chunkSizeAlignment;
    return alignedChunkSize;
}

void ParsePayloadFromSectorOffset(const TDiskFormat& format, ui64 firstSector, ui64 lastSector, ui64 currentSector,
        ui64 *outPayloadBytes, ui64 *outPayloadOffset) {
    Y_VERIFY_S(firstSector <= currentSector && currentSector <= lastSector, firstSector << " <= " << currentSector
            << " <= " << lastSector);

    *outPayloadBytes = (lastSector + 1 - currentSector) * format.SectorPayloadSize();
    *outPayloadOffset = (currentSector - firstSector) * format.SectorPayloadSize();
}

bool ParseSectorOffset(const TDiskFormat& format, TActorSystem *actorSystem, ui32 pDiskId, ui64 offset, ui64 size,
        ui64 &outSectorIdx, ui64 &outLastSectorIdx, ui64 &outSectorOffset) {
    const ui64 chunkSizeUsableSectors = format.ChunkSize / format.SectorSize;
    const ui64 sectorPayloadSize = format.SectorPayloadSize();
    Y_ABORT_UNLESS(sectorPayloadSize > 0);
    ui64 lastSectorIdx = (offset + size + sectorPayloadSize - 1) / sectorPayloadSize - 1;
    outLastSectorIdx = lastSectorIdx;

    ui64 sectorIdx = offset / sectorPayloadSize;
    outSectorIdx = sectorIdx;

    if (outSectorIdx >= chunkSizeUsableSectors || outLastSectorIdx >= chunkSizeUsableSectors) {
        if (outSectorIdx >= chunkSizeUsableSectors) {
            LOG_ERROR(*actorSystem, NKikimrServices::BS_PDISK, "PDiskId# %" PRIu32 " outSectorIdx# %" PRIu32
                " >= chunkSizeUsableSectors# %" PRIu32 " with offset# %" PRIu32 " size# %" PRIu32,
                (ui32)pDiskId, (ui32)outSectorIdx, (ui32)chunkSizeUsableSectors, (ui32)offset, (ui32)size);
        } else {
            LOG_ERROR(*actorSystem, NKikimrServices::BS_PDISK, "PDiskId# %" PRIu32 " outLastSectorIdx# %" PRIu32
                " >= chunkSizeUsableSectors# %" PRIu32 " with offset# %" PRIu32 " size# %" PRIu32,
                (ui32)pDiskId, (ui32)outLastSectorIdx, (ui32)chunkSizeUsableSectors, (ui32)offset, (ui32)size);
        }
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
            ss << "PDiskId# " << PDiskId << " Failed log canary at chunkIdx# " << chunkIdx
                << " sectorIdx# " << sectorIdx << " sectorOffset# " << Format.Offset(chunkIdx, sectorIdx)
                << " read canary# " << readCanary << " expected canary# " << Canary;
            LOG_ERROR_S(*ActorSystem, NKikimrServices::BS_PDISK, ss.Str());
            Y_FAIL_S(ss.Str());
        }
    }
}

TLogPosition TPDisk::LogPosition(TChunkIdx chunkIdx, ui64 sectorIdx, ui64 offsetInSector) const {
    ui64 offsetBytes = sectorIdx * Format.SectorSize + offsetInSector;
    Y_ABORT_UNLESS(offsetBytes <= Max<ui32>());
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
    if (LogChunks.size() > 3 && KIKIMR_PDISK_ENABLE_CUT_LOG_FROM_THE_MIDDLE) {
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
        LOG_INFO_S(*ActorSystem, NKikimrServices::BS_PDISK, "PDiskId# " << PDiskId
                << " chunkIdx# " << chunkIdx << " released as unused, ownerId# " << ui32(state.OwnerId)
                << " -> " << ui32(OwnerUnallocated) << " Marker# BPD55");
        Y_VERIFY_S(state.OwnerId == OwnerSystem, "PDiskId# " << PDiskId
                << " Unexpected ownerId# " << ui32(state.OwnerId));
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
        completion->Req = THolder<TRequestBase>(ReqCreator.CreateFromArgs<TReleaseChunks>(std::move(chunksToRelease), NWilson::TSpan{}));
        SysLogRecord.LogHeadChunkIdx = gapEnd->ChunkIdx;
        SysLogRecord.LogHeadChunkPreviousNonce = ChunkState[gapEnd->ChunkIdx].PreviousNonce;
        PrintLogChunksInfo("cut tail log");
        return true;
    // Case 2: Chunks to be deleted located in the middle of LogChunksList
    } else if (gapStart && gapEnd) {
        Y_ABORT_UNLESS(KIKIMR_PDISK_ENABLE_CUT_LOG_FROM_THE_MIDDLE);
        IsLogChunksReleaseInflight = true;
        Mon.SplicedLogChunks->Add(chunksToRelease.size());
        completion->Req = THolder<TRequestBase>(ReqCreator.CreateFromArgs<TReleaseChunks>(*gapStart, *gapEnd, std::move(chunksToRelease), NWilson::TSpan{}));
        PrintLogChunksInfo("log splice");
        return true;
    } else {
        TStringStream ss;
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
    return Max<i64>(0, Keeper.GetOwnerFree(ownerId));
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

void TPDisk::AskVDisksToCutLogs(TOwner ownerFilter, bool doForce) {
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
                        } else if (ownerFilter != OwnerSystem) {
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
                        LOG_DEBUG_S(*ActorSystem, NKikimrServices::BS_PDISK,
                                "PDiskId# " << (ui32)PDiskId
                                << " Send CutLog to# " << data.CutLogId.ToString().data()
                                << " ownerId#" << ui32(chunkOwner)
                                << " cutLog# " << cutLog->ToString()
                                << " Marker# BPD67");
                        Y_VERIFY_S(cutLog->FreeUpToLsn, "Error! Should not ask to cut log at 0 lsn."
                                "PDiskId# " << (ui32)PDiskId
                                << " Send CutLog to# " << data.CutLogId.ToString().data()
                                << " ownerId#" << ui32(chunkOwner)
                                << " cutLog# " << cutLog->ToString());
                        ActorSystem->Send(new IEventHandle(data.CutLogId, PDiskActor, cutLog.Release(),
                                    IEventHandle::FlagTrackDelivery, 0));
                        data.AskedFreeUpToLsn = lsn;
                        data.AskedToCutLogAt = now;
                        data.AskedLogChunkToCut = cutLogInfo.ChunksToCut;
                        data.LogChunkCountBeforeCut = ownedLogChunks;
                        // ADD_RECORD_WITH_TIMESTAMP_TO_OPERATION_LOG(data.OperationLog, "System owner asked to cut log, OwnerId# " << chunkOwner);
                    } else {
                        LOG_INFO_S(*ActorSystem, NKikimrServices::BS_PDISK,
                                "PDiskId# " << (ui32)PDiskId
                                << " Can't send CutLog to ownerId# " << ui32(chunkOwner) << " Marker# BPD14");
                    }
                }
            }
        } else if (ownerFilter < OwnerEndUser) {
            auto it = cutLogInfoForOwner.find(ownerFilter);
            if (it != cutLogInfoForOwner.end()) {
                auto &cutLogInfo = it->second;

                if (cutLogInfo.Lsn == 0) {
                    // Prevent cuts with lsn = 0.
                    return;
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
                    LOG_DEBUG_S(*ActorSystem, NKikimrServices::BS_PDISK,
                            "PDiskId# " << (ui32)PDiskId
                            << " Send CutLog to# " << data.CutLogId.ToString().data()
                            << " ownerId#" << ui32(ownerFilter)
                            << " cutLog# " << cutLog->ToString()
                            << " Marker# BPD68");
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
                        Y_VERIFY_S(cutLog->FreeUpToLsn, "Error! Should not ask to cut log at 0 lsn."
                                "PDiskId# " << (ui32)PDiskId
                                << " Send CutLog to# " << data.CutLogId.ToString().data()
                                << " ownerId#" << ui32(ownerFilter)
                                << " cutLog# " << cutLog->ToString()
                                << " LogChunks# " << str.Str());
                    }
                    ActorSystem->Send(new IEventHandle(data.CutLogId, PDiskActor, cutLog.Release(),
                                IEventHandle::FlagTrackDelivery, 0));
                    data.AskedFreeUpToLsn = lsn;
                    data.AskedToCutLogAt = now;
                    data.AskedLogChunkToCut = cutLogInfo.ChunksToCut;
                    data.LogChunkCountBeforeCut = ownedLogChunks;
                    // ADD_RECORD_WITH_TIMESTAMP_TO_OPERATION_LOG(data.OperationLog, "User owner asked to cut log, OwnerId# " << ownerFilter);
                } else {
                    LOG_INFO_S(*ActorSystem, NKikimrServices::BS_PDISK,
                            "PDiskId# " << (ui32)PDiskId
                            << " Can't send CutLog to ownerId# " << ui32(ownerFilter) << " Marker# BPD15");
                }
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Chunk writing
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
bool TPDisk::ChunkWritePiece(TChunkWrite *evChunkWrite, ui32 pieceShift, ui32 pieceSize) {
    if (evChunkWrite->IsReplied) {
        return true;
    }
    TGuard<TMutex> guard(StateMutex);
    Y_ABORT_UNLESS(pieceShift % Format.SectorPayloadSize() == 0);
    Y_VERIFY_S(pieceSize % Format.SectorPayloadSize() == 0 || pieceShift + pieceSize == evChunkWrite->TotalSize,
        "pieceShift# " << pieceShift << " pieceSize# " << pieceSize
        << " evChunkWrite->TotalSize# " << evChunkWrite->TotalSize);

    ui32 chunkIdx = evChunkWrite->ChunkIdx;

    Y_ABORT_UNLESS(chunkIdx != 0);


    ui64 desiredSectorIdx = 0;
    ui64 sectorOffset = 0;
    ui64 lastSectorIdx;
    if (!ParseSectorOffset(Format, ActorSystem, PDiskId, evChunkWrite->Offset + evChunkWrite->BytesWritten,
            evChunkWrite->TotalSize - evChunkWrite->BytesWritten, desiredSectorIdx, lastSectorIdx, sectorOffset)) {
        guard.Release();
        TString err = Sprintf("PDiskId# %" PRIu32 " Can't write chunk: incorrect offset/size offset# %" PRIu32
                " size# %" PRIu32 " chunkIdx# %" PRIu32 " ownerId# %" PRIu32, (ui32)PDiskId, (ui32)evChunkWrite->Offset,
                (ui32)evChunkWrite->TotalSize, (ui32)chunkIdx, (ui32)evChunkWrite->Owner);
        LOG_ERROR(*ActorSystem, NKikimrServices::BS_PDISK, "%s", err.c_str());
        SendChunkWriteError(*evChunkWrite, err, NKikimrProto::ERROR);
        return true;
    }

    TChunkState &state = ChunkState[chunkIdx];
    state.CurrentNonce = state.Nonce + (ui64)desiredSectorIdx;
    ui32 dataChunkSizeSectors = Format.ChunkSize / Format.SectorSize;
    TChunkWriter writer(Mon, *BlockDevice.Get(), Format, state.CurrentNonce, Format.ChunkKey, BufferPool.Get(),
            desiredSectorIdx, dataChunkSizeSectors, Format.MagicDataChunk, chunkIdx, nullptr, desiredSectorIdx,
            nullptr, ActorSystem, PDiskId, &DriveModel, Cfg->UseT1ha0HashInFooter, Cfg->EnableSectorEncryption);

    guard.Release();

    LWTRACK(PDiskChunkWritePieceSendToDevice, evChunkWrite->Orbit, PDiskId, evChunkWrite->Owner, chunkIdx,
            pieceShift, pieceSize);

    ui32 bytesAvailable = pieceSize;
    Y_ABORT_UNLESS(evChunkWrite->BytesWritten == pieceShift);
    const ui32 count = evChunkWrite->PartsPtr->Size();
    for (ui32 partIdx = evChunkWrite->CurrentPart; partIdx < count; ++partIdx) {
        ui32 remainingPartSize = (*evChunkWrite->PartsPtr)[partIdx].second - evChunkWrite->CurrentPartOffset;
        auto traceId = evChunkWrite->SpanStack.GetTraceId();
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
            Y_ABORT_UNLESS(remainingPartSize);
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
    Y_ABORT_UNLESS(evChunkWrite->RemainingSize == 0);

    LOG_DEBUG_S(*ActorSystem, NKikimrServices::BS_PDISK, "PDiskId# " << (ui32)PDiskId
            << " chunkIdx# " << (ui32)chunkIdx
            << " ownerId# " << evChunkWrite->Owner
            << " ChunkWrite writer at sectorIdx# " << writer.SectorIdx
            << " Marker# BPD79");

    if (!writer.IsEmptySector()) {
        auto traceId = evChunkWrite->SpanStack.GetTraceId();
        *Mon.BandwidthPChunkPadding += writer.SectorBytesFree;
        writer.WriteZeroes(writer.SectorBytesFree, evChunkWrite->ReqId, &traceId);
        LOG_INFO(*ActorSystem, NKikimrServices::BS_PDISK, "PDiskId# %" PRIu32 " chunkIdx# %" PRIu32
            " was zero-padded after writing", (ui32)PDiskId, (ui32)chunkIdx);
    }
    auto traceId = evChunkWrite->SpanStack.GetTraceId();
    evChunkWrite->Completion->Orbit = std::move(evChunkWrite->Orbit);
    writer.Flush(evChunkWrite->ReqId, &traceId, evChunkWrite->Completion.Release());


    evChunkWrite->IsReplied = true;
    return true;
}

void TPDisk::SendChunkWriteError(TChunkWrite &chunkWrite, const TString &errorReason,
        NKikimrProto::EReplyStatus status) {
    Y_DEBUG_ABORT_UNLESS(errorReason);
    Y_DEBUG_ABORT_UNLESS(status != NKikimrProto::OK);
    LOG_ERROR_S(*ActorSystem, NKikimrServices::BS_PDISK, errorReason);
    Y_ABORT_UNLESS(!chunkWrite.IsReplied);
    NPDisk::TStatusFlags flags = status == NKikimrProto::OUT_OF_SPACE
        ? NotEnoughDiskSpaceStatusFlags(chunkWrite.Owner, chunkWrite.OwnerGroupType)
        : GetStatusFlags(chunkWrite.Owner, chunkWrite.OwnerGroupType);
    auto ev = std::make_unique<NPDisk::TEvChunkWriteResult>(status, chunkWrite.ChunkIdx, chunkWrite.Cookie, flags, errorReason);
    ev->Orbit = std::move(chunkWrite.Orbit);
    ActorSystem->Send(chunkWrite.Sender, ev.release());
    Mon.GetWriteCounter(chunkWrite.PriorityClass)->CountResponse();
    chunkWrite.IsReplied = true;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Chunk reading
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

void TPDisk::SendChunkReadError(const TIntrusivePtr<TChunkRead>& read, TStringStream& error, NKikimrProto::EReplyStatus status) {
    error << " for ownerId# " << read->Owner << " can't read chunkIdx# " << read->ChunkIdx;
    Y_ABORT_UNLESS(status != NKikimrProto::OK);
    LOG_ERROR_S(*ActorSystem, NKikimrServices::BS_PDISK, error.Str());

    THolder<NPDisk::TEvChunkReadResult> result = MakeHolder<NPDisk::TEvChunkReadResult>(status,
            read->ChunkIdx, read->Offset, read->Cookie, GetStatusFlags(read->Owner, read->OwnerGroupType), error.Str());
    ActorSystem->Send(read->Sender, result.Release());
    read->IsReplied = true;
    Mon.GetReadCounter(read->PriorityClass)->CountResponse();
}

TPDisk::EChunkReadPieceResult TPDisk::ChunkReadPiece(TIntrusivePtr<TChunkRead> &read, ui64 pieceCurrentSector,
        ui64 pieceSizeLimit, ui64 *reallyReadDiskBytes, NWilson::TTraceId traceId) {
    if (read->IsReplied) {
        return ReadPieceResultOk;
    }

    Y_VERIFY_S(pieceCurrentSector == read->CurrentSector, pieceCurrentSector << " != " << read->CurrentSector);
    ui64 sectorsCount = read->LastSector - read->FirstSector + 1;
    ui64 sectorsToRead = sectorsCount - read->CurrentSector;
    ui64 bytesToRead = sectorsToRead * Format.SectorSize;
    if (bytesToRead > pieceSizeLimit) {
        sectorsToRead = pieceSizeLimit / Format.SectorSize;
        bytesToRead = sectorsToRead * Format.SectorSize;
    }

    Y_ABORT_UNLESS(sectorsToRead);

    if (reallyReadDiskBytes) {
        *reallyReadDiskBytes = bytesToRead;
    }

    ui64 firstSector;
    ui64 lastSector;
    ui64 sectorOffset;
    bool isOk = ParseSectorOffset(Format, ActorSystem, PDiskId,
            read->Offset, read->Size, firstSector, lastSector, sectorOffset);
    Y_ABORT_UNLESS(isOk);

    ui64 currentSectorOffset = (ui64)read->CurrentSector * (ui64)Format.SectorSize;
    bool isTheFirstPart = read->CurrentSector == 0;
    bool isTheLastPart = read->FirstSector + read->CurrentSector + sectorsToRead > read->LastSector;

    ui64 payloadBytesToRead;
    ui64 payloadOffset;
    ParsePayloadFromSectorOffset(Format, read->FirstSector, read->FirstSector + read->CurrentSector + sectorsToRead - 1,
            read->FirstSector + read->CurrentSector, &payloadBytesToRead, &payloadOffset);

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
        Y_ABORT_UNLESS(read->RemainingSize == 0);
    }

    ui64 footerTotalSize = sectorsToRead * sizeof(TDataSectorFooter);
    *Mon.BandwidthPChunkReadPayload += bytesToRead - footerTotalSize;
    *Mon.BandwidthPChunkReadSectorFooter += footerTotalSize;

    ui64 readOffset = Format.Offset(read->ChunkIdx, read->FirstSector, currentSectorOffset);
    // TODO: Get this from the drive
    NWilson::TSpan span(TWilson::PDiskBasic, std::move(traceId), "PDisk.CompletionChunkReadPart", NWilson::EFlags::NONE, ActorSystem);
    traceId = span.GetTraceId();
    THolder<TCompletionChunkReadPart> completion(new TCompletionChunkReadPart(this, read, bytesToRead,
                payloadBytesToRead, payloadOffset, read->FinalCompletion, isTheLastPart, Cfg->UseT1ha0HashInFooter, std::move(span)));
    completion->CostNs = DriveModel.TimeForSizeNs(bytesToRead, read->ChunkIdx, TDriveModel::OP_TYPE_READ);
    Y_ABORT_UNLESS(bytesToRead <= completion->GetBuffer()->Size());
    ui8 *data = completion->GetBuffer()->Data();
    BlockDevice->PreadAsync(data, bytesToRead, readOffset, completion.Release(),
            read->ReqId, &traceId);
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
    i64 ownerFree = Keeper.GetOwnerFree(owner);
    double occupancy;
    auto color = Keeper.EstimateSpaceColor(owner, count, &occupancy);

    auto makeError = [&](TString info) {
        guard.Release();
        TStringStream str;
        str << "PDiskId# " << PDiskId
            << " Can't lock " << count << " chunks"
            << " for ownerId# " << owner
            << " sharedFree# " << sharedFree
            << " ownerFree# " << ownerFree
            << " estimatedColor after lock# " << NKikimrBlobStorage::TPDiskSpaceColor::E_Name(color)
            << " occupancy after lock# " << occupancy
            << " " << info
            << " Marker# BPD21";
        errorReason = str.Str();
        LOG_ERROR_S(*ActorSystem, NKikimrServices::BS_PDISK, str.Str());
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
            "PDiskId# " << PDiskId << " chunkIdx# " << chunkIdx << " desired ownerId# " << owner
                << " state# " << state.ToString());
        LOG_INFO_S(*ActorSystem, NKikimrServices::BS_PDISK, "PDiskId# " << PDiskId << " chunkIdx# " << chunkIdx <<
                " locked, ownerId# " << state.OwnerId << " -> " << owner);
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
        LOG_ERROR_S(*ActorSystem, NKikimrServices::BS_PDISK,
            "Can't lock chunks by color# " << TPDiskSpaceColor_Name(color) <<
            ", this space color flag is already raised. Marker# BPD33");
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

        LOG_ERROR_S(*ActorSystem, NKikimrServices::BS_PDISK, str.Str());
        guard.Release();
        ActorSystem->Send(evChunkLock.Sender, new NPDisk::TEvChunkLockResult(
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

    LOG_INFO(*ActorSystem, NKikimrServices::BS_PDISK, "PDiskId# %" PRIu32 " Locked %" PRIu32 \
        " chunks from owner# " PRIu32, (ui32)PDiskId, (ui32)result->LockedChunks.size(), (ui32)owner);

    guard.Release();
    ActorSystem->Send(evChunkLock.Sender, new NPDisk::TEvChunkLockResult(NKikimrProto::OK, result->LockedChunks,
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

        LOG_ERROR_S(*ActorSystem, NKikimrServices::BS_PDISK, str.Str());
        guard.Release();
        ActorSystem->Send(evChunkUnlock.Sender, new NPDisk::TEvChunkUnlockResult(NKikimrProto::ERROR, 0, str.Str()));
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
    LOG_INFO(*ActorSystem, NKikimrServices::BS_PDISK, "PDiskId# %" PRIu32 " Unlocked %" PRIu32 "",
         (ui32)PDiskId, (ui32)unlockedChunks);
    ActorSystem->Send(evChunkUnlock.Sender, new NPDisk::TEvChunkUnlockResult(NKikimrProto::OK, unlockedChunks));
    return;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Chunk reservation
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

TVector<TChunkIdx> TPDisk::AllocateChunkForOwner(const TRequestBase *req, const ui32 count, TString &errorReason) {
    // chunkIdx = 0 is deprecated and will not be soon removed
    TGuard<TMutex> guard(StateMutex);
    Y_DEBUG_ABORT_UNLESS(IsOwnerUser(req->Owner));

    const ui32 sharedFree = Keeper.GetFreeChunkCount() - 1;
    i64 ownerFree = Keeper.GetOwnerFree(req->Owner);
    double occupancy;
    auto color = Keeper.EstimateSpaceColor(req->Owner, count, &occupancy);

    auto makeError = [&](TString info) {
        guard.Release();
        TStringStream str;
        str << "PDiskId# " << PDiskId
            << " Can't reserve " << count << " chunks"
            << " for ownerId# " << req->Owner
            << " sharedFree# " << sharedFree
            << " ownerFree# " << ownerFree
            << " estimatedColor after allocation# " << NKikimrBlobStorage::TPDiskSpaceColor::E_Name(color)
            << " occupancy after allocation# " << occupancy
            << " " << info
            << " Marker# BPD20";
        errorReason = str.Str();
        LOG_ERROR_S(*ActorSystem, NKikimrServices::BS_PDISK, str.Str());
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
        auto traceId = req->SpanStack.GetTraceId();
        OnNonceChange(NonceData, req->ReqId, &traceId);
        // Remember who owns the sector, save chunk Nonce in order to be able to continue writing the chunk
        TChunkState &state = ChunkState[chunkIdx];
        Y_VERIFY_S(state.OwnerId == OwnerUnallocated
                || state.OwnerId == OwnerUnallocatedTrimmed
                || state.CommitState == TChunkState::FREE,
            "PDiskId# " << PDiskId << " chunkIdx# " << chunkIdx << " desired ownerId# " << req->Owner
                << " state# " << state.ToString());
        state.Nonce = chunkNonce;
        state.CurrentNonce = chunkNonce;
        LOG_INFO_S(*ActorSystem, NKikimrServices::BS_PDISK, "PDiskId# " << PDiskId << " chunkIdx# " << chunkIdx <<
                " allocated, ownerId# " << state.OwnerId << " -> " << req->Owner);
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
    ActorSystem->Send(evChunkReserve.Sender, result.Release());
    Mon.ChunkReserve.CountResponse();

}
bool TPDisk::ValidateForgetChunk(ui32 chunkIdx, TOwner owner, TStringStream& outErrorReason) {
    TGuard<TMutex> guard(StateMutex);
    if (chunkIdx >= ChunkState.size()) {
        outErrorReason << "PDiskId# " << PDiskId
            << " Can't forget chunkIdx# " << chunkIdx
            << " > total# " << ChunkState.size()
            << " ownerId# " << owner
            << " Marker# BPD89";
        LOG_ERROR_S(*ActorSystem, NKikimrServices::BS_PDISK, outErrorReason.Str());
        return false;
    }
    if (ChunkState[chunkIdx].OwnerId != owner) {
        outErrorReason << "PDiskId# " << PDiskId
            << " Can't forget chunkIdx# " << chunkIdx
            << ", ownerId# " << owner
            << " != real ownerId# " << ChunkState[chunkIdx].OwnerId
            << " Marker# BPD90";
        LOG_ERROR_S(*ActorSystem, NKikimrServices::BS_PDISK, outErrorReason.Str());
        return false;
    }
    if (ChunkState[chunkIdx].CommitState != TChunkState::DATA_RESERVED_DECOMMIT_IN_PROGRESS
            && ChunkState[chunkIdx].CommitState != TChunkState::DATA_COMMITTED_DECOMMIT_IN_PROGRESS
            && ChunkState[chunkIdx].CommitState != TChunkState::DATA_DECOMMITTED) {
        outErrorReason << "PDiskId# " << PDiskId
            << " Can't forget chunkIdx# " << chunkIdx
            << " in CommitState# " << ChunkState[chunkIdx].CommitState
            << " ownerId# " << owner << " Marker# BPD91";
        LOG_ERROR_S(*ActorSystem, NKikimrServices::BS_PDISK, outErrorReason.Str());
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
                        Y_FAIL_S("PDiskId# " << PDiskId
                                << " ChunkForget with in flight, ownerId# " << (ui32)evChunkForget.Owner
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
                        Y_VERIFY_S(state.CommitsInProgress == 0,
                                "PDiskId# " << PDiskId << " chunkIdx# " << chunkIdx << " state# " << state.ToString());
                        LOG_INFO(*ActorSystem, NKikimrServices::BS_PDISK, "PDiskId# %" PRIu32 " chunkIdx# %" PRIu32
                                " forgotten, ownerId# %" PRIu32 " -> %" PRIu32,
                                (ui32)PDiskId, (ui32)chunkIdx, (ui32)state.OwnerId, (ui32)OwnerUnallocated);
                        Y_ABORT_UNLESS(state.OwnerId == evChunkForget.Owner);
                        Mon.UncommitedDataChunks->Dec();
                        state.OwnerId = OwnerUnallocated;
                        state.CommitState = TChunkState::FREE;
                        Keeper.PushFreeOwnerChunk(evChunkForget.Owner, chunkIdx);
                        break;
                    default:
                        Y_FAIL_S("PDiskId# " << PDiskId
                                << " ChunkForget, ownerId# " << (ui32)evChunkForget.Owner
                                << " chunkIdx# " << chunkIdx << " unexpected commitState# " << state.CommitState);
                }
            }
        }
        result = MakeHolder<NPDisk::TEvChunkForgetResult>(NKikimrProto::OK, 0);
        result->StatusFlags = GetStatusFlags(evChunkForget.Owner, evChunkForget.OwnerGroupType);
    }

    guard.Release();
    ActorSystem->Send(evChunkForget.Sender, result.Release());
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
        pdiskState.SetPDiskId(PDiskId);
        pdiskState.SetPath(Cfg->GetDevicePath());
        pdiskState.SetSerialNumber(Cfg->ExpectedSerial);
        pdiskState.SetAvailableSize(availableSize);
        pdiskState.SetTotalSize(totalSize);
        const auto& state = static_cast<NKikimrBlobStorage::TPDiskState::E>(Mon.PDiskState->Val());
        pdiskState.SetState(state);
        pdiskState.SetSystemSize(Format.ChunkSize * (Keeper.GetOwnerHardLimit(OwnerSystemLog) + Keeper.GetOwnerHardLimit(OwnerSystemReserve)));
        pdiskState.SetLogUsedSize(Format.ChunkSize * (Keeper.GetOwnerHardLimit(OwnerCommonStaticLog) - Keeper.GetOwnerFree(OwnerCommonStaticLog)));
        pdiskState.SetLogTotalSize(Format.ChunkSize * Keeper.GetOwnerHardLimit(OwnerCommonStaticLog));
        if (ExpectedSlotCount) {
            pdiskState.SetExpectedSlotCount(ExpectedSlotCount);
        }

        reportResult->DiskMetrics = MakeHolder<TEvBlobStorage::TEvControllerUpdateDiskStatus>();
        i64 minSlotSize = Max<i64>();
        for (const auto& [_, owner] : VDiskOwners) {
            minSlotSize = Min(minSlotSize, Keeper.GetOwnerHardLimit(owner) * Format.ChunkSize);
        }

        for (const auto& [vdiskId, owner] : VDiskOwners) {
            const TOwnerData &data = OwnerData[owner];
            // May be less than 0 if owner exceeded his quota
            i64 ownerAllocated = (i64)Keeper.GetOwnerUsed(owner) * Format.ChunkSize;
            i64 ownerFree = Max<i64>(0, minSlotSize - ownerAllocated);

            reportResult->VDiskStateVect.emplace_back(data.WhiteboardProxyId, NKikimrWhiteboard::TVDiskStateInfo());
            auto& vdiskInfo = std::get<1>(reportResult->VDiskStateVect.back());
            vdiskInfo.SetAvailableSize(ownerFree);
            vdiskInfo.SetAllocatedSize(ownerAllocated);

            NKikimrBlobStorage::TVDiskMetrics* vdiskMetrics = reportResult->DiskMetrics->Record.AddVDisksMetrics();
            VDiskIDFromVDiskID(vdiskId, vdiskMetrics->MutableVDiskId());
            vdiskMetrics->MutableVDiskId()->ClearGroupGeneration();
            vdiskMetrics->SetAvailableSize(ownerFree);
            vdiskMetrics->SetAllocatedSize(ownerAllocated);
            double occupancy;
            vdiskMetrics->SetStatusFlags(Keeper.GetSpaceStatusFlags(owner, &occupancy));
            vdiskMetrics->SetOccupancy(occupancy);
            auto *vslotId = vdiskMetrics->MutableVSlotId();
            vslotId->SetNodeId(ActorSystem->NodeId);
            vslotId->SetPDiskId(PDiskId);
            vslotId->SetVSlotId(data.VDiskSlotId);
        }

        NKikimrBlobStorage::TPDiskMetrics& pDiskMetrics = *reportResult->DiskMetrics->Record.AddPDisksMetrics();
        pDiskMetrics.SetPDiskId(PDiskId);
        pDiskMetrics.SetTotalSize(Format.DiskSize);
        pDiskMetrics.SetAvailableSize(availableSize);
        pDiskMetrics.SetMaxReadThroughput(DriveModel.Speed(TDriveModel::OP_TYPE_READ));
        pDiskMetrics.SetMaxWriteThroughput(DriveModel.Speed(TDriveModel::OP_TYPE_WRITE));
        pDiskMetrics.SetNonRealTimeMs(AtomicGet(NonRealTimeMs));
        pDiskMetrics.SetSlowDeviceMs(Max((ui64)AtomicGet(SlowDeviceMs), (ui64)*Mon.DeviceNonperformanceMs));
        pDiskMetrics.SetMaxIOPS(DriveModel.IOPS());
        if (minSlotSize != Max<i64>()) {
            pDiskMetrics.SetEnforcedDynamicSlotSize(minSlotSize);
            pdiskState.SetEnforcedDynamicSlotSize(minSlotSize);
        }
        pDiskMetrics.SetState(state);
    }

    ActorSystem->Send(whiteboardReport.Sender, reportResult);
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
                LOG_CRIT_S(*ActorSystem, NKikimrServices::BS_PDISK, "PDiskId# " << (ui32)PDiskId
                        << " TEvCutLog was undelivered to VDiskId# " << OwnerData[i].VDiskId.ToStringWOGeneration()
                        << " Marker# BPD24");
                return;
            }

        }
        LOG_INFO_S(*ActorSystem, NKikimrServices::BS_PDISK, "PDiskId# " << (ui32)PDiskId
                << " TEvCutLog was undelivered to unknown VDisk Marker# BPD25");
        return;
    }
    default:
        LOG_DEBUG_S(*ActorSystem, NKikimrServices::BS_PDISK, "PDiskId# " << (ui32)PDiskId
                << "Event#" << req.Event->ToString() << " was undelivered to ActorID# " << req.Sender
                << " Marker# BPD26");
        return;
    }
}

void TPDisk::CommitLogChunks(TCommitLogChunks &req) {
    TGuard<TMutex> guard(StateMutex);
    for (auto it = req.CommitedLogChunks.begin(); it != req.CommitedLogChunks.end(); ++it) {
        Y_VERIFY_S(ChunkState[*it].OwnerId == OwnerSystem, "Unexpected chunkIdx# " << *it << " ownerId# "
                << (ui32)ChunkState[*it].OwnerId << " in CommitLogChunks PDiskId# " << PDiskId);
        Y_DEBUG_ABORT_UNLESS(ChunkState[*it].CommitState == TChunkState::LOG_RESERVED);
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
        bool encrypt = true; // Always write encrypter format because some tests use wrong main key to initiate errors
        TSysLogWriter formatWriter(Mon, *BlockDevice.Get(), Format, nonce, mainKey, BufferPool.Get(),
                0, ReplicationFactor, Format.MagicFormatChunk, 0, nullptr, 0, nullptr, ActorSystem, PDiskId,
                &DriveModel, Cfg->UseT1ha0HashInFooter, encrypt);

        if (format.IsFormatInProgress()) {
            // Fill first bytes with magic pattern
            ui64 *formatBegin = reinterpret_cast<ui64*>(&format);
            ui64 *formatMagicEnd = reinterpret_cast<ui64*>((ui8*)&format + MagicIncompleteFormatSize);
            Y_ABORT_UNLESS((ui8*)formatMagicEnd - (ui8*)formatBegin <= (intptr_t)sizeof(format));
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
        TString textMessage, const bool isErasureEncodeUserLog, const bool trimEntireDevice) {
    TGuard<TMutex> guard(StateMutex);
    // Prepare format record
    alignas(16) TDiskFormat format;
    format.Clear();
    format.DiskSize = diskSizeBytes;
    format.SectorSize = sectorSizeBytes;
    ui64 erasureFlags = FormatFlagErasureEncodeUserLog;
    format.FormatFlags = (format.FormatFlags & (~erasureFlags)) | (isErasureEncodeUserLog ? erasureFlags : 0);
    format.ChunkSize = SystemChunkSize(format, userAccessibleChunkSizeBytes, sectorSizeBytes);
    format.Guid = diskGuid;
    format.ChunkKey = chunkKey;
    format.LogKey = logKey;
    format.SysLogKey = sysLogKey;
    format.InitMagic();
    memcpy(format.FormatText, textMessage.data(), Min(sizeof(format.FormatText) - 1, textMessage.size()));
    format.SysLogSectorCount = RecordsInSysLog * format.SysLogSectorsPerRecord();
    ui64 firstSectorIdx = format.FirstSysLogSectorIdx();
    ui64 endSectorIdx = firstSectorIdx + format.SysLogSectorCount * ReplicationFactor;
    format.SystemChunkCount = (endSectorIdx * format.SectorSize + format.ChunkSize - 1) / format.ChunkSize;
    // Check disk size
    {
        ui32 diskSizeChunks = format.DiskSizeChunks();
        Y_VERIFY_S(diskSizeChunks > format.SystemChunkCount + 2,
            "Incorrect disk parameters! Total chunks# " << diskSizeChunks
            << ", System chunks needed# " << format.SystemChunkCount << ", cant run with < 3 free chunks!"
            << " Debug format# " << format.ToString());
    }
    // Trim the entire device
    if (trimEntireDevice && DriveModel.IsTrimSupported()) {
        LOG_NOTICE_S(*ActorSystem, NKikimrServices::BS_PDISK, "Trim of the entire device started Marker# BPD28");
        NHPTimer::STime start = HPNow();
        TReqId reqId(TReqId::FormatTrim, AtomicIncrement(ReqCreator.LastReqId));
        BlockDevice->TrimSync(diskSizeBytes, 0);
        for (ui32 i = 0; i < ChunkState.size(); ++i) {
            if (ChunkState[i].OwnerId == OwnerUnallocated) {
                ChunkState[i].OwnerId = OwnerUnallocatedTrimmed;
            }
        }
        double trimDurationSec = HPSecondsFloat(HPNow() - start);
        LOG_NOTICE_S(*ActorSystem, NKikimrServices::BS_PDISK, "Trim of the entire device done, spent " <<
                trimDurationSec << " seconds, trim speed " << diskSizeBytes / (1u << 20) / trimDurationSec << " MiB/s"
                << " Marker# BPD29");
    }

    // Write and apply format record with magic in first bytes
    format.SetFormatInProgress(true);
    format.SetHash();
    WriteApplyFormatRecord(format, mainKey);

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
                nullptr, firstSectorIdx, nullptr, ActorSystem, PDiskId, &DriveModel, Cfg->UseT1ha0HashInFooter,
                Cfg->EnableSectorEncryption));

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

void TPDisk::ReplyErrorYardInitResult(TYardInit &evYardInit, const TString &str) {
    TStringStream error;
    error << "PDiskId# " << PDiskId << " YardInit error for VDiskId# " << evYardInit.VDisk.ToStringWOGeneration()
        << " reason# " << str;
    LOG_ERROR_S(*ActorSystem, NKikimrServices::BS_PDISK, error.Str());
    ui64 writeBlockSize = ForsetiOpPieceSizeCached;
    ui64 readBlockSize = ForsetiOpPieceSizeCached;
    ActorSystem->Send(evYardInit.Sender, new NPDisk::TEvYardInitResult(NKikimrProto::ERROR,
        DriveModel.SeekTimeNs() / 1000ull, DriveModel.Speed(TDriveModel::OP_TYPE_READ),
        DriveModel.Speed(TDriveModel::OP_TYPE_WRITE), readBlockSize, writeBlockSize,
        DriveModel.BulkWriteBlockSize(),
        GetUserAccessibleChunkSize(), GetChunkAppendBlockSize(), OwnerSystem, 0,
        GetStatusFlags(OwnerSystem, evYardInit.OwnerGroupType), TVector<TChunkIdx>(), error.Str()));
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

    TFirstUncommitted firstUncommitted = CommonLogger->FirstUncommitted.load();
    ownerData.LogEndPosition = TOwnerData::TLogEndPosition(firstUncommitted.ChunkIdx, firstUncommitted.SectorIdx);

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
    THolder<NPDisk::TEvYardInitResult> result(new NPDisk::TEvYardInitResult(NKikimrProto::OK,
                DriveModel.SeekTimeNs() / 1000ull, DriveModel.Speed(TDriveModel::OP_TYPE_READ),
                DriveModel.Speed(TDriveModel::OP_TYPE_WRITE), readBlockSize, writeBlockSize,
                DriveModel.BulkWriteBlockSize(), GetUserAccessibleChunkSize(), GetChunkAppendBlockSize(), owner,
                ownerRound, GetStatusFlags(OwnerSystem, evYardInit.OwnerGroupType), ownedChunks, nullptr));
    GetStartingPoints(owner, result->StartingPoints);
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

    AddCbsSet(owner);

    LOG_NOTICE_S(*ActorSystem, NKikimrServices::BS_PDISK, "PDiskId# " << PDiskId
            << " registered known VDisk# " << vDiskId << " as ownerId# " << owner
            << " ownerRound# " << ownerRound << " Marker# BPD30");

    ActorSystem->Send(evYardInit.Sender, result.Release());
    Mon.YardInit.CountResponse();
    AskVDisksToCutLogs(owner, false);
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
        Y_VERIFY_S(!data.HaveRequestsInFlight(), "owner# " << owner);
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

    LOG_INFO_S(*ActorSystem, NKikimrServices::BS_PDISK, "PDiskId# " << (ui32)PDiskId
            << " YardInitStart OwnerId " << owner
            << " new OwnerRound# " << evYardInit.OwnerRound
            << " ownerData.HaveRequestsInFlight()# " << ownerData.HaveRequestsInFlight()
            << " Marker# BPD56");
    // Update round and wait for all pending requests of old owner to finish
    ADD_RECORD_WITH_TIMESTAMP_TO_OPERATION_LOG(ownerData.OperationLog, "YardInitStart, OwnerId# "
            << owner << ", new OwnerRound# " << evYardInit.OwnerRound);
    ownerData.OwnerRound = evYardInit.OwnerRound;
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

        // Make sure owner round never decreases
        // Allocate quota for the owner
        // TODO(cthulhu): don't allocate more owners than expected
        Keeper.AddOwner(owner, vDiskId);

        TOwnerData& ownerData = OwnerData[owner];
        ownerData.Reset(false);
        // A new owner is created.

        AtomicIncrement(TotalOwners);
        ownerData.VDiskId = vDiskId;
        Y_ABORT_UNLESS(SysLogFirstNoncesToKeep.FirstNonceToKeep[owner] <= SysLogRecord.Nonces.Value[NonceLog]);
        SysLogFirstNoncesToKeep.FirstNonceToKeep[owner] = SysLogRecord.Nonces.Value[NonceLog];
        ownerData.CutLogId = evYardInit.CutLogId;
        ownerData.WhiteboardProxyId = evYardInit.WhiteboardProxyId;
        ownerData.VDiskSlotId = evYardInit.SlotId;
        ownerData.OwnerRound = evYardInit.OwnerRound;
        VDiskOwners[vDiskId] = owner;
        ownerData.Status = TOwnerData::VDISK_STATUS_SENT_INIT;
        SysLogRecord.OwnerVDisks[owner] = vDiskId;
        ownerRound = ownerData.OwnerRound;
        ADD_RECORD_WITH_TIMESTAMP_TO_OPERATION_LOG(ownerData.OperationLog, "YardInitFinish, OwnerId# " << owner);

        AddCbsSet(owner);

        LOG_NOTICE_S(*ActorSystem, NKikimrServices::BS_PDISK, "PDiskId# " << PDiskId
                << " new owner is created. ownerId# " << owner
                << " vDiskId# " << vDiskId.ToStringWOGeneration()
                << " FirstNonceToKeep# " << SysLogFirstNoncesToKeep.FirstNonceToKeep[owner]
                << " CutLogId# " << ownerData.CutLogId
                << " ownerRound# " << ownerData.OwnerRound
                << " Marker# BPD02");

        AskVDisksToCutLogs(OwnerSystem, false);
    }

    ui64 writeBlockSize = ForsetiOpPieceSizeCached;
    ui64 readBlockSize = ForsetiOpPieceSizeCached;

    THolder<NPDisk::TEvYardInitResult> result(new NPDisk::TEvYardInitResult(
        NKikimrProto::OK,
        DriveModel.SeekTimeNs() / 1000ull, DriveModel.Speed(TDriveModel::OP_TYPE_READ),
        DriveModel.Speed(TDriveModel::OP_TYPE_WRITE), readBlockSize, writeBlockSize,
        DriveModel.BulkWriteBlockSize(), GetUserAccessibleChunkSize(), GetChunkAppendBlockSize(), owner, ownerRound,
        GetStatusFlags(OwnerSystem, evYardInit.OwnerGroupType) | ui32(NKikimrBlobStorage::StatusNewOwner), TVector<TChunkIdx>(),
        nullptr));
    GetStartingPoints(result->PDiskParams->Owner, result->StartingPoints);
    WriteSysLogRestorePoint(new TCompletionEventSender(
        this, evYardInit.Sender, result.Release(), Mon.YardInit.Results), evYardInit.ReqId, {});
}

// Scheduler weight configuration

void TPDisk::ConfigureCbs(ui32 ownerId, EGate gate, ui64 weight) {
    NSchLab::TCbs *cbs = ForsetiScheduler.GetCbs(ownerId, gate);
    if (cbs) {
        cbs->Weight = weight;
    }
}

void TPDisk::SchedulerConfigure(const TConfigureScheduler &reqCfg) {
    // TODO(cthulhu): Check OwnerRound here
    const TPDiskSchedulerConfig& cfg = reqCfg.SchedulerCfg;
    ui32 ownerId = reqCfg.OwnerId;
    ui64 bytesTotalWeight = cfg.LogWeight + cfg.FreshWeight + cfg.CompWeight;
    ConfigureCbs(ownerId, GateLog, cfg.LogWeight * cfg.BytesSchedulerWeight);
    ConfigureCbs(ownerId, GateFresh, cfg.FreshWeight * cfg.BytesSchedulerWeight);
    ConfigureCbs(ownerId, GateComp, cfg.CompWeight * cfg.BytesSchedulerWeight);
    ConfigureCbs(ownerId, GateFastRead, cfg.FastReadWeight * bytesTotalWeight);
    ConfigureCbs(ownerId, GateOtherRead, cfg.OtherReadWeight * bytesTotalWeight);
    ConfigureCbs(ownerId, GateLoad, cfg.LoadWeight * bytesTotalWeight);
    ConfigureCbs(ownerId, GateHuge, cfg.HugeWeight * bytesTotalWeight);
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
                TString(),
                GetStatusFlags(OwnerSystem, evCheckSpace.OwnerGroupType));
    result->Occupancy = occupancy;
    ActorSystem->Send(evCheckSpace.Sender, result.release());
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
    Y_VERIFY_S(!state.HasAnyOperationsInProgress(), "PDiskId# " << PDiskId << " ForceDeleteChunk, ownerId# " << owner
                << " chunkIdx# " << chunkIdx << " has operationsInProgress, state# " << state.ToString());

    switch (state.CommitState) {
    case TChunkState::DATA_ON_QUARANTINE:
        LOG_NOTICE_S(*ActorSystem, NKikimrServices::BS_PDISK, "PDiskId# " << PDiskId
                << " chunkIdx# " << chunkIdx << " owned by ownerId# "  << state.OwnerId
                << " is released from quarantine and marked as free at ForceDeleteChunk");
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
        Y_FAIL_S("PDiskId# " << PDiskId << " ForceDeleteChunk, ownerId# " << owner
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
                if (state.CommitState == TChunkState::DATA_RESERVED
                        || state.CommitState == TChunkState::DATA_DECOMMITTED
                        || state.CommitState == TChunkState::DATA_RESERVED_DECOMMIT_IN_PROGRESS
                        || state.CommitState == TChunkState::DATA_COMMITTED_DECOMMIT_IN_PROGRESS) {
                    Mon.UncommitedDataChunks->Dec();
                } else if (state.CommitState == TChunkState::DATA_COMMITTED) {
                    Mon.CommitedDataChunks->Dec();
                    LOG_DEBUG(*ActorSystem, NKikimrServices::BS_PDISK, "PDiskId# %" PRIu32
                            " Line# %" PRIu32 " --CommitedDataChunks# %" PRIi64 " chunkIdx# %" PRIu32 " Marker# BPD84",
                            (ui32)PDiskId, (ui32)__LINE__, (i64)Mon.CommitedDataChunks->Val(), (ui32)i);
                } else if (state.CommitState == TChunkState::LOCKED) {
                    Mon.LockedChunks->Dec();
                }
                if (state.CommitState == TChunkState::DATA_ON_QUARANTINE) {
                    if (!pushedOwnerIntoQuarantine) {
                        pushedOwnerIntoQuarantine = true;
                        ADD_RECORD_WITH_TIMESTAMP_TO_OPERATION_LOG(OwnerData[owner].OperationLog, "KillOwner(), Add owner to quarantine, "
                                << "CommitState# DATA_ON_QUARANTINE, OwnerId# " << owner);
                        QuarantineOwners.push_back(owner);
                        LOG_NOTICE_S(*ActorSystem, NKikimrServices::BS_PDISK, "PDiskId# " << PDiskId
                                << " push ownerId# " << owner
                                << " into quarantine as there is a chunk in DATA_ON_QUARANTINE");
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
                        LOG_NOTICE_S(*ActorSystem, NKikimrServices::BS_PDISK, "PDiskId# " << PDiskId
                                << " push ownerId# " << owner << " into quarantine");
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
            LOG_NOTICE_S(*ActorSystem, NKikimrServices::BS_PDISK, "PDiskId# " << PDiskId
                    << " push ownerId# " << owner
                    << " into quarantine as there are requests in flight");
        }
        if (!pushedOwnerIntoQuarantine) {
            ADD_RECORD_WITH_TIMESTAMP_TO_OPERATION_LOG(OwnerData[owner].OperationLog, "KillOwner(), Remove owner without quarantine, OwnerId# " << owner);
            Keeper.RemoveOwner(owner);
            LOG_NOTICE_S(*ActorSystem, NKikimrServices::BS_PDISK, "PDiskId# " << PDiskId
                    << " removed ownerId# " << owner << " from chunks Keeper");
        }

        TryTrimChunk(false, 0, NWilson::TSpan{});
        ui64 lastSeenLsn = 0;
        auto it = LogChunks.begin();
        while (it != LogChunks.end()) {
            if (it->OwnerLsnRange.size() > owner && it->OwnerLsnRange[owner].IsPresent) {
                Y_ABORT_UNLESS(it->CurrentUserCount > 0);
                it->CurrentUserCount--;
                it->OwnerLsnRange[owner].IsPresent = false;
                it->OwnerLsnRange[owner].FirstLsn = 0;
                lastSeenLsn = Max(it->OwnerLsnRange[owner].LastLsn, lastSeenLsn);
                it->OwnerLsnRange[owner].LastLsn = 0;
            }
            ++it;
        }

        ReleaseUnusedLogChunks(completionAction);

        TVDiskID vDiskId = SysLogRecord.OwnerVDisks[owner];
        vDiskId.GroupGeneration = -1;  // Since it might be non-zero.
                                      // TODO(cthulhu): Replace with VERIFY.
        SysLogRecord.OwnerVDisks[owner] = TVDiskID::InvalidId;

        Y_ABORT_UNLESS(AtomicGet(TotalOwners) > 0);
        AtomicDecrement(TotalOwners);

        TOwnerRound ownerRound = OwnerData[owner].OwnerRound;
        OwnerData[owner].Reset(pushedOwnerIntoQuarantine);
        OwnerData[owner].OwnerRound = ownerRound;
        VDiskOwners.erase(vDiskId);

        LOG_NOTICE_S(*ActorSystem, NKikimrServices::BS_PDISK, "PDiskId# " << PDiskId
                << " KillOwner, ownerId# " << owner << " ownerRound# " << ownerRound
                << " VDiskId# " << vDiskId.ToStringWOGeneration() << " lastSeenLsn# " << lastSeenLsn << " Marker# BPD12");
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
        if (it == VDiskOwners.end()) {
            TStringStream str;
            str << "PDiskId# " << (ui32)PDiskId << " Can't slay VDiskId# " << evSlay.VDiskId;
            str << " as it is not created yet or is already slain"
                << " Marker# BPD31";
            LOG_ERROR(*ActorSystem, NKikimrServices::BS_PDISK, "%s", str.Str().c_str());
            THolder<NPDisk::TEvSlayResult> result(new NPDisk::TEvSlayResult(
                NKikimrProto::ALREADY,
                GetStatusFlags(evSlay.Owner, evSlay.OwnerGroupType), evSlay.VDiskId, evSlay.SlayOwnerRound,
                evSlay.PDiskId, evSlay.VSlotId, str.Str()));
            ActorSystem->Send(evSlay.Sender, result.Release());
            Mon.YardSlay.CountResponse();
            return;
        }
        TOwner owner = it->second;
        TOwnerRound ownerRound = OwnerData[owner].OwnerRound;
        if (evSlay.SlayOwnerRound <= ownerRound) {
            TStringStream str;
            str << "PDiskId# " << (ui32)PDiskId << " Can't slay VDiskId# " << evSlay.VDiskId;
            str << " as SlayOwnerRound# " << evSlay.SlayOwnerRound << " <= ownerRound# " << ownerRound
                << " Marker# BPD32";
            LOG_ERROR(*ActorSystem, NKikimrServices::BS_PDISK, "%s", str.Str().c_str());
            THolder<NPDisk::TEvSlayResult> result(new NPDisk::TEvSlayResult(
                NKikimrProto::RACE,
                GetStatusFlags(evSlay.Owner, evSlay.OwnerGroupType), evSlay.VDiskId, evSlay.SlayOwnerRound,
                evSlay.PDiskId, evSlay.VSlotId, str.Str()));
            ActorSystem->Send(evSlay.Sender, result.Release());
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
    NHPTimer::STime now = HPNow();
    for (auto it = JointChunkWrites.begin(); it != JointChunkWrites.end(); ++it) {
        TRequestBase *req = (*it);
        req->SpanStack.PopOk();
        req->SpanStack.Push(TWilson::PDiskDetailed, "PDisk.InBlockDevice", NWilson::EFlags::AUTO_END);
        switch (req->GetType()) {
            case ERequestType::RequestChunkWritePiece:
            {
                TChunkWritePiece *piece = static_cast<TChunkWritePiece*>(req);
                if (ChunkWritePiece(piece->ChunkWrite.Get(), piece->PieceShift, piece->PieceSize)) {
                    Mon.IncrementQueueTime(piece->ChunkWrite->PriorityClass,
                            piece->ChunkWrite->LifeDurationMs(now));
                }
                delete piece;
                break;
            }
            default:
                Y_FAIL_S("Unexpected request type# " << ui64(req->GetType()) << " in JointChunkWrites");
        }
    }
    JointChunkWrites.clear();
}

void TPDisk::ProcessChunkReadQueue() {
    if (JointChunkReads.empty()) {
        return;
    }

    NHPTimer::STime now = HPNow();
    // Size (bytes) of elementary sectors block, it is useless to read/write less than that blockSize
    ui64 bufferSize = BufferPool->GetBufferSize() / Format.SectorSize * Format.SectorSize;

    for (auto& req : JointChunkReads) {

        req->SpanStack.PopOk();
        req->SpanStack.Push(TWilson::PDiskDetailed, "PDisk.InBlockDevice", NWilson::EFlags::AUTO_END);
        switch (req->GetType()) {
            case ERequestType::RequestChunkReadPiece:
            {
                TChunkReadPiece *piece = static_cast<TChunkReadPiece*>(req.Get());
                Y_ABORT_UNLESS(!piece->SelfPointer);
                TIntrusivePtr<TChunkRead> &read = piece->ChunkRead;
                TReqId reqId = read->ReqId;
                ui32 chunkIdx = read->ChunkIdx;
                bool isComplete = false;
                ui8 priorityClass = read->PriorityClass;
                NHPTimer::STime creationTime = read->CreationTime;
                if (!read->IsReplied) {
                    LOG_DEBUG_S(*ActorSystem, NKikimrServices::BS_PDISK, "PDiskId# " << (ui32)PDiskId
                        << " ReqId# " << reqId
                        << " Performing TChunkReadPiece chunkIdx# " << chunkIdx
                        << " Marker# BPD36");

                    ui32 size = 0;
                    while (!isComplete && size < piece->PieceSizeLimit) {
                        ui64 currentLimit = Min(bufferSize, piece->PieceSizeLimit - size);
                        ui64 reallyReadDiskBytes;
                        EChunkReadPieceResult result = ChunkReadPiece(read, piece->PieceCurrentSector + size / Format.SectorSize,
                                currentLimit, &reallyReadDiskBytes, piece->SpanStack.GetTraceId());
                        isComplete = (result != ReadPieceResultInProgress);
                        // Read pieces is sliced previously and it is expected that ChunkReadPiece will read exactly
                        // currentLimit bytes
                        Y_VERIFY_S(reallyReadDiskBytes == currentLimit, reallyReadDiskBytes << " != " << currentLimit);
                        size += currentLimit;
                    }
                }
                piece->OnSuccessfulDestroy(ActorSystem);
                if (isComplete) {
                    //
                    // WARNING: Don't access "read" after this point.
                    // Don't add code before the warning!
                    //
                    Mon.IncrementQueueTime(priorityClass, HPMilliSeconds(now - creationTime));
                    LOG_DEBUG_S(*ActorSystem, NKikimrServices::BS_PDISK, "PDiskId# " << (ui32)PDiskId
                        << " ReqId# " << reqId
                        << " enqueued all TChunkReadPiece chunkIdx# " << chunkIdx
                        << " Marker# BPD37");
                }
                break;
            }
            default:
                Y_FAIL_S("Unexpected request type# " << ui64(req->GetType()) << " in JointChunkReads");
        }
    }
    JointChunkReads.clear();
}

void TPDisk::TrimAllUntrimmedChunks() {
    TGuard<TMutex> g(StateMutex);
    if (!DriveModel.IsTrimSupported()) {
        return;
    }

    while (ui32 idx = Keeper.PopUntrimmedFreeChunk()) {
        BlockDevice->TrimSync(Format.ChunkSize, idx * Format.ChunkSize);
        Y_VERIFY_S(ChunkState[idx].OwnerId == OwnerUnallocated || ChunkState[idx].OwnerId == OwnerUnallocatedTrimmed,
                "PDiskId# " << PDiskId << " Unexpected ownerId# " << ui32(ChunkState[idx].OwnerId));
        ChunkState[idx].OwnerId = OwnerUnallocatedTrimmed;
        Keeper.PushTrimmedFreeChunk(idx);
    }
}

void TPDisk::ProcessChunkTrimQueue() {
    Y_ABORT_UNLESS(JointChunkTrims.size() <= 1);
    for (auto it = JointChunkTrims.begin(); it != JointChunkTrims.end(); ++it) {
        TChunkTrim *trim = (*it);
        trim->SpanStack.PopOk();
        trim->SpanStack.Push(TWilson::PDiskDetailed, "PDisk.InBlockDevice", NWilson::EFlags::AUTO_END);
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

    {
        const auto it = std::partition(QuarantineOwners.begin(), QuarantineOwners.end(), [&] (TOwner i) {
            return Keeper.GetOwnerUsed(i) || OwnerData[i].HaveRequestsInFlight();
        });
        for (auto delIt = it; delIt != QuarantineOwners.end(); ++delIt) {
            ADD_RECORD_WITH_TIMESTAMP_TO_OPERATION_LOG(OwnerData[*delIt].OperationLog, "Remove owner from quarantine, OwnerId# " << *delIt);
            TOwnerRound ownerRound = OwnerData[*delIt].OwnerRound;
            OwnerData[*delIt].Reset(false);
            OwnerData[*delIt].OwnerRound = ownerRound;
            Keeper.RemoveOwner(*delIt);
            LOG_NOTICE_S(*ActorSystem, NKikimrServices::BS_PDISK, "PDiskId# " << PDiskId
                    << " removed ownerId# " << *delIt << " from chunks Keeper through QuarantineOwners");
        }
        QuarantineOwners.erase(it, QuarantineOwners.end());
        *Mon.QuarantineOwners = QuarantineOwners.size();
    }
}

// Should be called to initiate TRIM (on chunk delete or prev trim done)
void TPDisk::TryTrimChunk(bool prevDone, ui64 trimmedSize, const NWilson::TSpan& parentSpan) {
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
                    || ChunkState[ChunkBeingTrimmed].OwnerId == OwnerUnallocatedTrimmed, "PDiskId# " << PDiskId
                    << " Unexpected ownerId# " << ui32(ChunkState[ChunkBeingTrimmed].OwnerId));
        }
        TrimOffset = 0;
    } else if (TrimOffset >= Format.ChunkSize) { // Previous chunk entirely trimmed
        Y_VERIFY_S(ChunkState[ChunkBeingTrimmed].OwnerId == OwnerUnallocated
                || ChunkState[ChunkBeingTrimmed].OwnerId == OwnerUnallocatedTrimmed, "PDiskId# " << PDiskId
                << " Unexpected ownerId# " << ui32(ChunkState[ChunkBeingTrimmed].OwnerId));
        ChunkState[ChunkBeingTrimmed].OwnerId = OwnerUnallocatedTrimmed;
        Keeper.PushTrimmedFreeChunk(ChunkBeingTrimmed);
        ChunkBeingTrimmed = Keeper.PopUntrimmedFreeChunk();
        if (ChunkBeingTrimmed) {
            Y_VERIFY_S(ChunkState[ChunkBeingTrimmed].OwnerId == OwnerUnallocated
                    || ChunkState[ChunkBeingTrimmed].OwnerId == OwnerUnallocatedTrimmed, "PDiskId# " << PDiskId
                    << " Unexpected ownerId# " << ui32(ChunkState[ChunkBeingTrimmed].OwnerId));
        }
        TrimOffset = 0;
    }

    if (ChunkBeingTrimmed) { // Initiate trim of next part of chunk
        const ui64 trimStep = (Keeper.GetTrimmedFreeChunkCount() > 100 ? 2 << 20 : 32 << 20);
        ui64 trimSize = Min<ui64>(Format.ChunkSize - TrimOffset, trimStep);
        TChunkTrim* trim = ReqCreator.CreateChunkTrim(ChunkBeingTrimmed, TrimOffset, trimSize, parentSpan);
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
            case ERequestType::RequestConfigureScheduler:
                SchedulerConfigure(static_cast<TConfigureScheduler&>(*req));
                break;
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
                TryTrimChunk(true, static_cast<TTryTrimChunk&>(*req).TrimSize, req->SpanStack.PeekTop() ? *req->SpanStack.PeekTop() : static_cast<const NWilson::TSpan&>(NWilson::TSpan{}));
                break;
            case ERequestType::RequestReleaseChunks:
                MarkChunksAsReleased(static_cast<TReleaseChunks&>(*req));
                break;
            default:
                Y_FAIL_S("Unexpected request type# " << (ui64)req->GetType());
                break;
        }
    }
    FastOperationsQueue.clear();
}

void TPDisk::ProcessChunkForgetQueue() {
    if (JointChunkForgets.empty())
        return;

    for (auto& req : JointChunkForgets) {
        req->SpanStack.PopOk();
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

    str << " Path# \"" << Cfg->Path << "\"";
    if (ActorSystem) {
        LOG_NOTICE_S(*ActorSystem, NKikimrServices::BS_PDISK, "PDiskId# " << (ui32)PDiskId << str.Str()
                << " Marker# BPD38");
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Internal interface
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

bool TPDisk::Initialize(TActorSystem *actorSystem, const TActorId &pDiskActor) {
#define REGISTER_LOCAL_CONTROL(control) \
    actorSystem->AppData<TAppData>()->Icb->RegisterLocalControl(control, \
            TStringBuilder() << "PDisk_" << PDiskId << "_" << #control)

    PDiskActor = pDiskActor;
    if (!IsStarted) {
        if (actorSystem && actorSystem->AppData<TAppData>() && actorSystem->AppData<TAppData>()->Icb) {
            REGISTER_LOCAL_CONTROL(SlowdownAddLatencyNs);
            REGISTER_LOCAL_CONTROL(EnableForsetiBinLog);
            REGISTER_LOCAL_CONTROL(ForsetiMinLogCostNsControl);
            REGISTER_LOCAL_CONTROL(ForsetiMilliBatchSize);
            REGISTER_LOCAL_CONTROL(ForsetiMaxLogBatchNs);
            REGISTER_LOCAL_CONTROL(ForsetiOpPieceSizeSsd);
            REGISTER_LOCAL_CONTROL(ForsetiOpPieceSizeRot);
            REGISTER_LOCAL_CONTROL(Cfg->UseT1ha0HashInFooter);

            if (Cfg->SectorMap) {
                auto diskModeParams = Cfg->SectorMap->GetDiskModeParams();
                if (diskModeParams) {
                    REGISTER_LOCAL_CONTROL(SectorMapFirstSectorReadRate);
                    REGISTER_LOCAL_CONTROL(SectorMapLastSectorReadRate);
                    REGISTER_LOCAL_CONTROL(SectorMapFirstSectorWriteRate);
                    REGISTER_LOCAL_CONTROL(SectorMapLastSectorWriteRate);
                    REGISTER_LOCAL_CONTROL(SectorMapSeekSleepMicroSeconds);

                    LastSectorReadRateControlName = TStringBuilder() << "PDisk_" << PDiskId << "_SectorMapLastSectorReadRate";
                    LastSectorWriteRateControlName = TStringBuilder() << "PDisk_" << PDiskId << "_SectorMapLastSectorWriteRate";
                }
            }
        }
        Y_ABORT_UNLESS(BlockDevice);
        BlockDevice->Initialize(actorSystem, PDiskActor);
        ActorSystem = actorSystem;
        ReqCreator.SetActorSystem(actorSystem);
        IsStarted = true;

        BufferPool = THolder<TBufferPool>(CreateBufferPool(Cfg->BufferPoolBufferSizeBytes, Cfg->BufferPoolBufferCount,
                UseHugePages, {Mon.DeviceBufferPoolFailedAllocations, ActorSystem, PDiskId}));

        if (ActorSystem) {
            LOG_INFO(*ActorSystem, NKikimrServices::BS_PDISK,
                "PDiskId# %" PRIu32 " Initialize Cfg# %s DriveModel# %s",
                (ui32)PDiskId, Cfg->ToString().data(), DriveModel.ToString().data());
        }
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
        if (ActorSystem) {
            LOG_CRIT_S(*ActorSystem, NKikimrServices::BS_PDISK, "PDiskId# " << (ui32)PDiskId
                << " BlockDevice initialization error! " << errStr.Str()
                << " Marker# BPD39");
        }
        return false;
    }

    OnDriveStartup();

    if (!Cfg->CheckSerial(DriveData.SerialNumber)) {
        BlockDevice->Stop();

        TStringStream str;
        str << "serial number mismatch, expected# " << Cfg->ExpectedSerial.Quote()
            << " but got# " << DriveData.SerialNumber.Quote();
        if (ActorSystem) {
            LOG_WARN_S(*ActorSystem, NKikimrServices::BS_PDISK, "PDiskId# " << PDiskId << " " << str.Str());
        }
        ErrorStr = str.Str();

        *Mon.PDiskState = NKikimrBlobStorage::TPDiskState::OpenFileError;
        *Mon.PDiskBriefState = TPDiskMon::TPDisk::Error;
        *Mon.PDiskDetailedState = TPDiskMon::TPDisk::ErrorDeviceSerialMismatch;

        *Mon.SerialNumberMismatched = 1;
        return false;
    } else {
        *Mon.SerialNumberMismatched = 0;
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
    Y_DEBUG_ABORT_UNLESS(status != NKikimrProto::OK);
    if (logWrite->Result && logWrite->Result->Status != NKikimrProto::OK) {
        return;
    }

    err << " error in TLogWrite for ownerId# " << logWrite->Owner << " ownerRound# " << logWrite->OwnerRound
        << " lsn# " << logWrite->Lsn;
    LOG_ERROR_S(*ActorSystem, NKikimrServices::BS_PDISK, err.Str());

    logWrite->SpanStack.PopError(err.Str());
    logWrite->Result.Reset(new NPDisk::TEvLogResult(status,
            GetStatusFlags(logWrite->Owner, logWrite->OwnerGroupType), err.Str()));
    logWrite->Result->Results.push_back(NPDisk::TEvLogResult::TRecord(logWrite->Lsn, logWrite->Cookie));
}

NKikimrProto::EReplyStatus TPDisk::CheckOwnerAndRound(TRequestBase* req, TStringStream& err) {
    const auto& ownerData = OwnerData[req->Owner];

    if (!IsOwnerUser(req->Owner)) {
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
    err << "PDiskId# " << PDiskId << " ";

    // Advisory check, further code may ignore results
    NKikimrProto::EReplyStatus errStatus = CheckOwnerAndRound(request, err);

    LOG_TRACE_S(*ActorSystem, NKikimrServices::BS_PDISK, "PreprocessRequest " << TypeName(*request)
            << " from ownerId# " << request->Owner << " round# " << request->OwnerRound
            << " errStatus# " << errStatus);

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
                LOG_ERROR_S(*ActorSystem, NKikimrServices::BS_PDISK, err.Str());
                auto result = MakeHolder<NPDisk::TEvReadLogResult>(errStatus, evLog.Position, evLog.Position, true,
                        GetStatusFlags(evLog.Owner, evLog.OwnerGroupType), err.Str(), evLog.Owner);
                ActorSystem->Send(evLog.Sender, result.Release());
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
            if (state.CommitState != TChunkState::DATA_RESERVED
                    && state.CommitState != TChunkState::DATA_COMMITTED
                    && state.CommitState != TChunkState::DATA_DECOMMITTED
                    && state.CommitState != TChunkState::DATA_RESERVED_DECOMMIT_IN_PROGRESS
                    && state.CommitState != TChunkState::DATA_COMMITTED_DECOMMIT_IN_PROGRESS) {
                err << "chunk has unexpected CommitState# " << state.CommitState;
                SendChunkReadError(read, err, NKikimrProto::ERROR);
                return false;
            }
            ui64 offset = 0;
            if (!ParseSectorOffset(Format, ActorSystem, PDiskId, read->Offset, read->Size,
                        read->FirstSector, read->LastSector, offset)) {
                err << "invalid size# " << read->Size << " and offset# " << read->Offset;
                SendChunkReadError(read, err, NKikimrProto::ERROR);
                return false;
            }
            TOwnerData &ownerData = OwnerData[request->Owner];
            read->SetOwnerGroupType(ownerData.IsStaticGroupOwner());
            ownerData.ReadThroughput.Increment(read->Size, ActorSystem->Timestamp());
            request->JobKind = NSchLab::JobKindRead;

            Y_ABORT_UNLESS(read->FinalCompletion == nullptr);


            ++state.OperationsInProgress;
            ++ownerData.InFlight->ChunkReads;
            auto onDestroy = [&, inFlight = ownerData.InFlight]() {
                --state.OperationsInProgress;
                --inFlight->ChunkReads;
            };
            auto completionSpan = request->SpanStack.CreateChild(TWilson::PDiskTopLevel, "PDisk.CompletionChunkRead");
            read->FinalCompletion = new TCompletionChunkRead(this, read, std::move(onDestroy), state.Nonce, std::move(completionSpan));

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
                     Y_DEBUG_ABORT_UNLESS(chunks.size() == 1);
                     ev.ChunkIdx = chunks.front();
                 }
            }
            TChunkState &state = ChunkState[ev.ChunkIdx];
            if (state.OwnerId == OwnerSystem) {
                err << "Can't write chunkIdx# " << ev.ChunkIdx
                    << " destination chunk is owned by the system! ownerId# " << ev.Owner;
                SendChunkWriteError(ev, err.Str(), NKikimrProto::ERROR);
                delete request;
                return false;
            }
            if (state.CommitState != TChunkState::DATA_RESERVED
                    && state.CommitState != TChunkState::DATA_COMMITTED
                    && state.CommitState != TChunkState::DATA_DECOMMITTED
                    && state.CommitState != TChunkState::DATA_RESERVED_DECOMMIT_IN_PROGRESS
                    && state.CommitState != TChunkState::DATA_COMMITTED_DECOMMIT_IN_PROGRESS) {
                err << "Can't write chunkIdx# " << ev.ChunkIdx
                    << " destination chunk has CommitState# " << state.CommitState
                    << " ownerId# " << ev.Owner;
                SendChunkWriteError(ev, err.Str(), NKikimrProto::ERROR);
                delete request;
                return false;
            }

            ev.SetOwnerGroupType(ownerData.IsStaticGroupOwner());
            ownerData.WriteThroughput.Increment(ev.TotalSize, ActorSystem->Timestamp());
            request->JobKind = NSchLab::JobKindWrite;


            auto result = std::make_unique<TEvChunkWriteResult>(NKikimrProto::OK, ev.ChunkIdx, ev.Cookie,
                        GetStatusFlags(ev.Owner, ev.OwnerGroupType), TString());
            result->Orbit = std::move(ev.Orbit);

            ++state.OperationsInProgress;
            ++ownerData.InFlight->ChunkWrites;
            auto onDestroy = [&, inFlight = ownerData.InFlight]() {
                --state.OperationsInProgress;
                --inFlight->ChunkWrites;
            };
            ev.Completion = MakeHolder<TCompletionChunkWrite>(ev.Sender, result.release(), &Mon, PDiskId,
                    ev.CreationTime, ev.TotalSize, ev.PriorityClass, std::move(onDestroy), ev.ReqId,
                    ev.SpanStack.CreateChild(TWilson::PDiskTopLevel, "PDisk.CompletionChunkWrite"));

            return true;
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
            ownerData.WriteThroughput.Increment(log->Data.size(), ActorSystem->Timestamp());
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
                LOG_ERROR_S(*ActorSystem, NKikimrServices::BS_PDISK, err.Str());
                THolder<NPDisk::TEvCheckSpaceResult> result(new NPDisk::TEvCheckSpaceResult(errStatus,
                            GetStatusFlags(ev.Owner, ev.OwnerGroupType), 0, 0, 0, 0, err.Str()));
                ActorSystem->Send(ev.Sender, result.Release());
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
                ActorSystem->Send(ev.Sender, result.Release());
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
                LOG_ERROR_S(*ActorSystem, NKikimrServices::BS_PDISK, err.Str());
                THolder<NPDisk::TEvSlayResult> result(new NPDisk::TEvSlayResult(NKikimrProto::ERROR,
                            GetStatusFlags(ev.Owner, ev.OwnerGroupType),
                            ev.VDiskId, ev.SlayOwnerRound, ev.PDiskId, ev.VSlotId, err.Str()));
                ActorSystem->Send(ev.Sender, result.Release());
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
                LOG_ERROR_S(*ActorSystem, NKikimrServices::BS_PDISK, err.Str());
                THolder<NPDisk::TEvChunkReserveResult> result(new NPDisk::TEvChunkReserveResult(errStatus,
                            GetStatusFlags(ev.Owner, ev.OwnerGroupType), err.Str()));
                ActorSystem->Send(ev.Sender, result.Release());
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
                ActorSystem->Send(ev.Sender, new NPDisk::TEvYardControlResult(NKikimrProto::OK, ev.Cookie, TString()));
                Mon.YardControl.CountResponse();
                IsQueuePaused = true;
                delete request;
                return false; // OK
            }
            err << " Can't process yard control Action " << (ui32)ev.Action;
            LOG_ERROR_S(*ActorSystem, NKikimrServices::BS_PDISK, err.Str());
            ActorSystem->Send(ev.Sender, new NPDisk::TEvYardControlResult(NKikimrProto::ERROR, ev.Cookie, err.Str()));
            Mon.YardControl.CountResponse();
            delete request;
            return false;
        }
        case ERequestType::RequestAskForCutLog:
            break;
        case ERequestType::RequestConfigureScheduler:
            break;
        case ERequestType::RequestWhiteboartReport:
            break;
        case ERequestType::RequestHttpInfo:
            break;
        case ERequestType::RequestUndelivered:
            break;
        case ERequestType::RequestCommitLogChunks:
            break;
        case ERequestType::RequestLogCommitDone:
            break;
        case ERequestType::RequestTryTrimChunk:
            break;
        case ERequestType::RequestReleaseChunks:
            break;
        case ERequestType::RequestStopDevice:
            BlockDevice->Stop();
            delete request;
            return false;
        default:
            Y_ABORT();
            break;
    }
    return true;
}

void TPDisk::PushRequestToForseti(TRequestBase *request) {
    if (request->GateId != GateFastOperation) {
        bool isAdded = false;

        NSchLab::TCbs *cbs = ForsetiScheduler.GetCbs(request->Owner, request->GateId);
        if (!cbs) {
            LOG_ERROR_S(*ActorSystem, NKikimrServices::BS_PDISK, "PDiskId# " << (ui32)PDiskId
                        << " ReqId# " <<  request->ReqId
                        << " Cost# " << request->Cost
                        << " JobKind# " << (ui64)request->JobKind
                        << " ownerId# " << request->Owner
                        << " GateId# " << (ui64)request->GateId
                        << " PushRequestToForseti Can't push to Forseti! Trying system log gate."
                        << " Marker# BPD44");
            Mon.ForsetiCbsNotFound->Inc();
            ui8 originalGateId = request->GateId;
            request->GateId = GateLog;
            cbs = ForsetiScheduler.GetCbs(OwnerSystem, request->GateId);
            if (!cbs) {
                TStringStream str;
                str << "PDiskId# " << (ui32)PDiskId
                    << " ReqId# " <<  request->ReqId
                    << " Cost# " << request->Cost
                    << " JobKind# " << (ui64)request->JobKind
                    << " ownerId# " << request->Owner
                    << " GateId# " << (ui64)request->GateId
                    << " originalGateId# " << (ui64)originalGateId
                    << " PushRequestToForseti Can't push to Forseti! Request may get lost."
                    << " Marker# BPD45";
                Y_FAIL_S(str.Str());
            }
        }

        if (request->GetType() == ERequestType::RequestLogWrite) {
            TIntrusivePtr<NSchLab::TJob> job = cbs->PeekTailJob();
            if (job && job->Cost < ForsetiMaxLogBatchNsCached
                    && static_cast<TRequestBase *>(job->Payload)->GetType() == ERequestType::RequestLogWrite) {
                TLogWrite &batch = *static_cast<TLogWrite*>(job->Payload);

                if (auto span = request->SpanStack.Push(TWilson::PDiskDetailed, "PDisk.InScheduler.InLogWriteBatch")) {
                    span->Attribute("Batch.ReqId", static_cast<i64>(batch.ReqId.Id));
                }
                batch.AddToBatch(static_cast<TLogWrite*>(request));
                ui64 prevCost = job->Cost;
                job->Cost += request->Cost;

                ForsetiTimeNs++;
                ForsetiScheduler.OnJobCostChange(cbs, job, ForsetiTimeNs, prevCost);

                LOG_DEBUG(*ActorSystem, NKikimrServices::BS_PDISK, "PDiskId# %" PRIu32 " ReqId# %" PRIu64
                        " PushRequestToForseti AddToBatch in Forseti.",
                        (ui32)PDiskId, (ui64)request->ReqId.Id);

                isAdded = true;
            }
        }
        if (!isAdded) {
            if (request->GetType() == ERequestType::RequestChunkWrite) {
                TIntrusivePtr<TChunkWrite> whole(static_cast<TChunkWrite*>(request));
                ui32 smallJobSize = 0;
                ui32 smallJobCount = 0;
                ui32 largeJobSize = 0;
                SplitChunkJobSize(whole->TotalSize, &smallJobSize, &largeJobSize, &smallJobCount);
                for (ui32 idx = 0; idx < smallJobCount; ++idx) {
                    // Schedule small job.
                    auto span = request->SpanStack.CreateChild(TWilson::PDiskBasic, "PDisk.ChunkWritePiece", NWilson::EFlags::AUTO_END);
                    span.Attribute("small_job_idx", idx)
                        .Attribute("is_last_piece", false);
                    TChunkWritePiece *piece = new TChunkWritePiece(whole, idx * smallJobSize, smallJobSize, std::move(span));
                    piece->EstimateCost(DriveModel);
                    AddJobToForseti(cbs, piece, request->JobKind);
                }
                // Schedule large job (there always is one)
                auto span = request->SpanStack.CreateChild(TWilson::PDiskBasic, "PDisk.ChunkWritePiece", NWilson::EFlags::AUTO_END);
                span.Attribute("is_last_piece", true);
                TChunkWritePiece *piece = new TChunkWritePiece(whole, smallJobCount * smallJobSize, largeJobSize, std::move(span));
                piece->EstimateCost(DriveModel);
                AddJobToForseti(cbs, piece, request->JobKind);
                LWTRACK(PDiskAddWritePieceToScheduler, request->Orbit, PDiskId, request->ReqId.Id,
                        HPSecondsFloat(HPNow() - request->CreationTime), request->Owner, request->IsFast,
                        request->PriorityClass, whole->TotalSize);
            } else if (request->GetType() == ERequestType::RequestChunkRead) {
                TIntrusivePtr<TChunkRead> read = std::move(static_cast<TChunkRead*>(request)->SelfPointer);
                ui32 totalSectors = read->LastSector - read->FirstSector + 1;

                ui32 smallJobSize = (ForsetiOpPieceSizeCached + Format.SectorSize - 1) / Format.SectorSize;
                ui32 smallJobCount = totalSectors / smallJobSize;
                if (smallJobCount) {
                    smallJobCount--;
                }
                ui32 largeJobSize = totalSectors - smallJobSize * smallJobCount;

                for (ui32 idx = 0; idx < smallJobCount; ++idx) {
                    auto span = request->SpanStack.CreateChild(TWilson::PDiskBasic, "PDisk.ChunkReadPiece", NWilson::EFlags::AUTO_END);
                    span.Attribute("small_job_idx", idx)
                        .Attribute("is_last_piece", false);
                    // Schedule small job.
                    auto piece = new TChunkReadPiece(read, idx * smallJobSize,
                            smallJobSize * Format.SectorSize, false, std::move(span));
                    LWTRACK(PDiskChunkReadPieceAddToScheduler, read->Orbit, PDiskId, idx, idx * smallJobSize,
                            smallJobSize * Format.SectorSize);
                    piece->EstimateCost(DriveModel);
                    piece->SelfPointer = piece;
                    AddJobToForseti(cbs, piece, request->JobKind);
                }
                // Schedule large job (there always is one)
                auto span = request->SpanStack.CreateChild(TWilson::PDiskBasic, "PDisk.ChunkReadPiece");
                span.Attribute("is_last_piece", true);
                auto piece = new TChunkReadPiece(read, smallJobCount * smallJobSize,
                        largeJobSize * Format.SectorSize, true, std::move(span));
                LWTRACK(PDiskChunkReadPieceAddToScheduler, read->Orbit, PDiskId, smallJobCount,
                        smallJobCount * smallJobSize, largeJobSize * Format.SectorSize);
                piece->EstimateCost(DriveModel);
                piece->SelfPointer = piece;
                AddJobToForseti(cbs, piece, request->JobKind);

            } else {
                AddJobToForseti(cbs, request, request->JobKind);
            }
        }
    } else {
        FastOperationsQueue.push_back(std::unique_ptr<TRequestBase>(request));
        LOG_DEBUG(*ActorSystem, NKikimrServices::BS_PDISK, "PDiskId# %" PRIu32 " ReqId# %" PRIu64
                " PushRequestToForseti Push to FastOperationsQueue.size# %" PRIu64,
                (ui32)PDiskId, (ui64)request->ReqId.Id, (ui64)FastOperationsQueue.size());
    }
}

// Always produces a large job and sometimes produces some small jobs and a large job.
void TPDisk::SplitChunkJobSize(ui32 totalSize, ui32 *outSmallJobSize, ui32 *outLargeJobSize, ui32 *outSmallJobCount) {
    const ui64 sectorPayloadSize = Format.SectorPayloadSize();
    *outSmallJobSize = (ForsetiOpPieceSizeCached + sectorPayloadSize - 1) / sectorPayloadSize * sectorPayloadSize;
    *outSmallJobCount = totalSize / *outSmallJobSize;
    if (*outSmallJobCount) {
        (*outSmallJobCount)--;
    }
    *outLargeJobSize = totalSize - *outSmallJobSize * *outSmallJobCount;
}

void TPDisk::AddJobToForseti(NSchLab::TCbs *cbs, TRequestBase *request, NSchLab::EJobKind jobKind) {
    LWTRACK(PDiskAddToScheduler, request->Orbit, PDiskId, request->ReqId.Id, HPSecondsFloat(request->CreationTime),
            request->Owner, request->IsFast, request->PriorityClass);
    request->SpanStack.Push(TWilson::PDiskDetailed, "PDisk.InScheduler");
    TIntrusivePtr<NSchLab::TJob> job = ForsetiScheduler.CreateJob();
    job->Payload = request;
    job->Cost = request->Cost;
    job->JobKind = jobKind;
    ForsetiTimeNs++;
    ForsetiScheduler.AddJob(cbs, job, request->Owner, request->GateId, ForsetiTimeNs);
    LOG_DEBUG(*ActorSystem, NKikimrServices::BS_PDISK, "PDiskId# %" PRIu32 " ReqId# %" PRIu64
            " AddJobToForseti", (ui32)PDiskId, (ui64)request->ReqId.Id);
}

void TPDisk::RouteRequest(TRequestBase *request) {
    LOG_DEBUG(*ActorSystem, NKikimrServices::BS_PDISK, "PDiskId# %" PRIu32 " ReqId# %" PRIu64 " Type# %" PRIu64
            " RouteRequest",
            (ui32)PDiskId, (ui64)request->ReqId.Id, (ui64)request->GetType());

    LWTRACK(PDiskRouteRequest, request->Orbit, PDiskId, request->ReqId.Id, HPSecondsFloat(request->CreationTime),
            request->Owner, request->IsFast, request->PriorityClass);

    switch (request->GetType()) {
        case ERequestType::RequestLogRead:
            [[fallthrough]];
        case ERequestType::RequestLogReadContinue:
            [[fallthrough]];
        case ERequestType::RequestLogReadResultProcess:
            [[fallthrough]];
        case ERequestType::RequestLogSectorRestore:
            if (auto span = request->SpanStack.PeekTop()) {
                span->Event("move_to_batcher", {});
            }
            JointLogReads.push_back(request);
            break;
        case ERequestType::RequestChunkReadPiece:
        {
            TChunkReadPiece *piece = static_cast<TChunkReadPiece*>(request);
            if (auto span = request->SpanStack.PeekTop()) {
                span->Event("move_to_batcher", {});
            }
            JointChunkReads.emplace_back(piece->SelfPointer.Get());
            piece->SelfPointer.Reset();
            // FIXME(cthulhu): Unreserve() for TChunkReadPiece is called while processing to avoid requeueing issues
            break;
        }
        case ERequestType::RequestChunkWritePiece:
            if (auto span = request->SpanStack.PeekTop()) {
                span->Event("move_to_batcher", {});
            }
            JointChunkWrites.push_back(request);
            break;
        case ERequestType::RequestChunkTrim:
        {
            if (auto span = request->SpanStack.PeekTop()) {
                span->Event("move_to_batcher", {});
            }
            TChunkTrim *trim = static_cast<TChunkTrim*>(request);
            JointChunkTrims.push_back(trim);
            break;
        }
        case ERequestType::RequestLogWrite:
        {
            TLogWrite *log = static_cast<TLogWrite*>(request);
            while (log) {
                TLogWrite *batch = log->PopFromBatch();

                if (auto span = log->SpanStack.PeekTop()) {
                    (*span)
                        .Event("move_to_batcher", {})
                        .Attribute("HasCommitRecord", log->Signature.HasCommitRecord());
                }
                JointLogWrites.push_back(log);
                if (log->Signature.HasCommitRecord()) {
                    JointCommits.push_back(log);
                }
                log = batch;
            }
            break;
        }
        case ERequestType::RequestChunkForget:
        {
            if (auto span = request->SpanStack.PeekTop()) {
                span->Event("move_to_batcher", {});
            }
            TChunkForget *forget = static_cast<TChunkForget*>(request);
            JointChunkForgets.push_back(std::unique_ptr<TChunkForget>(forget));
            break;
        }
        default:
            Y_FAIL_S("RouteRequest, unexpected request type# " << ui64(request->GetType()));
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
                    Y_ABORT();
                }
                break;
            }
            if (ev->Action == NPDisk::TEvYardControl::ActionResume) {
                ActorSystem->Send(ev->Sender, new NPDisk::TEvYardControlResult(NKikimrProto::OK, ev->Cookie,
                            TString()));
                Mon.YardControl.CountResponse();
                delete ev;
                continue;
            }
            if (ev->Action == NPDisk::TEvYardControl::ActionStep) {
                ActorSystem->Send(ev->Sender, new NPDisk::TEvYardControlResult(NKikimrProto::OK, ev->Cookie,
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
                PushRequestToForseti(ev);
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
    TGuard<TMutex> guard(StateMutex);
    while (InputQueue.GetWaitingSize() > 0) {
        TRequestBase* request = InputQueue.Pop();
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
                        ActorSystem->Send(evControl.Sender, new NPDisk::TEvYardControlResult(
                            NKikimrProto::OK, evControl.Cookie, TString()));
                        Mon.YardControl.CountResponse();
                        break;
                    case NPDisk::TEvYardControl::ActionResume:
                    {
                        ActorSystem->Send(evControl.Sender, new NPDisk::TEvYardControlResult(
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
                PushRequestToForseti(request);
            }
        }
    }
}

void TPDisk::Update() {
    Mon.UpdateDurationTracker.UpdateStarted();

    // ui32 userSectorSize = 0;

    // Make input queue empty
    {
        TGuard<TMutex> guard(StateMutex);
        ForsetiMaxLogBatchNsCached = ForsetiMaxLogBatchNs;
        ForsetiOpPieceSizeCached = PDiskCategory.IsSolidState() ? ForsetiOpPieceSizeSsd : ForsetiOpPieceSizeRot;
        EnqueueAll();
        /*userSectorSize = */Format.SectorPayloadSize();

        // Switch the scheduler when possible
        ForsetiScheduler.SetIsBinLogEnabled(EnableForsetiBinLog);
    }

    // Make token injection to correct drive model underestimations and avoid disk underutilization

    Mon.UpdateDurationTracker.SchedulingStart();

    // Schedule using Forseti Scheduler
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
        } else {
            break;
        }
    }
    ui64 totalCost = totalLogCost + totalNonLogCost;
    LWPROBE(PDiskForsetiCycle, PDiskId, nowCycles, prevForsetiTimeNs, ForsetiPrevTimeNs, timeCorrection,
            realDuration, virtualDuration, ForsetiTimeNs, totalCost, virtualDeadline);
    LWPROBE(PDiskMilliBatchSize, PDiskId, totalLogCost, totalNonLogCost, totalLogReqs, totalNonLogReqs);
    ForsetiRealTimeCycles = nowCycles;


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
        if (JointCommits.size()) {
            logSeekCostNs += DriveModel.SeekTimeNs();
        }
        LogSeekCostLoop.Push(logSeekCostNs);
    }

    Mon.UpdateDurationTracker.ProcessingStart();

    ClearQuarantineChunks();

    if (tact == ETact::TactLc) {
        ProcessLogWriteQueueAndCommits();
    }
    ProcessChunkWriteQueue();
    ProcessFastOperationsQueue();
    ProcessChunkReadQueue();
    ProcessLogReadQueue();
    ProcessChunkTrimQueue();
    if (tact != ETact::TactLc) {
        ProcessLogWriteQueueAndCommits();
    }
    ProcessChunkForgetQueue();
    LastTact = tact;


    ProcessYardInitSet();


    Mon.UpdateDurationTracker.WaitingStart(isNothingToDo);

    if (Cfg->SectorMap) {
        auto diskModeParams = Cfg->SectorMap->GetDiskModeParams();
        if (diskModeParams) {
            TAtomic prevValue;

            diskModeParams->FirstSectorReadRate.store(SectorMapFirstSectorReadRate);
            if (SectorMapFirstSectorReadRate < SectorMapLastSectorReadRate) {
                ActorSystem->AppData<TAppData>()->Icb->SetValue(LastSectorReadRateControlName, SectorMapFirstSectorReadRate, prevValue);
                diskModeParams->LastSectorReadRate.store(SectorMapFirstSectorReadRate);
            } else {
                diskModeParams->LastSectorReadRate.store(SectorMapLastSectorReadRate);
            }

            diskModeParams->FirstSectorWriteRate.store(SectorMapFirstSectorWriteRate);
            if (SectorMapFirstSectorWriteRate < SectorMapLastSectorWriteRate) {
                ActorSystem->AppData<TAppData>()->Icb->SetValue(LastSectorWriteRateControlName, SectorMapFirstSectorWriteRate, prevValue);
                diskModeParams->LastSectorWriteRate.store(SectorMapFirstSectorWriteRate);
            } else {
                diskModeParams->LastSectorWriteRate.store(SectorMapLastSectorWriteRate);
            }

            diskModeParams->SeekSleepMicroSeconds.store(SectorMapSeekSleepMicroSeconds);
        }
    }

    // Wait for something to do
    if (isNothingToDo && InputQueue.GetWaitingSize() == 0 && ForsetiScheduler.IsEmpty()) {
        // use deadline to be able to wakeup in situation of pdisk destruction
        InputQueue.ProducedWait(TDuration::MilliSeconds(10));
    }

    Mon.UpdateDurationTracker.UpdateEnded();
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
    AddCbs(ownerId, GateHuge, "Huge", 0ull);
    AddCbs(ownerId, GateSyncLog, "SyncLog", 0ull);
    AddCbs(ownerId, GateLow, "LowRead", 0ull);

    TConfigureScheduler conf(ownerId, 0);
    SchedulerConfigure(conf);
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
        LWPROBE(PDiskInputRequestTimeout, PDiskId, request->ReqId.Id, HPMilliSecondsFloat(timeout));
        request->Deadline = HPNow() + timeout;
    }
    ui64 qla = InputQueue.GetWaitingSize();
    Mon.InputQLA.Increment(qla);
    ui64 qca = AtomicAdd(InputQueueCost, request->Cost);
    Mon.InputQCA.Increment(qca / 1000000ull);
    LWTRACK(PDiskInputRequest, request->Orbit, PDiskId, request->ReqId.Id, HPSecondsFloat(request->CreationTime),
            double(request->Cost) / 1000000.0, qla, double(qca) / 1000000.0,
            HPSecondsFloat(request->Deadline),
            request->Owner, request->IsFast, request->PriorityClass, InputQueue.GetWaitingSize());

    LOG_DEBUG(*ActorSystem, NKikimrServices::BS_PDISK, "PDiskId# %" PRIu32 " ReqId# %" PRIu64
            " InputRequest InputQueue.Push priortiyClass# %" PRIu64 " creationTime# %f",
            (ui32)PDiskId, (ui64)request->ReqId.Id, (ui64)request->PriorityClass,
            HPSecondsFloat(request->CreationTime));

    InputQueue.Push(request);
}

} // NPDisk
} // NKikimr
