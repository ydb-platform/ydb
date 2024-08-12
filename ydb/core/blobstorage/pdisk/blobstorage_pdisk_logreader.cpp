#include "defs.h"
#include "blobstorage_pdisk_crypto.h"
#include "blobstorage_pdisk_impl.h"
#include "blobstorage_pdisk_logreader.h"
#include "blobstorage_pdisk_sectorrestorator.h"


namespace NKikimr {
namespace NPDisk {

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TPDisk log reading
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

//
// Called by the TLogReader on success with the current chunkOwnerMap
//
void TPDisk::ProcessChunkOwnerMap(TMap<ui32, TChunkState> &chunkOwnerMap) {
    TGuard<TMutex> guard(StateMutex);
    auto print = [&] () {
        std::map<TOwner, std::vector<ui32>> ownerToChunks;
        for (const auto& [idx, state] : chunkOwnerMap) {
            ownerToChunks[state.OwnerId].push_back(idx);
        }

        TStringStream str;
        str << "PDiskId# " << PDiskId << " ProcessChunkOwnerMap; ";
        for (auto& [owner, chunks] : ownerToChunks) {
            std::sort(chunks.begin(), chunks.end());
            str << " Owner# " << owner << " [";
            bool first = true;
            for (auto idx : chunks) {
                str << (std::exchange(first, false) ? "" : " ") << idx;
            }
            str << "];";

        }
        return str.Str();
    };
    LOG_INFO_S(*ActorSystem, NKikimrServices::BS_PDISK, print());

    for (TMap<ui32, TChunkState>::iterator it = chunkOwnerMap.begin(); it != chunkOwnerMap.end(); ++it) {
        ui32 chunkIdx = it->first;
        TOwner ownerId = it->second.OwnerId;
        ui64 chunkNonce = it->second.Nonce;
        TChunkState &state = ChunkState[chunkIdx];

        if (state.OwnerId == OwnerSystem && IsOwnerAllocated(ownerId)) {
            // The situation is:
            // ChunkState record states that the chunk is used by System (log or syslog/format)
            // OwnerMap states that the chunk is used by some user

            // Make sure the chunk is not really a part of syslog/format
            Y_VERIFY_S(chunkIdx > Format.SystemChunkCount, "PDiskId# " << PDiskId << " chunkIdx# " << chunkIdx
                    << " SystemChunkCount# " << Format.SystemChunkCount);

            // Make sure the chunk is not really a part of the log
            for (const auto& logChunk : LogChunks) {
                if (logChunk.ChunkIdx == chunkIdx) {
                    TStringStream out;
                    out << "PDiskId# " << PDiskId << " chunkIdx# " << chunkIdx;
                    out << " is a part of the log and is owned by user, ownerIdx# " << ownerId;
                    out << " LogChunks# {";
                    for (const auto& chunk : LogChunks) {
                        out << chunk << ", ";
                    }
                    out << "}";
                    Y_FAIL_S(out.Str());
                }
            }
            // After all, it looks like it's OK
        }

        if (state.OwnerId != OwnerSystem || state.OwnerId == ownerId) {
            if (IsOwnerUser(state.OwnerId) && state.CommitState == TChunkState::DATA_COMMITTED) {
                Mon.CommitedDataChunks->Dec();
                LOG_DEBUG(*ActorSystem, NKikimrServices::BS_PDISK, "PDiskId# %" PRIu32
                    " Line# %" PRIu32 " --CommitedDataChunks# %" PRIi64 " chunkIdx# %" PRIu32 " prev ownerId# %" PRIu32,
                    (ui32)PDiskId, (ui32)__LINE__, (i64)Mon.CommitedDataChunks->Val(), (ui32)chunkIdx,
                    (ui32)state.OwnerId);
            }
            state.OwnerId = ownerId;
            state.Nonce = chunkNonce;
            if (IsOwnerAllocated(ownerId)) {
                state.CommitState = TChunkState::DATA_COMMITTED;
                if (IsOwnerUser(ownerId)) {
                    Mon.CommitedDataChunks->Inc();
                    LOG_DEBUG(*ActorSystem, NKikimrServices::BS_PDISK, "PDiskId# %" PRIu32
                        " Line# %" PRIu32 " ++CommitedDataChunks# %" PRIi64 " chunkIdx# %" PRIu32 " ownerId# %" PRIu32,
                        (ui32)PDiskId, (ui32)__LINE__, (i64)Mon.CommitedDataChunks->Val(), (ui32)chunkIdx,
                        (ui32)ownerId);
                }
            } else {
                state.CommitState = TChunkState::FREE;
            }
        }
    }
}

void TPDisk::ProcessReadLogRecord(TLogRecordHeader &header, TString &data, NPDisk::TOwner owner, ui64 nonce,
    NPDisk::TEvReadLogResult* result, TMap<ui32, TChunkState> *outChunkOwnerMap, bool isInitial,
    bool parseCommitMessage) {

    if (isInitial || header.OwnerId == owner) {
        if (header.Signature.HasCommitRecord()) {
            TCommitRecordFooter *footer = (TCommitRecordFooter*)
                ((ui8*)data.data() + data.size() - sizeof(TCommitRecordFooter));
            ui32 *deletes = (ui32*)((ui8*)footer - footer->DeleteCount * sizeof(ui32));
            ui64 *commitNonces = (ui64*)((ui8*)deletes - footer->CommitCount * sizeof(ui64));
            ui32 *commits = (ui32*)((ui8*)commitNonces - footer->CommitCount * sizeof(ui32));
            {
                TGuard<TMutex> guard(StateMutex);
                TOwnerData &ownerData = OwnerData[header.OwnerId];
                if (isInitial) {
                    if (parseCommitMessage) {
                        for (ui32 i = 0; i < footer->DeleteCount; ++i) {
                            const ui32 chunkId = ReadUnaligned<ui32>(&deletes[i]);
                            (*outChunkOwnerMap)[chunkId].OwnerId = OwnerUnallocated;
                        }
                        for (ui32 i = 0; i < footer->CommitCount; ++i) {
                            const ui32 chunkId = ReadUnaligned<ui32>(&commits[i]);
                            (*outChunkOwnerMap)[chunkId].OwnerId = header.OwnerId;
                            (*outChunkOwnerMap)[chunkId].Nonce = ReadUnaligned<ui64>(&commitNonces[i]);
                        }
                    }
                    if (ownerData.VDiskId != TVDiskID::InvalidId) {
                        if (ownerData.CurrentFirstLsnToKeep < footer->FirstLsnToKeep) {
                            LOG_INFO(*ActorSystem, NKikimrServices::BS_PDISK, "PDiskId# %" PRIu32
                                    " ProcessReadLogRecord ownerId# %" PRIu32 " set FirstLsnToKeep# %" PRIu64
                                    " caused by Lsn# %" PRIu64, (ui32)PDiskId, (ui32)header.OwnerId,
                                    (ui64)footer->FirstLsnToKeep, (ui64)header.OwnerLsn);
                            ownerData.CurrentFirstLsnToKeep = footer->FirstLsnToKeep;
                        }
                        ownerData.LogRecordsInitiallyRead++;
                    }
                } else {
                    if (ownerData.VDiskId != TVDiskID::InvalidId) {
                        ownerData.LogRecordsConsequentlyRead++;
                    }
                }
            }

            data.resize(footer->UserDataSize);
            if (isInitial && footer->IsStartingPoint) {
                TGuard<TMutex> guard(StateMutex);
                TOwnerData &ownerData = OwnerData[header.OwnerId];
                if (ownerData.VDiskId != TVDiskID::InvalidId) {
                    TLogSignature unmasked = header.Signature.GetUnmasked();
                    ownerData.StartingPoints[unmasked] = NPDisk::TLogRecord(unmasked, TRcBuf(data), header.OwnerLsn);
                }
            }
        } else {
            TGuard<TMutex> guard(StateMutex);
            TOwnerData &ownerData = OwnerData[header.OwnerId];
            if (ownerData.VDiskId != TVDiskID::InvalidId) {
                if (isInitial) {
                    ownerData.LogRecordsInitiallyRead++;
                } else {
                    ownerData.LogRecordsConsequentlyRead++;
                }
            }
        }

        if (header.OwnerId == owner) {
            {
                TGuard<TMutex> guard(StateMutex);
                TOwnerData &ownerData = OwnerData[header.OwnerId];

                if (ownerData.VDiskId != TVDiskID::InvalidId) {
                    if (!ownerData.IsNextLsnOk(header.OwnerLsn)) {
                        TStringStream str;
                        str << "Lsn reversal! PDiskId# " << (ui32)PDiskId
                            << " ownerId# " << (ui32)owner
                            << " LogStartPosition# " << ownerData.LogStartPosition
                            << " LastSeenLsn# " << ownerData.LastSeenLsn
                            << " header.OwnerLsn# " << header.OwnerLsn
                            << " nonce# " << nonce
                            << Endl;
                        Y_FAIL_S(str.Str() << " operation log# " << ownerData.OperationLog.ToString());
                    }
                    ownerData.LastSeenLsn = header.OwnerLsn;
                }
            }
            result->Results.push_back(NPDisk::TLogRecord(header.Signature.GetUnmasked(), TRcBuf(data), header.OwnerLsn));
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TLogReader part
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct TLogReader::TSectorData {
    ui64 Offset;
    TBuffer::TPtr Buffer;
    ui32 DataSize;
    ui64 PreparedOffset;
    ui64 PreparedSize;
    bool IsScheduled;

    TSectorData(TBuffer *buffer, ui32 dataSize)
        : Offset(0)
        , Buffer(buffer)
        , DataSize(dataSize)
        , PreparedOffset(0)
        , PreparedSize(0)
        , IsScheduled(false)
    {}

    TString ToString() {
        TStringStream str;
        str << "{Offset# " << Offset
            << " Buffer->Data()# " << (void*)Buffer->Data()
            << " DataSize# " << DataSize
            << " PreparedOffset# " << PreparedOffset
            << " PreparedSize# " << PreparedSize
            << " IsScheduled# " << (IsScheduled ? "true" : "false")
            << "}";
        return str.Str();
    }

    ui8* GetData() {
        return Buffer->Data() + (Offset - PreparedOffset);
    }

    bool IsAvailable(ui64 offset, ui64 size) const {
        return PreparedOffset <= offset && offset + size <= PreparedOffset + PreparedSize;
    }

    bool IsAvailable(ui64 size) const {
        return IsAvailable(Offset, size);
    }

    ui8* GetDataIfAvailable(ui64 offset, ui64 size) {
        return IsAvailable(offset, size) ? GetData() : nullptr;
    }

    bool SetOffset(ui64 newOffset) {
        bool isValid = false;
        if (PreparedOffset <= newOffset && newOffset < PreparedOffset + PreparedSize) {
            if (newOffset < Offset) {
                PreparedOffset = 0;
                PreparedSize = 0;
            } else {
                isValid = true;
            }
        }
        Offset = newOffset;
        return isValid;
    }

    void Prepare(ui64 size) {
        PreparedOffset = Offset;
        PreparedSize = size;
    }
};

class TLogReader::TDoubleBuffer {
    THolder<TSectorData> SectorA;
    THolder<TSectorData> SectorB;
    const ui64 PDiskSectorSize;

public:
    TDoubleBuffer(TPDisk *pdisk)
        : SectorA(MakeHolder<TSectorData>(pdisk->BufferPool->Pop(), pdisk->Format.SectorSize * BufferSizeSectors))
        , SectorB(MakeHolder<TSectorData>(pdisk->BufferPool->Pop(), pdisk->Format.SectorSize * BufferSizeSectors))
        , PDiskSectorSize(pdisk->Format.SectorSize)
    {}

    ui64 BufferIdxFromOffset(ui64 innerOffset) const {
        return (innerOffset / (PDiskSectorSize * BufferSizeSectors)) % 2;
    }

    TSectorData *DataByOffset(ui64 offset) const {
        return DataByIdx(BufferIdxFromOffset(offset));
    }

    TSectorData *DataByIdx(ui32 idx) const {
        if (idx % 2) {
            return SectorB.Get();
        } else {
            return SectorA.Get();
        }
    }
};

TLogReader::TLogReader(bool isInitial,TPDisk *pDisk, TActorSystem * const actorSystem, const TActorId &replyTo, NPDisk::TOwner owner,
        TLogPosition ownerLogStartPosition, EOwnerGroupType ownerGroupType, TLogPosition position, ui64 sizeLimit,
        ui64 lastNonce, ui32 logEndChunkIdx, ui64 logEndSectorIdx, TReqId reqId,
        TVector<TLogChunkItem> &&chunksToRead, ui64 firstLsnToKeep, ui64 firstNonceToKeep, TVDiskID ownerVDiskId)
    : IsInitial(isInitial)
    , PDisk(pDisk)
    , ActorSystem(actorSystem)
    , ReplyTo(replyTo)
    , Owner(owner)
    , OwnerLogStartPosition(ownerLogStartPosition)
    , Position(position)
    , SizeLimit(sizeLimit)
    , Result(new NPDisk::TEvReadLogResult(
        NKikimrProto::ERROR, position, TLogPosition::Invalid(), false,
        pDisk->GetStatusFlags(owner, ownerGroupType), nullptr, owner))
    , ChunkInfo(nullptr)
    , Sector(new TDoubleBuffer(pDisk))
    , ChunkOwnerMap(IsInitial ? new TMap<ui32, TChunkState>() : nullptr)
    , State(ELogReaderState::PrepareToRead)
    , IsReplied(false)
    , LastGoodToWriteLogPosition{0, 0}
    , MaxNonce(lastNonce)
    , LastNonce(lastNonce)
    , LastDataNonce(lastNonce)
    , OnEndOfSplice(false)
    , Cypher(pDisk->Cfg->EnableSectorEncryption)
    , OffsetInSector(0)
    , SetLastGoodToWritePosition(true)
    , ChunkIdx(0)
    , SectorIdx(0)
    , IsLastRecordHeaderValid(false)
    , FirstLsnToKeep(firstLsnToKeep)
    , FirstNonceToKeep(firstNonceToKeep)
    , OwnerVDiskId(ownerVDiskId)
    , IsLastRecordSkipped(true)
    , ResultSize(0)
    , LastRecordHeader(0, 0, 0)
    , LastRecordHeaderNonce(0)
    , LastRecordDataWritePosition(0)
    , EndSectorIdx(0)
    , ExpectedOffset(0)
    , LogEndChunkIdx(logEndChunkIdx)
    , LogEndSectorIdx(logEndSectorIdx)
    , ReqId(reqId)
    , ChunksToRead(std::move(chunksToRead))
    , CurrentChunkToRead(ChunksToRead.end())
    , ParseCommits(false) // Actual only if IsInitial
{
    Y_ABORT_UNLESS(PDisk->PDiskThread.Id() == TThread::CurrentThreadId(), "Constructor of TLogReader must be called"
            " from PDiskThread");
    Cypher.SetKey(PDisk->Format.LogKey);
    AtomicIncrement(PDisk->InFlightLogRead);

    // If there was no log chunks when SysLog was written FirstLogChunkToParseCommits is equals to LogHeadChunkIdx
    ParseCommits = PDisk->FirstLogChunkToParseCommits == PDisk->SysLogRecord.LogHeadChunkIdx;

    auto printChunks = [&]() {
        TStringStream ss;
        ss << SelfInfo() << " ChunksToRead# [";
        bool first = true;
        for (auto item : ChunksToRead) {
            ss << (first ? first = false, "" : " ");
            if (item.IsPreviousChunkDropped) {
                ss << "- ";
            }
            if (item.IsPreviousChunkCut) {
                ss << "x ";
            }
            ss << item.ChunkIdx;
        }
        ss << "]";
        return ss.Str();
    };

    LOG_INFO_S(*PDisk->ActorSystem, NKikimrServices::BS_PDISK, SelfInfo()
            << " ChunksToRead# " << printChunks());
}

TLogReader::~TLogReader() {
    AtomicDecrement(PDisk->InFlightLogRead);
}

void TLogReader::Exec(ui64 offsetRead, TVector<ui64> &badOffsets, TActorSystem *actorSystem) {
    Y_UNUSED(actorSystem);
    TGuard<TMutex> guard(ExecMutex);
    if (IsReplied.load()) {
        return;
    }
    TDiskFormat &format = PDisk->Format;
    if (badOffsets.size()) {
        bool isOk = RegisterBadOffsets(badOffsets);
        if (!isOk) {
            LOG_ERROR(*ActorSystem, NKikimrServices::BS_PDISK,
                "PDiskId# %" PRIu32 " Log is damaged and unrevocerable!",
                (ui32)PDisk->PDiskId);
            ReplyError();
            return;
        }
    }
    ui64 idxRead = Sector->BufferIdxFromOffset(offsetRead);
    ui64 idxExpected = Sector->BufferIdxFromOffset(ExpectedOffset);
    Sector->DataByIdx(idxRead)->IsScheduled = false;

    if (idxRead != idxExpected && Sector->DataByIdx(idxExpected)->IsScheduled) {
        return;
    }

    Sector->DataByIdx(idxRead)->SetOffset(ExpectedOffset);

    while (true) {
        switch(State) {
        case ELogReaderState::PrepareToRead:
            if (PrepareToRead()) {
                return; // Already replied
            }
            State = ELogReaderState::NewLogChunk;
            break;
        case ELogReaderState::NewLogChunk:
            if (ChunkIdx == 0) {
                if (IsInitial) {
                    LOG_NOTICE_S(*PDisk->ActorSystem, NKikimrServices::BS_PDISK, SelfInfo()
                            << " In case ELogReaderState::NewLogChunk got ChunkIdx# " << ChunkIdx
                            << " LastGoodToWriteLogPosition# " << LastGoodToWriteLogPosition
                            << " Marker# LR014");
                } else {
                    Y_FAIL_S(SelfInfo() << " File# " << __FILE__ << " Line# " << __LINE__);
                }

                ReplyOk();
                return; // Already replied
            }
            if (IsInitial) {
                PDisk->LogChunks.push_back(TLogChunkInfo(ChunkIdx, (ui32)PDisk->OwnerData.size()));
                PDisk->Mon.LogChunks->Inc();
                ChunkInfo = &PDisk->LogChunks.back();
                ChunkInfo->IsEndOfSplice = std::exchange(OnEndOfSplice, false);
                if (PDisk->LogChunks.size() > 1) {
                    auto last = PDisk->LogChunks.rbegin();
                    // May be set in NonceJump record processing, and if so it should not be changed
                    if (!last->DesiredPrevChunkLastNonce) {
                        LOG_INFO_S(*PDisk->ActorSystem, NKikimrServices::BS_PDISK, SelfInfo()
                                << " In case ELogReaderState::NewLogChunk strange."
                                << " changing last->DesiredPrevChunkLastNonce# " << last->DesiredPrevChunkLastNonce
                                << " to std::next(last)->LastNonce# " << std::next(last)->LastNonce
                                << " Marker# LR015");
                        last->DesiredPrevChunkLastNonce = std::next(last)->LastNonce;
                    }
                }
                UpdateNewChunkInfo(ChunkIdx, {});
            }
            State = ELogReaderState::ScheduleForwardReads;
            break;
        case ELogReaderState::ScheduleForwardReads:
        {
            if (SectorIdx >= EndSectorIdx) {
                // Read next chunk reference
                if (IsInitial) {
                    UpdateLastGoodToWritePosition();

                    ui64 offset = format.Offset(ChunkIdx, SectorIdx);
                    ui64 idxToRead = Sector->BufferIdxFromOffset(offset);
                    TSectorData *data = Sector->DataByIdx(idxToRead);
                    if (!data->IsScheduled) {
                        State = ELogReaderState::TryProceedNextChunk;
                        ExpectedOffset = offset;
                        if (!data->SetOffset(offset)) {
                            ReleaseUsedBadOffsets();
                            ScheduleReadAsync(*data, ReplicationFactor);
                            return; // Continue when the next sector arrives
                        }
                        idxRead = idxToRead;
                        break;
                    }
                    ExpectedOffset = offset;
                    ReleaseUsedBadOffsets();
                    State = ELogReaderState::ScheduleForwardReads;
                    return; // Continue when the next sector arrives
                } else {
                    ++CurrentChunkToRead;
                    if (CurrentChunkToRead == ChunksToRead.end()) {
                        ReplyOk();
                        return; // Already replied
                    }
                    SwitchToChunk(CurrentChunkToRead->ChunkIdx);
                    if (CurrentChunkToRead->IsPreviousChunkDropped || CurrentChunkToRead->IsPreviousChunkCut) {
                        IsLastRecordSkipped = true; // There are gaps in the log!
                        IsLastRecordHeaderValid = false;
                    }
                    State = ELogReaderState::ScheduleForwardReads;
                }
            }
            UpdateLastGoodToWritePosition();
            ui64 offset = format.Offset(ChunkIdx, SectorIdx);
            ExpectedOffset = offset;

            ui64 idxToRead = Sector->BufferIdxFromOffset(offset);
            TSectorData *data = Sector->DataByIdx(idxToRead);
            if (data->IsScheduled) {
                ReleaseUsedBadOffsets();
                ScheduleForward(offset);
                State = ELogReaderState::ScheduleForwardReads;
                return; // Continue when the next sector arrives
            }
            State = ELogReaderState::ProcessAlreadyReadSectors;
            if (!data->SetOffset(offset)) {
                ui64 sectorsToRead = Min(EndSectorIdx - SectorIdx + ReplicationFactor, (ui64)BufferSizeSectors);
                ReleaseUsedBadOffsets();
                ScheduleReadAsync(*data, sectorsToRead);
                ScheduleForward(offset);
                return; // Continue when the next sector arrives
            }
            if (idxRead != idxToRead) {
                ScheduleForward(offset);
            }
            idxRead = idxToRead;
            break;
        }
        case ELogReaderState::ProcessAlreadyReadSectors:
        {
            ui64 sizeToProcess = (ui64)format.SectorSize;
            TSectorData *data = Sector->DataByIdx(idxRead);
            Y_ABORT_UNLESS(data->IsAvailable(sizeToProcess));
            bool isEndOfLog = ProcessSectorSet(data);
            data->SetOffset(data->Offset + sizeToProcess);
            if (isEndOfLog) {
                return; // Nothing to read
            }
            State = ELogReaderState::ScheduleForwardReads;
            break;
        }
        case ELogReaderState::TryProceedNextChunk:
        {
            ui64 sectorsToProcess = format.IsErasureEncodeNextChunkReference() ?  ReplicationFactor : 1;
            ui64 sizeToProcess = format.SectorSize * sectorsToProcess;
            TSectorData *data = Sector->DataByIdx(idxRead);
            if (!data->IsAvailable(sizeToProcess)) {
                ui64 offset = format.Offset(ChunkIdx, SectorIdx);
                ExpectedOffset = offset;
                data->SetOffset(offset);
                ScheduleReadAsync(*data, sectorsToProcess);
                return; // Continue when the next sector arrives
            }
            if (ProcessNextChunkReference(*data)) {
                return; // There is no next chunk, already replied
            }
            State = ELogReaderState::NewLogChunk;
            break;
        }
        default:
            Y_ABORT();
            break;
        }
    }// while (true)
}

void TLogReader::NotifyError(ui64 offsetRead, TString& errorReason) {
    TGuard<TMutex> guard(ExecMutex);
    if (IsReplied.load()) {
        return;
    }

    Result->ErrorReason = errorReason;

    LOG_ERROR(*ActorSystem, NKikimrServices::BS_PDISK,
        "PDiskId# %" PRIu32 " Error reading log with offset %" PRIu64,
        (ui32)PDisk->PDiskId, offsetRead);

    ReplyError();
}

TString TLogReader::SelfInfo() {
    TStringStream ss;
    ss << "PDiskId# " << PDisk->PDiskId
        << " LogReader"
        << " IsInitial# " << IsInitial;
    if (!IsInitial) {
        ss << " Owner# " << ui32(Owner)
            << " VDiskId# " << OwnerVDiskId.ToStringWOGeneration();
    }
    ss << " ChunkIdx# " << ChunkIdx
        << " SectorIdx# " << SectorIdx
        << " OffsetInSector# " << OffsetInSector;
    return ss.Str();
}

bool TLogReader::PrepareToRead() {
    TDiskFormat &format = PDisk->Format;
    if (Position == TLogPosition::Invalid()) {
        if (IsInitial) {
            Y_ABORT();
        }
        ReplyOk();
        return true;
    }
    if (Position == TLogPosition{0, 0}) {
        if (IsInitial) {
            Position = PDisk->LogPosition(PDisk->SysLogRecord.LogHeadChunkIdx, 0, 0);
        } else {
            if (ChunksToRead.size() == 0) {
                ReplyOk();
                return true;
            }
            if (OwnerLogStartPosition != TLogPosition{0, 0}) {
                ui32 startChunkIdx = OwnerLogStartPosition.ChunkIdx;
                Y_ABORT_UNLESS(startChunkIdx == ChunksToRead[0].ChunkIdx);
                Position = OwnerLogStartPosition;
            } else {
                Position = PDisk->LogPosition(ChunksToRead[0].ChunkIdx, 0, 0);
            }
            CurrentChunkToRead = ChunksToRead.begin();
        }
        ChunkIdx = Position.ChunkIdx;
    } else {
        ChunkIdx = Position.ChunkIdx;
        if (!IsInitial) {
            auto keepIt = ChunksToRead.begin();
            while (keepIt != ChunksToRead.end() && keepIt->ChunkIdx != ChunkIdx) {
                ++keepIt;
            }
            ChunksToRead.erase(ChunksToRead.begin(), keepIt);
            if (ChunksToRead.size() == 0) {
                LOG_ERROR(*ActorSystem, NKikimrServices::BS_PDISK,
                    "PDiskId# %" PRIu32 " No chunks to read log from!",
                    (ui32)PDisk->PDiskId);
                ReplyError();
                return true;
            }
            CurrentChunkToRead = ChunksToRead.begin();
        }
    }
    EndSectorIdx = PDisk->UsableSectorsPerLogChunk();
    ui64 noErasureSectorIdx = Position.OffsetInChunk / format.SectorSize;
    // TODO(cthulhu): Check that Position format can be used with 16 TB HDD
    SectorIdx = noErasureSectorIdx;
    OffsetInSector = Position.OffsetInChunk - noErasureSectorIdx * format.SectorSize;

    LastGoodToWriteLogPosition = Position;

    if (!IsInitial && ChunkIdx == LogEndChunkIdx && SectorIdx >= LogEndSectorIdx) {
        ReplyOk();
        return true;
    }

    return false;
}

void TLogReader::ScheduleForward(ui64 offset) {
    ui64 sectorsToRead = Min((ui64)EndSectorIdx - SectorIdx + ReplicationFactor, (ui64)BufferSizeSectors);
    ui64 nextSectorIdx = SectorIdx + sectorsToRead;
    if (nextSectorIdx < EndSectorIdx + ReplicationFactor) {
        offset += (ui64)sectorsToRead * (ui64)PDisk->Format.SectorSize;
        sectorsToRead = Min((ui64)EndSectorIdx - nextSectorIdx + ReplicationFactor, (ui64)BufferSizeSectors);

        TSectorData *data = Sector->DataByOffset(offset);
        if (!data->IsScheduled && !data->SetOffset(offset)) {
            ScheduleReadAsync(*data, sectorsToRead);
        }
    }
}

void TLogReader::UpdateLastGoodToWritePosition() {
    // SET ? Initial ?
    if (IsInitial || SetLastGoodToWritePosition) {
        // TODO: consider fixing offset in sector!
        LastGoodToWriteLogPosition = PDisk->LogPosition(ChunkIdx, SectorIdx + 0, OffsetInSector);
        if (IsInitial) {
            PDisk->InitialPreviousNonce = MaxNonce;
        }
        SetLastGoodToWritePosition = false;
    }
}

void TLogReader::LogRawData(ui8* data, ui64 size, TString info) {
    TStringStream str;
    ui64 lineSize = 20;
    ui64 lines = size / lineSize;
    str << info << Endl;
    str << "    : ";
    for (ui64 columnIdx = 0; columnIdx < lineSize; ++columnIdx) {
        str << Sprintf("%04" PRIu64 " ", (ui64)columnIdx);
    }
    str << Endl;
    for (ui64 lineIdx = 0; lineIdx < lines; ++lineIdx) {
        ui64 columns = (size - lineIdx * lineSize < lineSize) ? size % lineSize : lineSize;
        str << Sprintf("%04" PRIu64 ": ", (ui64)(lineIdx * lineSize));
        for (ui64 columnIdx = 0; columnIdx < columns; ++columnIdx) {
            ui64 byteIdx = lineIdx * lineSize + columnIdx;
            ui8 c = data[byteIdx];
            str << Sprintf("%02" PRIx64 "_%c ", (ui64)c, c >= ' ' && c <= 'z' ? (char)c : '?');
        }
        str << Endl;
    }
    Cerr << str.Str();
}

void TLogReader::ProcessLogPageTerminator(ui8 *data, ui32 sectorPayloadSize) {
    // log terminator offset in sector = OffsetInSector
    // The rest of the sector contains no data.
    auto *firstPageHeader = reinterpret_cast<TFirstLogPageHeader*>(data);
    ui32 sizeLeft = sectorPayloadSize - OffsetInSector;
    Y_ABORT_UNLESS(firstPageHeader->Size + sizeof(TFirstLogPageHeader) == sizeLeft);
    OffsetInSector += sizeLeft;
    SetLastGoodToWritePosition = true;
}

void TLogReader::ProcessLogPageNonceJump2(ui8 *data, const ui64 previousNonce, const ui64 previousDataNonce) {
    auto *nonceJumpLogPageHeader2 = reinterpret_cast<TNonceJumpLogPageHeader2*>(data);
    OffsetInSector += sizeof(TNonceJumpLogPageHeader2);

    if (IsInitial) {
        PDisk->LastNonceJumpLogPageHeader2 = *nonceJumpLogPageHeader2;


        if (SectorIdx == 0) {
            LOG_WARN_S(*PDisk->ActorSystem, NKikimrServices::BS_PDISK, SelfInfo()
                    << " nonce jump2 "
                    << " IsEndOfSplice# " << ChunkInfo->IsEndOfSplice
                    << " replacing ChunkInfo->DesiredPrevChunkLastNonce# " << ChunkInfo->DesiredPrevChunkLastNonce
                    << " with nonceJumpLogPageHeader2->PreviousNonce# " << nonceJumpLogPageHeader2->PreviousNonce
                    << " Marker# LR016");


            if (ChunkInfo->IsEndOfSplice) {
                // NonceJump can't be interpreted the usual way
                ChunkInfo->DesiredPrevChunkLastNonce = nonceJumpLogPageHeader2->PreviousNonce;
            } else {
                // For future log splices DesiredPrevChunkLastNonce should be equal to expected in NonceJump record
                ChunkInfo->DesiredPrevChunkLastNonce = nonceJumpLogPageHeader2->PreviousNonce;
            }
        }

        // TODO: Investigate / process error the proper way here.
        if (previousNonce > nonceJumpLogPageHeader2->PreviousNonce &&
                previousDataNonce > nonceJumpLogPageHeader2->PreviousNonce) {
            // We just came across an outdated nonce jump. This means the end of the log.
            LOG_WARN_S(*PDisk->ActorSystem, NKikimrServices::BS_PDISK, SelfInfo()
                    << " currentSectorIdx# " << SectorIdx
                    << " previousNonce# " << previousNonce
                    << " previousDataNonce# " << previousDataNonce
                    << " nonceJumpLogPageHeader2->PreviousNonce# " << nonceJumpLogPageHeader2->PreviousNonce
                    << " LastGoodToWriteLogPosition# " << LastGoodToWriteLogPosition
                    << " ReplyOk, marker LR001");
            ReplyOk();
            return;
        } else if (previousNonce < nonceJumpLogPageHeader2->PreviousNonce &&
                previousDataNonce < nonceJumpLogPageHeader2->PreviousNonce) {
            TStringStream str;
            str << "PDiskId# " << PDisk->PDiskId
                << "previousNonce# " << previousNonce
                << " and previousDataNonce# " << previousDataNonce
                << " != header->PreviousNonce# " << nonceJumpLogPageHeader2->PreviousNonce
                << " OffsetInSector# " << OffsetInSector
                << " sizeof(TNonceJumpLogPageHeader)# " << sizeof(TNonceJumpLogPageHeader2)
                << " chunkIdx# " << ChunkIdx
                << " sectorIdx# " << SectorIdx
                << " header# " << nonceJumpLogPageHeader2->ToString(false)
                << Endl;
            Y_FAIL_S(str.Str());
        }
    } else if (ChunkIdx == LogEndChunkIdx && SectorIdx >= LogEndSectorIdx) {
        LOG_DEBUG_S(*PDisk->ActorSystem, NKikimrServices::BS_PDISK, SelfInfo()
                << " ReplyOk, marker LR003"
                << " LogEndChunkIdx# " << LogEndChunkIdx
                << " LogEndSectorId# " << LogEndSectorIdx);
        ReplyOk();
        return;
    }

    SetLastGoodToWritePosition = true;
    IsLastRecordSkipped = false;
    IsLastRecordHeaderValid = false;
}

void TLogReader::ProcessLogPageNonceJump1(ui8 *data, const ui64 previousNonce) {
    auto *nonceJumpLogPageHeader1 = reinterpret_cast<TNonceJumpLogPageHeader1*>(data);
    OffsetInSector += sizeof(TNonceJumpLogPageHeader1);
    if (IsInitial) {
        // TODO: Investigate / process error the proper way here.
        if (previousNonce > nonceJumpLogPageHeader1->PreviousNonce) {
            // We just came across an outdated nonce jump. This means the end of the log.
            LOG_NOTICE_S(*PDisk->ActorSystem, NKikimrServices::BS_PDISK, SelfInfo()
                    << " In ProcessLogPageNonceJump1 got previousNonce > nonceJumpLogPageHeader1->PreviousNonce! "
                    << " previousNonce# " << previousNonce
                    << " nonceJumpLogPageHeader1->PreviousNonce# " << nonceJumpLogPageHeader1->PreviousNonce
                    << " LastGoodToWriteLogPosition# " << LastGoodToWriteLogPosition
                    << " Marker# LR017");
            ReplyOk();
            return;
        }
        Y_ABORT_UNLESS(previousNonce == nonceJumpLogPageHeader1->PreviousNonce,
                "previousNonce# %" PRIu64 " != header->PreviousNonce# %" PRIu64
                " OffsetInSector# %" PRIu64 " sizeof(TNonceJumpLogPageHeader1)# %" PRIu64
                " chunkIdx# %" PRIu64 " sectorIdx# %" PRIu64, // " header->Flags# %" PRIu64,
                (ui64)previousNonce, (ui64)nonceJumpLogPageHeader1->PreviousNonce,
                (ui64)OffsetInSector, (ui64)sizeof(TNonceJumpLogPageHeader1),
                (ui64)ChunkIdx, (ui64)SectorIdx); //, (ui64)pageHeader->Flags);
    }

    if (!IsInitial && ChunkIdx == LogEndChunkIdx && SectorIdx >= LogEndSectorIdx) {
        ReplyOk();
        return;
    }

    SetLastGoodToWritePosition = true;
    IsLastRecordSkipped = false;
    IsLastRecordHeaderValid = false;
}

bool TLogReader::ProcessSectorSet(TSectorData *sector) {
    TDiskFormat &format = PDisk->Format;
    UpdateLastGoodToWritePosition();

    const ui64 magic = format.MagicLogChunk;
    TSectorRestorator restorator(false, LogErasureDataParts, false, format,
        PDisk->ActorSystem, PDisk->PDiskActor, PDisk->PDiskId, &PDisk->Mon, PDisk->BufferPool.Get());
    restorator.Restore(sector->GetData(), sector->Offset, magic, LastNonce, Owner);

    if (!restorator.GoodSectorFlags) {
        if (IsInitial) {
            LOG_NOTICE_S(*PDisk->ActorSystem, NKikimrServices::BS_PDISK, SelfInfo()
                    << " In ProcessSectorSet got !restorator.GoodSectorFlags !"
                    << " LastGoodToWriteLogPosition# " << LastGoodToWriteLogPosition
                    << " Marker# LR018");
        } else {
           Y_VERIFY_S(ChunkIdx == LogEndChunkIdx && SectorIdx >= LogEndSectorIdx, SelfInfo()
                   << " File# " << __FILE__
                   << " Line# " << __LINE__
                   << " LogEndChunkIdx# " << LogEndChunkIdx
                   << " LogEndSectorIdx# " << LogEndSectorIdx);
            if (!(ChunkIdx == LogEndChunkIdx && SectorIdx >= LogEndSectorIdx)) {
                LOG_WARN_S(*PDisk->ActorSystem, NKikimrServices::BS_PDISK, SelfInfo()
                        << " In ProcessSectorSet got !restorator.GoodSectorFlags outside the LogEndSector."
                        << " File# " << __FILE__
                        << " Line# " << __LINE__
                        << " LogEndChunkIdx# " << LogEndChunkIdx
                        << " LogEndSectorIdx# " << LogEndSectorIdx
                        << " Marker# LR004");
            }
        }

        ReplyOk();
        return true;
    }
    {
        UpdateLastGoodToWritePosition();
        if (!(restorator.GoodSectorFlags & 1)) {
            if (IsInitial) {
                LOG_NOTICE_S(*PDisk->ActorSystem, NKikimrServices::BS_PDISK, SelfInfo()
                        << " In ProcessSectorSet got !(restorator.GoodSectorFlags & 1) !"
                        << " restorator.GoodSectorFlags# " << restorator.GoodSectorFlags
                        << " LastGoodToWriteLogPosition# " << LastGoodToWriteLogPosition
                        << " Marker# LR005");
            } else {
                Y_VERIFY_S(ChunkIdx == LogEndChunkIdx && SectorIdx >= LogEndSectorIdx, SelfInfo()
                        << " File# " << __FILE__
                        << " Line# " << __LINE__
                        << " LogEndChunkIdx# " << LogEndChunkIdx
                        << " LogEndSectorIdx# " << LogEndSectorIdx);
            }
            ReplyOk();
            return true;
        }

        ui8* rawSector = sector->GetData();
        TDataSectorFooter *sectorFooter = (TDataSectorFooter*)
            (rawSector + format.SectorSize - sizeof(TDataSectorFooter));

        LOG_DEBUG_S(*PDisk->ActorSystem, NKikimrServices::BS_PDISK, SelfInfo()
                << " currentSectorIdx# " << SectorIdx
                << " sectorFooter->Nonce# " << sectorFooter->Nonce
                << " Marker# LR020");

        ui64 previousNonce = std::exchange(LastNonce, sectorFooter->Nonce);
        ui64 previousDataNonce = std::exchange(LastDataNonce, sectorFooter->Nonce);
        if (LastNonce > MaxNonce) {
            MaxNonce = LastNonce;
        }
        if (IsInitial) {
            if (ChunkInfo->FirstNonce == 0) {
                ChunkInfo->FirstNonce = sectorFooter->Nonce;
            }
            ChunkInfo->LastNonce = sectorFooter->Nonce;
        }

        ui8 *data = rawSector;
        // Decrypt data
        Cypher.StartMessage(sectorFooter->Nonce);
        Cypher.InplaceEncrypt(data, format.SectorSize - ui32(sizeof(TDataSectorFooter)));
        PDisk->CheckLogCanary(rawSector, ChunkIdx, SectorIdx);

        ui32 maxOffsetInSector = format.SectorPayloadSize() - ui32(sizeof(TFirstLogPageHeader));
        while (OffsetInSector <= maxOffsetInSector) {
            TLogPageHeader *pageHeader = (TLogPageHeader*)(data + OffsetInSector);
            Y_ABORT_UNLESS(pageHeader->Version == PDISK_DATA_VERSION, "PDiskId# %" PRIu32
                " incompatible log page header version: %" PRIu32
                " (expected: %" PRIu32 ") at chunk %" PRIu32 " SectorSet: %" PRIu32 " Sector: %" PRIu32
                " Offset in sector: %" PRIu32 " A: %" PRIu32 " B: %" PRIu32, (ui32)PDisk->PDiskId,
                (ui32)pageHeader->Version, (ui32)PDISK_DATA_VERSION, (ui32)ChunkIdx, (ui32)SectorIdx,
                (ui32)0, (ui32)OffsetInSector, (ui32)pageHeader->A, (ui32)pageHeader->B);

            if (pageHeader->Flags & LogPageTerminator) {
                ProcessLogPageTerminator(data + OffsetInSector, format.SectorPayloadSize());
                continue;
            }
            if (pageHeader->Flags & LogPageNonceJump2) {
                if (IsInitial) {
                    LOG_INFO_S(*PDisk->ActorSystem, NKikimrServices::BS_PDISK, SelfInfo()
                            << " In ProcessSectorSet saw LogPageNonceJump2, before processing"
                            << " LastNonce# " << LastNonce
                            << " LastDataNonce# " << LastDataNonce
                            << " previousNonce# " << previousNonce
                            << " previousDataNonce# " << previousDataNonce
                            << " LastGoodToWriteLogPosition# " << LastGoodToWriteLogPosition
                            << " ChunkInfo->LastNonce# " << ChunkInfo->LastNonce
                            << " Marker# LR006");
                }

                ProcessLogPageNonceJump2(data + OffsetInSector, previousNonce, previousDataNonce);
                if (IsInitial) {
                    LOG_INFO_S(*PDisk->ActorSystem, NKikimrServices::BS_PDISK, SelfInfo()
                            << " In ProcessSectorSet saw LogPageNonceJump2, afer  processing"
                            << " LastNonce# " << LastNonce
                            << " LastDataNonce# " << LastDataNonce
                            << " previousNonce# " << previousNonce
                            << " previousDataNonce# " << previousDataNonce
                            << " LastGoodToWriteLogPosition# " << LastGoodToWriteLogPosition
                            << " ChunkInfo->LastNonce# " << ChunkInfo->LastNonce
                            << " Marker# LR007");
                }
                if (IsReplied.load()) {
                    return true;
                }
                continue;
            }
            if (pageHeader->Flags & LogPageNonceJump1) {
                ProcessLogPageNonceJump1(data + OffsetInSector, previousNonce);
                if (IsReplied.load()) {
                    return true;
                }
                continue;
            }

            if (IsInitial && previousNonce != 0) {
                if (SectorIdx == 0) {
                    if (OffsetInSector == 0) {
                        // Mark the desired prev chunk nonce so that splicing the log works as intended
                        ChunkInfo->DesiredPrevChunkLastNonce = previousNonce;
                    }

                    if (LastNonce != previousNonce + 1) {
                        LOG_NOTICE_S(*PDisk->ActorSystem, NKikimrServices::BS_PDISK, SelfInfo()
                                << " In ProcessSectorSet got LastNonce != previousNonce + 1 !"
                                << " LastNonce# " << LastNonce
                                << " previousNonce# " << previousNonce
                                << " LastGoodToWriteLogPosition# " << LastGoodToWriteLogPosition
                                << " ChunkInfo->IsEndOfSplice# " << ChunkInfo->IsEndOfSplice
                                << " Marker# LR008");

                        Y_VERIFY_S(!ChunkInfo->IsEndOfSplice, SelfInfo() <<
                                " Unexpectedly got LastNonce != previosNonce + 1 at Sector0 at the EndOfSplice!"
                                << " LastNonce# " << LastNonce
                                << " previousNonce# " << previousNonce
                                << " LastGoodToWriteLogPosition# " << LastGoodToWriteLogPosition
                                << " ChunkInfo->IsEndOfSplice# " << ChunkInfo->IsEndOfSplice
                                << " Marker# LR009");

                        ReplyOk();
                        return true;
                    }
                } else {
                    if (LastNonce != previousNonce + 1) {
                        LOG_NOTICE_S(*PDisk->ActorSystem, NKikimrServices::BS_PDISK, SelfInfo()
                                << " In ProcessSectorSet got LastNonce != previousNonce + 1 !"
                                << " LastNonce# " << LastNonce
                                << " previousNonce# " << previousNonce
                                << " LastGoodToWriteLogPosition# " << LastGoodToWriteLogPosition
                                << " ChunkInfo->IsEndOfSplice# " << ChunkInfo->IsEndOfSplice
                                << " Marker# LR010");

                        Y_VERIFY_S(!ChunkInfo->IsEndOfSplice, SelfInfo() <<
                                " Unexpectedly got LastNonce != previosNonce + 1 at the EndOfSplice!"
                                << " LastNonce# " << LastNonce
                                << " previousNonce# " << previousNonce
                                << " LastGoodToWriteLogPosition# " << LastGoodToWriteLogPosition
                                << " ChunkInfo->IsEndOfSplice# " << ChunkInfo->IsEndOfSplice
                                << " Marker# LR011");

                        ReplyOk();
                        return true;
                    }
                }
            }

            if (pageHeader->Flags & LogPageFirst) {
                TFirstLogPageHeader *firstPageHeader = (TFirstLogPageHeader*)(data + OffsetInSector);
                OffsetInSector += sizeof(TFirstLogPageHeader);
                // TODO(cthulhu): ReplyError() and return true;
                Y_VERIFY_S(!IsLastRecordHeaderValid, SelfInfo() <<
                        " LogPageNonceJump must be the first record after the last abrupt log termination");
                if (IsInitial || firstPageHeader->LogRecordHeader.OwnerId == Owner) {
                    if (IsInitial) {
                        FirstNonceToKeep = (ui64)-1;
                        FirstLsnToKeep = (ui64)-1;
                        TGuard<TMutex> guard(PDisk->StateMutex);
                        ui32 recordOwnerId = firstPageHeader->LogRecordHeader.OwnerId;
                        TOwnerData &ownerData = PDisk->OwnerData[recordOwnerId];
                        if (ownerData.VDiskId != TVDiskID::InvalidId) {
                            FirstLsnToKeep = ownerData.CurrentFirstLsnToKeep;
                            FirstNonceToKeep = PDisk->SysLogFirstNoncesToKeep.FirstNonceToKeep[recordOwnerId];
                            if (ownerData.LogStartPosition == TLogPosition{0, 0}) {
                                ownerData.LogStartPosition = PDisk->LogPosition(ChunkIdx, SectorIdx,
                                    OffsetInSector - sizeof(TFirstLogPageHeader));
                                TStringStream str;
                                str << "(B) Initial ownerId# " << (ui32)recordOwnerId
                                    << " set LogStartPosition# " << ownerData.LogStartPosition
                                    << " FirstNonceToKeep# " << FirstNonceToKeep << Endl;
                                LOG_DEBUG(*ActorSystem, NKikimrServices::BS_PDISK, "PDiskId# %" PRIu32 " %s",
                                        (ui32)PDisk->PDiskId, str.Str().c_str());
                            }
                        }
                    }
                    if (sectorFooter->Nonce < FirstNonceToKeep ||
                            firstPageHeader->LogRecordHeader.OwnerLsn < FirstLsnToKeep) {
                        IsLastRecordSkipped = true;
                        OffsetInSector += firstPageHeader->Size;
                        continue;
                    }
                    if (!IsInitial) {
                        ResultSize += firstPageHeader->DataSize;
                        if (ResultSize > SizeLimit && ResultSize != firstPageHeader->DataSize) {
                            OffsetInSector -= sizeof(TFirstLogPageHeader);
                            ReplyOkInTheMiddle();
                            return true;
                        }
                    }
                    IsLastRecordSkipped = false;
                    LastRecordHeader = firstPageHeader->LogRecordHeader;
                    LastRecordHeaderNonce = sectorFooter->Nonce;
                    IsLastRecordHeaderValid = true;
                    LastRecordData = TString::Uninitialized(firstPageHeader->DataSize);
                    Y_ABORT_UNLESS(firstPageHeader->Size <= LastRecordData.size());
                    memcpy((void*)LastRecordData.data(), data + OffsetInSector, firstPageHeader->Size);
                    LastRecordDataWritePosition = firstPageHeader->Size;
                } else {
                    IsLastRecordSkipped = true;
                }
                OffsetInSector += firstPageHeader->Size;
            } else {
                OffsetInSector += sizeof(TLogPageHeader);
                if (IsLastRecordSkipped) {
                    OffsetInSector += pageHeader->Size;
                    continue;
                }
                if (!IsLastRecordHeaderValid) {
                    Y_FAIL_S(SelfInfo() << " Last record header is corrupted!"
                            << " Invalid last record, then middle of record."
                            << " IsLastRecordSkipped# " << IsLastRecordSkipped
                            << " OffsetInSector# " << OffsetInSector
                            << " LastRecordDataWritePosition# " << LastRecordDataWritePosition
                            << " pageHeader->Size# " << pageHeader->Size
                            << " LastRecordData.Size()# " << LastRecordData.size()
                            << " LastRecordHeaderNonce# " << LastRecordHeaderNonce
                            << " sectorFooter->Nonce# " << sectorFooter->Nonce
                            << " LastRecordHeader.OwnerId# " << LastRecordHeader.OwnerId
                            << " LastRecordHeader.OwnerLsn# " << LastRecordHeader.OwnerLsn
                            << " pageHeader# " << pageHeader->ToString()
                            << Endl);

                    // TODO: skip the incorrect entry gracefully?  ReplyError() and return true
                }
                if (IsInitial || LastRecordHeader.OwnerId == Owner) {
                    void* destination = (void*)((ui8*)LastRecordData.data() + LastRecordDataWritePosition);
                    if (LastRecordDataWritePosition + pageHeader->Size > LastRecordData.size()) {
                        Y_FAIL_S(SelfInfo() << " Last record header is corrupted! Size mismatch."
                            << " IsLastRecordSkipped# " << IsLastRecordSkipped
                            << " OffsetInSector# " << OffsetInSector
                            << " LastRecordDataWritePosition# " << LastRecordDataWritePosition
                            << " pageHeader->Size# " << pageHeader->Size
                            << " LastRecordData.Size()# " << LastRecordData.size()
                            << " LastRecordHeaderNonce# " << LastRecordHeaderNonce
                            << " sectorFooter->Nonce# " << sectorFooter->Nonce
                            << " LastRecordHeader.OwnerId# " << LastRecordHeader.OwnerId
                            << " LastRecordHeader.OwnerLsn# " << LastRecordHeader.OwnerLsn
                            << " pageHeader# " << pageHeader->ToString()
                            << Endl);
                    }
                    memcpy(destination, data + OffsetInSector, pageHeader->Size);
                    LastRecordDataWritePosition += pageHeader->Size;
                }
                OffsetInSector += pageHeader->Size;
            }
            if (IsInitial && IsLastRecordHeaderValid) {
                TGuard<TMutex> guard(PDisk->StateMutex);
                if (LastRecordHeaderNonce >=
                        PDisk->SysLogFirstNoncesToKeep.FirstNonceToKeep[LastRecordHeader.OwnerId]) {
                    TLogChunkInfo &info = *ChunkInfo;
                    info.RegisterLogSector<true>(LastRecordHeader.OwnerId, LastRecordHeader.OwnerLsn);
                }
            }
            if (IsLastRecordHeaderValid && (pageHeader->Flags & LogPageLast)) {
                // Log page last
                SetLastGoodToWritePosition = true;

                IsLastRecordHeaderValid = false;
                Y_VERIFY_S(LastRecordDataWritePosition == LastRecordData.size(),
                    SelfInfo() << " LastRecordDataWritePosition# " << LastRecordDataWritePosition
                    << " LastRecordData.size()# " << LastRecordData.size()
                    << " pageHeader->Size# " << pageHeader->Size
                    << " pageHeader->Flags# " << ui64(pageHeader->Flags)
                    << " OffsetInSector# " << OffsetInSector
                    << " maxOffsetInSector# " << maxOffsetInSector
                    << " rawSector# " << (void*)rawSector
                    << " IsLastRecordSkipped# " << IsLastRecordSkipped
                    << " LastRecordHeaderNonce# " << LastRecordHeaderNonce
                    << " sectorFooter->Nonce# " << sectorFooter->Nonce
                    << " LastRecordHeader.OwnerId# " << LastRecordHeader.OwnerId
                    << " LastRecordHeader.OwnerLsn# " << LastRecordHeader.OwnerLsn
                    << " pageHeader# " << pageHeader->ToString());

                PDisk->ProcessReadLogRecord(LastRecordHeader, LastRecordData, Owner, sectorFooter->Nonce,
                        Result.Get(), ChunkOwnerMap.Get(), IsInitial, ParseCommits);
                LastRecordData = TString();
            }
        }// while OffsetInSector <= maxOffsetInSector

        OffsetInSector = 0;
    }
    ++SectorIdx;
    return false;
}

void TLogReader::ReplyOk() {
    {
        TPDiskHashCalculator hasher;
        TGuard<TMutex> guard(PDisk->StateMutex);
        if (!IsInitial) {
            TOwnerData &ownerData = PDisk->OwnerData[Owner];
            if (OwnerVDiskId != TVDiskID::InvalidId &&
                    ownerData.LogRecordsInitiallyRead &&
                    !ownerData.LogRecordsConsequentlyRead) {
                TStringStream str;
                str << "LogRecordsConsequentlyRead# " << ownerData.LogRecordsConsequentlyRead
                    << " LogRecordsInitiallyRead# " << ownerData.LogRecordsInitiallyRead;
                Y_FAIL_S(str.Str());
            }
            // End of log reached
            if (OwnerVDiskId != TVDiskID::InvalidId) {
                ADD_RECORD_WITH_TIMESTAMP_TO_OPERATION_LOG(ownerData.OperationLog, "Has read the whole log, OwnerId# " << Owner);
                ownerData.HasReadTheWholeLog = true;
            }
        }
    }

    Result->Status = NKikimrProto::OK;
    Result->NextPosition = IsInitial ? LastGoodToWriteLogPosition : TLogPosition::Invalid();
    Result->IsEndOfLog = true;
    Reply();
}

void TLogReader::ReplyOkInTheMiddle() {
    Result->Status = NKikimrProto::OK;
    Result->NextPosition = PDisk->LogPosition(ChunkIdx, SectorIdx, OffsetInSector);
    Result->IsEndOfLog = false;
    Reply();
}

void TLogReader::ReplyError() {
    Result->Status = NKikimrProto::ERROR;
    Result->NextPosition = TLogPosition::Invalid();
    Result->IsEndOfLog = true;
    Reply();
}

void TLogReader::Reply() {
    Y_ABORT_UNLESS(!IsReplied.load());
    if (IsInitial) {
        PDisk->ProcessChunkOwnerMap(*ChunkOwnerMap.Get());
        ChunkOwnerMap.Destroy();

        PDisk->BlockDevice->EraseCacheRange(
            PDisk->Format.Offset(ChunkIdx, 0),
            PDisk->Format.Offset(ChunkIdx + 1, 0)
        );
    }
    LOG_DEBUG(*PDisk->ActorSystem, NKikimrServices::BS_PDISK, "PDiskId# %" PRIu32 " To ownerId# %" PRIu32 " %s",
        (ui32)PDisk->PDiskId, (ui32)Owner, Result->ToString().c_str());
    ActorSystem->Send(ReplyTo, Result.Release());
    if (!IsInitial) {
        PDisk->Mon.LogRead.CountResponse(ResultSize);
    }
    IsReplied.store(true);
}

bool TLogReader::GetIsReplied() const {
    return IsReplied.load();
}

bool TLogReader::ProcessNextChunkReference(TSectorData& sector) {
    TDiskFormat &format = PDisk->Format;

    TSectorRestorator restorator(true, 0, format.IsErasureEncodeNextChunkReference(),
            PDisk->Format, PDisk->ActorSystem, PDisk->PDiskActor, PDisk->PDiskId, &PDisk->Mon,
            PDisk->BufferPool.Get());
    restorator.Restore(sector.GetData(), sector.Offset, format.MagicNextLogChunkReference, LastNonce,
            Owner);
    LOG_DEBUG_S(*PDisk->ActorSystem, NKikimrServices::BS_PDISK, SelfInfo() << " ProcessNextChunkReference");

    if (restorator.LastGoodIdx < ReplicationFactor) {
        ui8* const rawSector = sector.GetData() + restorator.LastGoodIdx * format.SectorSize;
        TDataSectorFooter *sectorFooter = (TDataSectorFooter*)
            (rawSector + format.SectorSize - sizeof(TDataSectorFooter));
        if (sectorFooter->Nonce < LastNonce) {
            LOG_NOTICE_S(*PDisk->ActorSystem, NKikimrServices::BS_PDISK, SelfInfo()
                    << " ProcessNextChunkReference, Nonce reordering"
                    << " sectorFooter->Nonce# " << sectorFooter->Nonce
                    << " LastNonce# " << LastNonce
                    << " LastGoodToWriteLogPosition# " << LastGoodToWriteLogPosition
                    << " Marker# LR012");
            // This one came unexpectedly out of the blue!
            // TODO(cthulhu): Write a unit-test that hits this line.
            // Steps to reproduce:
            // B - Begin
            // C - Continuation
            // 123 - Idea of nonce
            // Wirte something like B1C2 C3C4 B5C6 B7C8
            // Cut up to B5, cut up to B7, stop
            // Backup C3C4
            // Start, write B9C10 C11C12 so that we get B9C10 C11C12 -B5C6- B7C8
            // Stop
            // Restore C3C4 and get B9C10 C3C4 -B5C6- B7C8
            // Start
            ReplyOk();
            return true;
        }

        // Decrypt data
        Cypher.StartMessage(sectorFooter->Nonce);
        Cypher.InplaceEncrypt(rawSector, ui32(format.SectorSize - sizeof(TDataSectorFooter)));
        PDisk->CheckLogCanary(rawSector);

        TNextLogChunkReference2 *nextLogChunkReference = (TNextLogChunkReference2*)rawSector;
        switch (nextLogChunkReference->Version) {
        case PDISK_DATA_VERSION_2:
            LastNonce = sectorFooter->Nonce;
            break;
        case PDISK_DATA_VERSION_3:
        default:
            auto *nextRef = static_cast<TNextLogChunkReference3*>(nextLogChunkReference);
            if (nextRef->NextChunkFirstNonce) {
                IsLastRecordHeaderValid = false;
                IsLastRecordSkipped = true;
                LastRecordData = TString();
                LastRecordDataWritePosition = 0;
                LastNonce = nextRef->NextChunkFirstNonce - 1;
                OnEndOfSplice = true;

                if (IsInitial) {
                    LOG_INFO_S(*PDisk->ActorSystem, NKikimrServices::BS_PDISK, SelfInfo()
                            << " ProcessNextChunkReference contains NextChunkFirstNonce. "
                            << " nextRef->NextChunkFirstNonce# " << nextRef->NextChunkFirstNonce
                            << " sectorFooter->Nonce# " << sectorFooter->Nonce
                            << " LastNonce# " << LastNonce);
                }
            } else {
                LastNonce = sectorFooter->Nonce;
                OnEndOfSplice = false;
            }

            if (nextRef->IsNotCompatible) {
                TStringStream ss;
                ss << SelfInfo() << " ReplyError: unexpected data version in TNextLogChunkReference, version# "
                    << (ui32)nextLogChunkReference->Version;
                LOG_ERROR_S(*PDisk->ActorSystem, NKikimrServices::BS_PDISK, ss.Str());
                Result->ErrorReason = ss.Str();
                ReplyError();
                return true;
            }
        }
        ChunkInfo->LastNonce = sectorFooter->Nonce;
        MaxNonce = Max(MaxNonce, LastNonce);

        ui32 prevChunkIdx = ChunkIdx;
        LOG_DEBUG_S(*PDisk->ActorSystem, NKikimrServices::BS_PDISK, SelfInfo()
                << " ProcessNextChunkReference, record is valid"
                << " prevChunkIdx# " << prevChunkIdx
                << " nextChunkIdx# " << nextLogChunkReference->NextChunk
                << " LastNonce# " << LastNonce
                << " OnEndOfSplice# " << OnEndOfSplice);
        SwitchToChunk(nextLogChunkReference->NextChunk);
        if (IsInitial) {
            UpdateNewChunkInfo(ChunkIdx, prevChunkIdx);
        }
        return false;
    } else {
        // As we always write next chunk reference, the situation we are in is impossible.
        LOG_NOTICE_S(*PDisk->ActorSystem, NKikimrServices::BS_PDISK, SelfInfo()
                << " ProcessNextChunkReference, nextLogChunkReference not in a valid state"
                << " !(restorator.LastGoodIdx < ReplicationFactor) "
                << " restorator.LastGoodIdx# " << restorator.LastGoodIdx
                << " ReplicationFactor# " << ReplicationFactor
                << " LastGoodToWriteLogPosition# " << LastGoodToWriteLogPosition
                << " Marker# LR013");
        ReplyOk();
        return true;
    }
}

void TLogReader::UpdateNewChunkInfo(ui32 currChunk, const TMaybe<ui32> prevChunkIdx) {
    TGuard<TMutex> guard(PDisk->StateMutex);
    if (prevChunkIdx) {
        PDisk->ChunkState[*prevChunkIdx].CommitState = TChunkState::LOG_COMMITTED;
    }

    TChunkState& state = PDisk->ChunkState[currChunk];
    if (IsOwnerUser(state.OwnerId)) {
        LOG_WARN_S(*PDisk->ActorSystem, NKikimrServices::BS_PDISK, SelfInfo()
                << " chunk will be treated as log chunk, but in ChunkState is marked as owned by user"
                << " ChunkState# " << state.ToString());
    }
    state.CommitState = TChunkState::LOG_RESERVED;
    LOG_INFO_S(*ActorSystem, NKikimrServices::BS_PDISK, SelfInfo() << " chunk is the next log chunk,"
            << " prevOwnerId# " << ui32(state.OwnerId) << " -> newOwnerId# " << ui32(OwnerSystem));
    state.OwnerId = OwnerSystem;
    state.PreviousNonce = LastNonce;
}

void TLogReader::SwitchToChunk(ui32 chunkIdx) {
    ChunkIdx = chunkIdx;
    SectorIdx = 0;
    OffsetInSector = 0;
    if (!ParseCommits) {
        ParseCommits = PDisk->FirstLogChunkToParseCommits == chunkIdx;
    }
}

void TLogReader::ScheduleReadAsync(TSectorData &sector, ui32 sectorsToRead) {
    TDiskFormat &format = PDisk->Format;
    sector.Prepare(sectorsToRead * format.SectorSize);
    TCompletionLogReadPart *completion = new TCompletionLogReadPart(this, sector.Offset);
    Sector->DataByOffset(sector.Offset)->IsScheduled = true;
    ui32 sizeToRead = format.SectorSize * sectorsToRead;
    completion->CostNs = PDisk->DriveModel.TimeForSizeNs(sizeToRead, sector.Offset / format.ChunkSize,
            TDriveModel::OP_TYPE_READ);
    PDisk->BlockDevice->CachedPreadAsync(sector.Buffer->Data(), sizeToRead, sector.Offset, completion, ReqId, {});
}

bool TLogReader::RegisterBadOffsets(TVector<ui64> &badOffsets) {
    BadOffsets.insert(BadOffsets.end(), badOffsets.begin(), badOffsets.end());
    Sort(BadOffsets.begin(), BadOffsets.end());
    TDiskFormat &format = PDisk->Format;

    TVector<ui64>::iterator it = BadOffsets.begin();
    if (it != BadOffsets.end()) {
        ui32 erasureSectors = PDisk->UsableSectorsPerLogChunk();
        Y_UNUSED(erasureSectors);

        ui64 prevOffset = *it;
        ui32 prevChunk = ui32(prevOffset / format.ChunkSize);
        ui64 prevSector = (prevOffset - ui64(prevChunk) * ui64(format.ChunkSize)) / ui64(format.SectorSize);
        ui32 prevErasurePartSetBads = 1;
        ui64 prevErasurePartSet = prevSector;
        if (prevSector >= erasureSectors) {
            prevErasurePartSet = (ui64)-1;
        }

        for (++it; it != BadOffsets.end(); ++it) {
            ui64 currOffset = *it;
            ui32 currChunk = ui32(currOffset / ui64(format.ChunkSize));
            ui64 currSector = (currOffset - ui64(currChunk) * ui64(format.ChunkSize)) / ui64(format.SectorSize);
            if (currChunk != prevChunk) {
                prevChunk = currChunk;
                prevErasurePartSetBads = 0;
            } else {
                if (prevSector == currSector) {
                    continue;
                }
            }
            prevSector = currSector;
            if (currSector < erasureSectors) {
                ui32 currErasurePartSet = currSector;
                if (currErasurePartSet == prevErasurePartSet) {
                    ++prevErasurePartSetBads;
                    if (prevErasurePartSetBads > 1) {
                        // Report unrecoverable error
                        return false;
                    }
                } else {
                    prevErasurePartSet = currErasurePartSet;
                    prevErasurePartSetBads = 1;
                }
            } else {
                if (prevErasurePartSet == (ui64)-1) {
                    ++prevErasurePartSetBads;
                    if (prevErasurePartSetBads >= ReplicationFactor) {
                        // Report unrecoverable error
                        return false;
                    }
                } else {
                    prevErasurePartSet = (ui64)-1;
                    prevErasurePartSetBads = 1;
                }
            }
        }
    }
    return true;
}

void TLogReader::ReleaseUsedBadOffsets() {
    ui64 firstSectorToKeep = SectorIdx;
    ui32 erasureSectors = PDisk->UsableSectorsPerLogChunk();
    if (SectorIdx >= erasureSectors) {
        firstSectorToKeep = erasureSectors;
    }
    TDiskFormat &format = PDisk->Format;
    ui64 offset = format.Offset(ChunkIdx, firstSectorToKeep);
    TVector<ui64>::iterator lowerBound = LowerBound(BadOffsets.begin(), BadOffsets.end(), offset);
    if (lowerBound != BadOffsets.begin()) {
        BadOffsets.erase(BadOffsets.begin(), lowerBound);
    }
}

} // NPDisk
} // NKikimr
