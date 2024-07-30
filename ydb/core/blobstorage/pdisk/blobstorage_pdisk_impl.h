#pragma once
#include "defs.h"

#include "blobstorage_pdisk_blockdevice.h"
#include <ydb/library/pdisk_io/buffers.h>
#include "blobstorage_pdisk_chunk_tracker.h"
#include "blobstorage_pdisk_crypto.h"
#include "blobstorage_pdisk_data.h"
#include "blobstorage_pdisk_delayed_cost_loop.h"
#include "blobstorage_pdisk_drivemodel.h"
#include "blobstorage_pdisk_free_chunks.h"
#include "blobstorage_pdisk_gate.h"
#include "blobstorage_pdisk_keeper.h"
#include "blobstorage_pdisk_req_creator.h"
#include "blobstorage_pdisk_requestimpl.h"
#include "blobstorage_pdisk_state.h"
#include "blobstorage_pdisk_tact.h"
#include "blobstorage_pdisk_thread.h"
#include "blobstorage_pdisk_util_countedqueuemanyone.h"
#include "blobstorage_pdisk_writer.h"
#include "blobstorage_pdisk_impl_metadata.h"

#include <ydb/core/control/immediate_control_board_impl.h>
#include <ydb/core/base/resource_profile.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/blobstorage/lwtrace_probes/blobstorage_probes.h>
#include <ydb/core/control/immediate_control_board_wrapper.h>
#include <ydb/core/driver_lib/version/version.h>
#include <ydb/library/schlab/schine/scheduler.h>
#include <ydb/library/schlab/schine/job_kind.h>

#include <util/generic/queue.h>
#include <util/system/condvar.h>
#include <util/system/mutex.h>

namespace NKikimr {
namespace NPDisk {

LWTRACE_USING(BLOBSTORAGE_PROVIDER);

class TCompletionEventSender;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TPDisk
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

class TPDisk : public IPDisk {
public:
    ui32 PDiskId;
    TActorId PDiskActor;

    // Monitoring
    TPDiskMon Mon;


    // Static state
    TDriveModel DriveModel;

    TReqCreator ReqCreator;

    // Real-Time Scheduler
    ui64 ReorderingMs;

    // Forseti Scheduler
    ui64 ForsetiTimeNs = 0;
    ui64 ForsetiRealTimeCycles = 0;
    ui64 ForsetiPrevTimeNs = 0;
    NSchLab::TScheduler ForsetiScheduler;

    // Request queue
    TCountedQueueManyOne<TRequestBase, 4096> InputQueue;
    TAtomic InputQueueCost = 0;

    TVector<TRequestBase*> JointLogReads;
    TVector<TIntrusivePtr<TRequestBase>> JointChunkReads;
    TVector<TRequestBase*> JointChunkWrites;
    TVector<TLogWrite*> JointLogWrites;
    TVector<TLogWrite*> JointCommits;
    TVector<TChunkTrim*> JointChunkTrims;
    TVector<std::unique_ptr<TChunkForget>> JointChunkForgets;
    TVector<std::unique_ptr<TRequestBase>> FastOperationsQueue;
    TDeque<TRequestBase*> PausedQueue;
    std::set<std::unique_ptr<TYardInit>> PendingYardInits;
    ui64 LastFlushId = 0;
    bool IsQueuePaused = false;
    bool IsQueueStep = false;

    ETact LastTact = ETact::TactCc;
    ui64 UpdateIdx = 0;
    TAtomic InFlightLogRead = 0;
    TAtomic InFlightChunkRead = 0;

    TDelayedCostLoop LogSeekCostLoop;

    // Immediate Controls
    TControlWrapper SlowdownAddLatencyNs;
    TControlWrapper EnableForsetiBinLog;
    TControlWrapper ForsetiMinLogCostNsControl;
    TControlWrapper ForsetiMilliBatchSize;
    TControlWrapper ForsetiMaxLogBatchNs;
    TControlWrapper ForsetiOpPieceSizeSsd;
    TControlWrapper ForsetiOpPieceSizeRot;

    // SectorMap Controls
    TControlWrapper SectorMapFirstSectorReadRate;
    TControlWrapper SectorMapLastSectorReadRate;
    TControlWrapper SectorMapFirstSectorWriteRate;
    TControlWrapper SectorMapLastSectorWriteRate;
    TControlWrapper SectorMapSeekSleepMicroSeconds;
    // used to store valid value in ICB if SectorMapFirstSector*Rate < SectorMapLastSector*Rate
    TString LastSectorReadRateControlName;
    TString LastSectorWriteRateControlName;

    ui64 ForsetiMinLogCostNs = 2000000ull;
    i64 ForsetiMaxLogBatchNsCached;
    i64 ForsetiOpPieceSizeCached;

    // Settings
    TNonceSet ForceLogNonceDiff;

    // Static state
    TActorSystem *ActorSystem;
    alignas(16) TDiskFormat Format;
    ui64 ExpectedDiskGuid;
    TPDiskCategory PDiskCategory;
    TNonceJumpLogPageHeader2 LastNonceJumpLogPageHeader2;

    THolder<TBufferPool> BufferPool;

    // In-memory dynamic state
    TMutex StateMutex; // The state is modified mainly by the PDisk thread, but can be accessed by other threads.
    const TOwnerRound NextOwnerRound;  // Next unique-id to use for owner creation
    TOwner LastOwnerId = OwnerBeginUser;
    TVector<TOwnerData> OwnerData; // Per-owner information
    TMap<TVDiskID, TOwner> VDiskOwners; // For fast VDisk -> OwnerID mapping
    TVector<TChunkState> ChunkState; // Per-chunk information
    TKeeper Keeper; // Chunk data manager
    bool TrimInFly = false; // TChunkTrim request is present somewhere in pdisk
    TAtomic ChunkBeingTrimmed = 0;
    TAtomic TrimOffset = 0;
    TList<TLogChunkInfo> LogChunks; // Log chunk list + log-specific information
    bool IsLogChunksReleaseInflight = false;
    ui64 InsaneLogChunks = 0;  // Set when pdisk sees insanely large log, to give vdisks a chance to cut it
    ui32 FirstLogChunkToParseCommits = 0;

    // Chunks that is owned by killed owner, but has operations InFlight
    TVector<TChunkIdx> QuarantineChunks;
    TVector<TOwner> QuarantineOwners;


    TSysLogRecord SysLogRecord; // Current sys log record state, part 1 of 2
    TSysLogFirstNoncesToKeep SysLogFirstNoncesToKeep; // Current sys log record state, part 2 of 2
    ui64 SysLogLsn = 0;
    TNonceSet LoggedNonces; // Latest on-disk Nonce set
    ui64 CostLimitNs;

    TDriveData DriveData;
    TAtomic EstimatedLogChunkIdx = 0; // For cost estimation only TDriveData DriveData;

    TString ErrorStr;

    // Incapsulated components
    TPDiskThread PDiskThread;
    THolder<IBlockDevice> BlockDevice;
    THolder<TLogWriter> CommonLogger;
    THolder<TSysLogWriter> SysLogger;

    // Initialization data
    ui64 InitialSysLogWritePosition = 0;
    EInitPhase InitPhase = EInitPhase::Uninitialized;
    TBuffer *InitialTailBuffer = nullptr;
    TLogPosition InitialLogPosition{0, 0};
    volatile ui64 InitialPreviousNonce = 0;
    volatile ui64 InitialNonceJumpSize = 0;
    TAtomic IsStarted = false;
    TMutex StopMutex;

    TIntrusivePtr<TPDiskConfig> Cfg;
    TInstant CreationTime;

    ui64 ExpectedSlotCount = 0; // Number of slots to use for space limit calculation.

    TAtomic TotalOwners = 0; // number of registered owners

    // stats
    TAtomic NonRealTimeMs = 0;
    TAtomic SlowDeviceMs = 0;

    const bool UseHugePages;

    // Chunk locking
    TMap<TOwner, ui32> OwnerLocks;

    // Serialized compatibility info record
    std::optional<TString> SerializedCompatibilityInfo;

    // Debug
    std::function<TString()> DebugInfoGenerator;

    // Metadata storage
    NMeta::TInfo Meta;

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Initialization
    TPDisk(const TIntrusivePtr<TPDiskConfig> cfg, const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters);
    TString DynamicStateToString(bool isMultiline);
    TCheckDiskFormatResult ReadChunk0Format(ui8* formatSectors, const TMainKey& mainKey); // Called by actor
    bool IsFormatMagicValid(ui8 *magicData, ui32 magicDataSize, const TMainKey& mainKey); // Called by actor
    bool CheckGuid(TString *outReason); // Called by actor
    bool CheckFormatComplete(); // Called by actor
    void ReadSysLog(const TActorId &pDiskActor); // Called by actor
    bool ProcessChunk0(const TEvReadLogResult &readLogResult, TString& errorReason);
    void PrintChunksDebugInfo();
    TRcBuf ProcessReadSysLogResult(ui64 &outWritePosition, ui64 &outLsn, const TEvReadLogResult &readLogResult);
    void ReadAndParseMainLog(const TActorId &pDiskActor);
    void WriteFormatAsync(TDiskFormat format, const TKey &mainKey);
    // Called by the log reader on success with the current chunkOwnerMap.
    void ProcessChunkOwnerMap(TMap<ui32, TChunkState> &chunkOwnerMap);
    void InitLogChunksInfo();
    void PrintLogChunksInfo(const TString& msg);
    void InitFreeChunks();
    void InitSysLogger();
    bool InitCommonLogger();
    bool LogNonceJump(ui64 previousNonce);
    void GetStartingPoints(TOwner owner, TMap<TLogSignature, TLogRecord> &outStartingPoints);
    TString StartupOwnerInfo();
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Destruction
    virtual ~TPDisk();
    void Stop(); // Called by actor
    void ObliterateCommonLogSectorSet();
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Generic format-related calculations
    ui32 GetUserAccessibleChunkSize() const;
    ui32 GetChunkAppendBlockSize() const;
    ui32 SystemChunkSize(const TDiskFormat& format, ui32 userAccessibleChunkSizeBytes, ui32 sectorSizeBytes) const;
    ui64 UsableSectorsPerLogChunk() const;
    void CheckLogCanary(ui8* sector, ui32 chunkIdx = 0, ui64 sectorIdx = 0) const;
    TLogPosition LogPosition(ui32 chunkIdx, ui64 sectorIdx, ui64 offsetInSector) const;
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Common operations
    bool ReleaseUnusedLogChunks(TCompletionEventSender *completion);
    void MarkChunksAsReleased(TReleaseChunks& req);
    void OnNonceChange(ui32 idx, TReqId reqId, NWilson::TTraceId *traceId);
    ui32 GetTotalChunks(ui32 ownerId, const EOwnerGroupType ownerGroupType) const;
    ui32 GetFreeChunks(ui32 ownerId, const EOwnerGroupType ownerGroupType) const;
    ui32 GetUsedChunks(ui32 ownerId, const EOwnerGroupType ownerGroupType) const;
    TStatusFlags GetStatusFlags(TOwner ownerId, const EOwnerGroupType ownerGroupType, double *occupancy = nullptr) const;
    TStatusFlags NotEnoughDiskSpaceStatusFlags(ui32 ownerId, const EOwnerGroupType ownerGroupType) const;

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Generic log writing
    void LogFlush(TCompletionAction *action, TVector<ui32> *logChunksToCommit, TReqId reqId, NWilson::TTraceId *traceId);
    void AskVDisksToCutLogs(TOwner ownerFilter, bool doForce);
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // SysLog writing
    void WriteSysLogRestorePoint(TCompletionAction *action, TReqId reqId, NWilson::TTraceId *traceId);
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Common log writing
    bool PreallocateLogChunks(ui64 headedRecordSize, TOwner owner, ui64 lsn, EOwnerGroupType ownerGroupType,
            bool isAllowedForSpaceRed);
    bool AllocateLogChunks(ui32 chunksNeeded, ui32 chunksContainingPayload, TOwner owner, ui64 lsn,
            EOwnerGroupType ownerGroupType, bool isAllowedForSpaceRed);
    void LogWrite(TLogWrite &evLog, TVector<ui32> &logChunksToCommit);
    void CommitLogChunks(TCommitLogChunks &req);
    void OnLogCommitDone(TLogCommitDone &req);
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Chunk commit log writing
    NKikimrProto::EReplyStatus BeforeLoggingCommitRecord(const TLogWrite &evLog, TStringStream& outErrorReason);
    bool ValidateCommitChunk(ui32 chunkIdx, TOwner owner, TStringStream& outErrorReason);
    void CommitChunk(ui32 chunkIdx);
    bool ValidateDeleteChunk(ui32 chunkIdx, TOwner owner, TStringStream& outErrorReason);
    void DeleteChunk(ui32 chunkIdx, TOwner owner);
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Log reading
    void ProcessReadLogRecord(TLogRecordHeader &header, TString &data, TOwner owner, ui64 nonce,
        TEvReadLogResult* result, TMap<ui32, TChunkState> *outChunkOwnerMap, bool isInitial,
        bool parseCommitMessage);
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Chunk writing
    bool ChunkWritePiece(TChunkWrite *evChunkWrite, ui32 pieceShift, ui32 pieceSize);
    void SendChunkWriteError(TChunkWrite &evChunkWrite, const TString &errorReason, NKikimrProto::EReplyStatus status);
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Chunk reading
    enum EChunkReadPieceResult {
        ReadPieceResultInProgress = 0,
        ReadPieceResultOk = 1,
        ReadPieceResultError = 2
    };

    void SendChunkReadError(const TIntrusivePtr<TChunkRead>& read, TStringStream& errorReason,
            NKikimrProto::EReplyStatus status);
    EChunkReadPieceResult ChunkReadPiece(TIntrusivePtr<TChunkRead> &read, ui64 pieceCurrentSector, ui64 pieceSizeLimit,
            ui64 *reallyReadBytes, NWilson::TTraceId traceId);
    void SplitChunkJobSize(ui32 totalSize, ui32 *outSmallJobSize, ui32 *outLargeJObSize, ui32 *outSmallJobCount);
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Chunk locking
    TVector<TChunkIdx> LockChunksForOwner(TOwner owner, const ui32 count, TString &errorReason);
    std::unique_ptr<TEvChunkLockResult> ChunkLockFromQuota(TOwner owner, ui32 number);
    std::unique_ptr<TEvChunkLockResult> ChunkLockFromQuota(TOwner owner, NKikimrBlobStorage::TPDiskSpaceColor::E color);
    void ChunkLock(TChunkLock &evChunkLock);
    void ChunkUnlock(TChunkUnlock &evChunkUnlock);
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Chunk reservation
    TVector<TChunkIdx> AllocateChunkForOwner(const TRequestBase *req, const ui32 count, TString &errorReason);
    void ChunkReserve(TChunkReserve &evChunkReserve);
    bool ValidateForgetChunk(ui32 chunkIdx, TOwner owner, TStringStream& outErrorReason);
    void ChunkForget(TChunkForget &evChunkForget);
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Whiteboard and HTTP reports creation
    void WhiteboardReport(TWhiteboardReport &whiteboardReport); // Called by actor
    void RenderState(IOutputStream &str, THttpInfo &httpInfo);
    void OutputHtmlOwners(TStringStream &str);
    void OutputHtmlLogChunksDetails(TStringStream &str);
    void OutputHtmlChunkLockUnlockInfo(TStringStream &str);
    void HttpInfo(THttpInfo &httpInfo); // Called by actor
    void EventUndelivered(TUndelivered &req);
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // PDisk formatting
    void WriteApplyFormatRecord(TDiskFormat format, const TKey &mainKey);
    void WriteDiskFormat(ui64 diskSizeBytes, ui32 sectorSizeBytes, ui32 userAccessibleChunkSizeBytes, const ui64 &diskGuid,
            const TKey &chunkKey, const TKey &logKey, const TKey &sysLogKey, const TKey &mainKey,
            TString textMessage, const bool isErasureEncodeUserLog, const bool trimEntireDevice,
            std::optional<TRcBuf> metadata);
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Owner initialization
    void ReplyErrorYardInitResult(TYardInit &evYardInit, const TString &str);
    TOwner FindNextOwnerId();
    bool YardInitStart(TYardInit &evYardInit);
    void YardInitFinish(TYardInit &evYardInit);
    bool YardInitForKnownVDisk(TYardInit &evYardInit, TOwner owner);
    // Scheduler weight configuration
    void ConfigureCbs(ui32 ownerId, EGate gate, ui64 weight);
    void SchedulerConfigure(const TConfigureScheduler &conf);
    void SendCutLog(TAskForCutLog &reqest);
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Free space check
    void CheckSpace(TCheckSpace &evCheckSpace);
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Owner self-destruction
    void Harakiri(THarakiri &evHarakiri);
    // Owner destruction
    void Slay(TSlay &evSlay);
    // Common implementation details
    void ForceDeleteChunk(TChunkIdx chunkIdx);
    void KillOwner(TOwner owner, TOwnerRound killOwnerRound, TCompletionEventSender *completionAction);
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Update process
    void ProcessLogWriteQueueAndCommits();
    void ProcessChunkForgetQueue();
    void ProcessChunkWriteQueue();
    void ProcessChunkReadQueue();
    void ProcessLogReadQueue();
    void ProcessYardInitSet();
    void TrimAllUntrimmedChunks();
    void ProcessChunkTrimQueue();
    void ClearQuarantineChunks();
    // Should be called to initiate TRIM (on chunk delete or prev trim done)
    void TryTrimChunk(bool prevDone, ui64 trimmedSize, const NWilson::TSpan& parentSpan);
    void ProcessFastOperationsQueue();
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Drive info and write cache
    void OnDriveStartup();
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Metadata processing
    void InitFormattedMetadata();
    void ReadFormattedMetadataIfNeeded();
    void ProcessInitialReadMetadataResult(TInitialReadMetadataResult& request);
    void FinishReadingFormattedMetadata();

    void ProcessPushUnformattedMetadataSector(TPushUnformattedMetadataSector& request);

    void ProcessMetadataRequestQueue();
    void ProcessReadMetadata(std::unique_ptr<TRequestBase> req);
    void HandleNextReadMetadata();
    void ProcessWriteMetadata(std::unique_ptr<TRequestBase> req);
    void HandleNextWriteMetadata();
    void ProcessWriteMetadataResult(TWriteMetadataResult& request);

    void DropAllMetadataRequests();

    TRcBuf CreateMetadataPayload(TRcBuf& metadata, size_t offset, size_t payloadSize, ui32 sectorSize, bool encryption,
        const TKey& key, ui64 sequenceNumber, ui32 recordIndex, ui32 totalRecords);
    bool WriteMetadataSync(TRcBuf&& metadata);

    static std::optional<TMetadataFormatSector> CheckMetadataFormatSector(const ui8 *data, size_t len, const TMainKey& mainKey);
    static void MakeMetadataFormatSector(ui8 *data, const TMainKey& mainKey, const TMetadataFormatSector& format);

    NMeta::TFormatted& GetFormattedMeta();
    NMeta::TUnformatted& GetUnformattedMeta();
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Internal interface

    // Schedules EvReadLogResult event for the system log
    void ResetInit();
    bool Initialize(TActorSystem *actorSystem, const TActorId &pDiskActorId); // Called by actor
    void InitiateReadSysLog(const TActorId &pDiskActor); // Called by actor
    void ProcessReadLogResult(const TEvReadLogResult &evReadLogResult, const TActorId &pDiskActor);

    NKikimrProto::EReplyStatus ValidateRequest(TLogWrite *logWrite, TStringStream& outErrorReason);
    void PrepareLogError(TLogWrite *logWrite, TStringStream& errorReason, NKikimrProto::EReplyStatus status);
    template<typename T>
    bool PreprocessRequestImpl(T *req); // const;
    NKikimrProto::EReplyStatus CheckOwnerAndRound(TRequestBase* req, TStringStream& err);
    bool PreprocessRequest(TRequestBase *request);
    void PushRequestToForseti(TRequestBase *request);
    void AddJobToForseti(NSchLab::TCbs *cbs, TRequestBase *request, NSchLab::EJobKind jobKind);
    void RouteRequest(TRequestBase *request);
    void ProcessPausedQueue();
    void ProcessPendingActivities();
    void EnqueueAll();
    void Update() override;
    void Wakeup() override;
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // External interface
    // Pushes request to the InputQueue; almost thread-safe
    void InputRequest(TRequestBase* request); // Called by actor

private:
    void AddCbs(ui32 ownerId, EGate gate, const char *gateName, ui64 minBudget);
    void AddCbsSet(ui32 ownerId);
    void UpdateMinLogCostNs();
};

void ParsePayloadFromSectorOffset(const TDiskFormat& format, ui64 firstSector, ui64 lastSector, ui64 currentSector,
        ui64 *outPayloadBytes, ui64 *outPayloadOffset);

bool ParseSectorOffset(const TDiskFormat& format, TActorSystem *actorSystem, ui32 pDiskId, ui64 offset, ui64 size,
        ui64 &outSectorIdx, ui64 &outLastSectorIdx, ui64 &outSectorOffset);

} // NPDisk
} // NKikimr

