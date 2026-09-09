#include "ddisk_actor.h"
#include "direct_io_op.h"
#include "write_persistent_buffers_request_actor.h"

#include <ydb/core/base/counters.h>
#include <ydb/core/blobstorage/base/common_latency_hist_bounds.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/util/stlog.h>

#if defined(__linux__)
#include <unistd.h>

#endif
#define YDB_LOG_THIS_FILE_COMPONENT BS_DDISK

namespace NKikimr::NDDisk {

    template<typename TEventPtr>
    void TDDiskActor::HandlePersistentBufferWriteRequest(TEventPtr& ev) {
        Y_ABORT_UNLESS(IsPersistentBufferActor);
        auto& record = ev->Get()->Record;
        TQueryCredentials requestCreds(record.GetCredentials());
        TQueryCredentials creds;
        EConnectionResolution resolution = ResolveConnection(requestCreds, &creds);

        if (resolution != EConnectionResolution::Resolved) {
            YDB_LOG_DEBUG("TDDiskActor::HandlePersistentBufferWriteRequest token validation failed",
                {"reason", DescribeConnectionFailure(requestCreds, resolution)},
                {"DDiskId", DDiskId},
                {"evType", ev->GetTypeRewrite()},
                {"sender", ev->Sender},
                {"cookie", ev->Cookie},
                {"ICSession", ev->InterconnectSession});

            auto result = std::make_unique<TEvWritePersistentBuffersResult>();
            const TStringBuf errorReason = ConnectionErrorReason(resolution);

            for (const auto& id : record.GetPersistentBufferIds()) {
                auto* item = result->Record.AddResult();
                item->MutablePersistentBufferId()->CopyFrom(id);
                item->MutableResult()->SetStatus(NKikimrBlobStorage::NDDisk::TReplyStatus::SESSION_MISMATCH);
                item->MutableResult()->SetErrorReason(errorReason.data(), errorReason.size());
            }

            SendReply(*ev, std::move(result));
            return;
        }

        creds.SerializeResolvedForRequest(record.MutableCredentials());
        const auto ownershipStatus = IsBroken() ? NKikimrBlobStorage::NDDisk::TReplyStatus::ERROR
            : !PersistentBufferReady ? NKikimrBlobStorage::NDDisk::TReplyStatus::BUSY
            : CheckPersistentBufferOwnership(creds);
        if (ownershipStatus != NKikimrBlobStorage::NDDisk::TReplyStatus::OK) {
            auto result = std::make_unique<TEvWritePersistentBuffersResult>();
            for (const auto& id : record.GetPersistentBufferIds()) {
                auto* item = result->Record.AddResult();
                item->MutablePersistentBufferId()->CopyFrom(id);
                item->MutableResult()->SetStatus(ownershipStatus);
                item->MutableResult()->SetErrorReason("persistent buffer namespace is not ready, registered, or active");
            }
            SendReply(*ev, std::move(result));
            return;
        }
        if constexpr (requires { record.ChecksumsSize(); record.GetSelector(); }) {
            if (!Config.EnableChecksums) {
                // Do not forward sender-supplied checksums into the checksum-less PB v0 format.
                // They are intentionally neither required nor validated in this mode.
                record.ClearChecksums();
            } else {
                const auto& selector = record.GetSelector();
                if (!HasRequiredBlockChecksums(record.ChecksumsSize(),
                        selector.GetOffsetInBytes(), selector.GetSize())) {
                    if (record.ChecksumsSize() == 0) {
                        Counters.Checksums.WritesWithoutChecksums->Inc();
                    }
                    auto result = std::make_unique<TEvWritePersistentBuffersResult>();
                    for (const auto& id : record.GetPersistentBufferIds()) {
                        auto* item = result->Record.AddResult();
                        item->MutablePersistentBufferId()->CopyFrom(id);
                        item->MutableResult()->SetStatus(
                            NKikimrBlobStorage::NDDisk::TReplyStatus::INCORRECT_REQUEST);
                        item->MutableResult()->SetErrorReason(
                            "one checksum per aligned 4 KiB block is required");
                    }
                    SendReply(*ev, std::move(result));
                    return;
                }
            }
        }
        Y_ABORT_UNLESS(WritePersistentBuffersActor);
        TActivationContext::Send(ev->Forward(WritePersistentBuffersActor));
    }

    void TDDiskActor::Handle(TEvReadThenWritePersistentBuffers::TPtr ev) {
        HandlePersistentBufferWriteRequest(ev);
    }

    void TDDiskActor::Handle(TEvWritePersistentBuffers::TPtr ev) {
        HandlePersistentBufferWriteRequest(ev);
    }

namespace {
    const TVector<double> WriteBatchSizeBounds = {
        1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 16, 24, 32, 40, 48, 64, 128
    };

    const TVector<double> NvmeLatencyHistBoundsMs = {
        0.01, 0.02, 0.03, 0.04, 0.05,                   // 10th us
        0.1, 0.25, 0.5, 0.75,                           // 100th us
        1, 2, 4, 8, 32, 128,                            // ms
        1'024,                                          // s
        65'536                                          // minutes
    };

    const TVector<double> RequestSizeBoundsKiB = {
        4, 8, 16, 32, 64, 128, 256, 512,                // KiB
        1024, 2048, 4096,                               // MiB
        1048576,                                        // GiB
    };

} // anonymous

    TDDiskActor::TDDiskActor(TVDiskConfig::TBaseInfo&& baseInfo, TIntrusivePtr<TBlobStorageGroupInfo> info,
            TPersistentBufferFormat&& pbFormat, TDDiskConfig&& ddiskConfig,
            TIntrusivePtr<NMonitoring::TDynamicCounters> counters, const std::vector<ui32>& initPersistentBufferChunks,
            ui64 persistentBufferUniqueId, TIntrusivePtr<TPDiskParams> pDiskParams, NPDisk::TDiskFormatPtr diskFormat
#if defined(__linux__)
            , std::shared_ptr<NPDisk::IUringRouterClient> uringRouter
#endif
            )
        : TDDiskActor(std::move(baseInfo), std::move(info), std::move(pbFormat), std::move(ddiskConfig), counters, true)
    {
        PersistentBufferUniqueId = persistentBufferUniqueId;
        PDiskParams = pDiskParams;
        DiskFormat = std::move(diskFormat);
#if defined(__linux__)
        UringRouter = std::move(uringRouter);
#endif
        InitPersistentBuffer();
        for (auto idx : initPersistentBufferChunks) {
            auto [it, inserted] = PersistentBufferDataSectorsInfo.insert({idx, {}});
            it->second.resize(SectorInChunk);
            if (!inserted) {
                YDB_LOG_ERROR("TDDiskActor::TDDiskActor persistent buffer has duplicated chunk index in log",
                    {"marker", "BSDD10"},
                    {"DDiskId", DDiskId},
                    {"PDiskActorId", BaseInfo.PDiskActorID},
                    {"chunkIdx", idx});
                continue;
            }
            PersistentBufferSpaceAllocator.AddNewChunk(idx);
            ++*Counters.Chunks.ChunksOwned;
        }
    }

    TDDiskActor::TDDiskActor(TVDiskConfig::TBaseInfo&& baseInfo, TIntrusivePtr<TBlobStorageGroupInfo> info,
            TPersistentBufferFormat&& pbFormat, TDDiskConfig&& ddiskConfig,
            TIntrusivePtr<NMonitoring::TDynamicCounters> counters, bool isPersistentBufferActor)
        : BaseInfo(std::move(baseInfo))
        , Config(std::move(ddiskConfig))
        , Info(std::move(info))
        , CountersParent(std::move(counters))
        , CountersBase(GetServiceCounters(CountersParent, "ddisks"))
        , IsPersistentBufferActor(isPersistentBufferActor)
        , MinChunksReserved(isPersistentBufferActor
            ? MinChunksReservedPersistentBuffer
            : MinChunksReservedDDisk)
        , SegmentManager(DDiskInstanceGuid)
        , PersistentBufferFormat(std::move(pbFormat))
    {
        if (IsPersistentBufferActor) {
            SetActivityType(NKikimrServices::TActivity::BS_PERSISTENT_BUFFER);
        } else {
            SetActivityType(NKikimrServices::TActivity::BS_DDISK);
        }

        StartedAt = TInstant::Now();
        TVector<double> latencyHistBounds;
        if (BaseInfo.DeviceType == NPDisk::DEVICE_TYPE_NVME || BaseInfo.DeviceType == NPDisk::DEVICE_TYPE_SSD) {
            latencyHistBounds = NvmeLatencyHistBoundsMs;
        } else {
            latencyHistBounds = GetCommonLatencyHistBounds(BaseInfo.DeviceType);
        }

        CountersChain.emplace_back("ddiskPool", BaseInfo.StoragePoolName);
        CountersChain.emplace_back("group", Sprintf("%09" PRIu32, Info->GroupID));
        CountersChain.emplace_back("orderNumber", Sprintf("%02" PRIu32, Info->GetOrderNumber(BaseInfo.VDiskIdShort)));
        CountersChain.emplace_back("pdisk", Sprintf("%09" PRIu32, BaseInfo.PDiskId));
        CountersChain.emplace_back("media", to_lower(NPDisk::DeviceTypeStr(BaseInfo.DeviceType, true)));

        counters = CountersBase;
        for (const auto& [name, value] : CountersChain) {
            counters = counters->GetSubgroup(name, value);
        }

        auto cInterface = counters->GetSubgroup("subsystem", "interface");

#define XX(NAME) auto cInterface##NAME = cInterface->GetSubgroup("operation", #NAME);
        LIST_COUNTERS_INTERFACE_OPS(XX)
#undef XX

        auto cRecoveryLog = counters->GetSubgroup("subsystem", "recovery_log");

        auto cChunks = counters->GetSubgroup("subsystem", "chunks");

        auto cDirectIO = counters->GetSubgroup("subsystem", "direct_io");
        auto cDirectIOWrite = cDirectIO->GetSubgroup("operation", "Write");
        auto cDirectIORead = cDirectIO->GetSubgroup("operation", "Read");

        auto cPersistentBuffer = counters->GetSubgroup("subsystem", "persistent_buffer");
        auto cChecksums = counters->GetSubgroup("subsystem", "checksums");

#define COUNTER(GROUP, NAME, DERIV) .NAME = c##GROUP->GetCounter(#NAME, DERIV),
#define HISTOGRAM(GROUP, NAME, BUCKETS) .NAME = c##GROUP->GetHistogram(#NAME, NMonitoring::ExplicitHistogram(BUCKETS)),
#define COUNTER_VALUE(GROUP, NAME, DERIV) c##GROUP->GetCounter(#NAME, DERIV)
#define HISTOGRAM_VALUE(GROUP, NAME, BUCKETS) c##GROUP->GetHistogram(#NAME, NMonitoring::ExplicitHistogram(BUCKETS))

        Counters = TCounters{
            .Interface = {
#define XX(OP) \
                .OP = [&] { \
                    TInterfaceOpCounters c; \
                    c.Requests = COUNTER_VALUE(Interface##OP, Requests, true); \
                    c.RequestsInFlight = COUNTER_VALUE(Interface##OP, RequestsInFlight, false); \
                    c.ReplyOk = COUNTER_VALUE(Interface##OP, ReplyOk, true); \
                    c.ReplyErr = COUNTER_VALUE(Interface##OP, ReplyErr, true); \
                    c.Bytes = COUNTER_VALUE(Interface##OP, Bytes, true); \
                    c.BytesInFlight = COUNTER_VALUE(Interface##OP, BytesInFlight, false); \
                    c.RequestSizeKiB = HISTOGRAM_VALUE(Interface##OP, RequestSizeKiB, RequestSizeBoundsKiB); \
                    c.ResponseTime = HISTOGRAM_VALUE(Interface##OP, ResponseTime, latencyHistBounds); \
                    return c; \
                }(),
                LIST_COUNTERS_INTERFACE_OPS(XX)
#undef XX
                COUNTER(Interface, UnalignedWritePayloads, true)
            },
            .RecoveryLog = {
                COUNTER(RecoveryLog, ReadLogChunks, false)
                COUNTER(RecoveryLog, LogRecordsProcessed, false)
                COUNTER(RecoveryLog, LogRecordsApplied, false)
                COUNTER(RecoveryLog, LogRecordsWritten, false)
                COUNTER(RecoveryLog, NumChunkMapSnapshots, false)
                COUNTER(RecoveryLog, NumChunkMapIncrements, false)
                COUNTER(RecoveryLog, CutLogMessages, false)
            },
            .Chunks = {
                COUNTER(Chunks, ChunksOwned, false)
            },
            .DirectIO = {
#define XX(OP) \
                .OP = { \
                    COUNTER(DirectIO##OP, Requests, true) \
                    COUNTER(DirectIO##OP, RequestsInFlight, false) \
                    COUNTER(DirectIO##OP, Bytes, true) \
                    COUNTER(DirectIO##OP, BytesInFlight, false) \
                    HISTOGRAM(DirectIO##OP, RequestSizeKiB, RequestSizeBoundsKiB) \
                    HISTOGRAM(DirectIO##OP, ResponseTime, latencyHistBounds) \
                },
                XX(Write)
                XX(Read)
#undef XX

                COUNTER(DirectIO, ShortReads, true)
                COUNTER(DirectIO, ShortWrites, true)

                COUNTER(DirectIO, RunningCount, false)
            },
            .PersistentBuffer = {
                COUNTER(PersistentBuffer, AllocatedChunks, false)
                COUNTER(PersistentBuffer, TotalBytes, false)
                COUNTER(PersistentBuffer, PendingEventsQueueSize, false)
                COUNTER(PersistentBuffer, InMemoryCacheSize, false)
                HISTOGRAM(PersistentBuffer, WriteBatchSize, WriteBatchSizeBounds)
            },
            .Checksums = {
                COUNTER(Checksums, WritesWithoutChecksums, true)
                COUNTER(Checksums, ChecksumMismatch, true)
                COUNTER(Checksums, IntegrityPairReads, true)
                COUNTER(Checksums, IntegrityPairWrites, true)
                COUNTER(Checksums, IntegrityCorruption, true)
                COUNTER(Checksums, IntegrityLostWriteDetected, true)
            },
        };

#undef COUNTER_VALUE
#undef HISTOGRAM_VALUE

        DDiskId = TStringBuilder() << '[' << BaseInfo.PDiskActorID.NodeId() << ':' << BaseInfo.PDiskId
            << ':' << BaseInfo.VDiskSlotId << ']';

        DdiskIoOpPool.Resize(IoOpPoolCapacity);
        PersistentBufferPartIoOpPool.Resize(IoOpPoolCapacity);
        InternalSyncWriteOpPool.Resize(IoOpPoolCapacity);
        IntegrityIoOpPool.Resize(IoOpPoolCapacity);
    }

    TDDiskActor::~TDDiskActor() {
        // unique_ptr members of incomplete TDirectIoOpBase (and derived ops) must be
        // destroyed here, where the type is complete. sizeof fails the build otherwise.
        [[maybe_unused]] constexpr size_t CompleteTypeGuard = sizeof(TDirectIoOpBase);

        // Forced runtime teardown only; normal stopping drains before destruction.
        const auto now = [&] { return DestructionNow ? DestructionNow() : TMonotonic::Now(); };
        const TMonotonic deadline = now() + TDuration::Seconds(10);
        while (GetDirectIoInflight()) {
            Y_ABORT_UNLESS(now() < deadline,
                "DDisk %s destroyed with unresolved I/O callbacks", DDiskId.c_str());
            if (DestructionSleep) {
                DestructionSleep();
            } else {
                Sleep(TDuration::MilliSeconds(1));
            }
        }

        ClearIoStalled();
    }

    void TDDiskActor::Bootstrap() {
        IoStalledCounter = CountersBase->GetCounter("io_stalled", false);
        FillPool(DdiskIoOpPool);
        FillPool(PersistentBufferPartIoOpPool);
        FillPool(InternalSyncWriteOpPool);
        FillPool(IntegrityIoOpPool);

        YDB_LOG_DEBUG("TDDiskActor::Bootstrap",
            {"marker", "BSDD09"},
            {"DDiskId", DDiskId});
        if (IsPersistentBufferActor) {
            InitUring();
            Become(&TThis::StateFuncPersistentBuffer);
            WritePersistentBuffersActor = Register(new TWritePersistentBuffersRequestActor(SelfId()));
            CollectPbStatsSnapshot();
            StartRestorePersistentBuffer();
        } else {
            Become(&TThis::StateFuncDDisk);
            RegisterMonPage();
            if (!Config.EnableChecksums) {
                YDB_LOG_NOTICE("TDDiskActor booting with integrity checksums disabled",
                    {"marker", "BSDD55"},
                    {"DDiskId", DDiskId});
            }
            InitPDiskInterface();
        }
    }

    bool TDDiskActor::IsBroken() const {
        return Broken;
    }

    TString TDDiskActor::GetBrokenReason() const {
        return BrokenReason ? BrokenReason : TString("DDisk is broken");
    }

    void TDDiskActor::FailPendingDDiskQuery(std::unique_ptr<IEventHandle> ev) {
        RejectQuery(*ev, NKikimrBlobStorage::NDDisk::TReplyStatus::ERROR, GetBrokenReason());
    }

    void TDDiskActor::FailDirectIoOp(std::unique_ptr<TDirectIoOpBase> op, TString reason) {
        switch (op->GetOperationType()) {
            case NPDisk::TUringOperationBase::EREAD:
                Counters.DirectIO.Read.Done(op->GetTotalSize());
                break;
            case NPDisk::TUringOperationBase::EWRITE:
                Counters.DirectIO.Write.Done(op->GetTotalSize());
                break;
            default:
                Y_ABORT("Unknown OperationType");
        }
        if (!reason) {
            reason = GetBrokenReason();
        }
        op->Reply(TActivationContext::ActorSystem(),
            NKikimrBlobStorage::NDDisk::TReplyStatus::ERROR, std::move(reason));
        op.reset();
        OnDirectIODone(TActivationContext::ActorSystem());
    }

    void TDDiskActor::EnterBroken(TString reason) {
        if (Broken) {
            return;
        }
        Broken = true;
        BrokenReason = reason
            ? std::move(reason)
            : TString("DDisk is broken");
        CancelRetries();

        YDB_LOG_ERROR("TDDiskActor entered Broken state",
            {"marker", "BSDD54"},
            {"DDiskId", DDiskId},
            {"errorReason", GetBrokenReason()});

        *Counters.PersistentBuffer.PendingEventsQueueSize -= PendingPersistentBufferEvents.size();
        while (!PendingPersistentBufferEvents.empty()) {
            auto ev = PendingPersistentBufferEvents.front().Release();
            PendingPersistentBufferEvents.pop();
            RejectQuery(*ev, NKikimrBlobStorage::NDDisk::TReplyStatus::ERROR, GetBrokenReason());
        }

        // Complete actor-owned fallback operations immediately. Submitted io_uring operations
        // post their result events later; the actor normalizes those to ERROR because Broken is
        // already set.
        while (!WriteCallbacks.empty()) {
            auto it = WriteCallbacks.begin();
            auto op = std::move(it->second.Op);
            WriteCallbacks.erase(it);
            FailDirectIoOp(std::move(op));
        }
        while (!ReadCallbacks.empty()) {
            auto it = ReadCallbacks.begin();
            auto op = std::move(it->second.Op);
            ReadCallbacks.erase(it);
            FailDirectIoOp(std::move(op));
        }

        RejectPendingDDiskQueries(NKikimrBlobStorage::NDDisk::TReplyStatus::ERROR, GetBrokenReason());

        // Fail every sync exactly once, remove any segment-manager state, and leave late source
        // reads/internal writes harmless (their handlers already tolerate an absent sync).
        std::vector<TSegmentManager::TSegment> removedSegments;
        while (!SyncsInFlight.empty()) {
            auto it = SyncsInFlight.begin();
            auto& sync = it->second;
            if (sync.FirstRequestId != Max<ui64>()) {
                for (ui64 i = 0; i < sync.Requests.size(); ++i) {
                    const ui64 requestId = sync.FirstRequestId + i;
                    SegmentManager.PopRequest(requestId, &removedSegments);
                    SyncReadCookiesInFlight.erase(requestId);
                    auto& request = sync.Requests[i];
                    if (request.Status == NKikimrBlobStorage::NDDisk::TReplyStatus::UNKNOWN) {
                        request.Status = NKikimrBlobStorage::NDDisk::TReplyStatus::ERROR;
                        request.ErrorReason << GetBrokenReason();
                    }
                }
            }
            sync.ErrorReason << GetBrokenReason();
            ReplySync(it);
        }
        SyncReadCookiesInFlight.clear();

        for (auto& [key, allocation] : DataChunkAllocationsInFlight) {
            Y_UNUSED(key);
            for (auto& parked : allocation.ParkedWriteResults) {
                parked.Status = NKikimrBlobStorage::NDDisk::TReplyStatus::ERROR;
                parked.ErrorMessage = GetBrokenReason();
            }
            FlushParkedAllocationReplies(allocation);
        }

        DataChunkAllocationsInFlight.clear();
        ChunkMapIncrementsInFlight.clear();

        std::vector<ui64> pendingWriteIds;
        pendingWriteIds.reserve(PendingClientWrites.size());
        for (auto& [operationId, pending] : PendingClientWrites) {
            pending.IntegrityCompleted = true;
            pending.IntegrityError = GetBrokenReason();
            pendingWriteIds.push_back(operationId);
        }
        for (const ui64 operationId : pendingWriteIds) {
            MaybeFinishClientWrite(operationId);
        }

        PendingSyncSegments.clear();

        if (IntegrityManager) {
            Y_UNUSED(IntegrityManager->TakeActions());
            Y_UNUSED(IntegrityManager->TakeCompletedOperations());
        }

        // DDisk and PersistentBuffer are separate actor instances sharing
        // this class. Only DDisk talks to PDisk, so PB chunk requests still
        // arrive here as TChunkForPersistentBuffer. Drop data/integrity work
        // and keep serving those PB allocations if DDisk is the one that
        // broke. The PersistentBuffer instance never uses this queue.
        if (!IsPersistentBufferActor) {
            decltype(ChunkAllocateQueue) persistentBufferAllocations;
            while (!ChunkAllocateQueue.empty()) {
                auto allocation = std::move(ChunkAllocateQueue.front());
                ChunkAllocateQueue.pop();
                if (std::holds_alternative<TChunkForPersistentBuffer>(
                        allocation)) {
                    persistentBufferAllocations.push(std::move(allocation));
                }
            }
            ChunkAllocateQueue.swap(persistentBufferAllocations);
            HandleChunkReserved();
        }
    }

    void TDDiskActor::Handle(TEvents::TEvUndelivered::TPtr ev) {
        auto sourceType = ev->Get()->SourceType;
        if (Stopping && !PersistentBufferGone && sourceType == TEvents::TSystem::Poison
                && ev->Cookie == PBShutdownCookie && ev->Sender == PersistentBufferActorId) {
            PersistentBufferGone = true;
            TryCompleteStop();
            return;
        }
        if (sourceType == TEv::EvRead || sourceType == TEv::EvReadPersistentBuffer) {
            SyncReadCookiesInFlight.erase(ev->Cookie);
            std::vector<TSegmentManager::TSegment> segments;
            ui64 syncId = SegmentManager.GetSync(ev->Cookie);
            SegmentManager.PopRequest(ev->Cookie, &segments);

            auto it = SyncsInFlight.find(syncId);
            if (it == SyncsInFlight.end()) {
                return;
            }
            auto& sync = it->second;

            if (ev->Cookie < sync.FirstRequestId || ev->Cookie >= sync.FirstRequestId + sync.Requests.size()) {
                YDB_LOG_ERROR("TDDiskActor::Handle(TEvUndelivered) request cookie out of range",
                    {"marker", "BSDD23"},
                    {"DDiskId", DDiskId},
                    {"cookie", ev->Cookie},
                    {"syncId", syncId},
                    {"firstRequestId", sync.FirstRequestId},
                    {"requestsCount", sync.Requests.size()},
                    {"sourceType", sourceType});
                return;
            }
            auto& request = sync.Requests[ev->Cookie - sync.FirstRequestId];

            if (request.Status != NKikimrBlobStorage::NDDisk::TReplyStatus::UNKNOWN) {
                return;
            }

            request.Status = NKikimrBlobStorage::NDDisk::TReplyStatus::ERROR;
            request.ErrorReason << "[" << request.Selector.OffsetInBytes << ';'
                << request.Selector.OffsetInBytes + request.Selector.Size
                << "] failed to read; reason: read event undelivered";
            sync.ErrorReason << "[request_idx=" << ev->Cookie - sync.FirstRequestId << "] failed to read; ";
            if (--sync.RequestsInFlight == 0) {
                MaybeReplySync(it);
            }
            return;
        }
    }

    STFUNC(TDDiskActor::StateFuncDDisk) {
        auto handleQuery = [&](auto& ev) {
            if (CanHandleQuery(ev)) {
                Handle(ev);
            }
        };

        STRICT_STFUNC_BODY(
            hFunc(TEvConnect, handleQuery)
            hFunc(TEvDisconnect, handleQuery)
            hFunc(TEvWrite, handleQuery)
            hFunc(TEvRead, handleQuery)
            hFunc(TEvSync, handleQuery)
            hFunc(TEvDeleteTabletChunks, handleQuery)
            hFunc(TEvPrivate::TEvIssuePersistentBufferChunkAllocation, Handle)
            hFunc(TEvPrivate::TEvDeallocatePersistentBufferChunk, Handle)

            hFunc(TEvents::TEvUndelivered, Handle)
            hFunc(TEvents::TEvGone, HandleGone)

            hFunc(TEvReadResult, Handle)
            hFunc(TEvPrivate::TEvInternalSyncWriteResult, Handle)

            hFunc(NPDisk::TEvYardInitResult, Handle)
            hFunc(NPDisk::TEvReadLogResult, Handle)
            cFunc(TEvPrivate::EvHandleSingleQuery, HandleSingleQuery)
            hFunc(NPDisk::TEvChunkReserveResult, Handle)
            hFunc(NPDisk::TEvLogResult, Handle)
            hFunc(TEvPrivate::TEvHandleEventForChunk, Handle)
            hFunc(TEvPrivate::TEvHandleSerializedWriteForChunk, Handle)
            hFunc(TEvPrivate::TEvDDiskIoResult, Handle)
            hFunc(TEvPrivate::TEvIntegrityIoResult, Handle)
            hFunc(TEvPrivate::TEvChunkFormatIoResult, Handle)
            hFunc(NPDisk::TEvCutLog, Handle)
            hFunc(TEvReadPersistentBufferResult, Handle)
            hFunc(NPDisk::TEvChunkWriteRawResult, Handle)
            hFunc(NPDisk::TEvChunkReadRawResult, Handle)
#if defined(__linux__)
            hFunc(TEvPrivate::TEvRetryIO, HandleRetryIO)
            hFunc(TEvPrivate::TEvRetryIODelayed, HandleRetryIODelayed)
#endif

            hFunc(NPDisk::TEvCheckSpaceResult, Handle);

            IgnoreFunc(NNodeWhiteboard::TEvWhiteboard::TEvVDiskStateUpdate)

            hFunc(NMon::TEvHttpInfo, Handle)

            hFunc(TEvents::TEvWakeup, HandleWakeup);
            cFunc(TEvents::TSystem::Poison, PassAway)
            cFunc(TEvPrivate::EvBeginStopping, HandleBeginStopping)
        )
    }

    STFUNC(TDDiskActor::StateFuncPersistentBuffer) {
        if (IsBroken()) {
            switch (ev->GetTypeRewrite()) {
            case TEvConnect::EventType:
            case TEvDisconnect::EventType:
            case TEvWritePersistentBuffer::EventType:
            case TEvReadPersistentBuffer::EventType:
            case TEvErasePersistentBuffer::EventType:
            case TEvBatchErasePersistentBuffer::EventType:
            case TEvListPersistentBuffer::EventType:
            case TEvWritePersistentBuffers::EventType:
            case TEvReadThenWritePersistentBuffers::EventType:
                RejectQuery(*ev, NKikimrBlobStorage::NDDisk::TReplyStatus::ERROR, GetBrokenReason());
                return;
            }
        }
        STRICT_STFUNC_BODY(
            hFunc(TEvRegisterPersistentBuffer, Handle)
            hFunc(TEvUnregisterPersistentBuffer, Handle)
            hFunc(TEvPrivate::TEvProcessPersistentBufferRemoval, Handle)
            hFunc(TEvConnect, Handle)
            hFunc(TEvDisconnect, Handle)
            hFunc(TEvWritePersistentBuffer, Handle)
            hFunc(TEvReadPersistentBuffer, Handle)
            hFunc(TEvErasePersistentBuffer, Handle)
            hFunc(TEvBatchErasePersistentBuffer, Handle)
            hFunc(TEvListPersistentBuffer, Handle)
            hFunc(TEvPrivate::TEvRetryListPersistentBuffer, Handle)
            hFunc(TEvGetPersistentBufferInfo, Handle)

            hFunc(TEvPrivate::TEvReadPersistentBufferPart, Handle)
            hFunc(TEvPrivate::TEvWritePersistentBufferPart, Handle)

            hFunc(TEvents::TEvUndelivered, Handle)

            hFunc(TEvPrivate::TEvHandlePersistentBufferEventForChunk, Handle)
            hFunc(TEvPrivate::TEvDeallocatePersistentBufferChunkResult, Handle)

            hFunc(NPDisk::TEvChunkWriteRawResult, Handle)
            hFunc(NPDisk::TEvChunkReadRawResult, Handle)
#if defined(__linux__)
            hFunc(TEvPrivate::TEvRetryIO, HandleRetryIO)
            hFunc(TEvPrivate::TEvRetryIODelayed, HandleRetryIODelayed)
#endif

            hFunc(NPDisk::TEvCheckSpaceResult, Handle);

            IgnoreFunc(NNodeWhiteboard::TEvWhiteboard::TEvVDiskStateUpdate)

            hFunc(TEvents::TEvWakeup, HandleWakeup);
            cFunc(TEvents::TSystem::Poison, PassAway)
            cFunc(TEvPrivate::EvBeginStopping, HandleBeginStopping)

            hFunc(TEvReadThenWritePersistentBuffers, Handle)
            hFunc(TEvWritePersistentBuffers, Handle)
        )
    }

    bool TDDiskActor::CheckPDiskReply(NKikimrProto::EReplyStatus status,
            const TString& errorReason, TStringBuf source) {
        switch (status) {
        case NKikimrProto::OK:
            return true;
        case NKikimrProto::ERROR:
        case NKikimrProto::INVALID_OWNER:
        case NKikimrProto::INVALID_ROUND:
        case NKikimrProto::CORRUPTED:
        case NKikimrProto::OUT_OF_SPACE:
            YDB_LOG_NOTICE("TDDiskActor: PDisk session lost, beginning shutdown",
                {"marker", "BSDD44"},
                {"DDiskId", DDiskId},
                {"source", source},
                {"status", NKikimrProto::EReplyStatus_Name(status)},
                {"errorReason", errorReason});
            BeginStopping(errorReason);
            return false;
        default:
            Y_ABORT("Unexpected PDisk status %s in %.*s: %s",
                NKikimrProto::EReplyStatus_Name(status).c_str(),
                static_cast<int>(source.size()), source.data(), errorReason.c_str());
        }
    }

    void TDDiskActor::RejectQueryWhenStopping(IEventHandle& ev) {
        RejectQuery(ev, NKikimrBlobStorage::NDDisk::TReplyStatus::SESSION_MISMATCH, TString(StoppingReason));
    }

    void TDDiskActor::RejectQuery(IEventHandle& ev,
            NKikimrBlobStorage::NDDisk::TReplyStatus::E status, const TString& reason) {
        std::unique_ptr<IEventBase> reply;
        TInterfaceOpCounters* counters = nullptr;
        switch (ev.GetTypeRewrite()) {
#define REJECT_QUERY(NAME, COUNTERS) \
            case TEv::Ev##NAME: \
                reply = std::make_unique<TEv##NAME::TResult>(status, reason); \
                counters = COUNTERS; \
                break;
            REJECT_QUERY(Connect, nullptr)
            REJECT_QUERY(Disconnect, nullptr)
            REJECT_QUERY(Write, &Counters.Interface.Write)
            REJECT_QUERY(Read, &Counters.Interface.Read)
            REJECT_QUERY(Sync, &Counters.Interface.Sync)
            REJECT_QUERY(DeleteTabletChunks, nullptr)
            REJECT_QUERY(WritePersistentBuffer, &Counters.Interface.WritePersistentBuffer)
            REJECT_QUERY(ReadPersistentBuffer, &Counters.Interface.ReadPersistentBuffer)
            REJECT_QUERY(ErasePersistentBuffer, &Counters.Interface.ErasePersistentBuffer)
            REJECT_QUERY(BatchErasePersistentBuffer, &Counters.Interface.ErasePersistentBuffer)
            REJECT_QUERY(ListPersistentBuffer, &Counters.Interface.ListPersistentBuffer)
#undef REJECT_QUERY
            case TEv::EvWritePersistentBuffers:
            case TEv::EvReadThenWritePersistentBuffers: {
                auto result = std::make_unique<TEvWritePersistentBuffersResult>();
                const auto& ids = ev.GetTypeRewrite() == TEv::EvWritePersistentBuffers
                    ? ev.Get<TEvWritePersistentBuffers>()->Record.GetPersistentBufferIds()
                    : ev.Get<TEvReadThenWritePersistentBuffers>()->Record.GetPersistentBufferIds();
                for (const auto& id : ids) {
                    auto* item = result->Record.AddResult();
                    item->MutablePersistentBufferId()->CopyFrom(id);
                    item->MutableResult()->SetStatus(status);
                    item->MutableResult()->SetErrorReason(reason);
                }
                reply = std::move(result);
                break;
            }
            default:
                // Queues can also contain internal source-read results; their
                // originating sync owns the client reply.
                return;
        }
        if (counters) {
            counters->Request();
            counters->Reply(false);
        }
        SendReply(ev, std::move(reply));
    }

    void TDDiskActor::RejectPendingDDiskQueries(
            NKikimrBlobStorage::NDDisk::TReplyStatus::E status, const TString& reason) {
        for (auto& [tabletId, chunks] : ChunkRefs) {
            Y_UNUSED(tabletId);
            for (auto& [vChunkIndex, chunk] : chunks) {
                Y_UNUSED(vChunkIndex);
                auto drain = [&](auto& queue) {
                    while (!queue.empty()) {
                        auto ev = queue.front().Release();
                        queue.pop();
                        RejectQuery(*ev, status, reason);
                    }
                };
                drain(chunk.PendingEventsForChunk);
                drain(chunk.PendingSerializedWrites);
                chunk.SerializedWriteResumeScheduled = false;
            }
        }

        for (auto& [operationId, read] : PendingChecksumReads) {
            Y_UNUSED(operationId);
            const auto size = read.Event->Get<TEvRead>()->Record.GetSelector().GetSize();
            Counters.Interface.Read.Reply(false, size);
            SendReply(*read.Event, std::make_unique<TEvReadResult>(
                status, reason));
        }
        PendingChecksumReads.clear();

        for (const auto& [tabletId, pending] : TabletChunkDeletionReplies) {
            Y_UNUSED(tabletId);
            auto reply = std::make_unique<IEventHandle>(pending.ReplyTo, SelfId(),
                new TEvDeleteTabletChunksResult(status, reason), 0, pending.Cookie);
            if (pending.InterconnectSession) {
                reply->Rewrite(TEvInterconnect::EvForward, pending.InterconnectSession);
            }
            TActivationContext::Send(reply.release());
        }
        TabletChunkDeletionReplies.clear();
    }

    void TDDiskActor::RejectQueuedQueries() {
        using TStatus = NKikimrBlobStorage::NDDisk::TReplyStatus;
        auto drain = [this](auto& queue) {
            while (!queue.empty()) {
                auto ev = queue.front().Release();
                queue.pop();
                RejectQueryWhenStopping(*ev);
            }
        };
        drain(PendingQueries);
        RejectPendingDDiskQueries(TStatus::SESSION_MISMATCH, TString(StoppingReason));
        *Counters.PersistentBuffer.PendingEventsQueueSize -= PendingPersistentBufferEvents.size();
        drain(PendingPersistentBufferEvents);

        for (auto& [syncId, sync] : SyncsInFlight) {
            Y_UNUSED(syncId);
            auto result = std::make_unique<TEvSyncResult>(TStatus::SESSION_MISMATCH, TString(StoppingReason));
            for (const auto& request : sync.Requests) {
                Y_UNUSED(request);
                result->AddSegmentResult(TStatus::SESSION_MISMATCH, TString(StoppingReason));
            }
            auto reply = std::make_unique<IEventHandle>(sync.Sender, SelfId(), result.release(), 0, sync.Cookie);
            if (sync.InterconnectionSessionId) {
                reply->Rewrite(TEvInterconnect::EvForward, sync.InterconnectionSessionId);
            }
            Counters.Interface.Sync.Reply(false);
            sync.Span.End();
            TActivationContext::Send(reply.release());
        }
        SyncsInFlight.clear();
        SyncReadCookiesInFlight.clear();
        PendingSyncSegments.clear();

        // The open PB batch has not submitted any I/O. Use the normal write
        // finalizer to reply to its records and duplicate waiters.
        if (PersistentBufferBatchWriteCookie) {
            const ui64 cookie = std::exchange(PersistentBufferBatchWriteCookie, 0);
            auto& batch = PersistentBufferDiskOperationInflight.at(cookie);
            batch.Status = TStatus::SESSION_MISMATCH;
            batch.ErrorMessage = TString(StoppingReason);
            FinishPersistentBufferWrite(cookie);
        }
    }

    STFUNC(TDDiskActor::StateFuncStopping) {
        auto reject = [this](auto& ev) { RejectQueryWhenStopping(*ev); };
        auto rejectListRetry = [this](auto& ev) { RejectQueryWhenStopping(*ev->Get()->Ev); };
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvConnect, reject)
            hFunc(TEvDisconnect, reject)
            hFunc(TEvWrite, reject)
            hFunc(TEvRead, reject)
            hFunc(TEvSync, reject)
            hFunc(TEvDeleteTabletChunks, reject)
            hFunc(TEvWritePersistentBuffer, reject)
            hFunc(TEvReadPersistentBuffer, reject)
            hFunc(TEvErasePersistentBuffer, reject)
            hFunc(TEvBatchErasePersistentBuffer, reject)
            hFunc(TEvListPersistentBuffer, reject)
            hFunc(TEvWritePersistentBuffers, reject)
            hFunc(TEvReadThenWritePersistentBuffers, reject)
            hFunc(TEvPrivate::TEvRetryListPersistentBuffer, rejectListRetry)

            hFunc(TEvPrivate::TEvDDiskIoResult, Handle)
            hFunc(TEvPrivate::TEvIntegrityIoResult, Handle)
            hFunc(TEvPrivate::TEvReadPersistentBufferPart, Handle)
            hFunc(TEvPrivate::TEvWritePersistentBufferPart, Handle)
#if defined(__linux__)
            hFunc(TEvPrivate::TEvRetryIO, HandleRetryIO)
            hFunc(TEvPrivate::TEvRetryIODelayed, HandleRetryIODelayed)
#endif
            cFunc(TEvents::TSystem::Poison, PassAway)
            hFunc(TEvents::TEvGone, HandleGone)
            hFunc(TEvents::TEvUndelivered, Handle)
            cFunc(TEvPrivate::EvFinishStopping, FinishStopping)
            cFunc(TEvPrivate::EvCompleteStop, CompleteStop)
            cFunc(TEvPrivate::EvStopIoTimeout, HandleStopIoTimeout)
            hFunc(NMon::TEvHttpInfo, Handle)
            hFunc(TEvGetPersistentBufferInfo, Handle)
            default:
                // No recovery, allocation, PDisk callbacks, retries, or periodic
                // work may resume the service while submitted I/O drains.
                break;
        }
    }

    void TDDiskActor::PassAway() {
        PoisonReceived = true;
        BeginStopping(TString(StoppingReason));
        TryCompleteStop();
    }

    void TDDiskActor::HandleBeginStopping() {
        BeginStopping("io_uring router rejected submission");
    }

    void TDDiskActor::BeginStopping(TString reason) {
        if (Stopping) {
            return;
        }
        Stopping = true;
        Become(&TThis::StateFuncStopping);
        YDB_LOG_NOTICE("DDisk stopping", {"DDiskId", DDiskId}, {"reason", reason});
        if (IsPersistentBufferActor) {
            Send(WritePersistentBuffersActor, new TEvents::TEvPoison());
        } else if (PersistentBufferActorId) {
            PersistentBufferGone = false;
            Send(PersistentBufferActorId, new TEvents::TEvPoison(),
                IEventHandle::FlagTrackDelivery, PBShutdownCookie);
        }
        RejectQueuedQueries();
        CancelRetries();
        for (auto* callbacks : {&ReadCallbacks, &WriteCallbacks}) {
            while (!callbacks->empty()) {
                auto it = callbacks->begin();
                auto op = std::move(it->second.Op);
                callbacks->erase(it);
                CancelPendingIo(std::move(op));
                Counters.DirectIO.RunningCount->Dec();
            }
        }
        const ui64 previous = DirectIoState.fetch_or(DirectIoStopping, std::memory_order_acq_rel);
        if (!previous) {
            // Includes fallback: cancellation results must precede destruction.
            Send(SelfId(), new TEvPrivate::TEvFinishStopping);
        }
        if (previous || !PersistentBufferGone) {
            Schedule(StopIoTimeout, new TEvPrivate::TEvStopIoTimeout);
        }
    }

    void TDDiskActor::HandleGone(TEvents::TEvGone::TPtr ev) {
        if (ev->Sender == PersistentBufferActorId) {
            PersistentBufferGone = true;
            TryCompleteStop();
        }
    }

    void TDDiskActor::TryCompleteStop() {
        if (!PoisonReceived || !OwnDrainComplete || !PersistentBufferGone) {
            return;
        }
#if defined(__linux__)
        UringRouter.reset();
#endif
        CountersBase->RemoveSubgroupChain(CountersChain);
        if (IsPersistentBufferActor) {
            if (ParentDDiskId) {
                Send(ParentDDiskId, new TEvents::TEvGone());
            }
        } else {
            Send(MakeBlobStorageNodeWardenID(SelfId().NodeId()), new TEvents::TEvGone());
        }
        TActorBootstrapped::PassAway();
    }

    void TDDiskActor::OnDirectIODone(NActors::TActorSystem* actorSystem) {
        Counters.DirectIO.RunningCount->Dec();
#if defined(__linux__)
        if (UringRouter) {
            const TActorId actorId = SelfId();
            // This is the last actor access: shutdown may destroy it as soon as
            // the count reaches zero. All callback cleanup must precede retirement.
            const ui64 previous = DirectIoState.fetch_sub(1, std::memory_order_acq_rel);
            Y_ABORT_UNLESS(previous & ~DirectIoStopping);
            if (previous == (DirectIoStopping | 1)) {
                // Result events were published before retirement. The final mailbox
                // turn must process them before destroying the actor's request state.
                actorSystem->Send(actorId, new TEvPrivate::TEvFinishStopping);
            }
        }
#else
        Y_UNUSED(actorSystem);
#endif
    }

    void TDDiskActor::HandleStopIoTimeout() {
        if (GetDirectIoInflight() && !IoStalled) {
            IoStalled = true;
            IoStalledCounter->Inc();
            YDB_LOG_ERROR("TDDiskActor I/O stalled during shutdown",
                {"DDiskId", DDiskId}, {"persistentBuffer", IsPersistentBufferActor});
        }
        if (!PersistentBufferGone) {
            YDB_LOG_ERROR("DDisk waiting for PersistentBuffer shutdown", {"DDiskId", DDiskId},
                {"child", PersistentBufferActorId});
        }
    }

    void TDDiskActor::ClearIoStalled() {
        if (std::exchange(IoStalled, false)) {
            IoStalledCounter->Dec();
        }
    }

    void TDDiskActor::FinishStopping() {
        Y_ABORT_UNLESS(Stopping && !GetDirectIoInflight());
        if (std::exchange(OwnDrainFinishing, true)) {
            return;
        }
        using TStatus = NKikimrBlobStorage::NDDisk::TReplyStatus;
        // Results whose integrity work or allocation log could not finish are
        // already represented here; no separate registry of client replies is needed.
        for (auto& [operationId, pending] : PendingClientWrites) {
            Y_UNUSED(operationId);
            if (pending.DataResult) {
                pending.DataResult->Status = TStatus::SESSION_MISMATCH;
                pending.DataResult->ErrorMessage = TString(StoppingReason);
                FinishClientWrite(std::move(*pending.DataResult));
            }
        }
        PendingClientWrites.clear();
        for (auto& [key, allocation] : DataChunkAllocationsInFlight) {
            Y_UNUSED(key);
            for (auto& reply : allocation.ParkedWriteResults) {
                reply.Status = TStatus::SESSION_MISMATCH;
                reply.ErrorMessage = TString(StoppingReason);
            }
            FlushParkedAllocationReplies(allocation);
        }
        ClearIoStalled();
        // A queued retry can have posted a cancellation result ahead of this
        // barrier. Do not let child Gone bypass that final result turn.
        Send(SelfId(), new TEvPrivate::TEvCompleteStop);
    }

    void TDDiskActor::CompleteStop() {
        OwnDrainComplete = true;
        TryCompleteStop();
    }

    IActor *CreateDDiskActor(TVDiskConfig::TBaseInfo&& baseInfo, TIntrusivePtr<TBlobStorageGroupInfo> info,
            TPersistentBufferFormat&& pbFormat, TDDiskConfig&& ddiskConfig,
            TIntrusivePtr<NMonitoring::TDynamicCounters> counters) {
        return new TDDiskActor(std::move(baseInfo), std::move(info), std::move(pbFormat),
            std::move(ddiskConfig), std::move(counters));
    }

} // NKikimr::NDDisk
