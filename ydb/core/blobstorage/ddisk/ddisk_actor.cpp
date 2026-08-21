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
        if constexpr (requires { record.ChecksumsSize(); record.GetSelector(); }) {
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
            ui64 persistentBufferUniqueId, TIntrusivePtr<TPDiskParams> pDiskParams, NPDisk::TDiskFormatPtr diskFormat,
            TFileHandle&& diskFd)
        : TDDiskActor(std::move(baseInfo), std::move(info), std::move(pbFormat), std::move(ddiskConfig), counters, true)
    {
        PersistentBufferUniqueId = persistentBufferUniqueId;
        PDiskParams = pDiskParams;
        DiskFormat = std::move(diskFormat);
        DiskFd = std::move(diskFd);
        InitPersistentBuffer();
        for (auto idx : initPersistentBufferChunks) {
            auto [it, inserted] = PersistentBufferSectorsChecksum.insert({idx, {}});
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

                COUNTER(DirectIO, RegularUringCount, false)
                COUNTER(DirectIO, FallbackUringCount, false)
                COUNTER(DirectIO, FallbackPDiskCount, false)

                COUNTER(DirectIO, QueueSize, false)
                COUNTER(DirectIO, RunningCount, false)
                HISTOGRAM(DirectIO, QueueTime, latencyHistBounds)
            },
#if defined(__linux__)
            .UringCounters = {
                COUNTER(DirectIO, CompletionThreadCPU, true)
                COUNTER(DirectIO, CompletionThreadBusyTimeNs, true)
            },
#endif
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
        [[maybe_unused]] constexpr size_t CompleteTypeGuard = sizeof(TDirectIoOpBase);
    }

    void TDDiskActor::Bootstrap() {
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
        const TString reason = GetBrokenReason();
        switch (ev->GetTypeRewrite()) {
            case TEv::EvWrite:
                Counters.Interface.Write.Request(0);
                Counters.Interface.Write.Reply(false);
                SendReply(*ev, std::make_unique<TEvWriteResult>(
                    NKikimrBlobStorage::NDDisk::TReplyStatus::ERROR, reason));
                break;
            case TEv::EvRead:
                Counters.Interface.Read.Request(0);
                Counters.Interface.Read.Reply(false);
                SendReply(*ev, std::make_unique<TEvReadResult>(
                    NKikimrBlobStorage::NDDisk::TReplyStatus::ERROR, reason));
                break;
            case TEv::EvSync:
                Counters.Interface.Sync.Request(0);
                Counters.Interface.Sync.Reply(false);
                SendReply(*ev, std::make_unique<TEvSyncResult>(
                    NKikimrBlobStorage::NDDisk::TReplyStatus::ERROR, reason));
                break;
            case TEv::EvDeleteTabletChunks:
                SendReply(*ev, std::make_unique<TEvDeleteTabletChunksResult>(
                    NKikimrBlobStorage::NDDisk::TReplyStatus::ERROR, reason));
                break;
            default:
                // Internal source-read results are covered by the SyncsInFlight drain below.
                break;
        }
    }

    void TDDiskActor::FailDirectIoOp(std::unique_ptr<TDirectIoOpBase> op, bool wasRunning) {
        if (wasRunning) {
            Counters.DirectIO.RunningCount->Dec();
        } else {
            Counters.DirectIO.QueueSize->Dec();
        }
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
        op->Reply(TActivationContext::ActorSystem(),
            NKikimrBlobStorage::NDDisk::TReplyStatus::ERROR, GetBrokenReason());
    }

    void TDDiskActor::EnterBroken(TString reason) {
        if (Broken) {
            return;
        }
        Broken = true;
        BrokenReason = reason
            ? std::move(reason)
            : TString("DDisk is broken");

        YDB_LOG_ERROR("TDDiskActor entered Broken state",
            {"marker", "BSDD54"},
            {"DDiskId", DDiskId},
            {"errorReason", GetBrokenReason()});

        // Complete actor-owned queued/fallback operations immediately. Submitted io_uring
        // operations post their result events later; the actor normalizes those to ERROR because
        // Broken is already set.
        while (!DirectIoQueue.empty()) {
            auto op = std::move(DirectIoQueue.front());
            DirectIoQueue.pop();
            FailDirectIoOp(std::move(op), false);
        }
        while (!WriteCallbacks.empty()) {
            auto it = WriteCallbacks.begin();
            auto op = std::move(it->second.Op);
            WriteCallbacks.erase(it);
            FailDirectIoOp(std::move(op), true);
        }
        while (!ReadCallbacks.empty()) {
            auto it = ReadCallbacks.begin();
            auto op = std::move(it->second.Op);
            ReadCallbacks.erase(it);
            FailDirectIoOp(std::move(op), true);
        }

        for (auto& [tabletId, chunks] : ChunkRefs) {
            for (auto& [vChunkIndex, chunkRef] : chunks) {
                Y_UNUSED(tabletId, vChunkIndex);
                while (!chunkRef.PendingEventsForChunk.empty()) {
                    auto pending = chunkRef.PendingEventsForChunk.front().Release();
                    chunkRef.PendingEventsForChunk.pop();
                    FailPendingDDiskQuery(std::unique_ptr<IEventHandle>(pending.Release()));
                }
                while (!chunkRef.PendingSerializedWrites.empty()) {
                    auto pending = chunkRef.PendingSerializedWrites.front().Release();
                    chunkRef.PendingSerializedWrites.pop();
                    FailPendingDDiskQuery(std::unique_ptr<IEventHandle>(pending.Release()));
                }
                chunkRef.SerializedWriteResumeScheduled = false;
            }
        }

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

        for (const auto& [tabletId, reply] : TabletChunkDeletionReplies) {
            Y_UNUSED(tabletId);
            auto h = std::make_unique<IEventHandle>(reply.ReplyTo, SelfId(),
                new TEvDeleteTabletChunksResult(
                    NKikimrBlobStorage::NDDisk::TReplyStatus::ERROR, GetBrokenReason()),
                0, reply.Cookie);
            if (reply.InterconnectSession) {
                h->Rewrite(TEvInterconnect::EvForward, reply.InterconnectSession);
            }
            TActivationContext::Send(h.release());
        }
        TabletChunkDeletionReplies.clear();

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

        for (auto& [operationId, pending] : PendingChecksumReads) {
            Y_UNUSED(operationId);
            Counters.Interface.Read.Reply(false);
            SendReply(*pending.Event, std::make_unique<TEvReadResult>(
                NKikimrBlobStorage::NDDisk::TReplyStatus::ERROR, GetBrokenReason()));
        }
        PendingChecksumReads.clear();
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
            hFunc(NPDisk::TEvCutLog, Handle)
            hFunc(TEvReadPersistentBufferResult, Handle)
            hFunc(NPDisk::TEvChunkWriteRawResult, Handle)
            hFunc(NPDisk::TEvChunkReadRawResult, Handle)
#if defined(__linux__)
            hFunc(TEvPrivate::TEvShortIO, HandleShortIO)
#endif

            hFunc(NPDisk::TEvCheckSpaceResult, Handle);

            IgnoreFunc(NNodeWhiteboard::TEvWhiteboard::TEvVDiskStateUpdate)

            hFunc(NMon::TEvHttpInfo, Handle)

            hFunc(TEvents::TEvWakeup, HandleWakeup);
            cFunc(TEvents::TSystem::Poison, PassAway)
        )
    }

    STFUNC(TDDiskActor::StateFuncPersistentBuffer) {
        STRICT_STFUNC_BODY(
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
            hFunc(TEvPrivate::TEvShortIO, HandleShortIO)
#endif

            hFunc(NPDisk::TEvCheckSpaceResult, Handle);

            IgnoreFunc(NNodeWhiteboard::TEvWhiteboard::TEvVDiskStateUpdate)

            hFunc(TEvents::TEvWakeup, HandleWakeup);
            cFunc(TEvents::TSystem::Poison, PassAway)

            hFunc(TEvReadThenWritePersistentBuffers, Handle)
            hFunc(TEvWritePersistentBuffers, Handle)
        )
    }

    STFUNC(TDDiskActor::StateFuncTerminate) {
        // Mirrors VDisk's PDISK_TERMINATE_STATE_FUNC_DEF: ignore everything except poison.
        // Reaching this state means PDisk's session for our owner is gone (INVALID_ROUND etc).
        // The owning environment (warden in production, test scaffolding in tests) is expected
        // to send TEvPoison and start a replacement DDisk actor with a fresh OwnerRound.
        switch (ev->GetTypeRewrite()) {
            cFunc(TEvents::TSystem::Poison, PassAway)
            default:
                break;
        }
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
            YDB_LOG_NOTICE("TDDiskActor: PDisk session lost, switching to terminate state",
                {"marker", "BSDD44"},
                {"DDiskId", DDiskId},
                {"source", source},
                {"status", NKikimrProto::EReplyStatus_Name(status)},
                {"errorReason", errorReason});
            Become(&TThis::StateFuncTerminate);
            return false;
        default:
            Y_ABORT("Unexpected PDisk status %s in %.*s: %s",
                NKikimrProto::EReplyStatus_Name(status).c_str(),
                static_cast<int>(source.size()), source.data(), errorReason.c_str());
        }
    }

    void TDDiskActor::PassAway() {
        if (IsPersistentBufferActor) {
            Send(WritePersistentBuffersActor, new NActors::TEvents::TEvPoison());
        } else {
            Send(PersistentBufferActorId, new NActors::TEvents::TEvPoison());
        }
#if defined(__linux__)
        if (UringRouter) {
            for (int i = 0; i < 1000 && UringRouter->GetInflight() > 0; ++i) {
                usleep(1000);
            }
            UringRouter->Stop();
            UringRouter.reset();
        }
#endif
        CountersBase->RemoveSubgroupChain(CountersChain);
        if (!IsPersistentBufferActor) {
            Send(MakeBlobStorageNodeWardenID(SelfId().NodeId()), new TEvents::TEvGone());
        }
        TActorBootstrapped::PassAway();
    }

    IActor *CreateDDiskActor(TVDiskConfig::TBaseInfo&& baseInfo, TIntrusivePtr<TBlobStorageGroupInfo> info,
            TPersistentBufferFormat&& pbFormat, TDDiskConfig&& ddiskConfig,
            TIntrusivePtr<NMonitoring::TDynamicCounters> counters) {
        return new TDDiskActor(std::move(baseInfo), std::move(info), std::move(pbFormat),
            std::move(ddiskConfig), std::move(counters));
    }

} // NKikimr::NDDisk
