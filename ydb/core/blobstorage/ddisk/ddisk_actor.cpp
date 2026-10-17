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
            using TStatus = NKikimrBlobStorage::NDDisk::TReplyStatus;
            const TStringBuf errorReason = [&] {
                switch (ownershipStatus) {
                    case TStatus::ERROR:
                        return TStringBuf("persistent buffer is broken");
                    case TStatus::BUSY:
                        return TStringBuf("persistent buffer is not ready yet");
                    case TStatus::OUTDATED:
                        return TStringBuf("persistent buffer registration is being retired");
                    case TStatus::INCORRECT_REQUEST:
                        return TStringBuf("persistent buffer is not registered");
                    default:
                        return TStringBuf("persistent buffer registration is not ready, registered, or active");
                }
            }();
            auto result = std::make_unique<TEvWritePersistentBuffersResult>();
            for (const auto& id : record.GetPersistentBufferIds()) {
                auto* item = result->Record.AddResult();
                item->MutablePersistentBufferId()->CopyFrom(id);
                item->MutableResult()->SetStatus(ownershipStatus);
                item->MutableResult()->SetErrorReason(errorReason.data(), errorReason.size());
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
                COUNTER(PersistentBuffer, RegisteredTablets, false)
                COUNTER(PersistentBuffer, RegisteredTabletsLimit, false)
                COUNTER(PersistentBuffer, AllocatedChunks, false)
                COUNTER(PersistentBuffer, TotalBytes, false)
                COUNTER(PersistentBuffer, PendingEventsQueueSize, false)
                COUNTER(PersistentBuffer, InMemoryCacheSize, false)
                HISTOGRAM(PersistentBuffer, WriteBatchSize, WriteBatchSizeBounds)
            },
            .Checksums = {
                COUNTER(Checksums, WritesWithoutChecksums, true)
                COUNTER(Checksums, ChecksumMismatch, true)
                // Preserve published names for existing monitoring consumers.
                .MetadataReads = COUNTER_VALUE(Checksums, IntegrityPairReads, true),
                .ChecksumWrites = COUNTER_VALUE(Checksums, IntegrityPairWrites, true),
                COUNTER(Checksums, IntegrityCorruption, true)
                COUNTER(Checksums, IntegrityLostWriteDetected, true)
            },
        };

        if (IsPersistentBufferActor) {
            *Counters.PersistentBuffer.RegisteredTablets = 0;
            *Counters.PersistentBuffer.RegisteredTabletsLimit =
                TPersistentBufferBarriersManager::MaxRegistrations(PersistentBufferFormat.MaxBarriersLimit);
        }

#undef COUNTER_VALUE
#undef HISTOGRAM_VALUE

        DDiskId = TStringBuilder() << '[' << BaseInfo.PDiskActorID.NodeId() << ':' << BaseInfo.PDiskId
            << ':' << BaseInfo.VDiskSlotId << ']';

        DdiskIoOpPool.Resize(IoOpPoolCapacity);
        PersistentBufferPartIoOpPool.Resize(IoOpPoolCapacity);
        BatchedIOAwaiterPool.reserve(IoOpPoolCapacity);
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
        for (ui32 i = 0; i < IoOpPoolCapacity; ++i) {
            BatchedIOAwaiterPool.push_back(std::make_shared<TBatchedIOAwaiter>(*this));
        }

        YDB_LOG_DEBUG("TDDiskActor::Bootstrap",
            {"marker", "BSDD09"},
            {"DDiskId", DDiskId});
        if (IsPersistentBufferActor) {
            InitUring();
            Become(&TThis::StateFuncPersistentBuffer);
            WritePersistentBuffersActor = Register(new TWritePersistentBuffersRequestActor(SelfId()));
            InitMemoryMetrics();
            CollectPbStatsSnapshot();
            StartRestorePersistentBuffer();
        } else {
            Become(&TThis::StateFuncDDisk);
            TabletStatsActor = Register(CreateTabletStatsActor(SelfId()));
            RegisterMonPage();
            InitMemoryMetrics();
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

    void TDDiskActor::FailDirectIoOp(std::unique_ptr<TDirectIoOpBase> op, TString reason) {
        op->GetCounters().Done(op->GetTotalSize());
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

        CancelPendingSyncSources();
        for (auto& [_, allocation] : DataChunkAllocationsInFlight) {
            if (allocation.ChunkIdx && !allocation.LogIssued) {
                PendingChunkRelease.insert(allocation.ChunkIdx);
            }
        }
        FailLogWaiters(true);
        if (IntegrityManager) {
            IntegrityManager->Stop();
            IntegrityManager->NotifyCompleted();
        }

        // DDisk and PersistentBuffer are separate actor instances sharing
        // this class. Only DDisk talks to PDisk, so PB chunk requests still
        // arrive here as TChunkForPersistentBuffer. Drop data/integrity work
        // and keep serving those PB allocations if DDisk is the one that
        // broke. The PersistentBuffer instance never uses this queue.
        if (!IsPersistentBufferActor) {
            ChunkManager.RetainPersistentBufferAllocations();
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
        if (sourceType == NPDisk::TEvLog::EventType) {
            for (const auto& [lsn, waiter] : LogWaiters) {
                Y_UNUSED(lsn);
                if (waiter.DeliveryCookie == ev->Cookie) {
                    BeginStopping("PDisk log request was not delivered");
                    return;
                }
            }
            return;
        }
        if (sourceType == NPDisk::TEvChunkReserve::EventType
                && ReservationCookie && *ReservationCookie == ev->Cookie) {
            ReservationCookie.reset();
            ChunkManager.FinishReservation();
            BeginStopping("PDisk reserve request was not delivered");
            TryCompleteStop();
            return;
        }
        if (sourceType == TEv::EvRead || sourceType == TEv::EvReadPersistentBuffer) {
            HandleSyncSourceUndelivered(ev->Cookie, sourceType == TEv::EvReadPersistentBuffer);
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

            hFunc(NPDisk::TEvYardInitResult, Handle)
            hFunc(NPDisk::TEvReadLogResult, Handle)
            IgnoreFunc(NPDisk::TEvChunkForgetResult)
            hFunc(NPDisk::TEvChunkReserveResult, Handle)
            cFunc(TEvPrivate::EvHandleSingleQuery, HandleSingleQuery)
            hFunc(NPDisk::TEvLogResult, Handle)
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

            hFunc(TEvCollectTabletStats, Handle)
            hFunc(TEvGetTabletStats, Handle)
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
            hFunc(TEvGetPersistentBufferRegistrationToken, Handle)
            hFunc(TEvPrivate::TEvExpirePersistentBufferRegistrationToken, Handle)
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
            REJECT_QUERY(GetPersistentBufferRegistrationToken, &Counters.Interface.GetPersistentBufferRegistrationToken)
            REJECT_QUERY(RegisterPersistentBuffer, nullptr)
            REJECT_QUERY(UnregisterPersistentBuffer, nullptr)
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
        for (auto& [tabletId, tablet] : Tablets) {
            auto& chunks = tablet.ChunkRefs;
            Y_UNUSED(tabletId);
            for (auto& [vChunkIndex, chunk] : chunks) {
                Y_UNUSED(vChunkIndex);
                // Wake allocation and commit awaiters before
                // the stopping mailbox barrier and Gone notification.
                chunk.AllocationPending = false;
                chunk.AllocationReady.NotifyAll();
                chunk.CommitReady.NotifyAll();
            }
        }

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

        CancelPendingSyncSources();
        if (IntegrityManager) {
            IntegrityManager->Stop();
            IntegrityManager->NotifyCompleted();
        }

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
            hFunc(TEvGetPersistentBufferRegistrationToken, reject)
            hFunc(TEvRegisterPersistentBuffer, reject)
            hFunc(TEvUnregisterPersistentBuffer, reject)
            hFunc(TEvDeleteTabletChunks, reject)
            hFunc(TEvWritePersistentBuffer, reject)
            hFunc(TEvReadPersistentBuffer, reject)
            hFunc(TEvErasePersistentBuffer, reject)
            hFunc(TEvBatchErasePersistentBuffer, reject)
            hFunc(TEvListPersistentBuffer, reject)
            hFunc(TEvWritePersistentBuffers, reject)
            hFunc(TEvReadThenWritePersistentBuffers, reject)
            hFunc(TEvPrivate::TEvRetryListPersistentBuffer, rejectListRetry)

            hFunc(NPDisk::TEvChunkReserveResult, Handle)
            hFunc(TEvReadResult, Handle)
            hFunc(TEvReadPersistentBufferResult, Handle)
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
            hFunc(TEvCollectTabletStats, Handle)
            hFunc(TEvGetTabletStats, Handle)
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
        MemoryMetric.Close();
        SpaceMetric.Close();
        OperationMetric.Close();
        PersistentBufferRegistrationTokens.clear();
        Become(&TThis::StateFuncStopping);
        YDB_LOG_NOTICE("DDisk stopping", {"DDiskId", DDiskId}, {"reason", reason});
        if (IsPersistentBufferActor) {
            Send(WritePersistentBuffersActor, new TEvents::TEvPoison());
        } else if (PersistentBufferActorId) {
            PersistentBufferGone = false;
            Send(PersistentBufferActorId, new TEvents::TEvPoison(),
                IEventHandle::FlagTrackDelivery, PBShutdownCookie);
        }
        FailLogWaiters(false);
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
        if (!(previous & ~DirectIoFlags)) {
            // Includes fallback: cancellation results must precede destruction.
            Send(SelfId(), new TEvPrivate::TEvFinishStopping);
        }
        if ((previous & ~DirectIoFlags) || !PersistentBufferGone || ChunkManager.IsReservationInFlight()) {
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
        if (!PoisonReceived || !OwnDrainComplete || DataRequestsInFlight || !SyncsInFlight.empty()
                || !SyncSourceCookies.empty()
                || !PersistentBufferGone || ChunkManager.IsReservationInFlight()) {
            return;
        }
#if defined(__linux__)
        UringRouter.reset();
#endif
        CountersBase->RemoveSubgroupChain(CountersChain);
        if (IsPersistentBufferActor) {
            Send(NNodeWhiteboard::MakeNodeWhiteboardServiceId(SelfId().NodeId()),
                new NNodeWhiteboard::TEvWhiteboard::TEvDDiskStateDelete(BaseInfo.PDiskId, BaseInfo.VDiskSlotId, BaseInfo.InitOwnerRound));
            if (ParentDDiskId) {
                Send(ParentDDiskId, new TEvents::TEvGone());
            }
        } else {
            Send(MakeBlobStorageNodeWardenID(SelfId().NodeId()), new TEvents::TEvGone());
        }
        if (TabletStatsActor) {
            Send(TabletStatsActor, new TEvents::TEvPoison());
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
            Y_ABORT_UNLESS(previous & ~DirectIoFlags);
            if ((previous & DirectIoStopping) && (previous & ~DirectIoFlags) == 1) {
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
        if (ChunkManager.IsReservationInFlight()) {
            YDB_LOG_ERROR("DDisk waiting for outstanding reservation", {"DDiskId", DDiskId});
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
        // RejectQueuedQueries wakes logical requests before the fallback cancellation loop
        // publishes all accepted I/O results. BeginStopping sets this bit only after that loop;
        // its final TEvFinishStopping (or the last router callback) retries
        // this barrier once every cancellation result has been queued.
        if (!(DirectIoState.load(std::memory_order_acquire) & DirectIoStopping)) {
            return;
        }
        if (DataRequestsInFlight || !SyncsInFlight.empty() || !SyncSourceCookies.empty()) {
            return;
        }
        if (std::exchange(OwnDrainFinishing, true)) {
            return;
        }
        ClearIoStalled();
        ReleaseUncommittedChunks();
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
