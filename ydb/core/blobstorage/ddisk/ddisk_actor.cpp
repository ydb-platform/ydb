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

namespace NKikimr::NDDisk {

namespace {

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
            TIntrusivePtr<TPDiskParams> pDiskParams, NPDisk::TDiskFormatPtr diskFormat, TFileHandle&& diskFd)
        : TDDiskActor(std::move(baseInfo), std::move(info), std::move(pbFormat), std::move(ddiskConfig), counters, true)
    {
        PDiskParams = pDiskParams;
        DiskFormat = std::move(diskFormat);
        DiskFd = std::move(diskFd);
        InitPersistentBuffer();
        for (auto idx : initPersistentBufferChunks) {
            auto [it, inserted] = PersistentBufferSectorsChecksum.insert({idx, {}});
            it->second.resize(SectorInChunk);
            if (!inserted) {
                STLOG(PRI_ERROR, BS_DDISK, BSDD10, "TDDiskActor::TDDiskActor persistent buffer has duplicated chunk index in log", (DDiskId, DDiskId), (PDiskActorId, BaseInfo.PDiskActorID), (ChunkIdx, idx));
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
        , CountersBase(GetServiceCounters(counters, "ddisks"))
        , IsPersistentBufferActor(isPersistentBufferActor)
        , PersistentBufferFormat(std::move(pbFormat))
    {
        StartedAt = TInstant::Now();
        TVector<double> latencyHistBounds;
        if (BaseInfo.DeviceType == NPDisk::DEVICE_TYPE_NVME) {
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
        };

#undef COUNTER_VALUE
#undef HISTOGRAM_VALUE

        DDiskId = TStringBuilder() << '[' << BaseInfo.PDiskActorID.NodeId() << ':' << BaseInfo.PDiskId
            << ':' << BaseInfo.VDiskSlotId << ']';

        DdiskIoOpPool.Resize(IoOpPoolCapacity);
        PersistentBufferPartIoOpPool.Resize(IoOpPoolCapacity);
        InternalSyncWriteOpPool.Resize(IoOpPoolCapacity);
    }

    TDDiskActor::~TDDiskActor() {
        [[maybe_unused]] constexpr size_t CompleteTypeGuard = sizeof(TDirectIoOpBase);
    }

    void TDDiskActor::Bootstrap() {
        FillPool(DdiskIoOpPool);
        FillPool(PersistentBufferPartIoOpPool);
        FillPool(InternalSyncWriteOpPool);

        STLOG(PRI_DEBUG, BS_DDISK, BSDD09, "TDDiskActor::Bootstrap", (DDiskId, DDiskId));
        if (IsPersistentBufferActor) {
            InitUring();
            Become(&TThis::StateFuncPersistentBuffer);
            WritePersistentBuffersActor = RegisterWithSameMailbox(new TWritePersistentBuffersRequestActor(SelfId()));
            StartRestorePersistentBuffer();
        } else {
            Become(&TThis::StateFuncDDisk);
            InitPDiskInterface();
        }
    }

    void TDDiskActor::Handle(TEvents::TEvUndelivered::TPtr ev) {
        auto sourceType = ev->Get()->SourceType;
        if (sourceType == TEv::EvRead || sourceType == TEv::EvReadPersistentBuffer) {
            std::vector<TSegmentManager::TSegment> segments;
            ui64 syncId = SegmentManager.GetSync(ev->Cookie);
            SegmentManager.PopRequest(ev->Cookie, &segments);

            auto it = SyncsInFlight.find(syncId);
            if (it == SyncsInFlight.end()) {
                return;
            }
            auto& sync = it->second;

            if (ev->Cookie < sync.FirstRequestId || ev->Cookie >= sync.FirstRequestId + sync.Requests.size()) {
                STLOG(PRI_ERROR, BS_DDISK, BSDD23,
                    "TDDiskActor::Handle(TEvUndelivered) request cookie out of range",
                    (DDiskId, DDiskId),
                    (Cookie, ev->Cookie),
                    (SyncId, syncId),
                    (FirstRequestId, sync.FirstRequestId),
                    (RequestsCount, sync.Requests.size()),
                    (SourceType, sourceType));
                return;
            }
            auto& request = sync.Requests[ev->Cookie - sync.FirstRequestId];

            request.Status = NKikimrBlobStorage::NDDisk::TReplyStatus::ERROR;
            request.ErrorReason << "[" << request.Selector.OffsetInBytes << ';'
                << request.Selector.OffsetInBytes + request.Selector.Size
                << "] failed to read; reason: read event undelivered";
            sync.ErrorReason << "[request_idx=" << ev->Cookie - sync.FirstRequestId << "] failed to read; ";
            if (--sync.RequestsInFlight == 0) {
                ReplySync(it);
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
            hFunc(TEvSyncWithPersistentBuffer, handleQuery)
            hFunc(TEvSyncWithDDisk, handleQuery)
            hFunc(TEvPrivate::TEvIssuePersistentBufferChunkAllocation, Handle)

            hFunc(TEvents::TEvUndelivered, Handle)

            hFunc(TEvReadResult, Handle)
            hFunc(TEvPrivate::TEvInternalSyncWriteResult, Handle)

            hFunc(NPDisk::TEvYardInitResult, Handle)
            hFunc(NPDisk::TEvReadLogResult, Handle)
            cFunc(TEvPrivate::EvHandleSingleQuery, HandleSingleQuery)
            hFunc(NPDisk::TEvChunkReserveResult, Handle)
            hFunc(NPDisk::TEvLogResult, Handle)
            hFunc(TEvPrivate::TEvHandleEventForChunk, Handle)
            hFunc(NPDisk::TEvCutLog, Handle)
            hFunc(TEvReadPersistentBufferResult, Handle)
            hFunc(NPDisk::TEvChunkWriteRawResult, Handle)
            hFunc(NPDisk::TEvChunkReadRawResult, Handle)
#if defined(__linux__)
            hFunc(TEvPrivate::TEvShortIO, HandleShortIO)
#endif

            hFunc(NPDisk::TEvCheckSpaceResult, Handle);

            IgnoreFunc(NNodeWhiteboard::TEvWhiteboard::TEvVDiskStateUpdate)

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
            hFunc(TEvGetPersistentBufferInfo, Handle)

            hFunc(TEvPrivate::TEvReadPersistentBufferPart, Handle)
            hFunc(TEvPrivate::TEvWritePersistentBufferPart, Handle)

            hFunc(TEvents::TEvUndelivered, Handle)

            hFunc(TEvPrivate::TEvHandlePersistentBufferEventForChunk, Handle)

            hFunc(NPDisk::TEvChunkWriteRawResult, Handle)
            hFunc(NPDisk::TEvChunkReadRawResult, Handle)
#if defined(__linux__)
            hFunc(TEvPrivate::TEvShortIO, HandleShortIO)
#endif

            hFunc(NPDisk::TEvCheckSpaceResult, Handle);

            IgnoreFunc(NNodeWhiteboard::TEvWhiteboard::TEvVDiskStateUpdate)

            hFunc(TEvents::TEvWakeup, HandleWakeup);
            cFunc(TEvents::TSystem::Poison, PassAway)

            case TEvReadThenWritePersistentBuffers::EventType:
            case TEvWritePersistentBuffers::EventType: {
                Y_ABORT_UNLESS(WritePersistentBuffersActor);
                TActivationContext::Forward(ev, WritePersistentBuffersActor);
                break;
            }
        )
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
        TActorBootstrapped::PassAway();
    }

    IActor *CreateDDiskActor(TVDiskConfig::TBaseInfo&& baseInfo, TIntrusivePtr<TBlobStorageGroupInfo> info,
            TPersistentBufferFormat&& pbFormat, TDDiskConfig&& ddiskConfig,
            TIntrusivePtr<NMonitoring::TDynamicCounters> counters) {
        return new TDDiskActor(std::move(baseInfo), std::move(info), std::move(pbFormat),
            std::move(ddiskConfig), std::move(counters));
    }

} // NKikimr::NDDisk
