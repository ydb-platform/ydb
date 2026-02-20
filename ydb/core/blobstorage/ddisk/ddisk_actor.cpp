#include "ddisk_actor.h"

#include <ydb/core/base/counters.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/util/stlog.h>

#if defined(__linux__)
#include <unistd.h>
#endif

namespace NKikimr::NDDisk {

    TDDiskActor::TDDiskActor(TVDiskConfig::TBaseInfo&& baseInfo, TIntrusivePtr<TBlobStorageGroupInfo> info,
            TIntrusivePtr<NMonitoring::TDynamicCounters> counters)
        : BaseInfo(std::move(baseInfo))
        , Info(std::move(info))
        , CountersBase(GetServiceCounters(counters, "ddisks"))
    {
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

#define COUNTER(GROUP, NAME, DERIV) .NAME = c##GROUP->GetCounter(#NAME, DERIV),

        Counters = {
            .Interface = {
#define XX(OP) \
                .OP = { \
                    COUNTER(Interface##OP, Requests, true) \
                    COUNTER(Interface##OP, ReplyOk, true) \
                    COUNTER(Interface##OP, ReplyErr, true) \
                    COUNTER(Interface##OP, Bytes, true) \
                },
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
                COUNTER(DirectIO, ShortReads, true)
                COUNTER(DirectIO, ShortWrites, true)
            },
        };

        DDiskId = TStringBuilder() << '[' << BaseInfo.PDiskActorID.NodeId() << ':' << BaseInfo.PDiskId
            << ':' << BaseInfo.VDiskSlotId << ']';
    }

    void TDDiskActor::Bootstrap() {
        Become(&TThis::StateFunc);
        STLOG(PRI_DEBUG, BS_DDISK, BSDD09, "TDDiskActor::Bootstrap", (DDiskId, DDiskId));
        InitPDiskInterface();
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
                // TODO(kruall): log error
                return;
            }
            auto& request = sync.Requests[ev->Cookie - sync.FirstRequestId];

            request.Status = NKikimrBlobStorage::NDDisk::TReplyStatus::ERROR;
            request.ErrorReason << "[" << request.Selector.OffsetInBytes << ';' << request.Selector.OffsetInBytes + request.Selector.Size << "] failed to read; reason: read event undelivered";
            sync.ErrorReason << "[request_idx=" << ev->Cookie - sync.FirstRequestId << "] failed to read; ";
            if (--sync.RequestsInFlight == 0) {
                ReplySync(it);
            }
            return;
        }
    }

    STFUNC(TDDiskActor::StateFunc) {
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
            hFunc(TEvWritePersistentBuffer, handleQuery)
            hFunc(TEvReadPersistentBuffer, handleQuery)
            hFunc(TEvErasePersistentBuffer, handleQuery)
            hFunc(TEvBatchErasePersistentBuffer, handleQuery)
            hFunc(TEvListPersistentBuffer, handleQuery)

            hFunc(TEvents::TEvUndelivered, Handle)

            hFunc(TEvReadResult, Handle)
            hFunc(TEvReadPersistentBufferResult, Handle)

            hFunc(NPDisk::TEvYardInitResult, Handle)
            hFunc(NPDisk::TEvReadLogResult, Handle)
            cFunc(TEvPrivate::EvHandleSingleQuery, HandleSingleQuery)
            hFunc(NPDisk::TEvChunkReserveResult, Handle)
            hFunc(NPDisk::TEvLogResult, Handle)
            hFunc(TEvPrivate::TEvHandleEventForChunk, Handle)
            hFunc(TEvPrivate::TEvHandlePersistentBufferEventForChunk, Handle)
            hFunc(NPDisk::TEvCutLog, Handle)
            hFunc(NPDisk::TEvChunkWriteRawResult, Handle)
            hFunc(NPDisk::TEvChunkReadRawResult, Handle)
#if defined(__linux__)
            hFunc(TEvPrivate::TEvShortIO, HandleShortIO)
#endif

            IgnoreFunc(NNodeWhiteboard::TEvWhiteboard::TEvVDiskStateUpdate)

            cFunc(TEvents::TSystem::Poison, PassAway)
        )
    }

    void TDDiskActor::PassAway() {
#if defined(__linux__)
        if (UringRouter) {
            for (int i = 0; i < 1000 && InFlightCount.load(std::memory_order_acquire) > 0; ++i) {
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
            TIntrusivePtr<NMonitoring::TDynamicCounters> counters) {
        return new TDDiskActor(std::move(baseInfo), std::move(info), std::move(counters));
    }

} // NKikimr::NDDisk
