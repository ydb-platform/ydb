#include "ddisk_actor.h"

#include <ydb/core/base/counters.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/util/stlog.h>

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
        };

        DDiskId = TStringBuilder() << '[' << BaseInfo.PDiskActorID.NodeId() << ':' << BaseInfo.PDiskId
            << ':' << BaseInfo.VDiskSlotId << ']';
    }

    void TDDiskActor::Bootstrap() {
        Become(&TThis::StateFunc);
        STLOG(PRI_DEBUG, BS_DDISK, BSDD09, "TDDiskActor::Bootstrap", (DDiskId, DDiskId));
        InitPDiskInterface();
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
            hFunc(TEvWritePersistentBuffer, handleQuery)
            hFunc(TEvReadPersistentBuffer, handleQuery)
            hFunc(TEvFlushPersistentBuffer, handleQuery)
            hFunc(TEvListPersistentBuffer, handleQuery)

            hFunc(TEvWriteResult, Handle)
            hFunc(TEvents::TEvUndelivered, Handle)

            hFunc(NPDisk::TEvYardInitResult, Handle)
            hFunc(NPDisk::TEvReadLogResult, Handle)
            cFunc(TEvPrivate::EvHandleSingleQuery, HandleSingleQuery)
            hFunc(NPDisk::TEvChunkReserveResult, Handle)
            hFunc(NPDisk::TEvLogResult, Handle)
            hFunc(TEvPrivate::TEvHandleEventForChunk, Handle)
            hFunc(NPDisk::TEvCutLog, Handle)
            hFunc(NPDisk::TEvChunkWriteRawResult, Handle)
            hFunc(NPDisk::TEvChunkReadRawResult, Handle)

            IgnoreFunc(NNodeWhiteboard::TEvWhiteboard::TEvVDiskStateUpdate)

            cFunc(TEvents::TSystem::Poison, PassAway)
        )
    }

    void TDDiskActor::PassAway() {
        CountersBase->RemoveSubgroupChain(CountersChain);
        TActorBootstrapped::PassAway();
    }

    IActor *CreateDDiskActor(TVDiskConfig::TBaseInfo&& baseInfo, TIntrusivePtr<TBlobStorageGroupInfo> info,
            TIntrusivePtr<NMonitoring::TDynamicCounters> counters) {
        return new TDDiskActor(std::move(baseInfo), std::move(info), std::move(counters));
    }

} // NKikimr::NDDisk
