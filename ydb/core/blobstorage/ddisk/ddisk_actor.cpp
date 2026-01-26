#include "ddisk_actor.h"

#include <ydb/core/util/stlog.h>

namespace NKikimr::NDDisk {

    TDDiskActor::TDDiskActor(TVDiskConfig::TBaseInfo&& baseInfo, TIntrusivePtr<TBlobStorageGroupInfo> info,
            TIntrusivePtr<NMonitoring::TDynamicCounters> counters)
        : BaseInfo(std::move(baseInfo))
        , Info(std::move(info))
        , Counters(std::move(counters))
    {
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
            hFunc(TEvDDiskConnect, handleQuery)
            hFunc(TEvDDiskDisconnect, handleQuery)
            hFunc(TEvDDiskWrite, handleQuery)
            hFunc(TEvDDiskRead, handleQuery)
            hFunc(TEvDDiskWritePersistentBuffer, handleQuery)
            hFunc(TEvDDiskReadPersistentBuffer, handleQuery)
            hFunc(TEvDDiskFlushPersistentBuffer, handleQuery)
            hFunc(TEvDDiskListPersistentBuffer, handleQuery)

            hFunc(TEvDDiskWriteResult, Handle)
            hFunc(TEvents::TEvUndelivered, Handle)

            hFunc(NPDisk::TEvYardInitResult, Handle)
            hFunc(NPDisk::TEvReadLogResult, Handle)
            cFunc(TEvPrivate::EvHandleSingleQuery, HandleSingleQuery)
            hFunc(NPDisk::TEvChunkReserveResult, Handle)
            hFunc(NPDisk::TEvLogResult, Handle)
            hFunc(TEvPrivate::TEvHandleEventForChunk, Handle)
            hFunc(NPDisk::TEvCutLog, Handle)
            hFunc(NPDisk::TEvChunkWriteResult, Handle)
            hFunc(NPDisk::TEvChunkReadResult, Handle)

            cFunc(TEvents::TSystem::Poison, PassAway)
        )
    }

    void TDDiskActor::PassAway() {
        TActorBootstrapped::PassAway();
    }

    IActor *CreateDDiskActor(TVDiskConfig::TBaseInfo&& baseInfo, TIntrusivePtr<TBlobStorageGroupInfo> info,
            TIntrusivePtr<NMonitoring::TDynamicCounters> counters) {
        return new TDDiskActor(std::move(baseInfo), std::move(info), std::move(counters));
    }

} // NKikimr::NDDisk
