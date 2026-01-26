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

        STFUNC_BODY(
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
            hFunc(NPDisk::TEvChunkWriteResult, Handle)
            hFunc(NPDisk::TEvChunkReadResult, Handle)

            cFunc(TEvents::TSystem::Poison, PassAway)
            // cFunc(NNodeWhiteboard::TEvWhiteboard::TEvVDiskStateUpdate, PassAway)
            ,Y_DEBUG_ABORT_UNLESS(true, "unexpected message type %s 0x%08" PRIx32, ev->GetTypeName().c_str(), etype);
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
