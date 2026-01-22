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
        STLOG(PRI_DEBUG, BS_DDISK, BSDD00, "TDDiskActor::Bootstrap", (DDiskId, DDiskId));
    }

    STRICT_STFUNC(TDDiskActor::StateFunc,
        hFunc(TEvDDiskConnect, Handle)
        hFunc(TEvDDiskWrite, Handle)
        hFunc(TEvDDiskRead, Handle)
        cFunc(TEvents::TSystem::Poison, PassAway)
    )

    void TDDiskActor::PassAway() {
        TActorBootstrapped::PassAway();
    }

    IActor *CreateDDiskActor(TVDiskConfig::TBaseInfo&& baseInfo, TIntrusivePtr<TBlobStorageGroupInfo> info,
            TIntrusivePtr<NMonitoring::TDynamicCounters> counters) {
        return new TDDiskActor(std::move(baseInfo), std::move(info), std::move(counters));
    }

} // NKikimr::NDDisk
