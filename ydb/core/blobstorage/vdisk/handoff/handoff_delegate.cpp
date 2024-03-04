#include "handoff_delegate.h"
#include "handoff_basic.h"
#include "handoff_proxy.h"
#include "handoff_mon.h"
#include <ydb/core/blobstorage/base/utility.h>

#include <library/cpp/actors/core/log.h>

using namespace NKikimrServices;
using namespace NKikimr::NHandoff;

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // TFields -- THandoffDelegate fields
    ////////////////////////////////////////////////////////////////////////////
    struct THandoffDelegate::TFields {
        const TVDiskID SelfVDisk; // FIXME: switch to TVDiskIdShort
        TIntrusivePtr<TBlobStorageGroupInfo> Info;
        NHandoff::TProxiesPtr ProxiesPtr;
        TActorId MonActorID = {};
        const THandoffParams Params;
        bool ProxiesStarted = false;

        TFields(const TVDiskIdShort &self,
                TIntrusivePtr<TBlobStorageGroupInfo> info,
                const THandoffParams &params)
            : SelfVDisk(info->GetVDiskId(self))
            , Info(info)
            , ProxiesPtr(new TProxies(SelfVDisk, info->PickTopology()))
            , Params(params)
        {}
    };

    ////////////////////////////////////////////////////////////////////////////
    // Handoff manager delegate
    ////////////////////////////////////////////////////////////////////////////
    THandoffDelegate::THandoffDelegate(const TVDiskIdShort &self,
                                       TIntrusivePtr<TBlobStorageGroupInfo> info,
                                       const THandoffParams &params)
        : Fields(std::make_unique<TFields>(self, info, params))
    {}

    TActiveActors THandoffDelegate::RunProxies(const TActorContext &ctx) {
        TActiveActors activeActors;
        for (auto &it : *Fields->ProxiesPtr) {
            if (!it.Myself) {
                auto vd = Fields->Info->GetVDiskId(it.OrderNumber);
                it.Get().Init(Fields->SelfVDisk, vd,
                              Fields->Params.MaxWaitQueueSize + Fields->Params.MaxInFlightSize,
                              Fields->Params.MaxWaitQueueByteSize + Fields->Params.MaxInFlightByteSize);
                auto aid = ctx.Register(CreateHandoffProxyActor(Fields->Info, Fields->ProxiesPtr,
                        &it, Fields->Params));
                activeActors.Insert(aid, __FILE__, __LINE__, ctx, NKikimrServices::BLOBSTORAGE);
                it.Get().ProxyID = aid;
            }
        }
        Fields->MonActorID = ctx.Register(CreateHandoffMonActor(Fields->SelfVDisk, Fields->Info->PickTopology(),
                Fields->ProxiesPtr));
        activeActors.Insert(Fields->MonActorID, __FILE__, __LINE__, ctx, NKikimrServices::BLOBSTORAGE);
        Fields->ProxiesStarted = true;
        return activeActors;
    }

    bool THandoffDelegate::Restore(const TActorContext &ctx,
                                   const TVDiskIdShort &vdisk,
                                   const TLogoBlobID &id,
                                   ui64 fullDataSize,
                                   TRope&& data) {
        TVDiskInfo &ref = (*Fields->ProxiesPtr)[vdisk];
        Y_VERIFY_DEBUG(Fields->ProxiesStarted &&
                       vdisk == Fields->Info->GetVDiskId(ref.OrderNumber) &&
                       vdisk != Fields->SelfVDisk &&
                       vdisk == ref.Get().TargetVDiskID);
        return ref.Get().Restore(ctx, id, fullDataSize, std::move(data));
    }

    TActorId THandoffDelegate::GetMonActorID() const {
        return Fields->MonActorID;
    }

} // NKikimr

