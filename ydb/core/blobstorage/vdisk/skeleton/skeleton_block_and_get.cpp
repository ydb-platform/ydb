#include "skeleton_block_and_get.h"
#include "blobstorage_skeletonerr.h"

#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_response.h>

#include <library/cpp/actors/core/actor.h>
#include <library/cpp/actors/core/actor_bootstrapped.h>

#include <memory>

namespace NKikimr {

class TBlockAndGetActor : public TActorBootstrapped<TBlockAndGetActor> {
public:
    TBlockAndGetActor() = delete;
    explicit TBlockAndGetActor(
        TEvBlobStorage::TEvVGet::TPtr ev,
        NActors::TActorId skeletonId,
        TIntrusivePtr<TVDiskContext> vCtx,
        TActorIDPtr skeletonFrontIDPtr,
        TVDiskID selfVDiskId,
        TVDiskIncarnationGuid vDiskIncarnationGuid,
        TIntrusivePtr<NKikimr::TBlobStorageGroupInfo> gInfo)
        : SenderId(ev->Sender)
        , SkeletonId(skeletonId)
        , VCtx(vCtx)
        , SkeletonFrontIDPtr(skeletonFrontIDPtr)
        , SelfVDiskId(selfVDiskId)
        , VDiskIncarnationGuid(vDiskIncarnationGuid)
        , GInfo(gInfo)
    {
        Y_VERIFY(ev->Get()->Record.GetForceBlockedGeneration() > 0);
        Request = std::move(ev);
    }

    void Bootstrap() {
        // create TEvVBlock request
        auto request = std::make_unique<TEvBlobStorage::TEvVBlock>(
            Request->Get()->Record.GetTabletId(),
            Request->Get()->Record.GetForceBlockedGeneration(),
            VDiskIDFromVDiskID(Request->Get()->Record.GetVDiskID()),
            Request->Get()->Record.GetMsgQoS().HasDeadlineSeconds() ? 
                TInstant::Seconds(Request->Get()->Record.GetMsgQoS().GetDeadlineSeconds()) :
                TInstant::Max()
        );

        // send TEvVBlock request
        Send(SkeletonId, request.release());

        Become(&TThis::StateWait);
    }

    STRICT_STFUNC(StateWait,
        hFunc(TEvBlobStorage::TEvVBlockResult, Handle);
        cFunc(TEvents::TSystem::Poison, PassAway);
    )

    void Handle(TEvBlobStorage::TEvVBlockResult::TPtr &ev) {
        switch (ev->Get()->Record.GetStatus()) {
        case NKikimrProto::OK:
            [[fallthrough]];
        case NKikimrProto::ALREADY:
            break;
        default: {
                // we failed to block required generation, so return failure
                auto response = NErrBuilder::ErroneousResult(
                    VCtx,
                    // return BLOCKED so that dsproxy returns right away and doesn't try remaining vdisks
                    NKikimrProto::BLOCKED,
                    "failed to block required generation",
                    Request,
                    TAppData::TimeProvider->Now(),
                    SkeletonFrontIDPtr,
                    SelfVDiskId,
                    VDiskIncarnationGuid,
                    GInfo
                );
                SendVDiskResponse(TActivationContext::AsActorContext(), SenderId, response.release(), Request->Cookie);
                return PassAway();
            }
        }

        // send TEvVGet request, the reply will go directly to sender.
        // there is no need to clear ForceBlockedGeneration parameter
        // since by now the required generation has been blocked.
        TActivationContext::Send(Request.Release());
        PassAway();
    }

private:
    NActors::TActorId SenderId;
    NActors::TActorId SkeletonId;
    TIntrusivePtr<TVDiskContext> VCtx;
    TActorIDPtr SkeletonFrontIDPtr;
    TVDiskID SelfVDiskId;
    TVDiskIncarnationGuid VDiskIncarnationGuid;
    TIntrusivePtr<NKikimr::TBlobStorageGroupInfo> GInfo;
    TEvBlobStorage::TEvVGet::TPtr Request;
};

std::unique_ptr<NActors::IActor> CreateBlockAndGetActor(
    TEvBlobStorage::TEvVGet::TPtr ev,
    NActors::TActorId skeletonId,
    TIntrusivePtr<TVDiskContext> vCtx,
    TActorIDPtr skeletonFrontIDPtr,
    TVDiskID selfVDiskId,
    TVDiskIncarnationGuid vDiskIncarnationGuid,
    TIntrusivePtr<NKikimr::TBlobStorageGroupInfo> gInfo)
{
    return std::make_unique<TBlockAndGetActor>(ev, skeletonId, vCtx, skeletonFrontIDPtr, selfVDiskId, vDiskIncarnationGuid, gInfo);
}

} // NKikimr