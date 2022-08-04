#include "blob_depot.h"
#include "blob_depot_tablet.h"
#include "blocks.h"
#include "garbage_collection.h"
#include "data.h"

namespace NKikimr::NBlobDepot {

    TBlobDepot::TBlobDepot(TActorId tablet, TTabletStorageInfo *info)
        : TActor(&TThis::StateInit)
        , TTabletExecutedFlat(info, tablet, new NMiniKQL::TMiniKQLFactory)
        , BlocksManager(new TBlocksManager(this))
        , BarrierServer(new TBarrierServer(this))
        , Data(new TData(this))
    {}

    TBlobDepot::~TBlobDepot()
    {}

    STFUNC(TBlobDepot::StateWork) {
        try {
            switch (const ui32 type = ev->GetTypeRewrite()) {
                cFunc(TEvents::TSystem::Poison, HandlePoison);

                hFunc(TEvBlobDepot::TEvApplyConfig, Handle);
                hFunc(TEvBlobDepot::TEvRegisterAgent, Handle);
                hFunc(TEvBlobDepot::TEvAllocateIds, Handle);
                hFunc(TEvBlobDepot::TEvCommitBlobSeq, Handle);
                hFunc(TEvBlobDepot::TEvResolve, Data->Handle);

                hFunc(TEvBlobDepot::TEvBlock, BlocksManager->Handle);
                hFunc(TEvBlobDepot::TEvQueryBlocks, BlocksManager->Handle);

                hFunc(TEvBlobDepot::TEvCollectGarbage, BarrierServer->Handle);

                hFunc(TEvBlobStorage::TEvCollectGarbageResult, Data->Handle);

                hFunc(TEvBlobDepot::TEvPushNotifyResult, Handle);

                hFunc(TEvTabletPipe::TEvServerConnected, Handle);
                hFunc(TEvTabletPipe::TEvServerDisconnected, Handle);

                fFunc(TEvents::TSystem::Gone, HandleGone);
                hFunc(TEvAssimilatedData, Handle);

                default:
                    if (!HandleDefaultEvents(ev, ctx)) {
                        Y_FAIL("unexpected event Type# 0x%08" PRIx32, type);
                    }
                    break;
            }
        } catch (...) {
            Y_FAIL_S("unexpected exception# " << CurrentExceptionMessage());
        }
    }

    void TBlobDepot::PassAway() {
        if (RunningGroupAssimilator) {
            TActivationContext::Send(new IEventHandle(TEvents::TSystem::Poison, 0, RunningGroupAssimilator, SelfId(),
                nullptr, 0));
        }

        TActor::PassAway();
    }

    IActor *CreateBlobDepot(const TActorId& tablet, TTabletStorageInfo *info) {
        return new TBlobDepot(tablet, info);
    }

} // NKikimr::NBlobDepot
