#include "blob_depot.h"
#include "blob_depot_tablet.h"
#include "blocks.h"
#include "garbage_collection.h"
#include "data.h"
#include "data_uncertain.h"
#include "space_monitor.h"

namespace NKikimr::NBlobDepot {

    TBlobDepot::TBlobDepot(TActorId tablet, TTabletStorageInfo *info)
        : TActor(&TThis::StateInit)
        , TTabletExecutedFlat(info, tablet, new NMiniKQL::TMiniKQLFactory)
        , BlocksManager(new TBlocksManager(this))
        , BarrierServer(new TBarrierServer(this))
        , Data(new TData(this))
        , SpaceMonitor(new TSpaceMonitor(this))
    {}

    TBlobDepot::~TBlobDepot()
    {}

    void TBlobDepot::HandleFromAgent(STATEFN_SIG) {
        switch (const ui32 type = ev->GetTypeRewrite()) {
            hFunc(TEvBlobDepot::TEvRegisterAgent, Handle);
            hFunc(TEvBlobDepot::TEvAllocateIds, Handle);
            hFunc(TEvBlobDepot::TEvCommitBlobSeq, Handle);
            hFunc(TEvBlobDepot::TEvDiscardSpoiledBlobSeq, Handle);
            hFunc(TEvBlobDepot::TEvResolve, Data->Handle);
            hFunc(TEvBlobDepot::TEvBlock, BlocksManager->Handle);
            hFunc(TEvBlobDepot::TEvQueryBlocks, BlocksManager->Handle);
            hFunc(TEvBlobDepot::TEvCollectGarbage, BarrierServer->Handle);
            hFunc(TEvBlobDepot::TEvPushNotifyResult, Handle);

            default:
                Y_FAIL();
        }
    }

    STFUNC(TBlobDepot::StateWork) {
        try {
            auto handleFromAgentPipe = [this](auto& ev) {
                const auto it = PipeServers.find(ev->Recipient);
                Y_VERIFY(it != PipeServers.end());

                STLOG(PRI_DEBUG, BLOB_DEPOT, BDT69, "HandleFromAgentPipe", (Id, GetLogId()), (RequestId, ev->Cookie),
                    (Postpone, it->second.PostponeFromAgent), (Sender, ev->Sender), (PipeServerId, ev->Recipient));

                if (it->second.PostponeFromAgent) {
                    it->second.PostponeQ.emplace_back(ev.Release());
                    return;
                }

                Y_VERIFY_S(ev->Cookie == it->second.NextExpectedMsgId, "pipe reordering detected Cookie# " << ev->Cookie
                    << " NextExpectedMsgId# " << it->second.NextExpectedMsgId << " Type# " << Sprintf("%08" PRIx32,
                    ev->GetTypeRewrite()) << " Id# " << GetLogId());

                ++it->second.NextExpectedMsgId;
                HandleFromAgent(ev);
            };

            switch (const ui32 type = ev->GetTypeRewrite()) {
                cFunc(TEvents::TSystem::Poison, HandlePoison);

                hFunc(TEvBlobDepot::TEvApplyConfig, Handle);

                fFunc(TEvBlobDepot::EvRegisterAgent, handleFromAgentPipe);
                fFunc(TEvBlobDepot::EvAllocateIds, handleFromAgentPipe);
                fFunc(TEvBlobDepot::EvCommitBlobSeq, handleFromAgentPipe);
                fFunc(TEvBlobDepot::EvDiscardSpoiledBlobSeq, handleFromAgentPipe);
                fFunc(TEvBlobDepot::EvResolve, handleFromAgentPipe);
                fFunc(TEvBlobDepot::EvBlock, handleFromAgentPipe);
                fFunc(TEvBlobDepot::EvQueryBlocks, handleFromAgentPipe);
                fFunc(TEvBlobDepot::EvPushNotifyResult, handleFromAgentPipe);
                fFunc(TEvBlobDepot::EvCollectGarbage, handleFromAgentPipe);

                hFunc(TEvBlobStorage::TEvCollectGarbageResult, Data->Handle);
                hFunc(TEvBlobStorage::TEvRangeResult, Data->Handle);
                hFunc(TEvBlobStorage::TEvGetResult, Data->UncertaintyResolver->Handle);

                hFunc(TEvBlobStorage::TEvStatusResult, SpaceMonitor->Handle);
                cFunc(TEvPrivate::EvKickSpaceMonitor, KickSpaceMonitor);

                hFunc(TEvTabletPipe::TEvServerConnected, Handle);
                hFunc(TEvTabletPipe::TEvServerDisconnected, Handle);

                cFunc(TEvPrivate::EvCommitCertainKeys, Data->HandleCommitCertainKeys);
                cFunc(TEvPrivate::EvDoGroupMetricsExchange, DoGroupMetricsExchange);
                hFunc(TEvBlobStorage::TEvControllerGroupMetricsExchange, Handle);

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
        for (const TActorId& actorId : {GroupAssimilatorId}) {
            if (actorId) {
                TActivationContext::Send(new IEventHandle(TEvents::TSystem::Poison, 0, actorId, SelfId(), nullptr, 0));
            }
        }

        TActor::PassAway();
    }

    IActor *CreateBlobDepot(const TActorId& tablet, TTabletStorageInfo *info) {
        return new TBlobDepot(tablet, info);
    }

} // NKikimr::NBlobDepot
