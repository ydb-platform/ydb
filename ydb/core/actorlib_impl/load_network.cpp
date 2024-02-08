#include "load_network.h"

#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>

namespace {

NActors::TActorId GetLoadNetworkActorID(ui32 myNodeId) noexcept {
    return NActors::TActorId(myNodeId, "loadnetwork");
}

using NActors::IEventHandle;
using NActors::TActorContext;
using NActors::TEvents;

class TLoadNetwork:  public NActors::TActorBootstrapped<TLoadNetwork> {
public:
    static constexpr NActors::IActor::EActivityType ActorActivityType() {
        return NActors::IActor::EActivityType::TEST_ACTOR_RUNTIME;
    }

    TLoadNetwork(ui32 selfNodeId, ui32 totalNodesCount)
        : SelfNodeId(selfNodeId)
        , TotalNodesCount(totalNodesCount)
    {

    }

    void Bootstrap(const TActorContext &ctx) {
        Become(&TThis::DelayMe);
        ctx.Schedule(TDuration::Seconds(30), new NActors::TEvents::TEvWakeup);
    }

private:
    ui32 SelfNodeId;
    ui32 TotalNodesCount;

    static const TString DataToSend;

    static constexpr ui32 PER_NODE_INFLIGHT_COUNT = 20240;

    STFUNC(DelayMe) {
        Y_UNUSED(ev);

        Become(&TThis::Working);

        for (ui32 i = 1; i <= TotalNodesCount; ++i) {
            if (i == SelfNodeId)
                continue;
            for (ui32 j = 0; j < PER_NODE_INFLIGHT_COUNT; j++) {
                Send(GetLoadNetworkActorID(i),
                         new TEvents::TEvBlob(DataToSend),
                         IEventHandle::MakeFlags(2,
                             IEventHandle::FlagTrackDelivery));
            }
        }
    }

    STFUNC(Working) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvents::TEvBlob, SendOneMoreBlob);
            HFunc(TEvents::TEvUndelivered, SendOneMoreBlob);
        }
    }

    void SendOneMoreBlob(TEvents::TEvBlob::TPtr &ev,
                         const TActorContext &ctx) noexcept
    {
        ctx.Send(ev->Sender,
                 new TEvents::TEvBlob(DataToSend),
                 IEventHandle::MakeFlags(2, 0));
    }

    void SendOneMoreBlob(TEvents::TEvUndelivered::TPtr &ev,
                         const TActorContext &ctx) noexcept
    {
        ctx.Send(ev->Sender,
                 new TEvents::TEvBlob(DataToSend),
                 IEventHandle::MakeFlags(2, 0));
    }
};

const TString TLoadNetwork::DataToSend = TString(3*1024, '!');

}

namespace IC_Load {
    void InitializeService(NActors::TActorSystemSetup* setup,
                           const NKikimr::TAppData* appData,
                           ui32 totalNodesCount)
    {
        auto actor = std::make_unique<TLoadNetwork>(setup->NodeId, totalNodesCount);
        setup->LocalServices.emplace_back(
            GetLoadNetworkActorID(setup->NodeId),
            NActors::TActorSetupCmd(std::move(actor),
                                    NActors::TMailboxType::Simple,
                                    appData->UserPoolId));
    }
}
