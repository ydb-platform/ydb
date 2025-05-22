#include "tablet_impl.h"
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <util/generic/hash_set.h>

namespace NKikimr {

constexpr ui32 MAX_ATTEMPTS = 10;

class TTabletReqDelete : public TActorBootstrapped<TTabletReqDelete> {
    struct TRequestInfo {
        ui32 GroupId;
        ui32 Channel;

        TRequestInfo(ui32 groupId, ui32 channel)
            : GroupId(groupId)
            , Channel(channel)
        {}
    };

    const TActorId Owner;
    ui64 TabletId;
    TVector<TRequestInfo> Requests;
    ui32 FinishedRequests;
    ui32 ErrorCount;
    ui32 Generation;

    void ReplyAndDie(NKikimrProto::EReplyStatus status, const TActorContext &ctx) {
        if (status == NKikimrProto::OK) {
            const TActorId tabletStateServiceId = NNodeWhiteboard::MakeNodeWhiteboardServiceId(ctx.SelfID.NodeId());
            ctx.Send(tabletStateServiceId, new NNodeWhiteboard::TEvWhiteboard::TEvTabletStateUpdate(
                         TabletId,
                         0,
                         NKikimrWhiteboard::TTabletStateInfo::Deleted,
                         std::numeric_limits<ui32>::max()),
                         true);
            // TODO(xenoxeno): broadcast message to more/all nodes ... maybe?
        }
        ctx.Send(Owner, new TEvTabletBase::TEvDeleteTabletResult(status, TabletId));
        Die(ctx);
    }

    void GenerateRequests(const TIntrusivePtr<TTabletStorageInfo>& tabletStorageInfo) {
        THashSet<std::pair<ui32, ui32>> groupChannels;
        for (const TTabletChannelInfo& channelInfo : tabletStorageInfo->Channels) {
            for (const TTabletChannelInfo::THistoryEntry& historyInfo : channelInfo.History) {
                if (historyInfo.FromGeneration <= Generation) {
                    if (groupChannels.emplace(historyInfo.GroupID, channelInfo.Channel).second) {
                        Requests.emplace_back(historyInfo.GroupID, channelInfo.Channel);
                    }
                }
            }
        }
    }

    void SendRequest(int numRequest, const TActorContext& ctx) {
        const TRequestInfo& info(Requests[numRequest]);
        bool total = Generation == std::numeric_limits<ui32>::max();
        const ui32 recordGeneration = total ? Generation : Generation + 1;
        const ui32 perGenerationCounter = total ? Max<ui32>() : 0;
        auto event = TEvBlobStorage::TEvCollectGarbage::CreateHardBarrier(
                    TabletId,                         // tabletId
                    recordGeneration,                 // recordGeneration
                    perGenerationCounter,             // perGenerationCounter
                    info.Channel,                     // channel
                    Generation,                       // collectGeneration
                    std::numeric_limits<ui32>::max(), // collectStep
                    TInstant::Max());                 // deadline
        event->IsMonitored = false;
        SendToBSProxy(ctx, info.GroupId, event.Release(), numRequest);
    }

    void Handle(TEvents::TEvUndelivered::TPtr&, const TActorContext &ctx) {
        return ReplyAndDie(NKikimrProto::ERROR, ctx);
    }

    void Handle(TEvBlobStorage::TEvCollectGarbageResult::TPtr& ev, const TActorContext& ctx) {
        const TEvBlobStorage::TEvCollectGarbageResult* msg = ev->Get();
        switch (msg->Status) {
        case NKikimrProto::OK:
        case NKikimrProto::ALREADY:
        case NKikimrProto::NO_GROUP:
            ++FinishedRequests;
            if (FinishedRequests >= Requests.size()) {
                if (Generation == std::numeric_limits<ui32>::max()) {
                    const TActorId proxyActorID = MakeStateStorageProxyID();
                    ctx.Send(proxyActorID, new TEvStateStorage::TEvDelete(TabletId));
                }

                ReplyAndDie(NKikimrProto::OK, ctx);
            }
            break;
        default:
            ++ErrorCount;
            if (ErrorCount >= Requests.size() * MAX_ATTEMPTS) {
                return ReplyAndDie(NKikimrProto::ERROR, ctx);
            }
            SendRequest(ev->Cookie, ctx);
            break;
        }
    }

    void Handle(TEvStateStorage::TEvDeleteResult::TPtr& ev, const TActorContext& ctx) {
        ReplyAndDie(ev->Get()->Status, ctx);
    }

    STFUNC(StateWait) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvBlobStorage::TEvCollectGarbageResult, Handle);
            HFunc(TEvStateStorage::TEvDeleteResult, Handle);
            HFunc(TEvents::TEvUndelivered, Handle);
        }
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::TABLET_REQ_DELETE_TABLET;
    }

    TTabletReqDelete(const TActorId &owner, const TIntrusivePtr<TTabletStorageInfo>& tabletStorageInfo, ui32 generation = std::numeric_limits<ui32>::max())
        : Owner(owner)
        , TabletId(tabletStorageInfo->TabletID)
        , FinishedRequests(0)
        , ErrorCount(0)
        , Generation(generation)
    {
        GenerateRequests(tabletStorageInfo);
    }

    void Bootstrap(const TActorContext& ctx) {
        for (std::size_t i = 0; i < Requests.size(); ++i) {
            SendRequest(i, ctx);
        }
        Become(&TTabletReqDelete::StateWait);
    }
};

IActor* CreateTabletReqDelete(const TActorId &owner, const TIntrusivePtr<TTabletStorageInfo>& tabletStorageInfo, ui32 generation) {
    return new TTabletReqDelete(owner, tabletStorageInfo, generation);
}

}
