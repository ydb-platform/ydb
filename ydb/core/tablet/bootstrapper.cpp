#include "bootstrapper.h"

#include <ydb/core/base/tablet.h>
#include <ydb/core/base/statestorage.h>
#include <ydb/core/base/appdata.h>

#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/core/hfunc.h>
#include <library/cpp/random_provider/random_provider.h>

#include <ydb/core/protos/bootstrapper.pb.h>

namespace NKikimr {

struct TEvBootstrapper::TEvWatch : public TEventPB<TEvWatch, NKikimrBootstrapper::TEvWatch, EvWatch> {
    TEvWatch()
    {}

    TEvWatch(ui64 tabletId, ui64 selfSeed, ui64 round)
    {
        Record.SetTabletID(tabletId);
        Record.SetSelfSeed(selfSeed);
        Record.SetRound(round);
    }
};

struct TEvBootstrapper::TEvWatchResult : public TEventPB<TEvWatchResult, NKikimrBootstrapper::TEvWatchResult, EvWatchResult> {
    TEvWatchResult()
    {}

    TEvWatchResult(ui64 tabletId, NKikimrBootstrapper::TEvWatchResult::EState state, ui64 seed, ui64 round)
    {
        Record.SetTabletID(tabletId);
        Record.SetState(state);
        Record.SetSeed(seed);
        Record.SetRound(round);
    }
};

struct TEvBootstrapper::TEvNotify : public TEventPB<TEvNotify, NKikimrBootstrapper::TEvNotify, EvNotify> {
    TEvNotify()
    {}

    TEvNotify(ui64 tabletId, NKikimrBootstrapper::TEvNotify::EOp op, ui64 round)
    {
        Record.SetTabletID(tabletId);
        Record.SetOp(op);
        Record.SetRound(round);
    }
};


class TBootstrapper : public TActor<TBootstrapper> {
    TIntrusivePtr<TTabletStorageInfo> TabletInfo;
    TIntrusivePtr<TBootstrapperInfo> BootstrapperInfo;

    TActorId LookOnActorID;
    TActorId FollowerActorID;

    ui64 RoundCounter;
    ui64 SelfSeed;

    TInstant BootDelayedUntil;

    // we watch someone and do not act w/o shutdown
    struct TWatch {
        struct TWatched {
            TActorId ActorID;
            bool Owner;

            TWatched()
                : Owner(false)
            {}
            TWatched(const TActorId &actorID, bool owner)
                : ActorID(actorID)
                , Owner(owner)
            {}
        };

        bool CheckWatch(ui32 fromNode) {
            bool seenOwner = false;
            for (TWatched &x : Watched) {
                seenOwner |= x.Owner;
                if (x.ActorID.NodeId() == fromNode)
                    return (x.Owner == false);
            }
            return !seenOwner;
        }

        bool RemoveAlienEntry(ui32 idx) {
            Watched[idx] = Watched.back();
            Watched.pop_back();
            if (!AnyOf(Watched, [](const TWatch::TWatched &x) { return x.Owner; }))
                Watched.clear();
            return Watched.empty();
        }

        bool RemoveAlien(const TActorId &alien) {
            for (ui32 i = 0, e = Watched.size(); i != e; ++i) {
                if (Watched[i].ActorID == alien)
                    return RemoveAlienEntry(i);
            }
            return false;
        }

        bool RemoveAlienNode(ui32 node) {
            for (ui32 i = 0, e = Watched.size(); i != e; ++i) {
                if (Watched[i].ActorID.NodeId() == node)
                    return RemoveAlienEntry(i);
            }
            return false;
        }

        TVector<TWatched> Watched;
    };

    // we are under watch, must notify on error
    struct TWatched {
        struct TWatcher {
            TActorId ActorID;
            ui64 Round;

            TWatcher()
                : ActorID()
                , Round()
            {}
            TWatcher(const TActorId &actorID, ui64 round)
                : ActorID(actorID)
                , Round(round)
            {}
        };

        void AddWatcher(const TActorId &actorId, ui64 round) {
            for (TWatcher &x : Watchers) {
                if (actorId.NodeId() == x.ActorID.NodeId()) {
                    x.ActorID = actorId;
                    x.Round = round;
                    return;
                }
            }
            Watchers.push_back(TWatcher(actorId, round));
        }

        TVector<TWatcher> Watchers;
    };

    struct TRound {
        enum class EAlienState {
            Wait,
            Unknown,
            Free,
            Undelivered,
            Disconnected,
        };

        struct TAlien {
            EAlienState State;
            ui64 Seed;

            TAlien()
                : State(EAlienState::Wait)
                , Seed()
            {}
            TAlien(EAlienState state, ui64 seed)
                : State(state)
                , Seed(seed)
            {}
        };

        TVector<TAlien> Aliens;
    };

    TAutoPtr<TWatch> Watches; // we watch them
    TAutoPtr<TWatched> Watched; // we are under watch
    TAutoPtr<TRound> Round;

    const char* GetTabletTypeName() {
        return TTabletTypes::TypeToStr((TTabletTypes::EType)TabletInfo->TabletType);
    }

    const char* GetStateName(NKikimrBootstrapper::TEvWatchResult::EState state) {
        return NKikimrBootstrapper::TEvWatchResult::EState_Name(state).c_str();
    }

    ui32 AlienIndex(ui32 alienNodeId) {
        for (ui32 i = 0, e = BootstrapperInfo->OtherNodes.size(); i != e; ++i)
            if (BootstrapperInfo->OtherNodes[i] == alienNodeId)
                return i;
        return Max<ui32>();
    }

    void BeginNewRound(const TActorContext &ctx) {
        if (BootstrapperInfo->OtherNodes.empty())
            return Boot(ctx);

        SelfSeed = AppData(ctx)->RandomProvider->GenRand64();
        LOG_INFO(ctx, NKikimrServices::BOOTSTRAPPER, "tablet: %" PRIu64 ", type: %s, begin new round, seed: %" PRIu64,
                 TabletInfo->TabletID, GetTabletTypeName(), SelfSeed);

        const ui64 tabletId = TabletInfo->TabletID;
        ++RoundCounter;

        Round.Reset(new TRound());
        Round->Aliens.resize(BootstrapperInfo->OtherNodes.size());

        for (ui32 alienNode : BootstrapperInfo->OtherNodes) {
            ctx.Send(MakeBootstrapperID(tabletId, alienNode), new TEvBootstrapper::TEvWatch(tabletId, SelfSeed, RoundCounter), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession, RoundCounter);
        }

        Become(&TThis::StateFree); // todo: global timeout?
    }

    void Boot(const TActorContext &ctx) {
        Y_ABORT_UNLESS(!LookOnActorID);

        LOG_NOTICE(ctx, NKikimrServices::BOOTSTRAPPER, "tablet: %" PRIu64 ", type: %s, boot",
                   TabletInfo->TabletID, GetTabletTypeName());

        if (FollowerActorID) {
            LookOnActorID = FollowerActorID;
            FollowerActorID = TActorId();
            ctx.Send(LookOnActorID, new TEvTablet::TEvPromoteToLeader(0, TabletInfo));
        } else {
            TTabletSetupInfo *x = BootstrapperInfo->SetupInfo.Get();
            LookOnActorID = x->Tablet(TabletInfo.Get(), ctx.SelfID, ctx, 0, AppData(ctx)->ResourceProfiles);
        }

        Y_ABORT_UNLESS(LookOnActorID);

        Watched.Reset(new TWatched());

        Become(&TThis::StateOwner);
    }

    void Handle(TEvTablet::TEvTabletDead::TPtr &ev, const TActorContext &ctx) {
        if (ev->Sender == LookOnActorID) {
            LOG_INFO(ctx, NKikimrServices::BOOTSTRAPPER, "tablet: %" PRIu64 ", type: %s, tablet dead",
                     TabletInfo->TabletID, GetTabletTypeName());

            LookOnActorID = TActorId();
            NotifyAndRound(ctx);
        }
    }

    void Stop() {
        if (LookOnActorID) {
            Send(LookOnActorID, new TEvents::TEvPoisonPill());
            LookOnActorID = TActorId();
        }

        if (FollowerActorID) {
            Send(FollowerActorID, new TEvents::TEvPoisonPill());
            FollowerActorID = TActorId();
        }

        NotifyWatchers();

        BootDelayedUntil = { };
        Round.Destroy();
    }

    void HandlePoison() {
        Stop();
        PassAway();
    }

    void Standby() {
        Stop();
        Become(&TThis::StateStandBy);
    }

    void BecomeWatch(const TActorId &watchOn, bool owner, const TActorContext &ctx) {
        Y_UNUSED(ctx);

        BootDelayedUntil = { };
        Round.Destroy();

        LOG_INFO(ctx, NKikimrServices::BOOTSTRAPPER, "tablet: %" PRIu64 ", type: %s, become watch",
                 TabletInfo->TabletID, GetTabletTypeName());

        Watches.Reset(new TWatch());
        Watched.Reset(new TWatched());

        LOG_DEBUG(ctx, NKikimrServices::BOOTSTRAPPER, "tablet: %" PRIu64 ", type: %s, add watched node: %" PRIu32,
                  TabletInfo->TabletID, GetTabletTypeName(), watchOn.NodeId());
        Watches->Watched.push_back(TWatch::TWatched(watchOn, owner));

        if (BootstrapperInfo->StartFollowers && !FollowerActorID) {
            LOG_NOTICE(ctx, NKikimrServices::BOOTSTRAPPER, "tablet: %" PRIu64 ", type: %s, boot follower",
                       TabletInfo->TabletID, GetTabletTypeName());
            TTabletSetupInfo *x = BootstrapperInfo->SetupInfo.Get();
            FollowerActorID = x->Follower(TabletInfo.Get(), ctx.SelfID, ctx, 0, AppData(ctx)->ResourceProfiles);
        }

        Become(&TThis::StateWatch);
    }

    bool ApplyAlienState(const TActorId &alien, NKikimrBootstrapper::TEvWatchResult::EState state, ui64 seed, const TActorContext &ctx) {
        const ui32 alienNodeIdx = AlienIndex(alien.NodeId());
        if (alienNodeIdx == Max<ui32>())
            return true;

        if (Round->Aliens[alienNodeIdx].State != TRound::EAlienState::Wait)
            return false;

        LOG_DEBUG(ctx, NKikimrServices::BOOTSTRAPPER, "tablet: %" PRIu64 ", type: %s, apply alien %" PRIu32 " state: %s",
                  TabletInfo->TabletID, GetTabletTypeName(), alien.NodeId(), GetStateName(state));

        switch (state) {
        case NKikimrBootstrapper::TEvWatchResult::UNKNOWN:
            Round->Aliens[alienNodeIdx] = TRound::TAlien(TRound::EAlienState::Unknown, Max<ui64>());
            return false;
        case NKikimrBootstrapper::TEvWatchResult::FREE:
            Round->Aliens[alienNodeIdx] = TRound::TAlien(TRound::EAlienState::Free, seed);
            return false;
        case NKikimrBootstrapper::TEvWatchResult::OWNER:
            BecomeWatch(alien, true, ctx);
            return true;
        case NKikimrBootstrapper::TEvWatchResult::WAITFOR:
            BecomeWatch(alien, false, ctx);
            return true;
        case NKikimrBootstrapper::TEvWatchResult::UNDELIVERED:
            Round->Aliens[alienNodeIdx] = TRound::TAlien(TRound::EAlienState::Undelivered, Max<ui64>());
            return false;
        case NKikimrBootstrapper::TEvWatchResult::DISCONNECTED:
            Round->Aliens[alienNodeIdx] = TRound::TAlien(TRound::EAlienState::Disconnected, Max<ui64>());
            return false;
        default:
            Y_ABORT("unhandled case");
        }
    }

    bool CheckBootPermitted(size_t undelivered, size_t disconnected, const TActorContext &ctx) {
        // Total number of nodes that participate in tablet booting
        size_t total = 1 + BootstrapperInfo->OtherNodes.size();
        Y_DEBUG_ABORT_UNLESS(total >= 1 + undelivered + disconnected);

        // Ignore nodes that don't have bootstrapper running
        total -= undelivered;

        // Number of nodes that need to be online for immediate boot (including us)
        // Clamp it to 2 other nodes on clusters with very large boot node set
        size_t quorum = Min(total / 2 + 1, (size_t) 3);
        Y_DEBUG_ABORT_UNLESS(quorum >= 1);

        // Number of nodes currently online (including us)
        size_t online = total - disconnected;
        Y_DEBUG_ABORT_UNLESS(online >= 1);

        // If there are enough nodes online, just boot immediately
        if (online >= quorum) {
            BootDelayedUntil = { };
            return true;
        }

        auto now = ctx.Now();
        if (!BootDelayedUntil) {
            // Delay boot decision until some later time
            BootDelayedUntil = now + BootstrapperInfo->OfflineDelay;
        } else if (BootDelayedUntil <= now) {
            // We don't have enough online nodes, but try to boot anyway
            BootDelayedUntil = { };
            return true;
        }

        LOG_DEBUG(ctx, NKikimrServices::BOOTSTRAPPER, "tablet: %" PRIu64 ", type: %s, %" PRISZT "/%" PRISZT " nodes online (need %" PRISZT "), wait for threshold",
                    TabletInfo->TabletID, GetTabletTypeName(), online, total, quorum);

        const ui64 wx = BootstrapperInfo->WatchThreshold.MicroSeconds();
        const auto sleepDuration = TDuration::MicroSeconds(wx / 2 + wx * (SelfSeed % 0x10000) / 0x20000);

        ctx.ExecutorThread.ActorSystem->Schedule(
            Min(sleepDuration, BootDelayedUntil - now),
            new IEventHandle(ctx.SelfID, ctx.SelfID, new TEvents::TEvWakeup(), 0, RoundCounter));
        Become(&TThis::StateSleep);
        return false;
    }

    void CheckRoundCompletion(const TActorContext &ctx) {
        ui64 minAlienSeed = Max<ui64>();
        ui32 minAlien = Max<ui32>();
        size_t undelivered = 0;
        size_t disconnected = 0;
        for (ui32 i = 0, e = Round->Aliens.size(); i != e; ++i) {
            const TRound::TAlien &alien = Round->Aliens[i];
            switch (alien.State) {
            case TRound::EAlienState::Wait:
                return;
            case TRound::EAlienState::Unknown:
                break;
            case TRound::EAlienState::Free:
                if (minAlienSeed > alien.Seed) {
                    minAlienSeed = alien.Seed;
                    minAlien = BootstrapperInfo->OtherNodes[i];
                }
                break;
            case TRound::EAlienState::Undelivered:
                ++undelivered;
                break;
            case TRound::EAlienState::Disconnected:
                ++disconnected;
                break;
            }
        }

        Round.Destroy();

        // ok, we got all reactions, now boot tablet or sleep for threshold
        if (minAlienSeed < SelfSeed || minAlienSeed == SelfSeed && ctx.SelfID.NodeId() > minAlien) {
            LOG_DEBUG(ctx, NKikimrServices::BOOTSTRAPPER, "tablet: %" PRIu64 ", type: %s, lost round, wait for threshold",
                      TabletInfo->TabletID, GetTabletTypeName());

            const ui64 wx = BootstrapperInfo->WatchThreshold.MicroSeconds();
            const auto sleepDuration = TDuration::MicroSeconds(wx / 2 + wx * (SelfSeed % 0x10000) / 0x20000);

            Become(&TThis::StateSleep);
            ctx.ExecutorThread.ActorSystem->Schedule(sleepDuration, new IEventHandle(ctx.SelfID, ctx.SelfID, new TEvents::TEvWakeup(), 0, RoundCounter));
            return;
        } else if (!CheckBootPermitted(undelivered, disconnected, ctx)) {
            return;
        } else {
            Boot(ctx);
            return;
        }
    }

    void NotifyWatchers() {
        if (Watched) {
            for (const TWatched::TWatcher &xw : Watched->Watchers)
                Send(xw.ActorID, new TEvBootstrapper::TEvNotify(TabletInfo->TabletID, NKikimrBootstrapper::TEvNotify::DROP, xw.Round));
            Watched.Destroy();
            Watches.Destroy();
        }
    }

    void NotifyAndRound(const TActorContext &ctx) {
        NotifyWatchers();
        BeginNewRound(ctx);
    }

    void HandleFree(TEvBootstrapper::TEvWatchResult::TPtr &ev, const TActorContext &ctx) {
        const NKikimrBootstrapper::TEvWatchResult &record = ev->Get()->Record;
        Y_ABORT_UNLESS(record.GetTabletID() == TabletInfo->TabletID);

        if (record.GetRound() != RoundCounter)
            return;

        if (ApplyAlienState(ev->Sender, record.GetState(), record.GetSeed(), ctx))
            return;

        CheckRoundCompletion(ctx);
    }

    void HandleFree(TEvents::TEvUndelivered::TPtr &ev, const TActorContext &ctx) {
        const ui64 round = ev->Cookie;
        LOG_DEBUG(ctx, NKikimrServices::BOOTSTRAPPER, "tablet: %" PRIu64 ", type: %s, undelivered from %s, round %" PRIu64,
                    TabletInfo->TabletID, GetTabletTypeName(), ev->Sender.ToString().c_str(), round);

        if (round != RoundCounter)
            return;

        if (ApplyAlienState(ev->Sender, NKikimrBootstrapper::TEvWatchResult::UNDELIVERED, Max<ui64>(), ctx))
            return;

        CheckRoundCompletion(ctx);
    }

    void HandleFree(TEvInterconnect::TEvNodeDisconnected::TPtr &ev, const TActorContext &ctx) {
        const ui32 node = ev->Get()->NodeId;
        LOG_DEBUG(ctx, NKikimrServices::BOOTSTRAPPER, "tablet: %" PRIu64 ", type: %s, disconnected from %" PRIu32,
                    TabletInfo->TabletID, GetTabletTypeName(), node);

        if (ApplyAlienState(TActorId(node, 0, 0, 0), NKikimrBootstrapper::TEvWatchResult::DISCONNECTED, Max<ui64>(), ctx))
            return;

        CheckRoundCompletion(ctx);
    }

    void HandleFree(TEvBootstrapper::TEvWatch::TPtr &ev, const TActorContext &ctx) {
        const NKikimrBootstrapper::TEvWatch &record = ev->Get()->Record;
        Y_ABORT_UNLESS(record.GetTabletID() == TabletInfo->TabletID);

        ctx.Send(ev->Sender, new TEvBootstrapper::TEvWatchResult(record.GetTabletID(), NKikimrBootstrapper::TEvWatchResult::FREE, SelfSeed, record.GetRound()));
    }

    void HandleWatch(TEvBootstrapper::TEvNotify::TPtr &ev, const TActorContext &ctx) {
        const TActorId alien = ev->Sender;
        if (Watches->RemoveAlien(alien)) {
            NotifyAndRound(ctx);
        }
    }

    void HandleWatch(TEvInterconnect::TEvNodeDisconnected::TPtr &ev, const TActorContext &ctx) {
        const ui32 node = ev->Get()->NodeId;
        LOG_DEBUG(ctx, NKikimrServices::BOOTSTRAPPER, "tablet: %" PRIu64 ", type: %s, disconnected from %" PRIu32,
                    TabletInfo->TabletID, GetTabletTypeName(), node);

        if (Watches->RemoveAlienNode(node)) {
            NotifyAndRound(ctx);
        }
    }

    void HandleOwner(TEvBootstrapper::TEvWatch::TPtr &ev, const TActorContext &ctx) {
        const NKikimrBootstrapper::TEvWatch &record = ev->Get()->Record;
        Y_ABORT_UNLESS(record.GetTabletID() == TabletInfo->TabletID);

        // add to watchers list (if node not already there)
        Watched->AddWatcher(ev->Sender, record.GetRound());
        ctx.Send(ev->Sender, new TEvBootstrapper::TEvWatchResult(record.GetTabletID(), NKikimrBootstrapper::TEvWatchResult::OWNER, 0, record.GetRound()));
    }

    void HandleWatch(TEvBootstrapper::TEvWatch::TPtr &ev, const TActorContext &ctx) {
        const NKikimrBootstrapper::TEvWatch &record = ev->Get()->Record;
        Y_ABORT_UNLESS(record.GetTabletID() == TabletInfo->TabletID);

        if (Watches->CheckWatch(ev->Sender.NodeId())) {
            ctx.Send(ev->Sender, new TEvBootstrapper::TEvWatchResult(record.GetTabletID(), NKikimrBootstrapper::TEvWatchResult::UNKNOWN, 0, record.GetRound()));
        } else {
            Watched->AddWatcher(ev->Sender, record.GetRound());
            ctx.Send(ev->Sender, new TEvBootstrapper::TEvWatchResult(record.GetTabletID(), NKikimrBootstrapper::TEvWatchResult::WAITFOR, 0, record.GetRound()));
        }
    }

    void HandleSleep(TEvBootstrapper::TEvWatch::TPtr &ev, const TActorContext &ctx) {
        HandleFree(ev, ctx);
    }

    void HandleSleep(TEvents::TEvWakeup::TPtr &ev, const TActorContext &ctx) {
        const ui64 roundCookie = ev->Cookie;
        if (roundCookie != RoundCounter)
            return;

        BeginNewRound(ctx);
    }

    void HandleStandBy(TEvBootstrapper::TEvWatch::TPtr &ev, const TActorContext &ctx) {
        const NKikimrBootstrapper::TEvWatch &record = ev->Get()->Record;
        Y_ABORT_UNLESS(record.GetTabletID() == TabletInfo->TabletID);

        ctx.Send(ev->Sender, new TEvBootstrapper::TEvWatchResult(record.GetTabletID(), NKikimrBootstrapper::TEvWatchResult::UNKNOWN, Max<ui64>(), record.GetRound()));
    }

    void HandlePoisonStandBy() {
        PassAway();
    }

    void PassAway() override {
        for (ui32 nodeId : BootstrapperInfo->OtherNodes) {
            Send(TActivationContext::InterconnectProxy(nodeId), new TEvents::TEvUnsubscribe);
        }
        NotifyWatchers();
        TActor::PassAway();
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::TABLET_BOOTSTRAPPER;
    }

    TBootstrapper(TTabletStorageInfo *tabletInfo, TBootstrapperInfo *bootstrapperInfo, bool standby)
        : TActor(standby ? &TThis::StateStandBy : &TThis::StateBoot)
        , TabletInfo(tabletInfo)
        , BootstrapperInfo(bootstrapperInfo)
        , RoundCounter(0xdeadbeefdeadbeefull)
        , SelfSeed(0xdeadbeefdeadbeefull)
    {
        Y_ABORT_UNLESS(TTabletTypes::TypeInvalid != TabletInfo->TabletType);
    }

    TAutoPtr<IEventHandle> AfterRegister(const TActorId &selfId, const TActorId &parentId) override {
        Y_UNUSED(parentId);
        return new IEventHandle(selfId, selfId, new TEvents::TEvBootstrap());
    }

    STFUNC(StateBoot) {
        switch (ev->GetTypeRewrite()) {
            CFunc(TEvents::TSystem::Bootstrap, BeginNewRound);
        }
    }

    STFUNC(StateFree) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvBootstrapper::TEvWatchResult, HandleFree); // => noop|sleep|owner|watch
            HFunc(TEvBootstrapper::TEvWatch, HandleFree); // => reply
            HFunc(TEvents::TEvUndelivered, HandleFree); // => watchresult with unknown
            cFunc(TEvents::TSystem::PoisonPill, HandlePoison); // => die
            HFunc(TEvInterconnect::TEvNodeDisconnected, HandleFree); // => watchresult with unknown
            cFunc(TEvBootstrapper::EvStandBy, Standby);
        }
    }

    STFUNC(StateSleep) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvBootstrapper::TEvWatch, HandleSleep);
            HFunc(TEvents::TEvWakeup, HandleSleep);
            cFunc(TEvents::TSystem::PoisonPill, HandlePoison);
            cFunc(TEvBootstrapper::EvStandBy, Standby);
        }
    }

    STFUNC(StateWatch) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvBootstrapper::TEvWatch, HandleWatch);
            HFunc(TEvBootstrapper::TEvNotify, HandleWatch);
            HFunc(TEvInterconnect::TEvNodeDisconnected, HandleWatch);
            cFunc(TEvents::TSystem::PoisonPill, HandlePoison);
            cFunc(TEvBootstrapper::EvStandBy, Standby);
        }
    }

    STFUNC(StateOwner) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvBootstrapper::TEvWatch, HandleOwner);
            HFunc(TEvTablet::TEvTabletDead, Handle);
            cFunc(TEvents::TSystem::PoisonPill, HandlePoison);
            cFunc(TEvBootstrapper::EvStandBy, Standby);
        }
    }

    STFUNC(StateStandBy) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvBootstrapper::TEvWatch, HandleStandBy);
            CFunc(TEvBootstrapper::EvActivate, BeginNewRound);
            cFunc(TEvents::TSystem::PoisonPill, HandlePoisonStandBy);
        }
    }
};

IActor* CreateBootstrapper(TTabletStorageInfo *tabletInfo, TBootstrapperInfo *bootstrapperInfo, bool standby) {
    return new TBootstrapper(tabletInfo, bootstrapperInfo, standby);
}

TActorId MakeBootstrapperID(ui64 tablet, ui32 node) {
    char x[12] ={'b', 'o', 'o', 't'};
    memcpy(x + 4, &tablet, sizeof(ui64));
    return TActorId(node, TStringBuf(x, x + 12));
}

}
