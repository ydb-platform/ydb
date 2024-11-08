#include "bootstrapper.h"
#include "bootstrapper_impl.h"

#include <ydb/core/base/tablet.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/base/statestorage.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/core/hfunc.h>
#include <library/cpp/random_provider/random_provider.h>

#include <ydb/core/protos/bootstrapper.pb.h>

namespace NKikimr {

class TBootstrapper : public TActorBootstrapped<TBootstrapper> {
    const TIntrusivePtr<TTabletStorageInfo> TabletInfo;
    const TIntrusivePtr<TBootstrapperInfo> BootstrapperInfo;
    bool ModeStandby;
    TVector<ui32> OtherNodes;
    THashMap<ui32, size_t> OtherNodesIndex;

    TActorId KnownLeaderPipe;

    TActorId LookOnActorID;
    TActorId FollowerActorID;

    ui64 RoundCounter = 0xdeadbeefdeadbeefull;
    ui64 SelfSeed = 0xdeadbeefdeadbeefull;

    TInstant BootDelayedUntil;

private:
    /**
     * A remote watcher waiting for our notification
     */
    struct TWatcher {
        TActorId ActorId{};
        ui64 Round{};
    };

private:
    /**
     * Present when this bootstrapper initiates a voting round
     * We wait for the state of other nodes, when all other nodes are either
     * free or unavailable the node with the minimum seed becomes owner and
     * boots the tablet.
     */
    struct TRound {
        enum class EAlienState {
            Wait,
            Unknown,
            Free,
            Undelivered,
            Disconnected,
        };

        struct TAlien {
            EAlienState State = EAlienState::Wait;
            ui64 Seed = Max<ui64>();
        };

        TVector<TAlien> Aliens;
        TVector<TWatcher> Watchers;
        size_t Waiting;

        explicit TRound(size_t count)
            : Aliens(count)
            , Watchers(count)
            , Waiting(count)
        {}
    };

    std::optional<TRound> Round;

private:
    /**
     * Present when we watch some other node
     * To avoid cycles we only ever watch a single node per round
     */
    struct TWatching {
        TActorId ActorId;
        bool Owner;

        TWatching(const TActorId& actorId, bool owner)
            : ActorId(actorId)
            , Owner(owner)
        {}

        /**
         * Called when TEvWatch is received from another node while watching
         * Returns true when we should move to a new cycle
         */
        bool OnWatch(ui32 node) {
            return ActorId.NodeId() == node;
        }

        /**
         * Called when TEvNotify indicates that a node is no longer owner/waiting
         * Returns true when we should move to a new cycle
         */
        bool OnNotify(const TActorId& actorId) {
            return ActorId == actorId;
        }

        /**
         * Called when a node has disconnected
         * Returns true when we should move to a new cycle
         */
        bool OnDisconnect(ui32 node) {
            return ActorId.NodeId() == node;
        }
    };

    std::optional<TWatching> Watching;

private:
    /**
     * A set of watchers waiting for our notification
     */
    struct TWatchedBy : public THashMap<ui32, TWatcher> {
        void Add(const TActorId& actorId, ui64 round) {
            (*this)[actorId.NodeId()] = TWatcher{ actorId, round };
        }
    };

    std::optional<TWatchedBy> WatchedBy; // we are watched by others

private:
    const char* GetTabletTypeName() {
        return TTabletTypes::TypeToStr((TTabletTypes::EType)TabletInfo->TabletType);
    }

    const char* GetStateName(NKikimrBootstrapper::TEvWatchResult::EState state) {
        return NKikimrBootstrapper::TEvWatchResult::EState_Name(state).c_str();
    }

    void BuildOtherNodes() {
        ui32 selfNodeId = SelfId().NodeId();
        for (ui32 nodeId : BootstrapperInfo->Nodes) {
            if (nodeId != selfNodeId && !OtherNodesIndex.contains(nodeId)) {
                size_t index = OtherNodes.size();
                OtherNodes.push_back(nodeId);
                OtherNodesIndex[nodeId] = index;
            }
        }
    }

    size_t AlienIndex(ui32 alienNodeId) {
        auto it = OtherNodesIndex.find(alienNodeId);
        if (it != OtherNodesIndex.end()) {
            return it->second;
        }
        return Max<size_t>();
    }

    TDuration GetSleepDuration() {
        TDuration wx = BootstrapperInfo->WatchThreshold;
        // Note: we use RandomProvider for repeatability between test runs
        ui64 seed = AppData()->RandomProvider->GenRand64();
        float k = float(seed % 0x10000) / 0x20000;
        return wx * (0.5f + k);
    }

    ui64 GenerateSeed() {
        ui64 seed = AppData()->RandomProvider->GenRand64();
        if (Y_UNLIKELY(seed == 0)) {
            seed = 1; // avoid value zero (used by forced winners)
        } else if (Y_UNLIKELY(seed == Max<ui64>())) {
            seed = Max<ui64>() - 1; // avoid max value (used by non-participants)
        }
        return seed;
    }

private:
    /**
     * Starts a new cycle:
     * - lookup tablet in state storage (sleep when unavailable)
     * - begin voting round when tablet has no leader address
     * - try connecting to leader address
     * - begin voting round when connect fails
     * - wait until pipe disconnects
     * - notify watchers and start new cycle
     */
    void BeginNewCycle() {
        ++RoundCounter;

        LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::BOOTSTRAPPER,
            "tablet: " << TabletInfo->TabletID << ", type: " << GetTabletTypeName()
            << ", begin new cycle (lookup in state storage)");

        // We shouldn't start a new cycle with a connected leader pipe
        Y_ABORT_UNLESS(!KnownLeaderPipe);

        Send(MakeStateStorageProxyID(), new TEvStateStorage::TEvLookup(TabletInfo->TabletID, 0), IEventHandle::FlagTrackDelivery);
        Become(&TThis::StateLookup);
    }

    void HandleUnknown(TEvBootstrapper::TEvWatch::TPtr& ev) {
        const NKikimrBootstrapper::TEvWatch& record = ev->Get()->Record;
        Y_ABORT_UNLESS(record.GetTabletID() == TabletInfo->TabletID);

        Send(ev->Sender, new TEvBootstrapper::TEvWatchResult(record.GetTabletID(),
            NKikimrBootstrapper::TEvWatchResult::UNKNOWN, Max<ui64>(), record.GetRound()));
    }

    void HandleLookup(TEvStateStorage::TEvInfo::TPtr& ev) {
        auto* msg = ev->Get();

        LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::BOOTSTRAPPER,
            "tablet: " << TabletInfo->TabletID << ", type: " << GetTabletTypeName()
            << ", lookup: " << msg->Status << ", leader: " << msg->CurrentLeader);

        switch (msg->Status) {
            case NKikimrProto::OK: {
                // We have state storage quorum and some known leader
                KnownLeaderPipe = RegisterWithSameMailbox(
                    NTabletPipe::CreateClient(SelfId(), TabletInfo->TabletID, {
                        .RetryPolicy = NTabletPipe::TClientRetryPolicy::WithoutRetries(),
                        .HintTablet = msg->CurrentLeader,
                    }));
                Become(&TThis::StateConnectLeader);
                return;
            }
            case NKikimrProto::RACE: {
                // State storage is working, but data is inconsistent
                [[fallthrough]];
            }
            case NKikimrProto::NODATA: {
                // We have state storage quorum and no known leader
                BeginNewRound();
                return;
            }
            default: {
                // We have unavailable storage storage, sleep and retry
                auto sleepDuration = GetSleepDuration();
                LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::BOOTSTRAPPER,
                    "tablet: " << TabletInfo->TabletID << ", type: " << GetTabletTypeName()
                    << ", state storage unavailable, sleeping for " << sleepDuration);
                Schedule(sleepDuration, new TEvents::TEvWakeup(RoundCounter));
                return;
            }
        }
    }

    void HandleLookup(TEvents::TEvWakeup::TPtr& ev) {
        if (ev->Get()->Tag != RoundCounter) {
            return;
        }

        BeginNewCycle();
    }

    void HandleConnectLeader(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        if (ev->Sender != KnownLeaderPipe) {
            return;
        }

        auto* msg = ev->Get();

        LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::BOOTSTRAPPER,
            "tablet: " << TabletInfo->TabletID << ", type: " << GetTabletTypeName()
            << ", connect: " << msg->Status);

        if (msg->Status != NKikimrProto::OK) {
            // Current leader unavailable, begin new round
            KnownLeaderPipe = {};
            BeginNewRound();
            return;
        }

        LOG_INFO_S(*TlsActivationContext, NKikimrServices::BOOTSTRAPPER,
            "tablet: " << TabletInfo->TabletID << ", type: " << GetTabletTypeName()
            << ", connected to leader, waiting");

        // We have connected to leader, wait until it disconnects
        WatchedBy.emplace();
        BootDelayedUntil = {};
        Become(&TThis::StateWatchLeader);

        BootFollower();
    }

    void HandleWatchLeader(TEvBootstrapper::TEvWatch::TPtr& ev) {
        const NKikimrBootstrapper::TEvWatch& record = ev->Get()->Record;
        Y_ABORT_UNLESS(record.GetTabletID() == TabletInfo->TabletID);

        WatchedBy.value().Add(ev->Sender, record.GetRound());
        Send(ev->Sender, new TEvBootstrapper::TEvWatchResult(record.GetTabletID(),
            NKikimrBootstrapper::TEvWatchResult::WAITFOR, 0, record.GetRound()));
    }

    void HandleWatchLeader(TEvTabletPipe::TEvClientDestroyed::TPtr& ev) {
        if (ev->Sender != KnownLeaderPipe) {
            return;
        }

        LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::BOOTSTRAPPER,
            "tablet: " << TabletInfo->TabletID << ", type: " << GetTabletTypeName()
            << ", disconnected");

        KnownLeaderPipe = {};
        NotifyWatchers();
        BeginNewCycle();
    }

private:
    /**
     * Begins new voting round between bootstrapper nodes
     * - Starts watching all other nodes
     * - Based on the cluster state may become owner, watcher or sleeper
     * - Owner boots a new leader tablet
     * - Watcher waits for notification from some other node
     * - Sleeper sleeps before starting a new cycle
     */
    void BeginNewRound() {
        // Note: make sure notifications from previous states don't interfere
        ++RoundCounter;

        if (OtherNodes.empty()) {
            return Boot();
        }

        SelfSeed = GenerateSeed();
        LOG_INFO_S(*TlsActivationContext, NKikimrServices::BOOTSTRAPPER,
            "tablet:" << TabletInfo->TabletID << ", type: " << GetTabletTypeName()
            << ", begin new round, seed: " << SelfSeed);

        const ui64 tabletId = TabletInfo->TabletID;

        Round.emplace(OtherNodes.size());
        for (ui32 alienNode : OtherNodes) {
            Send(MakeBootstrapperID(tabletId, alienNode),
                new TEvBootstrapper::TEvWatch(tabletId, SelfSeed, RoundCounter),
                IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession,
                RoundCounter);
        }

        Become(&TThis::StateFree);
    }

    void HandleFree(TEvBootstrapper::TEvWatch::TPtr& ev) {
        const NKikimrBootstrapper::TEvWatch& record = ev->Get()->Record;
        Y_ABORT_UNLESS(record.GetTabletID() == TabletInfo->TabletID);

        Send(ev->Sender, new TEvBootstrapper::TEvWatchResult(record.GetTabletID(),
            NKikimrBootstrapper::TEvWatchResult::FREE, SelfSeed, record.GetRound()));

        const size_t alienNodeIdx = AlienIndex(ev->Sender.NodeId());
        if (alienNodeIdx == Max<size_t>())
            return;

        auto& watcher = Round.value().Watchers.at(alienNodeIdx);
        watcher.ActorId = ev->Sender;
        watcher.Round = record.GetRound();

        // We may have previously observed some state (e.g. UNKNOWN or DISCONNECTED)
        // Since we have received a new TEvWatch afterwards, it implies that node
        // has started a new voting round. Make sure we reflect that in our
        // current state.
        if (ApplyAlienState(ev->Sender, NKikimrBootstrapper::TEvWatchResult::FREE, record.GetSelfSeed(), /* updateOnly */ true))
            return;

        CheckRoundCompletion();
    }

    void HandleFree(TEvBootstrapper::TEvWatchResult::TPtr& ev) {
        const NKikimrBootstrapper::TEvWatchResult& record = ev->Get()->Record;
        Y_ABORT_UNLESS(record.GetTabletID() == TabletInfo->TabletID);

        if (record.GetRound() != RoundCounter)
            return;

        if (ApplyAlienState(ev->Sender, record.GetState(), record.GetSeed()))
            return;

        CheckRoundCompletion();
    }

    void HandleFree(TEvents::TEvUndelivered::TPtr& ev) {
        const ui64 round = ev->Cookie;
        LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::BOOTSTRAPPER,
            "tablet: " << TabletInfo->TabletID << ", type: " << GetTabletTypeName()
            << ", undelivered from " << ev->Sender << ", round " << round);

        if (round != RoundCounter)
            return;

        if (ApplyAlienState(ev->Sender, NKikimrBootstrapper::TEvWatchResult::UNDELIVERED, Max<ui64>()))
            return;

        CheckRoundCompletion();
    }

    void HandleFree(TEvInterconnect::TEvNodeDisconnected::TPtr& ev) {
        const ui32 node = ev->Get()->NodeId;
        const ui64 round = ev->Cookie;
        LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::BOOTSTRAPPER,
            "tablet: " << TabletInfo->TabletID << ", type: " << GetTabletTypeName()
            << ", disconnected from " << node << ", round " << round);

        if (round != RoundCounter)
            return;

        if (ApplyAlienState(TActorId(node, 0, 0, 0), NKikimrBootstrapper::TEvWatchResult::DISCONNECTED, Max<ui64>()))
            return;

        CheckRoundCompletion();
    }

    bool ApplyAlienState(const TActorId& alien, NKikimrBootstrapper::TEvWatchResult::EState state, ui64 seed, bool updateOnly = false) {
        const size_t alienNodeIdx = AlienIndex(alien.NodeId());
        if (alienNodeIdx == Max<size_t>())
            return true;

        // Note: a single alien may be updated multiple times
        auto& alienEntry = Round.value().Aliens.at(alienNodeIdx);
        if (updateOnly && alienEntry.State == TRound::EAlienState::Wait) {
            // This update should only be applied after the initial result
            return true;
        }

        LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::BOOTSTRAPPER,
            "tablet: " << TabletInfo->TabletID << ", type: " << GetTabletTypeName()
            << ", apply alien " << alien.NodeId() << " state: " << GetStateName(state));

        if (alienEntry.State == TRound::EAlienState::Wait) {
            Y_ABORT_UNLESS(Round->Waiting-- > 0);
        }

        alienEntry.Seed = seed;

        switch (state) {
            case NKikimrBootstrapper::TEvWatchResult::UNKNOWN:
                alienEntry.State = TRound::EAlienState::Unknown;
                return false;
            case NKikimrBootstrapper::TEvWatchResult::FREE:
                alienEntry.State = TRound::EAlienState::Free;
                return false;
            case NKikimrBootstrapper::TEvWatchResult::OWNER:
                BecomeWatch(alien, true);
                return true;
            case NKikimrBootstrapper::TEvWatchResult::WAITFOR:
                BecomeWatch(alien, false);
                return true;
            case NKikimrBootstrapper::TEvWatchResult::UNDELIVERED:
                alienEntry.State = TRound::EAlienState::Undelivered;
                return false;
            case NKikimrBootstrapper::TEvWatchResult::DISCONNECTED:
                alienEntry.State = TRound::EAlienState::Disconnected;
                return false;
            default:
                Y_ABORT("unhandled case");
        }
    }

    void CheckRoundCompletion() {
        auto& round = Round.value();
        if (round.Waiting > 0) {
            return;
        }

        ui64 winnerSeed = SelfSeed;
        ui32 winner = SelfId().NodeId();

        size_t undelivered = 0;
        size_t disconnected = 0;
        for (size_t i = 0, e = round.Aliens.size(); i != e; ++i) {
            const auto& alien = round.Aliens[i];
            const ui32 node = OtherNodes.at(i);
            switch (alien.State) {
                case TRound::EAlienState::Wait:
                    Y_DEBUG_ABORT("Unexpected Wait state");
                    return;
                case TRound::EAlienState::Unknown:
                    break;
                case TRound::EAlienState::Free:
                    if (winnerSeed > alien.Seed || winnerSeed == alien.Seed && winner > node) {
                        winnerSeed = alien.Seed;
                        winner = node;
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

        if (winner != SelfId().NodeId()) {
            auto sleepDuration = GetSleepDuration();
            LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::BOOTSTRAPPER,
                "tablet: " << TabletInfo->TabletID << ", type: " << GetTabletTypeName()
                << ", lost round, wait for " << sleepDuration);

            Round.reset();
            Schedule(sleepDuration, new TEvents::TEvWakeup(RoundCounter));
            Become(&TThis::StateSleep);
            return;
        }

        if (!CheckBootPermitted(undelivered, disconnected)) {
            return;
        }

        // Note: current Round is used by Boot to update watchers
        Boot();
    }

    bool CheckBootPermitted(size_t undelivered, size_t disconnected) {
        // Total number of nodes that participate in tablet booting
        size_t total = 1 + OtherNodes.size();
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
            BootDelayedUntil = {};
            return true;
        }

        auto now = TActivationContext::Now();
        if (!BootDelayedUntil) {
            // Delay boot decision until some later time
            BootDelayedUntil = now + BootstrapperInfo->OfflineDelay;
        } else if (BootDelayedUntil <= now) {
            // We don't have enough online nodes, but try to boot anyway
            BootDelayedUntil = {};
            return true;
        }

        auto sleepDuration = Min(GetSleepDuration(), BootDelayedUntil - now);
        LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::BOOTSTRAPPER,
            "tablet:" << TabletInfo->TabletID << ", type: " << GetTabletTypeName()
            << ", " << online << "/" << total << " nodes online (need " << quorum << ")"
            << ", wait for " << sleepDuration);

        Round.reset();
        Schedule(sleepDuration, new TEvents::TEvWakeup(RoundCounter));
        Become(&TThis::StateSleep);
        return false;
    }

private:
    /**
     * Starts a watcher phase
     * - Starts an optional follower
     * - Waits for notification from some other owner or watcher
     * - When watched by others will notify on state change
     * - Begins a new cycle when notified or disconnected
     */
    void BecomeWatch(const TActorId& watchOn, bool owner) {
        LOG_INFO_S(*TlsActivationContext, NKikimrServices::BOOTSTRAPPER,
            "tablet: " << TabletInfo->TabletID << ", type: " << GetTabletTypeName()
            << ", become watch on node " << watchOn.NodeId() << (owner ? " (owner)" : ""));

        Watching.emplace(watchOn, owner);
        WatchedBy.emplace();

        FinishRound(NKikimrBootstrapper::TEvWatchResult::WAITFOR);

        Become(&TThis::StateWatch);
        BootFollower();
    }

    void FinishRound(NKikimrBootstrapper::TEvWatchResult::EState state) {
        if (Round) {
            for (auto& watcher : Round->Watchers) {
                if (watcher.ActorId) {
                    Send(watcher.ActorId, new TEvBootstrapper::TEvWatchResult(
                        TabletInfo->TabletID, state, 0, watcher.Round));
                    WatchedBy.value().Add(watcher.ActorId, watcher.Round);
                }
            }

            BootDelayedUntil = {};
            Round.reset();
        }
    }

    void BootFollower() {
        if (BootstrapperInfo->StartFollowers && !FollowerActorID) {
            LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::BOOTSTRAPPER,
                "tablet: " << TabletInfo->TabletID << ", type: " << GetTabletTypeName()
                << ", boot follower");
            TTabletSetupInfo* x = BootstrapperInfo->SetupInfo.Get();
            FollowerActorID = x->Follower(TabletInfo.Get(),
                SelfId(), TActivationContext::ActorContextFor(SelfId()),
                0, AppData()->ResourceProfiles);
        }
    }

    void HandleWatch(TEvBootstrapper::TEvWatch::TPtr& ev) {
        const NKikimrBootstrapper::TEvWatch& record = ev->Get()->Record;
        Y_ABORT_UNLESS(record.GetTabletID() == TabletInfo->TabletID);

        if (Watching.value().OnWatch(ev->Sender.NodeId())) {
            // We have been watching this node, but now it's trying to watch us
            // This guards against notify/disconnect getting lost (shouldn't happen)
            NotifyWatchers();
            Send(ev->Sender, new TEvBootstrapper::TEvWatchResult(record.GetTabletID(),
                NKikimrBootstrapper::TEvWatchResult::UNKNOWN, 0, record.GetRound()));
            BeginNewCycle();
            return;
        }

        WatchedBy.value().Add(ev->Sender, record.GetRound());
        Send(ev->Sender, new TEvBootstrapper::TEvWatchResult(record.GetTabletID(),
            NKikimrBootstrapper::TEvWatchResult::WAITFOR, 0, record.GetRound()));
    }

    void HandleWatch(TEvBootstrapper::TEvNotify::TPtr& ev) {
        if (ev->Get()->Record.GetRound() != RoundCounter)
            return;

        if (Watching.value().OnNotify(ev->Sender)) {
            NotifyWatchers();
            BeginNewCycle();
        }
    }

    void HandleWatch(TEvInterconnect::TEvNodeDisconnected::TPtr& ev) {
        const ui32 node = ev->Get()->NodeId;
        const ui64 round = ev->Cookie;
        LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::BOOTSTRAPPER,
            "tablet: " << TabletInfo->TabletID << ", type: " << GetTabletTypeName()
            << ", disconnected from " << node << ", round " << round);

        if (round != RoundCounter)
            return;

        if (Watching.value().OnDisconnect(node)) {
            NotifyWatchers();
            BeginNewCycle();
        }
    }

    void NotifyWatchers() {
        if (WatchedBy) {
            for (const auto& pr : *WatchedBy) {
                Send(pr.second.ActorId,
                    new TEvBootstrapper::TEvNotify(
                        TabletInfo->TabletID, NKikimrBootstrapper::TEvNotify::DROP, pr.second.Round));
            }

            WatchedBy.reset();
            Watching.reset();
        }
    }

private:
    /**
     * Starts an owner phase
     * - Boots a new leader tablet (or promotes an existing follower)
     * - When watched by others will notify on state change
     * - Waits until the new instance stops
     * - Begins a new cycle
     */
    void Boot() {
        Y_ABORT_UNLESS(!LookOnActorID);

        LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::BOOTSTRAPPER,
            "tablet: " << TabletInfo->TabletID << ", type: " << GetTabletTypeName()
            << ", boot");

        if (FollowerActorID) {
            LookOnActorID = FollowerActorID;
            FollowerActorID = {};
            Send(LookOnActorID, new TEvTablet::TEvPromoteToLeader(0, TabletInfo));
        } else {
            TTabletSetupInfo* x = BootstrapperInfo->SetupInfo.Get();
            LookOnActorID = x->Tablet(TabletInfo.Get(),
                SelfId(), TActivationContext::ActorContextFor(SelfId()),
                0, AppData()->ResourceProfiles);
        }

        Y_ABORT_UNLESS(LookOnActorID);

        WatchedBy.emplace();
        FinishRound(NKikimrBootstrapper::TEvWatchResult::OWNER);

        Become(&TThis::StateOwner);
    }

    void HandleOwner(TEvBootstrapper::TEvWatch::TPtr& ev) {
        const NKikimrBootstrapper::TEvWatch& record = ev->Get()->Record;
        Y_ABORT_UNLESS(record.GetTabletID() == TabletInfo->TabletID);

        // add to watchers list (if node not already there)
        WatchedBy.value().Add(ev->Sender, record.GetRound());
        Send(ev->Sender, new TEvBootstrapper::TEvWatchResult(record.GetTabletID(),
            NKikimrBootstrapper::TEvWatchResult::OWNER, 0, record.GetRound()));
    }

    void Handle(TEvTablet::TEvTabletDead::TPtr& ev) {
        if (ev->Sender == LookOnActorID) {
            LOG_INFO_S(*TlsActivationContext, NKikimrServices::BOOTSTRAPPER,
                "tablet: " << TabletInfo->TabletID << ", type: " << GetTabletTypeName()
                << ", tablet dead");

            LookOnActorID = {};
            NotifyWatchers();
            BeginNewCycle();
        } else if (ev->Sender == FollowerActorID) {
            FollowerActorID = {};
            BootFollower();
        }
    }

private:
    void HandleSleep(TEvBootstrapper::TEvWatch::TPtr& ev) {
        const NKikimrBootstrapper::TEvWatch& record = ev->Get()->Record;
        Y_ABORT_UNLESS(record.GetTabletID() == TabletInfo->TabletID);

        // We have lost the round and are not going to boot right now
        // However make sure our round seed is somewhat stable while sleeping
        Send(ev->Sender, new TEvBootstrapper::TEvWatchResult(record.GetTabletID(),
            NKikimrBootstrapper::TEvWatchResult::FREE, SelfSeed, record.GetRound()));
    }

    void HandleSleep(TEvents::TEvWakeup::TPtr& ev) {
        if (ev->Get()->Tag != RoundCounter)
            return;

        BeginNewCycle();
    }

private:
    /**
     * Switches bootstrapper to a cold standby
     * - Stops all activity (including followers)
     * - Waits until explicitly activated
     */
    void Standby() {
        Y_ABORT_UNLESS(!ModeStandby);
        Stop();
        ModeStandby = true;
        Become(&TThis::StateStandBy);
    }

    /**
     * Activates a cold standby and begins a new cycle
     */
    void Activate() {
        Y_ABORT_UNLESS(ModeStandby);
        ModeStandby = false;
        OnActivated();
        BeginNewCycle();
    }

    void OnActivated() {
        auto localNodeId = SelfId().NodeId();
        auto whiteboardId = NNodeWhiteboard::MakeNodeWhiteboardServiceId(localNodeId);
        Send(whiteboardId, new NNodeWhiteboard::TEvWhiteboard::TEvSystemStateAddRole("Bootstrapper"));
    }

    void HandleStandBy(TEvBootstrapper::TEvWatch::TPtr& ev) {
        const NKikimrBootstrapper::TEvWatch& record = ev->Get()->Record;
        Y_ABORT_UNLESS(record.GetTabletID() == TabletInfo->TabletID);

        Send(ev->Sender, new TEvBootstrapper::TEvWatchResult(record.GetTabletID(),
            NKikimrBootstrapper::TEvWatchResult::UNKNOWN, Max<ui64>(), record.GetRound()));
    }

    void HandlePoisonStandBy() {
        PassAway();
    }

private:
    /**
     * Common cleanup that may stop any phase
     */
    void Stop() {
        if (KnownLeaderPipe) {
            NTabletPipe::CloseClient(SelfId(), KnownLeaderPipe);
            KnownLeaderPipe = {};
        }

        if (LookOnActorID) {
            Send(LookOnActorID, new TEvents::TEvPoisonPill());
            LookOnActorID = {};
        }

        if (FollowerActorID) {
            Send(FollowerActorID, new TEvents::TEvPoisonPill());
            FollowerActorID = {};
        }

        NotifyWatchers();

        BootDelayedUntil = {};
        Round.reset();
    }

    void HandlePoison() {
        Stop();
        PassAway();
    }

    void PassAway() override {
        for (ui32 nodeId : OtherNodes) {
            Send(TActivationContext::InterconnectProxy(nodeId), new TEvents::TEvUnsubscribe);
        }
        NotifyWatchers();
        TActor::PassAway();
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::TABLET_BOOTSTRAPPER;
    }

    TBootstrapper(TTabletStorageInfo* tabletInfo, TBootstrapperInfo* bootstrapperInfo, bool standby)
        : TabletInfo(tabletInfo)
        , BootstrapperInfo(bootstrapperInfo)
        , ModeStandby(standby)
    {
        Y_ABORT_UNLESS(TTabletTypes::TypeInvalid != TabletInfo->TabletType);
    }

    void Bootstrap() {
        BuildOtherNodes();
        if (ModeStandby) {
            Become(&TThis::StateStandBy);
        } else {
            OnActivated();
            BeginNewCycle();
        }
    }

    STFUNC(StateLookup) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvBootstrapper::TEvWatch, HandleUnknown);
            hFunc(TEvStateStorage::TEvInfo, HandleLookup);
            hFunc(TEvTablet::TEvTabletDead, Handle);
            hFunc(TEvents::TEvWakeup, HandleLookup);
            cFunc(TEvents::TSystem::PoisonPill, HandlePoison); // => die
            cFunc(TEvBootstrapper::EvStandBy, Standby);
        }
    }

    STFUNC(StateConnectLeader) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvBootstrapper::TEvWatch, HandleUnknown);
            hFunc(TEvTabletPipe::TEvClientConnected, HandleConnectLeader);
            hFunc(TEvTablet::TEvTabletDead, Handle);
            cFunc(TEvents::TSystem::PoisonPill, HandlePoison); // => die
            cFunc(TEvBootstrapper::EvStandBy, Standby);
        }
    }

    STFUNC(StateWatchLeader) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvBootstrapper::TEvWatch, HandleWatchLeader);
            hFunc(TEvTabletPipe::TEvClientDestroyed, HandleWatchLeader);
            hFunc(TEvTablet::TEvTabletDead, Handle);
            cFunc(TEvents::TSystem::PoisonPill, HandlePoison); // => die
            cFunc(TEvBootstrapper::EvStandBy, Standby);
        }
    }

    STFUNC(StateFree) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvBootstrapper::TEvWatch, HandleFree); // => reply
            hFunc(TEvBootstrapper::TEvWatchResult, HandleFree); // => noop|sleep|owner|watch
            hFunc(TEvents::TEvUndelivered, HandleFree); // => watchresult with unknown
            hFunc(TEvInterconnect::TEvNodeDisconnected, HandleFree); // => watchresult with unknown
            hFunc(TEvTablet::TEvTabletDead, Handle);
            cFunc(TEvents::TSystem::PoisonPill, HandlePoison); // => die
            cFunc(TEvBootstrapper::EvStandBy, Standby);
        }
    }

    STFUNC(StateWatch) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvBootstrapper::TEvWatch, HandleWatch);
            hFunc(TEvBootstrapper::TEvNotify, HandleWatch);
            hFunc(TEvInterconnect::TEvNodeDisconnected, HandleWatch);
            hFunc(TEvTablet::TEvTabletDead, Handle);
            cFunc(TEvents::TSystem::PoisonPill, HandlePoison);
            cFunc(TEvBootstrapper::EvStandBy, Standby);
        }
    }

    STFUNC(StateOwner) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvBootstrapper::TEvWatch, HandleOwner);
            hFunc(TEvTablet::TEvTabletDead, Handle);
            cFunc(TEvents::TSystem::PoisonPill, HandlePoison);
            cFunc(TEvBootstrapper::EvStandBy, Standby);
        }
    }

    STFUNC(StateSleep) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvBootstrapper::TEvWatch, HandleSleep);
            hFunc(TEvents::TEvWakeup, HandleSleep);
            hFunc(TEvTablet::TEvTabletDead, Handle);
            cFunc(TEvents::TSystem::PoisonPill, HandlePoison);
            cFunc(TEvBootstrapper::EvStandBy, Standby);
        }
    }

    STFUNC(StateStandBy) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvBootstrapper::TEvWatch, HandleStandBy);
            cFunc(TEvBootstrapper::EvActivate, Activate);
            cFunc(TEvents::TSystem::PoisonPill, HandlePoisonStandBy);
        }
    }
};

IActor* CreateBootstrapper(TTabletStorageInfo* tabletInfo, TBootstrapperInfo* bootstrapperInfo, bool standby) {
    return new TBootstrapper(tabletInfo, bootstrapperInfo, standby);
}

TActorId MakeBootstrapperID(ui64 tablet, ui32 node) {
    char x[12] ={'b', 'o', 'o', 't'};
    memcpy(x + 4, &tablet, sizeof(ui64));
    return TActorId(node, TStringBuf(x, x + 12));
}

}
