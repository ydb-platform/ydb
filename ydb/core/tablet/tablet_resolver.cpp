#include <ydb/core/base/tablet_resolver.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/base/statestorage.h>
#include <ydb/core/base/tabletid.h>
#include <ydb/core/base/tablet.h>
#include <ydb/core/base/appdata.h>
#include <ydb/library/actors/async/async.h>
#include <ydb/library/actors/async/cancellation.h>
#include <ydb/library/actors/async/event.h>
#include <ydb/library/actors/async/sleep.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/interconnect/interconnect.h>
#include <ydb/core/util/cache.h>
#include <ydb/core/util/queue_inplace.h>
#include <util/generic/map.h>
#include <util/generic/deque.h>
#include <library/cpp/random_provider/random_provider.h>


namespace NKikimr {

const TDuration TabletResolverRefreshNodesPeriod = TDuration::Seconds(60);

static constexpr ui32 TabletSuspectInstantRetryCount = 0;
static constexpr TDuration TabletSuspectMinRetryDelay = TDuration::MilliSeconds(1);
static constexpr TDuration TabletSuspectMaxRetryDelay = TDuration::MilliSeconds(50);

static constexpr ui32 TabletSubscriptionInstantRetryCount = 1;
static constexpr ui32 TabletSubscriptionMaxRetryCount = 70;
static constexpr TDuration TabletSubscriptionMinRetryDelay = TDuration::MilliSeconds(1);
static constexpr TDuration TabletSubscriptionMaxRetryDelay = TDuration::MilliSeconds(500);

#define BLOG_TRACE(stream) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::TABLET_RESOLVER, stream)
#define BLOG_DEBUG(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::TABLET_RESOLVER, stream)
#define BLOG_INFO(stream) LOG_INFO_S(*TlsActivationContext, NKikimrServices::TABLET_RESOLVER, stream)
#define BLOG_WARN(stream) LOG_WARN_S(*TlsActivationContext, NKikimrServices::TABLET_RESOLVER, stream)
#define BLOG_ERROR(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::TABLET_RESOLVER, stream)

class TTabletResolver : public TActorBootstrapped<TTabletResolver> {
    struct TEvPrivate {
        enum EEv {
            EvRefreshNodes = EventSpaceBegin(TEvents::ES_PRIVATE),
            EvEnd
        };

        struct TEvRefreshNodes : public TEventLocal<TEvRefreshNodes, EvRefreshNodes> {
        };

        static_assert(EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE)");
    };

    struct TEntry {
        enum EState : ui8 {
            StResolve,
            StNormal,
            StRemove,
        };

        static const char* StateToString(EState state) {
            switch (state) {
            case StResolve: return "StResolve";
            case StNormal: return "StNormal";
            case StRemove: return "StRemove";
            }
        }

        struct TQueueEntry {
            TInstant AddInstant;
            TEvTabletResolver::TEvForward::TPtr Ev;

            TQueueEntry(TInstant instant, TEvTabletResolver::TEvForward::TPtr&& ev)
                : AddInstant(instant)
                , Ev(std::move(ev))
            {}
        };

        void MarkLeaderAlive() {
            CurrentLeaderSuspect = false;
            CurrentLeaderProblem = false;
            CurrentLeaderProblemPermanent = false;
        }

        const ui64 TabletId;
        EState State = StResolve;

        bool InCache : 1 = false;
        bool CurrentLeaderSuspect : 1 = false;
        bool CurrentLeaderProblem : 1 = false;
        bool CurrentLeaderProblemPermanent : 1 = false;

        TQueueInplace<TQueueEntry, 128> Queue;
        TActorId KnownLeader;
        TActorId KnownLeaderTablet;

        TMonotonic LastResolved;

        TVector<TEvStateStorage::TEvInfo::TFollowerInfo> KnownFollowers;

        ui64 CacheEpoch = 0;
        ui64 LastCheckEpoch = 0;

        TAsyncEvent Wakeup;

        explicit TEntry(ui64 tabletId)
            : TabletId(tabletId)
        {}
    };

    struct TResolveInfo {
    public:
        TEvTabletResolver::TEvForward::TResolveFlags ResFlags;
        TDuration SinceLastResolve;
        ui32 NumLocal;
        ui32 NumLocalDc;
        ui32 NumOtherDc;
        bool* NeedFollowerUpdate;

        TResolveInfo(const TEvTabletResolver::TEvForward& msg, const TDuration& sinceResolve, bool* needFollowerUpdate)
            : ResFlags(msg.ResolveFlags)
            , SinceLastResolve(sinceResolve)
            , NumLocal(0)
            , NumLocalDc(0)
            , NumOtherDc(0)
            , NeedFollowerUpdate(needFollowerUpdate)
        {}

        ~TResolveInfo() {
            if (!NeedFollowerUpdate)
                return;
            else if (!ResFlags.AllowFollower()) {
                *NeedFollowerUpdate = false;
                return;
            }
            *NeedFollowerUpdate = (!NumLocalDc && SinceLastResolve > TDuration::Seconds(2)) ||
                (SinceLastResolve > TDuration::Minutes(5));
        }

        void Count(bool isLocal, bool isLocalDc) {
            if (isLocal)
                ++NumLocal;
            if (isLocal || isLocalDc)
                ++NumLocalDc;
            else
                ++NumOtherDc;
        }
    };

    /**
     * Allows local coroutines to receive supported events with a specific cookie
     */
    class TEventStream : public TIntrusiveListItem<TEventStream> {
    public:
        TEventStream(TTabletResolver* self, ui64 cookie)
            : Self(self)
            , Cookie(cookie)
        {
            Self->EventStreams[Cookie] = this;
        }

        ~TEventStream() {
            Self->EventStreams.erase(Cookie);
        }

        async<IEventHandle::TPtr> Next() {
            Y_ABORT_UNLESS(!NextEvent, "Unexpected event pending before Next()");
            co_await ReceivedEvent.Wait();
            // Note: NextEvent is nullptr on TEvNodeDisconnected
            co_return std::move(NextEvent);
        }

        void OnConnect() {
            Connected = true;
        }

        void OnEvent(IEventHandle::TPtr&& ev) {
            Y_ABORT_UNLESS(ReceivedEvent.HasAwaiters(), "Nobody is waiting");
            NextEvent = std::move(ev);
            ReceivedEvent.NotifyOne();
        }

        void OnDisconnect() {
            Y_ABORT_UNLESS(ReceivedEvent.HasAwaiters(), "Nobody is waiting");
            Y_ABORT_UNLESS(!NextEvent, "Unexpected event pending in OnDisconnect()");
            ReceivedEvent.NotifyOne();
        }

        bool HadConnect() const {
            return Connected;
        }

    private:
        TTabletResolver* const Self;
        const ui64 Cookie;
        IEventHandle::TPtr NextEvent;
        TAsyncEvent ReceivedEvent;
        bool Connected = false;
    };

    absl::flat_hash_map<ui64, TEventStream*> EventStreams;

    struct TTabletStateSubscription {
        TActorId ActorId;
        ui64 TabletId = 0;
        ui64 SeqNo = 0;
        NKikimrTabletBase::TEvTabletStateUpdate::EState State = NKikimrTabletBase::TEvTabletStateUpdate::StateUnknown;
        TAsyncCancellationScope Scope;
    };

    struct TInterconnectSession {
        TIntrusiveList<TEventStream> EventStreams;
    };

    typedef NCache::T2QCache<ui64, TEntry*> TTabletCache;

    TIntrusivePtr<TTabletResolverConfig> Config;
    THashMap<ui64, TEntry> Tablets;
    TTabletCache TabletCache;
    THashMap<ui32, TString> NodeToDcMapping;

    THashMap<ui32, ui64> NodeProblems;
    ui64 LastNodeProblemsUpdateEpoch = 0;

    ui64 LastCacheEpoch = 0;

    THashMap<TActorId, TTabletStateSubscription> TabletStateSubscriptions;
    THashMap<TActorId, TInterconnectSession> InterconnectSessions;
    ui64 LastSeqNo = 0;

    ::NMonitoring::TDynamicCounters::TCounterPtr SelectedLeaderLocal;
    ::NMonitoring::TDynamicCounters::TCounterPtr SelectedLeaderLocalDc;
    ::NMonitoring::TDynamicCounters::TCounterPtr SelectedLeaderOtherDc;

    ::NMonitoring::TDynamicCounters::TCounterPtr SelectedFollowerLocal;
    ::NMonitoring::TDynamicCounters::TCounterPtr SelectedFollowerLocalDc;
    ::NMonitoring::TDynamicCounters::TCounterPtr SelectedFollowerOtherDc;

    ::NMonitoring::TDynamicCounters::TCounterPtr SelectedNone;

    ::NMonitoring::TDynamicCounters::TCounterPtr InFlyResolveCounter;

    std::optional<TString> FindNodeDc(ui32 nodeId) const {
        auto it = NodeToDcMapping.find(nodeId);
        return it != NodeToDcMapping.end() ? std::make_optional(it->second) : std::nullopt;
    }

    async<TEvStateStorage::TEvInfo::TPtr> LookupTablet(ui64 tabletId) {
        const ui64 cookie = ++LastSeqNo;
        const TActorId proxy = MakeStateStorageProxyID();
        Send(proxy, new TEvStateStorage::TEvLookup(tabletId, 0), IEventHandle::FlagTrackDelivery, cookie);

        InFlyResolveCounter->Inc();
        Y_DEFER { InFlyResolveCounter->Dec(); };

        // We may receive either TEvStateStorage::TEvInfo or TEvUndelivered
        TEventStream stream(this, cookie);
        auto ev = co_await stream.Next();
        Y_ABORT_UNLESS(ev); // local event cannot disconnect
        switch (ev->GetTypeRewrite()) {
            case TEvStateStorage::TEvInfo::EventType:
                co_return std::move(reinterpret_cast<TEvStateStorage::TEvInfo::TPtr&>(ev));

            case TEvents::TEvUndelivered::EventType:
                co_return nullptr;

            default:
                Y_ABORT_S("Unexpected resolve reply " << Hex(ev->GetTypeRewrite()) << " " << ev->ToString());
        }
    }

    void TabletResolveLoop(ui64 tabletId) {
        TEntry& entry = Tablets.at(tabletId);
        Y_ABORT_UNLESS(entry.State == TEntry::StResolve);

        Y_DEFER {
            TabletCache.Erase(tabletId);
            Tablets.erase(tabletId);
        };

        Y_DEFER {
            if (TlsActivationContext && entry.KnownLeader) {
                UnsubscribeTabletState(entry.KnownLeader);
            }
        };

        ui32 retryNumber = 0;
        TDuration retryDelay;
        for (;;) {
            // On every iteration we make a new resolve request
            Y_ABORT_UNLESS(entry.State == TEntry::StResolve,
                "Unexpected entry.State: %s", TEntry::StateToString(entry.State));

            if (retryDelay) {
                TDuration delay = (entry.LastResolved + retryDelay) - TActivationContext::Monotonic();
                if (delay) {
                    BLOG_TRACE("TabletResolveLoop tabletId: " << tabletId
                        << " sleeping for retry delay " << delay);
                    co_await AsyncSleepFor(delay);
                }
            }

            {
                BLOG_TRACE("TabletResolveLoop tabletId: " << tabletId
                        << " sending TEvLookup");
                auto ev = co_await LookupTablet(tabletId);
                if (!ev) {
                    // StateStorage proxy is not configured on this node
                    BLOG_INFO("TabletResolveLoop tabletId: " << tabletId
                            << " StateStorage proxy not configured");
                    SendQueuedError(entry, NKikimrProto::ERROR);
                    break;
                }

                auto* msg = ev->Get();

                BLOG_TRACE("TabletResolveLoop tabletId: " << tabletId
                    << " received TEvInfo Status: " << msg->Status
                    << " event: " << msg->ToString());

                // This will handle errors and send replies
                ApplyEntryInfo(entry, *msg);
            }

            if (!entry.InCache) {
                // This entry was evicted while resolving, stop processing
                BLOG_TRACE("TabletResolveLoop tabletId: " << tabletId
                    << " processing stopped for an evicted entry");
                break;
            }

            if (entry.KnownLeader && entry.CurrentLeaderSuspect && !entry.CurrentLeaderProblemPermanent) {
                // Send a best-effort ping request to a suspicious leader
                // Don't track delivery or node disconnections, since this is
                // only needed when older stable versions don't support tablet
                // state subscriptions.
                // TODO(snaury): remove in 2026.
                Send(entry.KnownLeader, new TEvTablet::TEvPing(tabletId, 0), 0, entry.CacheEpoch);
            }

            // Note: we move to StNormal and wait even when resolve fails, but
            // any attempt to resolve leader will wake us up and we will retry
            // with an appropriate delay when necessary.
            entry.State = TEntry::StNormal;

            // Wait until the next iteration
            co_await entry.Wakeup.Wait();

            if (!entry.InCache || entry.State == TEntry::StRemove) {
                BLOG_TRACE("TabletResolveLoop tabletId: " << tabletId
                    << " processing stopped for an evicted entry");
                break;
            }

            // Don't overload state storage when leader is inaccessible
            if (entry.CurrentLeaderSuspect && entry.CurrentLeaderProblem) {
                if (++retryNumber <= TabletSuspectInstantRetryCount) {
                    retryDelay = {};
                } else if (!retryDelay) {
                    retryDelay = TabletSuspectMinRetryDelay;
                } else {
                    retryDelay = Min(retryDelay * 2, TabletSuspectMaxRetryDelay);
                }
            } else {
                retryNumber = 0;
                retryDelay = {};
            }
        }
    }

    void OnTabletAlive(ui64 tabletId, const TActorId& actorId) {
        auto it = Tablets.find(tabletId);
        if (it == Tablets.end()) {
            return;
        }

        TEntry& entry = it->second;
        if (entry.KnownLeader == actorId) {
            // We just confirmed this leader is alive
            entry.MarkLeaderAlive();
        }
    }

    void OnTabletActive(ui64 tabletId, const TActorId& actorId, const TActorId& userActorId) {
        auto it = Tablets.find(tabletId);
        if (it == Tablets.end()) {
            return;
        }

        TEntry& entry = it->second;
        if (entry.KnownLeader == actorId) {
            // When leader is active it implies it's alive
            entry.MarkLeaderAlive();
            if (!entry.KnownLeaderTablet) {
                entry.KnownLeaderTablet = userActorId;
            }
            return;
        }

        for (auto& followerInfo : entry.KnownFollowers) {
            if (followerInfo.Follower == actorId) {
                if (!followerInfo.FollowerTablet) {
                    followerInfo.FollowerTablet = userActorId;
                }
                break;
            }
        }
    }

    void OnTabletProblem(ui64 tabletId, const TActorId& actorId, bool permanent = false) {
        auto it = Tablets.find(tabletId);
        if (it == Tablets.end()) {
            return;
        }

        TEntry& entry = it->second;
        BLOG_TRACE("OnTabletProblem tabletId: " << tabletId << " actorId: " << actorId
            << " entry.State: " << TEntry::StateToString(entry.State));

        if (!actorId) {
            // TEvTabletProblem without actorId is used for forced eviction
            if (entry.State == TEntry::StNormal) {
                entry.State = TEntry::StRemove;
                entry.Wakeup.NotifyOne();
            }
            return;
        }

        if (entry.KnownLeader == actorId || entry.KnownLeaderTablet == actorId) {
            BLOG_TRACE("OnTabletProblem tabletId: " << tabletId
                << " marking leader " << entry.KnownLeader
                << " with a" << (permanent ? " permanent" : "") << " problem");
            entry.CurrentLeaderProblem = true;
            if (permanent) {
                entry.CurrentLeaderProblemPermanent = true;
            }
            // Note: we want to delay resolve until the next request
            return;
        }

        for (auto it = entry.KnownFollowers.begin(); it != entry.KnownFollowers.end(); ++it) {
            if (it->Follower == actorId || it->FollowerTablet == actorId) {
                BLOG_TRACE("OnTabletProblem tabletId: " << tabletId
                    << " removing follower " << it->Follower);
                entry.KnownFollowers.erase(it);
                if (entry.State == TEntry::StNormal) {
                    entry.State = TEntry::StResolve;
                    entry.Wakeup.NotifyOne();
                }
                break;
            }
        }
    }

    void TabletStateSubscriptionLoop(ui64 tabletId, TActorId actorId) {
        Y_ABORT_UNLESS(!TabletStateSubscriptions.contains(actorId));
        auto& subscription = TabletStateSubscriptions[actorId];
        subscription.ActorId = actorId;
        subscription.TabletId = tabletId;
        Y_DEFER { TabletStateSubscriptions.erase(actorId); };

        subscription.Scope = co_await TAsyncCancellationScope::WithCurrentHandler();

        ui32 retryNumber = 0;
        TDuration retryDelay;
        for (;;) {
            subscription.SeqNo = -1;
            subscription.State = NKikimrTabletBase::TEvTabletStateUpdate::StateUnknown;

            if (retryDelay) {
                BLOG_TRACE("TabletStateSubscriptionLoop tabletId: " << tabletId << " actor: " << actorId
                    << " sleeping for " << retryDelay);
            }
            // Note: this yields and checks for scope cancellation even when delay is zero
            co_await AsyncSleepFor(retryDelay);

            const ui64 seqNo = ++LastSeqNo;
            subscription.SeqNo = seqNo;
            ui32 flags = IEventHandle::FlagTrackDelivery;
            if (actorId.NodeId() != SelfId().NodeId()) {
                flags |= IEventHandle::FlagSubscribeOnSession;
            }
            BLOG_TRACE("TabletStateSubscriptionLoop tabletId: " << tabletId << " actor: " << actorId
                    << " sending TEvTabletStateSubscribe seqNo=" << seqNo);
            Send(actorId, new TEvTablet::TEvTabletStateSubscribe(tabletId, seqNo), flags, seqNo);

            bool subscribed = true;
            auto unsubscribe = [&]() {
                if (subscribed) {
                    BLOG_TRACE("TabletStateSubscriptionLoop tabletId: " << tabletId << " actor: " << actorId
                        << " sending TEvTabletStateUnsubscribe seqNo=" << seqNo);
                    Send(actorId, new TEvTablet::TEvTabletStateUnsubscribe(tabletId, seqNo));
                    subscribed = false;
                }
            };

            Y_DEFER {
                // Note: don't unsubscribe during actor system shutdown
                if (TlsActivationContext) {
                    unsubscribe();
                }
            };

            TEventStream stream(this, seqNo);
            while (subscribed) {
                auto ev = co_await stream.Next();
                if (!ev) {
                    // Node disconnected, start a new iteration
                    subscribed = false;
                    BLOG_TRACE("TabletStateSubscriptionLoop tabletId: " << tabletId << " actor: " << actorId
                        << " node " << (stream.HadConnect() ? "disconnected" : "failed to connect"));
                    if (!stream.HadConnect()) {
                        // Mark tablet as having a problem only when connection fails
                        OnTabletProblem(tabletId, actorId);
                    }
                    break;
                }

                switch (ev->GetTypeRewrite()) {
                    case TEvents::TEvUndelivered::EventType: {
                        // Tablet actor doesn't exist (note: this will not be delivered after a disconnect)
                        subscribed = false;
                        BLOG_TRACE("TabletStateSubscriptionLoop tabletId: " << tabletId << " actor: " << actorId
                            << " undelivered, assuming actor permanently unavailable");
                        OnTabletProblem(tabletId, actorId, /* permanent */ true);
                        co_return;
                    }
                    case TEvTablet::TEvTabletStateUpdate::EventType: {
                        retryNumber = 0;
                        auto* msg = ev->Get<TEvTablet::TEvTabletStateUpdate>();
                        BLOG_TRACE("TabletStateSubscriptionLoop tabletId: " << tabletId << " actor: " << actorId
                            << " received state update: " << msg->ToString());
                        subscription.State = msg->Record.GetState();
                        switch (subscription.State) {
                            case NKikimrTabletBase::TEvTabletStateUpdate::StateTerminating:
                            case NKikimrTabletBase::TEvTabletStateUpdate::StateDead:
                                OnTabletProblem(tabletId, actorId, /* permanent */ true);
                                unsubscribe();
                                co_return;

                            case NKikimrTabletBase::TEvTabletStateUpdate::StateActive:
                                OnTabletActive(tabletId, actorId, msg->GetUserActorId());
                                break;

                            default:
                                OnTabletAlive(tabletId, actorId);
                                break;
                        }
                        break;
                    }
                    default: {
                        Y_ABORT_S("Unexpected tablet state notification " << Hex(ev->GetTypeRewrite()) << " " << ev->ToString());
                    }
                }
            }

            // Don't use exponential backoff as long as we keep connecting
            if (stream.HadConnect()) {
                retryNumber = 0;
                retryDelay = {};
                continue;
            }

            // A small exponential backoff on retries: 0ms, 1ms, 2ms, 4ms, 8ms, 10ms
            if (++retryNumber <= TabletSubscriptionInstantRetryCount) {
                retryDelay = {};
            } else if (!retryDelay) {
                retryDelay = TabletSubscriptionMinRetryDelay;
            } else {
                retryDelay = Min(retryDelay * 2, TabletSubscriptionMaxRetryDelay);
            }

            if (retryNumber > TabletSubscriptionMaxRetryCount) {
                break;
            }
        }
    }

    void SubscribeTabletState(ui64 tabletId, const TActorId& actorId) {
        if (!TabletStateSubscriptions.contains(actorId)) {
            TabletStateSubscriptionLoop(tabletId, actorId);
            Y_DEBUG_ABORT_UNLESS(TabletStateSubscriptions.contains(actorId));
        }
    }

    void UnsubscribeTabletState(const TActorId& actorId) {
        auto it = TabletStateSubscriptions.find(actorId);
        if (it != TabletStateSubscriptions.end()) {
            it->second.Scope.Cancel();
        }
    }

    std::pair<TActorId, TActorId> SelectForward(const TEntry& entry, TResolveInfo& info, ui64 tabletId) {
        const ui32 selfNode = SelfId().NodeId();
        const std::optional<TString> selfDc = FindNodeDc(selfNode);
        const std::optional<TString> leaderDc = FindNodeDc(entry.KnownLeader.NodeId());

        struct TCandidate {
            TActorId KnownLeader;
            TActorId KnownLeaderTablet;
            bool IsLocal;
            bool IsLocalDc;
            bool IsLeader;
        };

        ui32 disallowed = 0; // do not save (prio == 0), just count

        ui32 winnersPrio = 0;
        TStackVec<TCandidate, 8> winners;

        auto addCandidate = [&](ui32 prio, TCandidate&& candidate) {
            if (winnersPrio < prio) {
                winnersPrio = prio;
                winners.clear();
            }
            if (winnersPrio == prio) {
                winners.push_back(std::move(candidate));
            }
        };

        // Specifying an explicit follower ID overrides other options,
        // but followerId == 0 goes through the same leader selection code below
        bool allowLeader = (entry.KnownLeader && !entry.CurrentLeaderProblem);

        if (info.ResFlags.FollowerId && (*info.ResFlags.FollowerId != 0)) {
            // An explicit followerId > 0 is set, so do not even try the leader
            allowLeader = false;
        }

        if (allowLeader) {
            bool isLocal = (entry.KnownLeader.NodeId() == selfNode);
            bool isLocalDc = selfDc && leaderDc == selfDc;
            info.Count(isLocal, isLocalDc);

            ui32 prio = info.ResFlags.GetTabletPriority(isLocal, isLocalDc, false);
            if (prio) {
                addCandidate(prio, TCandidate{ entry.KnownLeader, entry.KnownLeaderTablet, isLocal, isLocalDc, true });
            } else {
                ++disallowed;
            }
        }

        // Select followers either automatically or through an explicit follower ID,
        // but only if the requested follower ID is not zero (zero == select leader)
        bool allowFollowers = info.ResFlags.AllowFollower();

        if (info.ResFlags.FollowerId && (*info.ResFlags.FollowerId == 0)) {
            allowFollowers = false;
        }

        if (allowFollowers) {
            for (const auto& followerInfo : entry.KnownFollowers) {
                // If the caller requested an explicit follower ID, disregard all followers,
                // which have different follower IDs
                //
                // NOTE: There may still be multiple followers with the same follower ID,
                //       thus the code here goes through all followers and, if multiple
                //       followers are found, goes through the same random selection path below
                if (info.ResFlags.FollowerId) {
                    if (!followerInfo.FollowerId.Defined()
                        || (followerInfo.FollowerId.Get() != *info.ResFlags.FollowerId)) {
                        // If the follower ID for the given follower is not known,
                        // it should be skipped for the explicit follower ID case!
                        continue;
                    }
                }

                bool isLocal = (followerInfo.Follower.NodeId() == selfNode);
                bool isLocalDc = selfDc && FindNodeDc(followerInfo.Follower.NodeId()) == selfDc;
                info.Count(isLocal, isLocalDc);

                ui32 prio = info.ResFlags.GetTabletPriority(isLocal, isLocalDc, true);

                if (!prio) {
                    ++disallowed;
                    continue;
                }

                addCandidate(
                    prio,
                    TCandidate{
                        .KnownLeader = followerInfo.Follower,
                        .KnownLeaderTablet = followerInfo.FollowerTablet,
                        .IsLocal = isLocal,
                        .IsLocalDc = isLocalDc,
                        .IsLeader = false,
                    }
                );
            }
        }

        auto dcName = [](const std::optional<TString>& x) { return x ? x->data() : "<none>"; };

        if (!winners.empty()) {
            size_t winnerIndex = (winners.size() == 1 ? 0 : (AppData()->RandomProvider->GenRand64() % winners.size()));
            const TCandidate& winner = winners[winnerIndex];

            BLOG_DEBUG("SelectForward"
                << " node: " << selfNode
                << " selfDC: " << dcName(selfDc)
                << " leaderDC: " << dcName(leaderDc)
                << " resolveFlags: " << info.ResFlags.ToString()
                << " local: " << info.NumLocal
                << " localDc: " << info.NumLocalDc
                << " other: " << info.NumOtherDc
                << " disallowed: " << disallowed
                << " tabletId: " << tabletId
                << " followers: " << entry.KnownFollowers.size()
                << " allowLeader: " << allowLeader
                << " allowFollowers: " << allowFollowers
                << " candidates: " << winners.size()
                << " winner: " << winner.KnownLeader);

            if (winner.IsLeader) {
                if (winner.IsLocal)
                    SelectedLeaderLocal->Inc();
                if (winner.IsLocal || winner.IsLocalDc)
                    SelectedLeaderLocalDc->Inc();
                else
                    SelectedLeaderOtherDc->Inc();
            } else {
                if (winner.IsLocal)
                    SelectedFollowerLocal->Inc();
                if (winner.IsLocal || winner.IsLocalDc)
                    SelectedFollowerLocalDc->Inc();
                else
                    SelectedFollowerOtherDc->Inc();
            }

            return std::make_pair(winner.KnownLeader, winner.KnownLeaderTablet);
        }

        BLOG_INFO("No candidates for SelectForward,"
            << " node: " << selfNode
            << " selfDC: " << dcName(selfDc)
            << " leaderDC: " << dcName(leaderDc)
            << " resolveFlags: " << info.ResFlags.ToString()
            << " local: " << info.NumLocal
            << " localDc: " << info.NumLocalDc
            << " other: " << info.NumOtherDc
            << " disallowed: " << disallowed
            << " tabletId: " << tabletId
            << " followers: " << entry.KnownFollowers.size()
            << " allowLeader: " << allowLeader
            << " allowFollowers: " << allowFollowers);

        SelectedNone->Inc();

        return std::make_pair(TActorId(), TActorId());
    }

    bool SendForward(const TActorId& sender, ui64 cookie, const TEntry& entry, TEvTabletResolver::TEvForward* msg,
                     bool* needFollowerUpdate = nullptr) {
        TResolveInfo info(*msg, TActivationContext::Monotonic() - entry.LastResolved, needFollowerUpdate); // fills needFollowerUpdate in dtor
        const std::pair<TActorId, TActorId> endpoint = SelectForward(entry, info, msg->TabletID);
        if (endpoint.first) {
            Send(sender, new TEvTabletResolver::TEvForwardResult(msg->TabletID, endpoint.second, endpoint.first, LastCacheEpoch), 0, cookie);
            if (!!msg->Ev) {
                Send(IEventHandle::Forward(std::move(msg->Ev), msg->SelectActor(endpoint.second, endpoint.first)));
            }
            return true;
        } else {
            return false;
        }
    }

    bool PushQueue(TEntry& entry, TEvTabletResolver::TEvForward::TPtr& ev) {
        entry.Queue.Emplace(TActivationContext::Now(), std::move(ev));
        return true;
    }

    void SendQueued(TEntry& entry) {
        while (TEntry::TQueueEntry* x = entry.Queue.Head()) {
            TEvTabletResolver::TEvForward* msg = x->Ev->Get();
            if (!SendForward(x->Ev->Sender, x->Ev->Cookie, entry, msg)) {
                Send(x->Ev->Sender, new TEvTabletResolver::TEvForwardResult(NKikimrProto::ERROR, entry.TabletId), 0, x->Ev->Cookie);
            }
            entry.Queue.Pop();
        }
        // Free buffer memory
        entry.Queue.Clear();
    }

    void SendQueuedError(TEntry& entry, NKikimrProto::EReplyStatus status) {
        while (TEntry::TQueueEntry* x = entry.Queue.Head()) {
            Send(x->Ev->Sender, new TEvTabletResolver::TEvForwardResult(status, entry.TabletId), 0, x->Ev->Cookie);
            entry.Queue.Pop();
        }
        // Free buffer memory
        entry.Queue.Clear();
    }

    void ApplyEntryInfo(TEntry& entry, TEvStateStorage::TEvInfo& msg) {
        if (entry.KnownLeader != msg.CurrentLeader || !msg.CurrentLeader) {
            if (entry.KnownLeader) {
                UnsubscribeTabletState(entry.KnownLeader);
            }
            entry.KnownLeader = msg.CurrentLeader;
            entry.KnownLeaderTablet = msg.CurrentLeaderTablet;
            if (entry.KnownLeader) {
                // Assume leader is alive when encountered for the first time
                entry.MarkLeaderAlive();
            } else {
                // Note: currently state storage will reply with an error when
                // the leader is missing, but this code path will handle
                // followers without a leader in the future.
                entry.CurrentLeaderSuspect = true;
                entry.CurrentLeaderProblem = true;
                entry.CurrentLeaderProblemPermanent = true;
            }
        } else {
            if (entry.CurrentLeaderProblem) {
                entry.CurrentLeaderSuspect = true;
            }
            if (!entry.CurrentLeaderProblemPermanent) {
                entry.CurrentLeaderProblem = false;
            }
            if (msg.CurrentLeaderTablet) {
                entry.KnownLeaderTablet = msg.CurrentLeaderTablet;
            }
        }

        entry.KnownFollowers = std::move(msg.Followers);

        entry.LastResolved = TActivationContext::Monotonic();
        entry.CacheEpoch = ++LastCacheEpoch;

        BLOG_DEBUG("ApplyEntry tabletId: " << msg.TabletID
            << " leader: " << entry.KnownLeader
            << " followers: " << entry.KnownFollowers.size());

        if (entry.KnownLeader && entry.InCache) {
            SubscribeTabletState(entry.TabletId, entry.KnownLeader);
        }

        if (msg.Status == NKikimrProto::OK || !entry.KnownFollowers.empty()) {
            SendQueued(entry);
        } else {
            SendQueuedError(entry, NKikimrProto::ERROR);
        }
    }

    TEntry& GetEntry(ui64 tabletId) {
        bool created = false;
        auto it = Tablets.find(tabletId);
        if (it == Tablets.end()) {
            auto res = Tablets.emplace(
                std::piecewise_construct,
                std::forward_as_tuple(tabletId),
                std::forward_as_tuple(tabletId));
            it = res.first;
            created = true;
        }

        TEntry& entry = it->second;
        if (created) {
            TabletResolveLoop(tabletId);
        }

        // Entry may be evicted while resolving, reinsert back into cache on additional hits
        TEntry** ignored;
        if (!entry.InCache) {
            bool ok = TabletCache.Insert(tabletId, &entry, ignored);
            Y_DEBUG_ABORT_UNLESS(ok, "Unexpected failure to reinsert previously evicted tablet");
            entry.InCache = true;
        } else {
            bool ok = TabletCache.Find(tabletId, ignored);
            Y_DEBUG_ABORT_UNLESS(ok, "Unexpected failure to find an unevicted tablet");
        }

        return entry;
    }

    void CheckDelayedNodeProblem(ui64 tabletId) {
        auto it = Tablets.find(tabletId);
        if (it == Tablets.end()) {
            return;
        }

        TEntry& entry = it->second;
        if (entry.CacheEpoch < LastNodeProblemsUpdateEpoch && entry.LastCheckEpoch < LastNodeProblemsUpdateEpoch) {
            entry.LastCheckEpoch = LastCacheEpoch;

            if (entry.State == TEntry::StNormal) {
                auto* pMaxProblemEpoch = NodeProblems.FindPtr(entry.KnownLeader.NodeId());
                if (pMaxProblemEpoch && entry.CacheEpoch <= *pMaxProblemEpoch) {
                    BLOG_DEBUG("Delayed invalidation of tabletId: " << tabletId
                        << " leader: " << entry.KnownLeader << " by nodeId");
                    entry.CurrentLeaderProblem = true;
                }
            }

            auto itDst = entry.KnownFollowers.begin();
            auto itSrc = entry.KnownFollowers.begin();
            while (itSrc != entry.KnownFollowers.end()) {
                auto* pMaxProblemEpoch = NodeProblems.FindPtr(itSrc->Follower.NodeId());
                if (pMaxProblemEpoch && entry.CacheEpoch <= *pMaxProblemEpoch) {
                    BLOG_DEBUG("Delayed invalidation of tabletId: " << tabletId
                        << " follower: " << itSrc->Follower << " by nodeId");
                    ++itSrc;
                    continue;
                }
                if (itDst != itSrc) {
                    *itDst = *itSrc;
                }
                ++itDst;
                ++itSrc;
            }
            if (itDst != itSrc) {
                entry.KnownFollowers.erase(itDst, itSrc);
                if (entry.State == TEntry::StNormal) {
                    entry.State = TEntry::StResolve;
                    entry.Wakeup.NotifyOne();
                }
            }
        }
    }

    void Handle(TEvTabletResolver::TEvForward::TPtr& ev) {
        TEvTabletResolver::TEvForward* msg = ev->Get();
        const ui64 tabletId = msg->TabletID;

        CheckDelayedNodeProblem(tabletId);

        TEntry& entry = GetEntry(tabletId);
        BLOG_DEBUG("Handle TEvForward tabletId: " << tabletId
                << " entry.State: " << TEntry::StateToString(entry.State)
                << " leader: " << entry.KnownLeader
                << (entry.CurrentLeaderProblem ? " (known problem)" : "")
                << " followers: " << entry.KnownFollowers.size()
                << " ev: " << msg->ToString());

        switch (entry.State) {
            case TEntry::StNormal: {
                bool needUpdate = false;
                if (!SendForward(ev->Sender, ev->Cookie, entry, msg, &needUpdate)) {
                    PushQueue(entry, ev);
                    needUpdate = true;
                }
                if (needUpdate) {
                    entry.State = TEntry::StResolve;
                    entry.Wakeup.NotifyOne();
                }
                break;
            }
            case TEntry::StResolve: {
                // Try to handle requests even while resolving
                if (entry.CacheEpoch == 0 || !SendForward(ev->Sender, ev->Cookie, entry, msg)) {
                    PushQueue(entry, ev);
                }
                break;
            }
            case TEntry::StRemove: {
                Y_ABORT("Unexpected StRemove state");
            }
        }
    }

    void Handle(TEvTabletResolver::TEvTabletProblem::TPtr& ev) {
        TEvTabletResolver::TEvTabletProblem* msg = ev->Get();
        const ui64 tabletId = msg->TabletID;

        // Note: avoid promoting tablet entry in the cache
        auto it = Tablets.find(tabletId);
        if (it == Tablets.end()) {
            BLOG_DEBUG("Handle TEvTabletProblem tabletId: " << tabletId << " not cached");
            return;
        }

        TEntry& entry = it->second;
        BLOG_DEBUG("Handle TEvTabletProblem tabletId: " << tabletId << " actor: " << msg->Actor
            << " entry.State: " << TEntry::StateToString(entry.State));

        OnTabletProblem(tabletId, msg->Actor, /* permanent */ false);
    }

    void Handle(TEvTabletResolver::TEvNodeProblem::TPtr& ev) {
        TEvTabletResolver::TEvNodeProblem* msg = ev->Get();
        const ui32 nodeId = msg->NodeId;
        const ui64 problemEpoch = msg->CacheEpoch;

        ui64& maxProblemEpoch = NodeProblems[nodeId];
        if (maxProblemEpoch < problemEpoch) {
            BLOG_DEBUG("Handle TEvNodeProblem nodeId: " << nodeId
                << " max(problemEpoch): " << problemEpoch);
            maxProblemEpoch = problemEpoch;
            LastNodeProblemsUpdateEpoch = ++LastCacheEpoch;
        } else {
            BLOG_DEBUG("Handle TEvNodeProblem nodeId: " << nodeId
                << " problemEpoch: " << problemEpoch << " <= max(problemEpoch): " << maxProblemEpoch);
        }
    }

    void Handle(TEvTablet::TEvPong::TPtr& ev) {
        auto* msg = ev->Get();
        const ui64 tabletId = msg->Record.GetTabletID();

        BLOG_TRACE("Handle TEvPong tabletId: " << tabletId << " actor: " << ev->Sender << " cookie: " << ev->Cookie);

        auto it = Tablets.find(tabletId);
        if (it == Tablets.end()) {
            return;
        }

        TEntry& entry = it->second;
        if (ev->Sender != entry.KnownLeader || entry.CurrentLeaderProblemPermanent) {
            return;
        }

        if (ev->Cookie == 0 || ev->Cookie == entry.CacheEpoch) {
            // We have confirmed this tablet instance was alive in the current cache epoch
            // Note: older versions don't relay request cookie and it will always be zero
            entry.MarkLeaderAlive();
        }
    }

    void Handle(TEvInterconnect::TEvNodeConnected::TPtr& ev) {
        auto& session = InterconnectSessions[ev->Sender];

        auto it = EventStreams.find(ev->Cookie);
        if (it != EventStreams.end()) {
            session.EventStreams.PushBack(it->second);
            it->second->OnConnect();
        }
    }

    void Handle(TEvInterconnect::TEvNodeDisconnected::TPtr& ev) {
        auto itSession = InterconnectSessions.find(ev->Sender);
        if (itSession != InterconnectSessions.end()) {
            auto& session = itSession->second;
            while (!session.EventStreams.Empty()) {
                auto* stream = session.EventStreams.PopFront();
                stream->OnDisconnect();
            }
            InterconnectSessions.erase(itSession);
        }

        auto it = EventStreams.find(ev->Cookie);
        if (it != EventStreams.end() && !it->second->HadConnect()) {
            it->second->OnDisconnect();
        }
    }

    void HandleStream(IEventHandle::TPtr& ev) {
        auto it = EventStreams.find(ev->Cookie);
        if (it != EventStreams.end()) {
            it->second->OnEvent(std::move(ev));
        }
    }

    void Handle(TEvInterconnect::TEvNodesInfo::TPtr& ev) {
        const TEvInterconnect::TEvNodesInfo* msg = ev->Get();
        bool distinct = false;
        for (const auto& nodeInfo : msg->Nodes) {
            if (nodeInfo.Location.GetDataCenterId() != msg->Nodes[0].Location.GetDataCenterId())
                distinct = true;
            NodeToDcMapping[nodeInfo.NodeId] = nodeInfo.Location.GetDataCenterId();
        }

        if (!distinct)
            NodeToDcMapping.clear();

        Schedule(TabletResolverRefreshNodesPeriod, new TEvPrivate::TEvRefreshNodes);
    }

    void Handle(TEvPrivate::TEvRefreshNodes::TPtr&) {
        RefreshNodes();
    }

    void RefreshNodes() {
        Send(GetNameserviceActorId(), new TEvInterconnect::TEvListNodes());
    }

    void OnEvictEntry(TEntry& entry) {
        entry.InCache = false;
        if (entry.State == TEntry::StNormal) {
            entry.State = TEntry::StRemove;
            entry.Wakeup.NotifyOne();
        }
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::TABLET_RESOLVER_ACTOR;
    }

    TTabletResolver(const TIntrusivePtr<TTabletResolverConfig>& config)
        : Config(config)
        , TabletCache(new NCache::T2QCacheConfig())
    {
        TabletCache.SetOverflowCallback([this](const NCache::ICache<ui64, TEntry*>& cache) {
            return cache.GetUsedSize() >= Config->TabletCacheLimit;
        });

        TabletCache.SetEvictionCallback([this](const ui64& key, TEntry*& value, ui64 size) {
            Y_UNUSED(key);
            Y_UNUSED(size);

            if (!value) {
                // Moved from, but not erased?
                return;
            }

            OnEvictEntry(*value);
        });
    }

    ~TTabletResolver() {
        TabletCache.SetEvictionCallback(&TTabletCache::DefaultEvictionCallback);
        TabletCache.SetOverflowCallback(&TTabletCache::DefaultOverflowCallback);
    }

    void Bootstrap() {
        RefreshNodes();
        Become(&TThis::StateWork);

        auto tablets = GetServiceCounters(AppData()->Counters, "tablets");

        SelectedLeaderLocal = tablets->GetCounter("TabletResolver/SelectedLeaderLocal", true);
        SelectedLeaderLocalDc = tablets->GetCounter("TabletResolver/SelectedLeaderLocalDc", true);
        SelectedLeaderOtherDc = tablets->GetCounter("TabletResolver/SelectedLeaderOtherDc", true);
        SelectedFollowerLocal = tablets->GetCounter("TabletResolver/SelectedFollowerLocal", true);
        SelectedFollowerLocalDc = tablets->GetCounter("TabletResolver/SelectedFollowerLocalDc", true);
        SelectedFollowerOtherDc = tablets->GetCounter("TabletResolver/SelectedFollowerOtherDc", true);
        SelectedNone = tablets->GetCounter("TabletResolver/SelectedNone", true);

        InFlyResolveCounter = tablets->GetCounter("TabletResolver/InFly");
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTabletResolver::TEvForward, Handle);
            hFunc(TEvTabletResolver::TEvTabletProblem, Handle);
            hFunc(TEvTabletResolver::TEvNodeProblem, Handle);
            hFunc(TEvTablet::TEvPong, Handle);
            hFunc(TEvInterconnect::TEvNodeConnected, Handle);
            hFunc(TEvInterconnect::TEvNodeDisconnected, Handle);
            fFunc(TEvStateStorage::TEvInfo::EventType, HandleStream);
            fFunc(TEvTablet::TEvTabletStateUpdate::EventType, HandleStream);
            fFunc(TEvents::TEvUndelivered::EventType, HandleStream);
            hFunc(TEvInterconnect::TEvNodesInfo, Handle);
            hFunc(TEvPrivate::TEvRefreshNodes, Handle);
            default:
                BLOG_WARN("TTabletResolver::StateWork unexpected event type: " << Hex(ev->GetTypeRewrite()) << " event: " << ev->ToString());
                break;
        }
    }
};

IActor* CreateTabletResolver(const TIntrusivePtr<TTabletResolverConfig>& config) {
    return new TTabletResolver(config);
}

TActorId MakeTabletResolverID() {
    const char x[12] = "TabletProxy";
    return TActorId(0, TStringBuf(x, 12));
}

}
