#include <ydb/core/base/tablet_resolver.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/base/statestorage.h>
#include <ydb/core/base/tabletid.h>
#include <ydb/core/base/tablet.h>
#include <ydb/core/base/appdata.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/interconnect/interconnect.h>
#include <ydb/core/util/cache.h>
#include <ydb/core/util/queue_oneone_inplace.h>
#include <util/generic/map.h>
#include <util/generic/deque.h>
#include <library/cpp/random_provider/random_provider.h>


namespace NKikimr {

const TDuration TabletResolverNegativeCacheTimeout = TDuration::MilliSeconds(300);
const TDuration TabletResolverRefreshNodesPeriod = TDuration::Seconds(60);

class TTabletResolver : public TActorBootstrapped<TTabletResolver> {
    struct TEvPrivate {
        enum EEv {
            EvPingTimeout = EventSpaceBegin(TEvents::ES_PRIVATE),
            EvStopListRemoval,
            EvRefreshNodes,
            EvEnd
        };

        struct TEvPingTimeout : public TEventLocal<TEvPingTimeout, EvPingTimeout> {
            const ui64 TabletID;
            TSchedulerCookieHolder Cookie;

            TEvPingTimeout(ui64 tabletId, ISchedulerCookie *cookie)
                : TabletID(tabletId)
                , Cookie(cookie)
            {}

            TString ToString() const {
                TStringStream str;
                str << "{EvPingTimeout TabletID: " << TabletID;
                if (Cookie.Get()) {
                    str << " Cookie: present";
                }
                else {
                    str << " Cookie: nullptr";
                }
                str << "}";
                return str.Str();
            }
        };

        struct TEvStopListRemoval : public TEventLocal<TEvStopListRemoval, EvStopListRemoval> {
            const ui64 TabletID;

            TEvStopListRemoval(ui64 tabletId)
                : TabletID(tabletId)
            {}
        };

        struct TEvRefreshNodes : public TEventLocal<TEvRefreshNodes, EvRefreshNodes> {
        };

        static_assert(EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE)");
    };

    struct TEntry {
        enum EState {
            StInit,
            StInitResolve,
            StNormal,
            StProblemResolve,
            StProblemPing,
            StFollowerUpdate,
        };

        static const char* StateToString(EState state) {
            switch (state) {
            case StInit: return "StInit";
            case StInitResolve: return "StInitResolve";
            case StNormal: return "StNormal";
            case StProblemResolve: return "StProblemResolve";
            case StProblemPing: return "StProblemPing";
            case StFollowerUpdate: return "StFollowerUpdate";
            default: return "Unknown";
            }
        }

        struct TQueueEntry {
            TInstant AddInstant;
            TEvTabletResolver::TEvForward::TPtr Ev;

            TQueueEntry(TInstant instant, TEvTabletResolver::TEvForward::TPtr &ev)
                : AddInstant(instant)
                , Ev(ev)
            {}
        };

        typedef TOneOneQueueInplace<TQueueEntry *, 64> TQueueType;

        EState State = StInit;

        TAutoPtr<TQueueType, TQueueType::TPtrCleanDestructor> Queue;
        TActorId KnownLeader;
        TActorId KnownLeaderTablet;

        TInstant LastResolved;
        TInstant LastPing;

        TSchedulerCookieHolder Cookie;

        TVector<std::pair<TActorId, TActorId>> KnownFollowers;

        ui64 CacheEpoch = 0;
        ui64 LastCheckEpoch = 0;

        TEntry() {}
    };

    struct TResolveInfo {
    public:
        TEvTabletResolver::TEvForward::TResolveFlags ResFlags;
        TDuration SinceLastResolve;
        ui32 NumLocal;
        ui32 NumLocalDc;
        ui32 NumOtherDc;
        bool * NeedFollowerUpdate;

        TResolveInfo(const TEvTabletResolver::TEvForward& msg, const TDuration& sinceResolve, bool * needFollowerUpdate)
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

    typedef NCache::TUnboundedCacheOnMap<ui64, TAutoPtr<TEntry>> TUnresolvedTablets;
    typedef NCache::T2QCache<ui64, TAutoPtr<TEntry>> TResolvedTablets;

    TIntrusivePtr<TTabletResolverConfig> Config;
    TActorSystem* ActorSystem;
    TUnresolvedTablets UnresolvedTablets;
    TResolvedTablets ResolvedTablets;
    THashSet<ui64> TabletsOnStopList;
    THashMap<ui32, TString> NodeToDcMapping;

    THashMap<ui32, ui64> NodeProblems;
    ui64 LastNodeProblemsUpdateEpoch = 0;

    ui64 LastCacheEpoch = 0;

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

    void ResolveRequest(ui64 tabletId, const TActorContext &ctx) {
        const TActorId ssproxy = MakeStateStorageProxyID();
        ctx.Send(ssproxy, new TEvStateStorage::TEvLookup(tabletId, 0), IEventHandle::FlagTrackDelivery, tabletId);

        InFlyResolveCounter->Inc();
    }

    bool PushQueue(TEvTabletResolver::TEvForward::TPtr &ev, TEntry &entry, const TActorContext &ctx) {
        if (!entry.Queue)
            entry.Queue.Reset(new TEntry::TQueueType());
        entry.Queue->Push(new TEntry::TQueueEntry(ctx.Now(), ev));
        return true;
    }

    std::pair<TActorId, TActorId> SelectForward(const TActorContext& ctx, const TEntry& entry, TResolveInfo& info, ui64 tabletId)
    {
        const ui32 selfNode = ctx.SelfID.NodeId();
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

        bool countLeader = (entry.State == TEntry::StNormal || entry.State == TEntry::StFollowerUpdate);
        if (countLeader) {
            bool isLocal = (entry.KnownLeader.NodeId() == selfNode);
            bool isLocalDc = selfDc && leaderDc == selfDc;
            info.Count(isLocal, isLocalDc);

            ui32 prio = info.ResFlags.GetTabletPriority(isLocal, isLocalDc, false);
            if (prio)
                addCandidate(prio, TCandidate{ entry.KnownLeader, entry.KnownLeaderTablet, isLocal, isLocalDc, true });
            else
                ++disallowed;
        }

        if (info.ResFlags.AllowFollower()) {
            for (const auto &x : entry.KnownFollowers) {
                bool isLocal = (x.first.NodeId() == selfNode);
                bool isLocalDc = selfDc && FindNodeDc(x.first.NodeId()) == selfDc;
                info.Count(isLocal, isLocalDc);

                ui32 prio = info.ResFlags.GetTabletPriority(isLocal, isLocalDc, true);
                if (prio)
                    addCandidate(prio, TCandidate{ x.first, x.second, isLocal, isLocalDc, false });
                else
                    ++disallowed;
            }
        }

        auto dcName = [](const std::optional<TString>& x) { return x ? x->data() : "<none>"; };

        if (!winners.empty()) {
            size_t winnerIndex = (winners.size() == 1 ? 0 : (AppData(ctx)->RandomProvider->GenRand64() % winners.size()));
            const TCandidate& winner = winners[winnerIndex];

            LOG_DEBUG(ctx, NKikimrServices::TABLET_RESOLVER,
                "SelectForward node %" PRIu32 " selfDC %s leaderDC %s %s"
                " local %" PRIu32 " localDc %" PRIu32 " other %" PRIu32 " disallowed %" PRIu32
                " tabletId: %" PRIu64 " followers: %" PRIu64 " countLeader %" PRIu32
                " allowFollowers %" PRIu32 " winner: %s",
                selfNode, dcName(selfDc), dcName(leaderDc), info.ResFlags.ToString().data(),
                info.NumLocal, info.NumLocalDc, info.NumOtherDc, disallowed,
                tabletId, entry.KnownFollowers.size(), countLeader, info.ResFlags.AllowFollower(),
                winner.KnownLeader.ToString().c_str());

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

        LOG_INFO(ctx, NKikimrServices::TABLET_RESOLVER,
            "No candidates for SelectForward, node %" PRIu32 " selfDC %s leaderDC %s %s"
            " local %" PRIu32 " localDc %" PRIu32 " other %" PRIu32 " disallowed %" PRIu32,
            selfNode, dcName(selfDc), dcName(leaderDc), info.ResFlags.ToString().data(),
            info.NumLocal, info.NumLocalDc, info.NumOtherDc, disallowed);

        SelectedNone->Inc();

        return std::make_pair(TActorId(), TActorId());
    }

    bool SendForward(const TActorId &sender, const TEntry &entry, TEvTabletResolver::TEvForward *msg,
                     const TActorContext &ctx, bool * needFollowerUpdate = nullptr) {
        TResolveInfo info(*msg, ctx.Now() - entry.LastResolved, needFollowerUpdate); // fills needFollowerUpdate in dtor
        const std::pair<TActorId, TActorId> endpoint = SelectForward(ctx, entry, info, msg->TabletID);
        if (endpoint.first) {
            ctx.Send(sender, new TEvTabletResolver::TEvForwardResult(msg->TabletID, endpoint.second, endpoint.first, LastCacheEpoch));
            if (!!msg->Ev) {
                ctx.ExecutorThread.Send(IEventHandle::Forward(std::move(msg->Ev), msg->SelectActor(endpoint.second, endpoint.first)));
            }
            return true;
        } else {
            return false;
        }
    }

    void SendQueued(ui64 tabletId, TEntry &entry, const TActorContext &ctx) {
        if (TEntry::TQueueType *queue = entry.Queue.Get()) {
            for (TAutoPtr<TEntry::TQueueEntry> x = queue->Pop(); !!x; x.Reset(queue->Pop())) {
                TEvTabletResolver::TEvForward *msg = x->Ev->Get();
                if (!SendForward(x->Ev->Sender, entry, msg, ctx))
                    ctx.Send(x->Ev->Sender, new TEvTabletResolver::TEvForwardResult(NKikimrProto::ERROR, tabletId));
            }
            entry.Queue.Destroy();
        }
    }

    void SendPing(ui64 tabletId, TEntry &entry, const TActorContext &ctx) {
        ctx.Send(entry.KnownLeader, new TEvTablet::TEvPing(tabletId, 0), IEventHandle::FlagTrackDelivery, tabletId); // no subscribe for reason
        entry.Cookie.Reset(ISchedulerCookie::Make3Way());

        const TDuration timeout = TDuration::MilliSeconds(500);
        ctx.Schedule(timeout, new TEvPrivate::TEvPingTimeout(tabletId, entry.Cookie.Get()), entry.Cookie.Get());
    }

    void ApplyEntryInfo(TEvStateStorage::TEvInfo& msg, TEntry &entry, const TActorContext &ctx) {
        entry.KnownLeader = msg.CurrentLeader;
        entry.KnownLeaderTablet = msg.CurrentLeaderTablet;
        entry.LastResolved = ctx.Now();
        entry.KnownFollowers = std::move(msg.Followers);
        entry.CacheEpoch = ++LastCacheEpoch;

        LOG_DEBUG(ctx, NKikimrServices::TABLET_RESOLVER,
                  "ApplyEntry leader tabletId: %" PRIu64 " followers: %" PRIu64,
                  msg.TabletID, entry.KnownFollowers.size());
        SendQueued(msg.TabletID, entry, ctx);
    }

    void DropEntry(ui64 tabletId, TEntry& entry, const TActorContext &ctx) {
        LOG_DEBUG(ctx, NKikimrServices::TABLET_RESOLVER,
                  "DropEntry tabletId: %" PRIu64 " followers: %" PRIu64,
                  tabletId, entry.KnownFollowers.size());
        if (TEntry::TQueueType *queue = entry.Queue.Get()) {
            for (TAutoPtr<TEntry::TQueueEntry> x = queue->Pop(); !!x; x.Reset(queue->Pop())) {
                ctx.Send(x->Ev->Sender, new TEvTabletResolver::TEvForwardResult(NKikimrProto::ERROR, tabletId));
            }
        }
        ResolvedTablets.Erase(tabletId);
        UnresolvedTablets.Erase(tabletId);

        if (TabletResolverNegativeCacheTimeout) {
            if (TabletsOnStopList.emplace(tabletId).second)
                Schedule(TabletResolverNegativeCacheTimeout, new TEvPrivate::TEvStopListRemoval(tabletId));
        }
    }

    TAutoPtr<TEntry>& GetEntry(ui64 tabletId, const TActorContext &ctx) {
        TAutoPtr<TEntry>* entryPtr;
        if (!ResolvedTablets.Find(tabletId, entryPtr)) {
            if (!UnresolvedTablets.Find(tabletId, entryPtr)) {
                ActorSystem = ctx.ExecutorThread.ActorSystem;
                UnresolvedTablets.Insert(tabletId, TAutoPtr<TEntry>(new TEntry()), entryPtr);
            }
        }

        return *entryPtr;
    }

    TAutoPtr<TEntry>* FindEntry(ui64 tabletId) {
        TAutoPtr<TEntry>* entryPtr;
        if (!ResolvedTablets.Find(tabletId, entryPtr)) {
            if (!UnresolvedTablets.Find(tabletId, entryPtr)) {
                return nullptr;
            }
        }

        return entryPtr;
    }

    void MoveEntryToResolved(ui64 tabletId, TAutoPtr<TEntry>& entry) {
        TAutoPtr<TEntry>* resolvedEntryPtr;
        Y_ABORT_UNLESS(ResolvedTablets.Insert(tabletId, entry, resolvedEntryPtr));
        Y_ABORT_UNLESS(UnresolvedTablets.Erase(tabletId));
    }

    void MoveEntryToUnresolved(ui64 tabletId, TAutoPtr<TEntry>& entry) {
        TAutoPtr<TEntry>* unresolvedEntryPtr;
        Y_ABORT_UNLESS(UnresolvedTablets.Insert(tabletId, entry, unresolvedEntryPtr));
        Y_ABORT_UNLESS(ResolvedTablets.Erase(tabletId));
    }

    void CheckDelayedNodeProblem(ui64 tabletId, const TActorContext &ctx) {
        TAutoPtr<TEntry>* entryHolder = FindEntry(tabletId);
        if (!entryHolder) {
            return;
        }

        TEntry &entry = *entryHolder->Get();

        switch (entry.State) {
        case TEntry::StNormal:
        case TEntry::StFollowerUpdate:
            if (entry.CacheEpoch < LastNodeProblemsUpdateEpoch && entry.LastCheckEpoch < LastNodeProblemsUpdateEpoch) {
                entry.LastCheckEpoch = LastCacheEpoch;

                auto *pMaxProblemEpoch = NodeProblems.FindPtr(entry.KnownLeader.NodeId());
                if (pMaxProblemEpoch && entry.CacheEpoch <= *pMaxProblemEpoch) {
                    LOG_DEBUG(ctx, NKikimrServices::TABLET_RESOLVER,
                            "Delayed invalidation of tabletId: %" PRIu64
                            " leader: %s by NodeId", tabletId, entry.KnownLeader.ToString().c_str());
                    ResolveRequest(tabletId, ctx);
                    entry.State = TEntry::StProblemResolve;
                    MoveEntryToUnresolved(tabletId, *entryHolder);
                    return;
                }

                auto itDst = entry.KnownFollowers.begin();
                auto itSrc = entry.KnownFollowers.begin();
                while (itSrc != entry.KnownFollowers.end()) {
                    pMaxProblemEpoch = NodeProblems.FindPtr(itSrc->first.NodeId());
                    if (pMaxProblemEpoch && entry.CacheEpoch <= *pMaxProblemEpoch) {
                        LOG_DEBUG(ctx, NKikimrServices::TABLET_RESOLVER,
                            "Delayed invalidation of tabletId: %" PRIu64
                            " follower: %s by nodeId", tabletId, itSrc->first.ToString().c_str());
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
                    ResolveRequest(tabletId, ctx);
                    entry.State = TEntry::StFollowerUpdate;
                }
            }
            break;

        default:
            break;
        }
    }

    void Handle(TEvTabletResolver::TEvForward::TPtr &ev, const TActorContext &ctx) {
        TEvTabletResolver::TEvForward *msg = ev->Get();
        const ui64 tabletId = msg->TabletID;

        // allow some requests to bypass negative caching in low-load case
        if (InFlyResolveCounter->Val() > 20 && TabletsOnStopList.contains(tabletId)) {
            Send(ev->Sender, new TEvTabletResolver::TEvForwardResult(NKikimrProto::ERROR, tabletId));
            return;
        }

        CheckDelayedNodeProblem(tabletId, ctx);

        TAutoPtr<TEntry> &entryHolder = GetEntry(tabletId, ctx);
        TEntry& entry = *entryHolder.Get();

        LOG_DEBUG(ctx, NKikimrServices::TABLET_RESOLVER,
                  "Handle TEvForward tabletId: %" PRIu64 " entry.State: %s ev: %s",
                  tabletId, TEntry::StateToString(entry.State), msg->ToString().data());

        switch (entry.State) {
        case TEntry::StInit:
            {
                PushQueue(ev, entry, ctx);
                ResolveRequest(tabletId, ctx);
                entry.State = TEntry::StInitResolve;
            }
            break;
        case TEntry::StInitResolve:
            PushQueue(ev, entry, ctx);
            break;
        case TEntry::StNormal: {
            bool needFollowerUpdate = false;
            if (!SendForward(ev->Sender, entry, msg, ctx, &needFollowerUpdate)) {
                PushQueue(ev, entry, ctx);
                ResolveRequest(tabletId, ctx);
                entry.State = TEntry::StFollowerUpdate;
            }
            if (needFollowerUpdate) {
                ResolveRequest(tabletId, ctx);
                entry.State = TEntry::StFollowerUpdate;
            }
            break;
        }
        case TEntry::StProblemResolve:
        case TEntry::StProblemPing:
            PushQueue(ev, entry, ctx);
            break;
        case TEntry::StFollowerUpdate:
            if (!SendForward(ev->Sender, entry, msg, ctx))
                PushQueue(ev, entry, ctx);
            break;
        default:
            Y_ABORT();
        }
    }

    void Handle(TEvTabletResolver::TEvTabletProblem::TPtr &ev, const TActorContext &ctx) {
        TEvTabletResolver::TEvTabletProblem *msg = ev->Get();
        const ui64 tabletId = msg->TabletID;

        TAutoPtr<TEntry>* entryHolder = FindEntry(tabletId);
        if (!entryHolder) {
            LOG_DEBUG(ctx, NKikimrServices::TABLET_RESOLVER, "Handle TEvTabletProblem tabletId: %" PRIu64
                " no entyHolder", tabletId);
            return;
        }
        TEntry &entry = *entryHolder->Get();

        LOG_DEBUG(ctx, NKikimrServices::TABLET_RESOLVER, "Handle TEvTabletProblem tabletId: %" PRIu64
            " entry.State: %s", tabletId, TEntry::StateToString(entry.State));

        switch (entry.State) {
        case TEntry::StInit:
        case TEntry::StInitResolve:
            break;
        case TEntry::StNormal:
            if (!msg->TabletActor || entry.KnownLeaderTablet == msg->TabletActor) {
                ResolveRequest(tabletId, ctx);
                entry.State = TEntry::StProblemResolve;
                MoveEntryToUnresolved(tabletId, *entryHolder);
            } else {
                // find in follower list
                for (auto it = entry.KnownFollowers.begin(), end = entry.KnownFollowers.end(); it != end; ++it) {
                    if (it->second == msg->TabletActor) {
                        entry.KnownFollowers.erase(it);
                        ResolveRequest(tabletId, ctx);
                        entry.State = TEntry::StFollowerUpdate;
                        break;
                    }
                }
            }

            break;
        case TEntry::StProblemResolve:
        case TEntry::StProblemPing:
        case TEntry::StFollowerUpdate:
            for (auto it = entry.KnownFollowers.begin(), end = entry.KnownFollowers.end(); it != end; ++it) {
                if (it->second == msg->TabletActor) {
                    entry.KnownFollowers.erase(it);
                    break;
                }
            }
            break;
        default:
            Y_ABORT();
        }
    }

    void Handle(TEvTabletResolver::TEvNodeProblem::TPtr &ev, const TActorContext &ctx) {
        TEvTabletResolver::TEvNodeProblem *msg = ev->Get();
        const ui32 nodeId = msg->NodeId;
        const ui64 problemEpoch = msg->CacheEpoch;

        ui64 &maxProblemEpoch = NodeProblems[nodeId];
        if (maxProblemEpoch < problemEpoch) {
            LOG_DEBUG(ctx, NKikimrServices::TABLET_RESOLVER, "Handle TEvNodeProblem nodeId: %" PRIu32
                " max(problemEpoch): %" PRIu64, nodeId, problemEpoch);
            maxProblemEpoch = problemEpoch;
            LastNodeProblemsUpdateEpoch = ++LastCacheEpoch;
        } else {
            LOG_DEBUG(ctx, NKikimrServices::TABLET_RESOLVER, "Handle TEvNodeProblem nodeId: %" PRIu32
                " problemEpoch: %" PRIu64 " <= max(problemEpoch): %" PRIu64,
                nodeId, problemEpoch, maxProblemEpoch);
        }
    }

    void Handle(TEvStateStorage::TEvInfo::TPtr &ev, const TActorContext &ctx) {
        InFlyResolveCounter->Dec();

        TEvStateStorage::TEvInfo *msg = ev->Get();
        const ui64 tabletId = msg->TabletID;
        const bool success = (msg->Status == NKikimrProto::OK); // todo: handle 'locked' state

        TAutoPtr<TEntry>* entryHolder = FindEntry(tabletId);
        if (!entryHolder) {
            LOG_DEBUG(ctx, NKikimrServices::TABLET_RESOLVER, "Handle TEvInfo tabletId: %" PRIu64 " no entryHolder",
                tabletId);
            return;
        }
        TEntry &entry = *entryHolder->Get();

        LOG_DEBUG(ctx, NKikimrServices::TABLET_RESOLVER, "Handle TEvInfo tabletId: %" PRIu64
            " entry.State: %s success: %s ev: %s", tabletId, TEntry::StateToString(entry.State),
            (success ? "true" : "false"), ev->GetBase()->ToString().data());

        switch (entry.State) {
        case TEntry::StInit:
            Y_ABORT("must not happens");
        case TEntry::StInitResolve:
            if (success) {
                if (msg->CurrentLeaderTablet) {
                    entry.State = TEntry::StNormal;
                    ApplyEntryInfo(*msg, entry, ctx);
                    MoveEntryToResolved(tabletId, *entryHolder);
                } else {
                    // HACK: Don't cache invalid CurrentLeaderTablet
                    // FIXME: Use subscription + cache here to reduce the workload
                    ApplyEntryInfo(*msg, entry, ctx);
                    DropEntry(tabletId, entry, ctx);
                }
            } else {
                DropEntry(tabletId, entry, ctx);
            }
            break;
        case TEntry::StFollowerUpdate:
            if (success) {
                if (msg->CurrentLeaderTablet) {
                    entry.State = TEntry::StNormal;
                    ApplyEntryInfo(*msg, entry, ctx);
                } else {
                    // HACK: Don't cache invalid CurrentLeaderTablet
                    // FIXME: Use subscription + cache here to reduce the workload
                    ApplyEntryInfo(*msg, entry, ctx);
                    DropEntry(tabletId, entry, ctx);
                }
            } else {
                DropEntry(tabletId, entry, ctx);
            }
            break;
        case TEntry::StProblemResolve:
            if (success) {
                if (entry.KnownLeader == msg->CurrentLeader) {
                    if (!(entry.KnownLeaderTablet == msg->CurrentLeaderTablet || !entry.KnownLeaderTablet)) {
                        DropEntry(tabletId, entry, ctx); // got info but not full, occurs on transitional cluster states
                    } else {
                        entry.State = TEntry::StProblemPing;
                        entry.KnownLeaderTablet = msg->CurrentLeaderTablet;
                        entry.KnownFollowers = std::move(msg->Followers);
                        SendPing(tabletId, entry, ctx);
                    }
                } else {
                    if (msg->CurrentLeaderTablet) {
                        entry.State = TEntry::StNormal;
                        ApplyEntryInfo(*msg, entry, ctx);
                        MoveEntryToResolved(tabletId, *entryHolder);
                    } else {
                        ApplyEntryInfo(*msg, entry, ctx);
                        DropEntry(tabletId, entry, ctx);
                    }
                }
            } else {
                DropEntry(tabletId, entry, ctx);
            }
            break;
        case TEntry::StProblemPing:
        case TEntry::StNormal:
            break;
        default:
            Y_ABORT();
        }
    }

    void Handle(TEvTablet::TEvPong::TPtr &ev, const TActorContext &ctx) {
        NKikimrTabletBase::TEvPong &record = ev->Get()->Record;
        const ui64 tabletId = record.GetTabletID();

        TAutoPtr<TEntry>* entryHolder = FindEntry(tabletId);
        if (!entryHolder) {
            LOG_DEBUG(ctx, NKikimrServices::TABLET_RESOLVER, "Handle TEvPong tabletId: %" PRIu64 " no entryHolder",
                tabletId);
            return;
        }
        TEntry &entry = *entryHolder->Get();

        LOG_DEBUG(ctx, NKikimrServices::TABLET_RESOLVER, "Handle TEvPong tabletId: %" PRIu64 " entry.State: %s",
            tabletId, TEntry::StateToString(entry.State));

        switch (entry.State) {
        case TEntry::StInit:
        case TEntry::StInitResolve:
        case TEntry::StNormal:
        case TEntry::StProblemResolve:
        case TEntry::StFollowerUpdate:
            break;
        case TEntry::StProblemPing:
            if (ev->Sender == entry.KnownLeader) {
                entry.Cookie.Detach();
                entry.State = TEntry::StNormal;
                SendQueued(tabletId, entry, ctx);
                MoveEntryToResolved(tabletId, *entryHolder);
            }
            break;
        default:
            Y_ABORT();
        }
    }

    void Handle(TEvents::TEvUndelivered::TPtr &ev, const TActorContext &ctx) {
        // cold be Ping or Initial Statestorage resolve
        const ui64 tabletId = ev->Cookie;
        TAutoPtr<TEntry>* entryHolder = FindEntry(tabletId);
        if (!entryHolder)
            return;

        TEntry &entry = *entryHolder->Get();
        if (ev->Get()->SourceType == TEvStateStorage::TEvLookup::EventType) {
            InFlyResolveCounter->Dec();
            DropEntry(tabletId, entry, ctx);
            return;
        }

        switch (entry.State) {
        case TEntry::StInit:
        case TEntry::StInitResolve:
        case TEntry::StNormal:
        case TEntry::StProblemResolve:
        case TEntry::StFollowerUpdate:
            break;
        case TEntry::StProblemPing:
            if (ev->Sender == entry.KnownLeader) {
                DropEntry(tabletId, entry, ctx);
            }
            break;
        default:
            Y_ABORT();
        }
    }

    void Handle(TEvPrivate::TEvPingTimeout::TPtr &ev, const TActorContext &ctx) {
        TEvPrivate::TEvPingTimeout *msg = ev->Get();
        const ui64 tabletId = msg->TabletID;

        TAutoPtr<TEntry>* entryHolder = FindEntry(tabletId);
        if (!entryHolder) {
            LOG_DEBUG(ctx, NKikimrServices::TABLET_RESOLVER, "Handle TEvPingTimeout tabletId: %" PRIu64
                " no entryHolder", tabletId);
            return;
        }
        TEntry &entry = *entryHolder->Get();

        LOG_DEBUG(ctx, NKikimrServices::TABLET_RESOLVER, "Handle TEvPingTimeout tabletId: %" PRIu64 " entry.State: %s",
            tabletId, TEntry::StateToString(entry.State));

        switch (entry.State) {
        case TEntry::StInit:
        case TEntry::StInitResolve:
        case TEntry::StNormal:
        case TEntry::StProblemResolve:
        case TEntry::StFollowerUpdate:
            break;
        case TEntry::StProblemPing:
            if (msg->Cookie.DetachEvent()) {
                DropEntry(tabletId, entry, ctx);
            }
            break;
        default:
            Y_ABORT();
        }
    }

    void Handle(TEvPrivate::TEvStopListRemoval::TPtr &ev, const TActorContext &) {
        TEvPrivate::TEvStopListRemoval *msg = ev->Get();
        TabletsOnStopList.erase(msg->TabletID);
    }

    void Handle(TEvInterconnect::TEvNodesInfo::TPtr &ev, const TActorContext &ctx) {
        Y_UNUSED(ctx);

        const TEvInterconnect::TEvNodesInfo *msg = ev->Get();
        bool distinct = false;
        for (const auto &nodeInfo : msg->Nodes) {
            if (nodeInfo.Location.GetDataCenterId() != msg->Nodes[0].Location.GetDataCenterId())
                distinct = true;
            NodeToDcMapping[nodeInfo.NodeId] = nodeInfo.Location.GetDataCenterId();
        }

        if (!distinct)
            NodeToDcMapping.clear();

        ctx.Schedule(TabletResolverRefreshNodesPeriod, new TEvPrivate::TEvRefreshNodes);
    }

    void Handle(TEvPrivate::TEvRefreshNodes::TPtr &, const TActorContext &ctx) {
        RefreshNodes(ctx);
    }

    void RefreshNodes(const TActorContext &ctx) {
        ctx.Send(GetNameserviceActorId(), new TEvInterconnect::TEvListNodes());
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::TABLET_RESOLVER_ACTOR;
    }

    TTabletResolver(const TIntrusivePtr<TTabletResolverConfig> &config)
        : Config(config)
        , ActorSystem(nullptr)
        , ResolvedTablets(new NCache::T2QCacheConfig())
    {
        ResolvedTablets.SetOverflowCallback([=](const NCache::ICache<ui64, TAutoPtr<TEntry>>& cache) {
            return cache.GetUsedSize() >= Config->TabletCacheLimit;
        });

        ResolvedTablets.SetEvictionCallback([&](const ui64& key, TAutoPtr<TEntry>& value, ui64 size) {
            Y_UNUSED(size);

            if (!value)
                return;

            if (TEntry::TQueueType *queue = value->Queue.Get()) {
                for (TAutoPtr<TEntry::TQueueEntry> x = queue->Pop(); !!x; x.Reset(queue->Pop())) {
                    ActorSystem->Send(x->Ev->Sender, new TEvTabletResolver::TEvForwardResult(NKikimrProto::RACE, key));
                }
            }
        });
    }

    ~TTabletResolver() {
        ResolvedTablets.SetEvictionCallback(&TResolvedTablets::DefaultEvictionCallback);
    }

    void Bootstrap(const TActorContext &ctx) {
        RefreshNodes(ctx);
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
            HFunc(TEvTabletResolver::TEvForward, Handle);
            HFunc(TEvTabletResolver::TEvTabletProblem, Handle);
            HFunc(TEvTabletResolver::TEvNodeProblem, Handle);
            HFunc(TEvStateStorage::TEvInfo, Handle);
            HFunc(TEvTablet::TEvPong, Handle);
            HFunc(TEvPrivate::TEvPingTimeout, Handle);
            HFunc(TEvPrivate::TEvStopListRemoval, Handle);
            HFunc(TEvInterconnect::TEvNodesInfo, Handle);
            HFunc(TEvPrivate::TEvRefreshNodes, Handle);
            HFunc(TEvents::TEvUndelivered, Handle);
            default:
                LOG_WARN(*TlsActivationContext, NKikimrServices::TABLET_RESOLVER, "TTabletResolver::StateWork unexpected event type: %" PRIx32
                    " event: %s", ev->GetTypeRewrite(), ev->ToString().data());
                break;
        }
    }
};

IActor* CreateTabletResolver(const TIntrusivePtr<TTabletResolverConfig> &config) {
    return new TTabletResolver(config);
}

TActorId MakeTabletResolverID() {
    const char x[12] = "TabletProxy";
    return TActorId(0, TStringBuf(x, 12));
}

}
