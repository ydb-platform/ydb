#include "statestorage_impl.h"
#include "tabletid.h"
#include "appdata.h"
#include "tablet.h"

#include <ydb/library/services/services.pb.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>

#include <util/generic/map.h>
#include <util/generic/hash_set.h>

#if defined BLOG_D || defined BLOG_I || defined BLOG_ERROR || defined BLOG_TRACE
#error log macro definition clash
#endif

#define BLOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::STATESTORAGE, stream)
#define BLOG_I(stream) LOG_INFO_S(*TlsActivationContext, NKikimrServices::STATESTORAGE, stream)
#define BLOG_W(stream) LOG_WARN_S(*TlsActivationContext, NKikimrServices::STATESTORAGE, stream)
#define BLOG_ERROR(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::STATESTORAGE, stream)

namespace NKikimr {

class TStateStorageReplica : public TActor<TStateStorageReplica> {
    TIntrusivePtr<TStateStorageInfo> Info;
    const ui32 ReplicaIndex;

    struct TTabletState {
        enum ETabletState {
            Unknown,
            Normal,
            Locked,
        };
    };

    struct TFollowerEntryInfo {
        TActorId FollowerSys;
        TActorId FollowerTablet;
        bool Candidate = false;

        TFollowerEntryInfo() = default;
        TFollowerEntryInfo(TActorId sys, TActorId tablet, bool candidate)
            : FollowerSys(sys)
            , FollowerTablet(tablet)
            , Candidate(candidate)
        {}
    };

    struct TEntry {
        TTabletState::ETabletState TabletState;
        TActorId CurrentLeader;
        TActorId CurrentLeaderTablet;
        TActorId CurrentGuardian;
        ui32 CurrentGeneration;
        ui32 CurrentStep;
        ui64 LockedFrom;

        TMap<TActorId, TFollowerEntryInfo> Followers; // guardian -> follower actors

        TEntry()
            : TabletState(TTabletState::Unknown)
            , CurrentGeneration(0)
            , CurrentStep(0)
            , LockedFrom(0)
        {}
    };

    typedef TMap<ui64, TEntry> TTablets;
    TTablets Tablets;

    TMap<ui32, std::map<ui64, ui64>> FollowerIndex; // node, tablet, refcounter

    ui64 Signature() const {
        return SelfId().LocalId();
    }

    template<typename TEv>
    bool CheckSignature(const TEv *msg) const {
        const bool ok = msg->Record.HasSignature() && msg->Record.GetSignature() == Signature();
        return ok;
    }

    void NotifyWithTabletInfo(const TActorId &recp, ui64 tabletId, ui64 cookie, const TEntry *entry) {
        THolder<TEvStateStorage::TEvReplicaInfo> msg;
        if (entry && entry->CurrentLeader) {
            const bool locked = (entry->TabletState == TTabletState::Locked);
            auto now = TActivationContext::Now();

            const ui64 lockedFor = (locked && (now.MicroSeconds() > entry->LockedFrom)) ? (now.MicroSeconds() - entry->LockedFrom) : 0;
            msg.Reset(new TEvStateStorage::TEvReplicaInfo(tabletId, entry->CurrentLeader, entry->CurrentLeaderTablet, entry->CurrentGeneration, entry->CurrentStep, locked, lockedFor));
            if (entry->Followers.size()) {
                msg->Record.MutableFollowerTablet()->Reserve(entry->Followers.size());
                msg->Record.MutableFollower()->Reserve(entry->Followers.size());
                for (const auto &xpair : entry->Followers) {
                    const TFollowerEntryInfo &followerInfo = xpair.second;
                    if (followerInfo.Candidate) {
                        ActorIdToProto(followerInfo.FollowerSys, msg->Record.AddFollowerCandidates());
                    } else {
                        ActorIdToProto(followerInfo.FollowerSys, msg->Record.AddFollower());
                        ActorIdToProto(followerInfo.FollowerTablet, msg->Record.AddFollowerTablet());
                    }
                }
            }
        } else {
            msg.Reset(new TEvStateStorage::TEvReplicaInfo(tabletId, NKikimrProto::ERROR));
        }
        msg->Record.SetCookie(cookie);
        msg->Record.SetSignature(Signature());
        msg->Record.SetConfigContentHash(Info->ContentHash());
        Send(recp, msg.Release());
    }

    void ReplyWithStatus(const TActorId &recp, ui64 tabletId, ui64 cookie, NKikimrProto::EReplyStatus status) {
        THolder<TEvStateStorage::TEvReplicaInfo> msg(new TEvStateStorage::TEvReplicaInfo(tabletId, status));
        msg->Record.SetCookie(cookie);
        msg->Record.SetSignature(Signature());
        msg->Record.SetConfigContentHash(Info->ContentHash());
        Send(recp, msg.Release());
    }

    bool EraseFollowerAndNotify(ui64 tabletId, TEntry &tabletEntry, TActorId followerGuardian) {
        if (!tabletEntry.Followers.erase(followerGuardian))
            return false;

        if (tabletEntry.CurrentGuardian)
            NotifyWithTabletInfo(followerGuardian, tabletId, 0, &tabletEntry);

        return true;
    }

    void ForgetFollower(ui64 tabletId, TActorId followerGuardian) {
        auto tabletIt = Tablets.find(tabletId);
        if (tabletIt == Tablets.end())
            return;

        const bool erased = EraseFollowerAndNotify(tabletId, tabletIt->second, followerGuardian);
        if (!erased)
            return;

        const ui32 followerNodeId = followerGuardian.NodeId();
        auto *followerIndex = FollowerIndex.FindPtr(followerNodeId);
        if (followerIndex == nullptr)
            return;

        auto it = followerIndex->find(tabletId);
        if (it == followerIndex->end())
            return;

        if (--it->second == 0)
            followerIndex->erase(it);
    }

    void PassAway() override {
        const ui32 selfNode = SelfId().NodeId();
        THashSet<ui32> nodesToUnsubscribe;

        for (auto &xpair : Tablets) {
            const auto &entry = xpair.second;

            if (entry.CurrentGuardian)
                Send(entry.CurrentGuardian, new TEvStateStorage::TEvReplicaShutdown());

            for (auto &spair : entry.Followers) {
                const TActorId followerGuardian = spair.first;
                if (followerGuardian.NodeId() != selfNode)
                    nodesToUnsubscribe.insert(followerGuardian.NodeId());
                Send(followerGuardian, new TEvStateStorage::TEvReplicaShutdown());
            }
        }

        for (ui32 xnode : nodesToUnsubscribe) {
            Send(TActivationContext::InterconnectProxy(xnode), new TEvents::TEvUnsubscribe());
        }

        TActor::PassAway();
    }

    void Handle(TEvStateStorage::TEvReplicaLookup::TPtr &ev) {
        TEvStateStorage::TEvReplicaLookup *msg = ev->Get();
        BLOG_D("Replica::Handle ev: " << msg->ToString());
        const ui64 tabletId = msg->Record.GetTabletID();
        TTablets::const_iterator it = Tablets.find(msg->Record.GetTabletID());
        if (it != Tablets.end())
            NotifyWithTabletInfo(ev->Sender, it->first, msg->Record.GetCookie(), &it->second);
        else
            NotifyWithTabletInfo(ev->Sender, tabletId, msg->Record.GetCookie(), nullptr);
    }

    void Handle(TEvStateStorage::TEvReplicaDumpRequest::TPtr &ev) {
        TAutoPtr<TEvStateStorage::TEvReplicaDump> dump = new TEvStateStorage::TEvReplicaDump();
        for (const auto &it : Tablets) {
            NKikimrStateStorage::TEvInfo& info = *dump->Record.AddInfo();
            info.SetTabletID(it.first);
            info.SetCurrentGeneration(it.second.CurrentGeneration);
            info.SetCurrentStep(it.second.CurrentStep);
            ActorIdToProto(it.second.CurrentLeader, info.MutableCurrentLeader());
            ActorIdToProto(it.second.CurrentLeaderTablet, info.MutableCurrentLeaderTablet());
            if (it.second.TabletState == TTabletState::Locked) {
                info.SetLockedFor(TActivationContext::Now().MicroSeconds() - it.second.LockedFrom);
            }
        }
        Send(ev->Sender, dump.Release(), 0, ev->Cookie);
    }

    void Handle(TEvStateStorage::TEvReplicaUpdate::TPtr &ev) {
        TEvStateStorage::TEvReplicaUpdate *msg = ev->Get();
        BLOG_D("Replica::Handle ev: " << msg->ToString());
        const ui64 tabletId = msg->Record.GetTabletID();

        TEntry *x = nullptr;
        auto tabletIt = Tablets.find(tabletId);
        if (tabletIt != Tablets.end())
            x = &tabletIt->second;

        const TActorId proposedLeader = ActorIdFromProto(msg->Record.GetProposedLeader());
        const ui32 proposedGeneration = msg->Record.GetProposedGeneration();
        const ui32 proposedStep = msg->Record.GetProposedStep();

        const bool allow = CheckSignature(msg) &&
            (!x
                || (proposedGeneration > x->CurrentGeneration)
                || (proposedGeneration == x->CurrentGeneration && proposedLeader == x->CurrentLeader && proposedStep >= x->CurrentStep)
            );

        if (allow) {
            if (tabletIt == Tablets.end())
                tabletIt = Tablets.insert(std::make_pair(tabletId, TEntry())).first;
            x = &tabletIt->second;

            if (x->CurrentLeader && proposedLeader != x->CurrentLeader)
                Send(x->CurrentLeader, new TEvStateStorage::TEvReplicaLeaderDemoted(tabletId, Signature()));

            x->CurrentGeneration = proposedGeneration;
            x->CurrentStep = proposedStep;
            x->CurrentLeader = proposedLeader;

            if (msg->Record.HasProposedLeaderTablet())
                x->CurrentLeaderTablet = ActorIdFromProto(msg->Record.GetProposedLeaderTablet());

            if (msg->Record.GetIsGuardian())
                x->CurrentGuardian = ev->Sender;

            x->TabletState = TTabletState::Normal;
            x->LockedFrom = 0;
        }

        NotifyWithTabletInfo(ev->Sender, tabletId, msg->Record.GetCookie(), x);
    }

    void Handle(TEvStateStorage::TEvReplicaCleanup::TPtr &ev) {
        const auto &record = ev->Get()->Record;
        BLOG_D("Replica::Handle ev: " << ev->Get()->ToString());
        const ui64 tabletId = record.GetTabletID();
        const TActorId proposedLeader = ActorIdFromProto(record.GetProposedLeader());

        auto tabletIt = Tablets.find(tabletId);
        if (tabletIt == Tablets.end() || tabletIt->second.CurrentLeader != proposedLeader)
            return;

        if (tabletIt->second.Followers) {
            BLOG_ERROR("trying to cleanup entry with attached followers. Suspicious! TabletId: " << tabletId);
            return;
        }

        Tablets.erase(tabletIt);
        // do not reply anything
    }

    void Handle(TEvStateStorage::TEvReplicaDelete::TPtr &ev) {
        TEvStateStorage::TEvReplicaDelete *msg = ev->Get();
        BLOG_D("Replica::Handle ev: " << msg->ToString());
        const ui64 tabletId = msg->Record.GetTabletID();

        auto tabletIt = Tablets.find(tabletId);
        if (tabletIt == Tablets.end()) {
            ReplyWithStatus(ev->Sender, tabletId, 0/*msg->Record.GetCookie()*/, NKikimrProto::ALREADY);
            return;
        }

        for (auto &sp : tabletIt->second.Followers) {
            const ui32 followerNodeId = sp.first.NodeId();
            if (auto *x = FollowerIndex.FindPtr(followerNodeId))
                x->erase(tabletId);
        }

        Tablets.erase(tabletIt);
        ReplyWithStatus(ev->Sender, tabletId, 0/*msg->Record.GetCookie()*/, NKikimrProto::OK);
    }

    void Handle(TEvStateStorage::TEvReplicaLock::TPtr &ev) {
        TEvStateStorage::TEvReplicaLock *msg = ev->Get();
        BLOG_D("Replica::Handle ev: " << msg->ToString());
        const ui64 tabletId = msg->Record.GetTabletID();
        const TActorId &sender = ev->Sender;

        if (CheckSignature(msg)) {
            TEntry &x = Tablets[tabletId];

            const ui32 proposedGeneration = msg->Record.GetProposedGeneration();
            const TActorId proposedLeader = ActorIdFromProto(msg->Record.GetProposedLeader());

            const bool allow = (proposedGeneration > x.CurrentGeneration
                || (x.TabletState == TTabletState::Locked && proposedGeneration == x.CurrentGeneration && proposedLeader == x.CurrentLeader));

            if (allow) {
                if (x.CurrentLeader && proposedLeader != x.CurrentLeader)
                    Send(x.CurrentLeader, new TEvStateStorage::TEvReplicaLeaderDemoted(tabletId, Signature()));

                x.CurrentGeneration = proposedGeneration;
                x.CurrentStep = 0;
                x.CurrentLeader = proposedLeader;
                x.CurrentLeaderTablet = TActorId();
                x.CurrentGuardian = TActorId();
                x.TabletState = TTabletState::Locked;
                x.LockedFrom = TActivationContext::Now().MicroSeconds();
            }

            NotifyWithTabletInfo(sender, tabletId, msg->Record.GetCookie(), &x);
        } else {
            NotifyWithTabletInfo(sender, tabletId, msg->Record.GetCookie(), nullptr);
        }
    }

    void Handle(TEvStateStorage::TEvReplicaRegFollower::TPtr &ev) {
        const NKikimrStateStorage::TEvRegisterFollower &record = ev->Get()->Record;
        const ui64 tabletId = record.GetTabletID();
        TEntry &x = Tablets[tabletId]; // could lead to creation of zombie entries when follower exist w/o leader so we must filter on info

        const TActorId follower = ActorIdFromProto(record.GetFollower());
        const TActorId tablet = ActorIdFromProto(record.GetFollowerTablet());
        const bool isCandidate = record.HasCandidate() && record.GetCandidate();

        auto insIt = x.Followers.emplace(ev->Sender, TFollowerEntryInfo(follower, tablet, isCandidate));
        TFollowerEntryInfo &followerInfo = insIt.first->second;

        if (insIt.second == false) { // already known
            Y_ABORT_UNLESS(insIt.first->second.FollowerSys == follower);

            const bool hasChanges = (followerInfo.FollowerTablet != tablet) || (followerInfo.Candidate != isCandidate);
            if (!hasChanges)
                return;

            followerInfo.Candidate = isCandidate;
            followerInfo.FollowerTablet = tablet;
        } else { // new entry
            auto indIt = FollowerIndex[follower.NodeId()].insert(std::make_pair(tabletId, 1));
            if (indIt.second == false)
                ++indIt.first->second;

            // and now send ping to detect lost unreg event and subscribe to session
            Send(ev->Sender, // ping replica guardian, not tablet as follower could be promoted to leader
                new TEvTablet::TEvPing(tabletId, 0),
                IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession,
                tabletId);
        }

        if (x.CurrentGuardian)
            NotifyWithTabletInfo(x.CurrentGuardian, tabletId, 0, &x);
    }

    void Handle(TEvStateStorage::TEvReplicaUnregFollower::TPtr &ev) {
        const TEvStateStorage::TEvReplicaUnregFollower *msg = ev->Get();
        const ui64 tabletId = msg->Record.GetTabletID();
        ForgetFollower(tabletId, ev->Sender);
    }

    void Handle(TEvents::TEvUndelivered::TPtr &ev) {
        const ui64 tabletId = ev->Cookie;
        ForgetFollower(tabletId, ev->Sender);
    }

    void Handle(TEvents::TEvPing::TPtr &ev) {
        Send(ev->Sender, new TEvents::TEvPong());
    }

    void Handle(TEvInterconnect::TEvNodeDisconnected::TPtr &ev) {
        const ui32 nodeId = ev->Get()->NodeId;

        auto *followerIndex = FollowerIndex.FindPtr(nodeId);
        if (followerIndex == nullptr)
            return;

        for (const auto &xpair : *followerIndex) {
            const ui64 tabletId = xpair.first;

            if (auto *x = Tablets.FindPtr(tabletId)) {
                bool changed = false;

                for (auto it = x->Followers.begin(); it != x->Followers.end();) {
                    if (it->first.NodeId() == nodeId) {
                        changed = true;
                        it = x->Followers.erase(it);
                    }
                    else
                        ++it;
                }

                if (changed && x->CurrentGuardian)
                    NotifyWithTabletInfo(x->CurrentGuardian, tabletId, 0, x);
            }
        }

        followerIndex->clear();
    }

    void Handle(TEvStateStorage::TEvUpdateGroupConfig::TPtr ev) {
        Info = ev->Get()->GroupConfig;
        Y_ABORT_UNLESS(!ev->Get()->BoardConfig);
        Y_ABORT_UNLESS(!ev->Get()->SchemeBoardConfig);
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::SS_REPLICA;
    }

    TStateStorageReplica(const TIntrusivePtr<TStateStorageInfo> &info, ui32 replicaIndex)
        : TActor(&TThis::StateInit)
        , Info(info)
        , ReplicaIndex(replicaIndex)
    {
        Y_UNUSED(ReplicaIndex);
    }

    STATEFN(StateInit) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvStateStorage::TEvReplicaLookup, Handle);
            hFunc(TEvStateStorage::TEvReplicaDumpRequest, Handle);
            hFunc(TEvStateStorage::TEvReplicaUpdate, Handle);
            hFunc(TEvStateStorage::TEvReplicaLock, Handle);
            hFunc(TEvStateStorage::TEvReplicaRegFollower, Handle);
            hFunc(TEvStateStorage::TEvReplicaUnregFollower, Handle);
            hFunc(TEvStateStorage::TEvReplicaDelete, Handle);
            hFunc(TEvStateStorage::TEvReplicaCleanup, Handle);
            hFunc(TEvents::TEvPing, Handle);
            hFunc(TEvents::TEvUndelivered, Handle);
            cFunc(TEvents::TEvPoison::EventType, PassAway);
            hFunc(TEvInterconnect::TEvNodeDisconnected, Handle);
            IgnoreFunc(TEvInterconnect::TEvNodeConnected);
            hFunc(TEvStateStorage::TEvUpdateGroupConfig, Handle);

            default:
                BLOG_W("Replica::StateInit unexpected event type# " << ev->GetTypeRewrite()
                    << " event: " << ev->ToString());
                break;
        }
    }
};

IActor* CreateStateStorageReplica(const TIntrusivePtr<TStateStorageInfo> &info, ui32 replicaIndex) {
    return new TStateStorageReplica(info, replicaIndex);
}

}
