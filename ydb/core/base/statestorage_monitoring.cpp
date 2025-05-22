#include "statestorage_impl.h"
#include "tabletid.h"
#include <ydb/library/services/services.pb.h>
#include <ydb/library/actors/core/interconnect.h>
#include <library/cpp/monlib/service/pages/templates.h>
#include <ydb/library/actors/core/mon.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr {

class TStateStorageMonitoringActor : public TActorBootstrapped<TStateStorageMonitoringActor> {
    struct TReplicaInfo {
        TActorId ActorID;
        TInstant ReplyTime;

        TActorId CurrentLeader;
        TActorId CurrentLeaderTablet;
        TVector<TActorId> Followers;
        ui32 CurrentGeneration;
        ui64 ConfigContentHash;
        bool Locked;
        ui64 LockedFor;

        TReplicaInfo(const TActorId &x)
            : ActorID(x)
            , ReplyTime(TInstant::MicroSeconds(Max<ui64>()))
            , CurrentLeader()
            , CurrentLeaderTablet()
            , CurrentGeneration(Max<ui32>())
            , ConfigContentHash(0)
            , Locked(false)
            , LockedFor(0)
        {}
    };

    const ui64 TabletID;
    const TActorId Sender;
    const TString Query;

    TInstant BeginMoment;
    TInstant ReplicasRequestMoment;

    TDuration ProxyReplyTime;
    TVector<TReplicaInfo> ReplicasInfo;
    ui64 WaitingForReplicas;
    ui64 SelfConfigContentHash;

    void Reply(const TString &response, const TActorContext &ctx) {
        TStringStream str;

        HTML(str) {
            TAG(TH3) { str << "State Storage";}
            DIV_CLASS("container") {
                DIV_CLASS("row") {str << "TabletID: " << TabletID;}
                DIV_CLASS("row") {str << "Response: " << response;}

                if (ProxyReplyTime.GetValue() != Max<ui64>()) {
                    DIV_CLASS("row") {str << "Proxy reply time: " << ProxyReplyTime.ToString(); }
                }

                DIV_CLASS("CfgHash") {str << "Config hash: " << SelfConfigContentHash; }
                DIV_CLASS("row") {str << "&nbsp;";}
            }


            TABLE_SORTABLE_CLASS("table") {
                TABLEHEAD() {
                    TABLER() {
                        TABLEH() { str << "NodeId";}
                        TABLEH() { str << "Leader";}
                        TABLEH() { str << "Followers"; }
                        TABLEH() { str << "Locked";}
                        TABLEH() { str << "Generation";}
                        TABLEH() { str << "Reply time";}
                        TABLEH() { str << "CfgHash";}
                    }
                }
                TABLEBODY() {
                    for (auto &replica : ReplicasInfo) {
                        TABLER() {
                            TABLED() {str << replica.ActorID.NodeId();}
                            if (replica.ReplyTime.GetValue() == Max<ui64>()) { // general timeout
                                TABLED() { str << "timeout";}
                                TABLED() { str << "-"; }
                                TABLED() { str << "-"; }
                                TABLED() { str << "-"; }
                                TABLED() { str << "-"; }
                                TABLED() { str << "-"; }
                            } else if (replica.CurrentGeneration == Max<ui32>()) {
                                TABLED() { str << "not available";}
                                TABLED() { str << "-"; }
                                TABLED() { str << "-"; }
                                TABLED() { str << "-"; }
                                TABLED() { str << "-"; }
                                TABLED() { str << replica.ConfigContentHash; }
                            } else {
                                TABLED() {str << replica.CurrentLeader;}
                                TABLED() {
                                    if (replica.Followers)
                                        for (auto &s : replica.Followers)
                                            str << s << "; ";
                                    else
                                        str << "-";
                                }
                                TABLED() { str << replica.Locked; }
                                TABLED() { str << replica.CurrentGeneration; }
                                TABLED() { str << (replica.ReplyTime - ReplicasRequestMoment); }
                                TABLED() { str << replica.ConfigContentHash; }
                            }
                        }
                    }
                }
            }
        }

        ctx.Send(Sender, new NMon::TEvHttpInfoRes(str.Str()));
        return Die(ctx);
    }

    void CheckCompletion(const TActorContext &ctx) {
        if (WaitingForReplicas > 0)
            return;

        return Reply("complete", ctx);
    }

    void Handle(TEvStateStorage::TEvResolveReplicasList::TPtr &ev, const TActorContext &ctx) {
        const TVector<TActorId> &replicasList = ev->Get()->Replicas;

        if (replicasList.empty())
            return Reply("empty replica list", ctx);

        SelfConfigContentHash = ev->Get()->ConfigContentHash;

        ReplicasRequestMoment = ctx.Now();
        ProxyReplyTime = ReplicasRequestMoment - BeginMoment;

        ReplicasInfo.reserve(replicasList.size());
        for (ui64 cookie = 0, e = replicasList.size(); cookie < e; ++cookie) {
            const TActorId &replica = replicasList[cookie];
            ReplicasInfo.push_back(replica);
            ctx.Send(replica, new TEvStateStorage::TEvReplicaLookup(TabletID, cookie));
        }
        WaitingForReplicas = ReplicasInfo.size();

        Become(&TThis::StateCollect);
    }

    void Handle(TEvStateStorage::TEvReplicaInfo::TPtr &ev, const TActorContext &ctx) {
        const NKikimrStateStorage::TEvInfo &record = ev->Get()->Record;
        const ui64 cookie = record.GetCookie();
        Y_ABORT_UNLESS(cookie < ReplicasInfo.size());

        auto &xinfo = ReplicasInfo[cookie];

        if (xinfo.ReplyTime.GetValue() != Max<ui64>())
            return;

        xinfo.ReplyTime = ctx.Now();
        --WaitingForReplicas;

        if (record.GetStatus() == NKikimrProto::OK) {
            if (record.HasCurrentLeader())
                xinfo.CurrentLeader = ActorIdFromProto(record.GetCurrentLeader());
            if (record.HasCurrentLeaderTablet())
                xinfo.CurrentLeaderTablet = ActorIdFromProto(record.GetCurrentLeaderTablet());
            xinfo.CurrentGeneration = record.HasCurrentGeneration() ? record.GetCurrentGeneration() : 0;
            xinfo.Locked = record.HasLocked() ? record.GetLocked() : false;
            xinfo.LockedFor = record.HasLockedFor() ? record.GetLockedFor() : 0;
        }

        xinfo.ConfigContentHash = record.GetConfigContentHash();
        return CheckCompletion(ctx);
    }

    void HandleInit(TEvents::TEvUndelivered::TPtr &ev, const TActorContext &ctx) {
        Y_UNUSED(ev);
        return Reply("unknown state storage", ctx);
    }

    void HandleCollect(TEvents::TEvUndelivered::TPtr &ev, const TActorContext &ctx) {
        for (auto &x : ReplicasInfo) {
            if (x.ActorID == ev->Sender) {
                if (x.ReplyTime.GetValue() == Max<ui64>()) {
                    x.ReplyTime = ctx.Now();
                    --WaitingForReplicas;
                }
                break;
            }
        }

        return CheckCompletion(ctx);
    }

    void Timeout(const TActorContext &ctx) {
        return Reply("timeout", ctx);
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::SS_MON;
    }

    TStateStorageMonitoringActor(ui64 tabletId, const TActorId &sender, const TString &query)
        : TabletID(tabletId)
        , Sender(sender)
        , Query(query)
        , ProxyReplyTime(TDuration::MicroSeconds(Max<ui64>()))
        , WaitingForReplicas(0)
        , SelfConfigContentHash(0)
    {}

    void Bootstrap(const TActorContext &ctx) {
        // try to send monitoring request to proxy
        const TActorId proxyActorID = MakeStateStorageProxyID();

        BeginMoment = ctx.Now();

        ctx.Send(proxyActorID, new TEvStateStorage::TEvResolveReplicas(TabletID), IEventHandle::FlagTrackDelivery);
        ctx.Schedule(TDuration::Seconds(10), new TEvents::TEvWakeup()); // global timeout
        Become(&TThis::StateInit);
    }

    STFUNC(StateInit) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvStateStorage::TEvResolveReplicasList, Handle);
            HFunc(TEvents::TEvUndelivered, HandleInit);
            CFunc(TEvents::TEvWakeup::EventType, Timeout);
        }
    }

    STFUNC(StateCollect) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvStateStorage::TEvReplicaInfo, Handle);
            HFunc(TEvents::TEvUndelivered, HandleCollect);
            CFunc(TEvents::TEvWakeup::EventType, Timeout);
        }
    }
};

IActor* CreateStateStorageMonitoringActor(ui64 targetTablet, const TActorId &sender, const TString &query) {
    return new TStateStorageMonitoringActor(targetTablet, sender, query);
}

}
