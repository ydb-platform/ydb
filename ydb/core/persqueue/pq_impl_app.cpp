#include "pq_impl.h"
#include "pq_impl_types.h"

#include "common_app.h"
#include "partition_log.h"

/******************************************************* MonitoringProxy *********************************************************/

namespace NKikimr::NPQ {

class TMonitoringProxy : public TActorBootstrapped<TMonitoringProxy> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::PERSQUEUE_MON_ACTOR;
    }

    TMonitoringProxy(const TActorId& sender, const TString& query, const TMap<ui32, TActorId>& partitions, const TActorId& cache,
                     const TString& topicName, ui64 tabletId, ui32 inflight)
    : Sender(sender)
    , Query(query)
    , Partitions(partitions)
    , Cache(cache)
    , TotalRequests(partitions.size() + 1)
    , TotalResponses(0)
    , TopicName(topicName)
    , TabletID(tabletId)
    , Inflight(inflight)
    {
        for (auto& p : Partitions) {
            Results[p.first].push_back(Sprintf("Partition %u: NO DATA", p.first));
        }
    }

    void Bootstrap(const TActorContext& ctx)
    {
        Become(&TThis::StateFunc);
        ctx.Send(Cache, new TEvPQ::TEvMonRequest(Sender, Query));
        for (auto& p : Partitions) {
            ctx.Send(p.second, new TEvPQ::TEvMonRequest(Sender, Query));
        }
        ctx.Schedule(TDuration::Seconds(10), new TEvents::TEvWakeup());
    }

private:

    void Reply(const TActorContext& ctx) {
        TStringStream str;
        ui32 mx = 0;
        for (auto& r: Results) mx = Max<ui32>(mx, r.second.size());

        HTML(str) {
            TAG(TH2) {str << "PersQueue Tablet";}
            TAG(TH3) {str << "Topic: " << TopicName;}
            TAG(TH4) {str << "inflight: " << Inflight;}
            UL_CLASS("nav nav-tabs") {
                LI_CLASS("active") {
                    str << "<a href=\"#main\" data-toggle=\"tab\">main</a>";
                }
                LI() {
                    str << "<a href=\"#cache\" data-toggle=\"tab\">cache</a>";
                }
                for (auto& r: Results) {
                    LI() {
                        str << "<a href=\"#partition_" << r.first << "\" data-toggle=\"tab\">" << r.first << "</a>";
                    }
                }
            }
            DIV_CLASS("tab-content") {
                DIV_CLASS_ID("tab-pane fade in active", "main") {
                    TABLE() {
                        for (ui32 i = 0; i < mx; ++i) {
                            TABLER() {
                                for (auto& r : Results) {
                                    TABLED() {
                                        if (r.second.size() > i)
                                            str << r.second[i];
                                    }
                                }
                            }
                        }
                    }
                }
                for (auto& s: Str) {
                    str << s;
                }
            }
            TAG(TH3) {str << "<a href=\"app?TabletID=" << TabletID << "&kv=1\">KV-tablet internals</a>";}
        }
        PQ_LOG_D("Answer TEvRemoteHttpInfoRes: to " << Sender << " self " << ctx.SelfID);
        ctx.Send(Sender, new NMon::TEvRemoteHttpInfoRes(str.Str()));
        Die(ctx);
    }

    void Wakeup(const TActorContext& ctx) {
        Reply(ctx);
    }

    void Handle(TEvPQ::TEvMonResponse::TPtr& ev, const TActorContext& ctx)
    {
        if (ev->Get()->Partition.Defined()) {
            Results[ev->Get()->Partition->InternalPartitionId] = ev->Get()->Res;
        }
        Str.push_back(ev->Get()->Str);
        if(++TotalResponses == TotalRequests) {
            Reply(ctx);
        }
    }

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            CFunc(TEvents::TSystem::Wakeup, Wakeup);
            HFunc(TEvPQ::TEvMonResponse, Handle);
        default:
            break;
        };
    }

    TActorId Sender;
    TString Query;
    TMap<ui32, TVector<TString>> Results;
    TVector<TString> Str;
    TMap<ui32, TActorId> Partitions;
    TActorId Cache;
    ui32 TotalRequests;
    ui32 TotalResponses;
    TString TopicName;
    ui64 TabletID;
    ui32 Inflight;
};


bool TPersQueue::OnRenderAppHtmlPage(NMon::TEvRemoteHttpInfo::TPtr ev, const TActorContext& ctx)
{
    if (!ev)
        return true;

    if (ev->Get()->Cgi().Has("kv")) {
        return TKeyValueFlat::OnRenderAppHtmlPage(ev, ctx);
    }
    PQ_LOG_I("Handle TEvRemoteHttpInfo: " << ev->Get()->Query);
    TMap<ui32, TActorId> res;
    for (auto& p : Partitions) {
        res.emplace(p.first.InternalPartitionId, p.second.Actor);
    }
    ctx.Register(new TMonitoringProxy(ev->Sender, ev->Get()->Query, res, CacheActor, TopicName, TabletID(), ResponseProxy.size()));
    return true;
}

}
