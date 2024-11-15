#include "pq_impl.h"
#include "pq_impl_types.h"

#include "common_app.h"
#include "partition_log.h"

#include <ydb/library/protobuf_printer/security_printer.h>

/******************************************************* MonitoringProxy *********************************************************/

namespace NKikimr::NPQ {

class TMonitoringProxy : public TActorBootstrapped<TMonitoringProxy> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::PERSQUEUE_MON_ACTOR;
    }

    TMonitoringProxy(const TActorId& sender, const TString& query, const TMap<ui32, TActorId>&& partitions, const TActorId& cache,
                     const TString& topicName, ui64 tabletId, ui32 inflight, TString&& config)
    : Sender(sender)
    , Query(query)
    , Partitions(std::move(partitions))
    , Cache(cache)
    , TotalRequests(partitions.size() + 1)
    , TopicName(topicName)
    , TabletID(tabletId)
    , Inflight(inflight)
    , Config(std::move(config))
    , TotalResponses(0)
    {
        for (auto& p : Partitions) {
            PartitionResults[p.first] = Sprintf("Partition %u: NO DATA", p.first);
        }
        CacheResult = "Cache: NO DATA";
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
        HTML_APP_PAGE(str, "PersQueue Tablet " << TabletID << " (" << TopicName << ")") {
            NAVIGATION_BAR() {
                NAVIGATION_TAB("generic", "Generic Info");
                NAVIGATION_TAB("cache", "Cache");
                for (auto& [partitionId, _]: PartitionResults) {
                    NAVIGATION_TAB("partition_" << partitionId, partitionId);
                }

                NAVIGATION_TAB_CONTENT("generic") {
                    LAYOUT_ROW() {
                        LAYOUT_COLUMN() {
                            PROPERTIES("Tablet info") {
                                PROPERTY("Topic", TopicName);
                                PROPERTY("TabletID", TabletID);
                                PROPERTY("Inflight", Inflight);
                            }
                        }
                    }

                    LAYOUT_ROW() {
                        LAYOUT_COLUMN() {
                            CONFIGURATION(Config);
                        }
                    }

                    LAYOUT_ROW() {
                        LAYOUT_COLUMN() {
                            str << "<a href=\"app?TabletID=" << TabletID << "&kv=1\">KV-tablet internals</a>";
                        }
                    }
                }

                str << CacheResult;
                for (auto& [_, v] : PartitionResults) {
                    str << v;
                }
            }
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
            PartitionResults[ev->Get()->Partition->InternalPartitionId] = std::move(ev->Get()->Str);
        } else {
            CacheResult = std::move(ev->Get()->Str);
        }

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

    const TActorId Sender;
    const TString Query;
    const TMap<ui32, TActorId> Partitions;
    const TActorId Cache;
    const ui32 TotalRequests;
    const TString TopicName;
    const ui64 TabletID;
    const ui32 Inflight;
    const TString Config;

    TString CacheResult;
    TMap<ui32, TString> PartitionResults;
    ui32 TotalResponses;
};


bool TPersQueue::OnRenderAppHtmlPage(NMon::TEvRemoteHttpInfo::TPtr ev, const TActorContext& ctx)
{
    if (!ev) {
        return true;
    }

    if (ev->Get()->Cgi().Has("kv")) {
        return TKeyValueFlat::OnRenderAppHtmlPage(ev, ctx);
    }

    PQ_LOG_I("Handle TEvRemoteHttpInfo: " << ev->Get()->Query);

    TMap<ui32, TActorId> res;
    for (auto& p : Partitions) {
        res.emplace(p.first.InternalPartitionId, p.second.Actor);
    }

    TString config = SecureDebugStringMultiline(Config);
    ctx.Register(new TMonitoringProxy(ev->Sender, ev->Get()->Query, std::move(res), CacheActor, TopicName, TabletID(), ResponseProxy.size(), std::move(config)));

    return true;
}

}
