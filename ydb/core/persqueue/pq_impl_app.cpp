#include "pq_impl.h"
#include "pq_impl_types.h"

#include "common_app.h"
#include "partition_log.h"

#include <ydb/library/protobuf_printer/security_printer.h>

/******************************************************* MonitoringProxy *********************************************************/

namespace NKikimr::NPQ {

using EState = NKikimrPQ::TTransaction::EState;

namespace {
    struct TTransactionSnapshot {
        explicit TTransactionSnapshot(const TDistributedTransaction& tx)
            : TxId(tx.TxId)
            , Step(tx.Step)
            , State(tx.State)
            , MinStep(tx.MinStep)
            , MaxStep(tx.MaxStep) {
        }

        ui64 TxId;
        ui64 Step;
        EState State;
        ui64 MinStep;
        ui64 MaxStep;
    };
}


class TMonitoringProxy : public TActorBootstrapped<TMonitoringProxy> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::PERSQUEUE_MON_ACTOR;
    }

    TMonitoringProxy(const TActorId& sender, const TString& query, const TMap<ui32, TActorId>&& partitions, const TActorId& cache,
                     const TString& topicName, ui64 tabletId, ui32 inflight, TString&& config, std::vector<TTransactionSnapshot>&& transactions)
    : Sender(sender)
    , Query(query)
    , Partitions(std::move(partitions))
    , Cache(cache)
    , TotalRequests(partitions.size() + 1)
    , TopicName(topicName)
    , TabletID(tabletId)
    , Inflight(inflight)
    , Config(std::move(config))
    , Transactions(std::move(transactions))
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

                    LAYOUT_ROW() {
                        LAYOUT_COLUMN() {
                            TABLE_CLASS("table") {
                                CAPTION() {str << "Transactions";}
                                TABLEHEAD() {
                                    TABLER() {
                                        TABLEH() {str << "TxId";}
                                        TABLEH() {str << "Step";}
                                        TABLEH() {str << "State";}
                                        TABLEH() {str << "MinStep";}
                                        TABLEH() {str << "MaxStep";}
                                    }
                                }
                                TABLEBODY() {
                                    for (auto& tx : Transactions) {
                                        TABLER() {
                                            TABLED() {
                                                HREF(TStringBuilder() << "?TabletID=" << TabletID << "&TxId=" << tx.TxId) {
                                                    str << tx.TxId;
                                                }
                                            }
                                            TABLED() {str << GetTxStep(tx);}
                                            TABLED() {str << NKikimrPQ::TTransaction_EState_Name(tx.State);}
                                            TABLED() {str << tx.MinStep;}
                                            TABLED() {str << tx.MaxStep;}
                                        }
                                    }
                                }
                            }
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

    static TString GetTxStep(const TTransactionSnapshot& tx) {
        if (tx.Step == Max<ui64>()) {
            return "-";
        }
        return ToString(tx.Step);
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
    const std::vector<TTransactionSnapshot> Transactions;

    TString CacheResult;
    TMap<ui32, TString> PartitionResults;
    ui32 TotalResponses;
};

bool TPersQueue::OnRenderAppHtmlPageTx(NMon::TEvRemoteHttpInfo::TPtr ev, const TActorContext& ctx) {
    auto txIdStr = ev->Get()->Cgi().Get("TxId");

    TStringStream str;

    char *endptr;
    const ui64 txId = strtoull(txIdStr.c_str(), &endptr, 10);
    if (*endptr) {
        str << "Wrong id: '" << txIdStr << "'";
    } else {
        auto* tx = Txs.FindPtr(txId);
        if (tx) {
            HTML_APP_PAGE(str, "PersQueue Tablet " << TabletID() << " (" << TopicName << ") Transaction id " << txId) {
                PRE() {
                    str << SecureDebugStringMultiline(tx->Serialize());
                }
            }
        } else {
            str << "Transaction " << txId << " not found";
        }
    }

    ctx.Send(ev->Sender, new NMon::TEvRemoteHttpInfoRes(str.Str()));
    return true;
}

bool TPersQueue::OnRenderAppHtmlPage(NMon::TEvRemoteHttpInfo::TPtr ev, const TActorContext& ctx)
{
    if (!ev) {
        return true;
    }

    if (ev->Get()->Cgi().Has("kv")) {
        return TKeyValueFlat::OnRenderAppHtmlPage(ev, ctx);
    }

    if (ev->Get()->Cgi().Has("TxId")) {
        return OnRenderAppHtmlPageTx(ev, ctx);
    }

    PQ_LOG_I("Handle TEvRemoteHttpInfo: " << ev->Get()->Query);

    TMap<ui32, TActorId> res;
    for (auto& p : Partitions) {
        res.emplace(p.first.InternalPartitionId, p.second.Actor);
    }

    TString config = SecureDebugStringMultiline(Config);
    std::vector<TTransactionSnapshot> transactions;
    transactions.reserve(Txs.size());
    for (auto& [_, tx] : Txs) {
        transactions.emplace_back(tx);
    }

    auto isLess = [](const TTransactionSnapshot& lhs, const TTransactionSnapshot& rhs) {
        auto makeValue = [](const TTransactionSnapshot& v) {
            return std::make_tuple(v.Step, v.TxId);
        };

        return makeValue(lhs) < makeValue(rhs);
    };
    std::sort(transactions.begin(), transactions.end(), isLess);

    ctx.Register(new TMonitoringProxy(ev->Sender, ev->Get()->Query, std::move(res), CacheActor, TopicName,
        TabletID(), ResponseProxy.size(), std::move(config), std::move(transactions)));

    return true;
}

}
