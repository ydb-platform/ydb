#include "change_sender_common_ops.h"
#include "change_sender_monitoring.h"

#include <library/cpp/monlib/service/pages/mon_page.h>
#include <library/cpp/monlib/service/pages/templates.h>

#include <util/generic/size_literals.h>

namespace NKikimr::NDataShard {

void TBaseChangeSender::CreateSenders(const TVector<ui64>& partitionIds) {
    THashMap<ui64, TSender> senders;

    for (const auto& partitionId : partitionIds) {
        auto it = Senders.find(partitionId);
        if (it != Senders.end()) {
            senders.emplace(partitionId, std::move(it->second));
            Senders.erase(it);
        } else {
            Y_VERIFY(!senders.contains(partitionId));
            auto& sender = senders[partitionId];
            sender.ActorId = ActorOps->Register(CreateSender(partitionId));
        }
    }

    for (const auto& [_, sender] : Senders) {
        if (sender.Pending) {
            Enqueued.insert(sender.Pending.begin(), sender.Pending.end());
        }

        ActorOps->Send(sender.ActorId, new TEvents::TEvPoisonPill());
    }

    Senders = std::move(senders);

    if (!Enqueued || !RequestRecords()) {
        SendRecords();
    }
}

void TBaseChangeSender::KillSenders() {
    for (const auto& [_, sender] : Senders) {
        ActorOps->Send(sender.ActorId, new TEvents::TEvPoisonPill());
    }

    Senders.clear();
}

void TBaseChangeSender::EnqueueRecords(TVector<TEvChangeExchange::TEvEnqueueRecords::TRecordInfo>&& records) {
    for (auto& record : records) {
        Y_VERIFY_S(PathId == record.PathId, "Unexpected record's path id"
            << ": expected# " << PathId
            << ", got# " << record.PathId);
        Enqueued.emplace(record.Order, record.BodySize);
    }

    RequestRecords();
}

bool TBaseChangeSender::RequestRecords() {
    if (!Enqueued) {
        return false;
    }

    auto it = Enqueued.begin();
    TVector<TRequestedRecord> records;

    while (it != Enqueued.end()) {
        if (MemUsage && (MemUsage + it->BodySize) > MemLimit) {
            break;
        }

        MemUsage += it->BodySize;

        records.emplace_back(it->Order, it->BodySize);
        PendingBody.emplace(it->Order, it->BodySize);
        it = Enqueued.erase(it);
    }

    if (!records) {
        return false;
    }

    ActorOps->Send(DataShard.ActorId, new TEvChangeExchange::TEvRequestRecords(std::move(records)));
    return true;
}

void TBaseChangeSender::ProcessRecords(TVector<TChangeRecord>&& records) {
    for (auto& record : records) {
        auto it = PendingBody.find(record.GetOrder());
        if (it == PendingBody.end()) {
            continue;
        }

        if (it->BodySize != record.GetBody().size()) {
            MemUsage -= it->BodySize;
            MemUsage += record.GetBody().size();
        }

        PendingSent.emplace(record.GetOrder(), std::move(record));
        PendingBody.erase(it);
    }

    SendRecords();
}

void TBaseChangeSender::SendRecords() {
    if (!Resolver->IsResolved()) {
        return;
    }

    if (!PendingSent) {
        return;
    }

    auto it = PendingSent.begin();
    THashMap<ui64, TVector<TChangeRecord>> forward;
    bool needToResolve = false;

    while (it != PendingSent.end()) {
        const ui64 partitionId = Resolver->GetPartitionId(it->second);
        if (!Senders.contains(partitionId)) {
            needToResolve = true;
            ++it;
            continue;
        }

        const auto& sender = Senders.at(partitionId);
        if (!sender.Ready) {
            ++it;
            continue;
        }

        MemUsage -= it->second.GetBody().size();

        forward[partitionId].push_back(std::move(it->second));
        it = PendingSent.erase(it);
    }

    for (auto& [partitionId, records] : forward) {
        Y_VERIFY(Senders.contains(partitionId));
        auto& sender = Senders.at(partitionId);

        Y_VERIFY(sender.Ready);
        sender.Ready = false;

        sender.Pending.reserve(records.size());
        for (const auto& record : records) {
            sender.Pending.emplace_back(record.GetOrder(), record.GetBody().size());
        }

        ActorOps->Send(sender.ActorId, new TEvChangeExchange::TEvRecords(std::move(records)));
    }

    if (needToResolve && !Resolver->IsResolving()) {
        Resolver->Resolve();
    }

    RequestRecords();
}

void TBaseChangeSender::ForgetRecords(TVector<ui64>&& records) {
    for (const auto& record : records) {
        auto it = PendingBody.find(record);
        if (it == PendingBody.end()) {
            continue;
        }

        MemUsage -= it->BodySize;
        PendingBody.erase(it);
    }

    RequestRecords();
}

void TBaseChangeSender::OnReady(ui64 partitionId) {
    auto it = Senders.find(partitionId);
    if (it == Senders.end()) {
        return;
    }

    auto& sender = it->second;
    sender.Ready = true;

    if (sender.Pending) {
        TVector<ui64> remove(Reserve(sender.Pending.size()));
        for (const auto& record : sender.Pending) {
            remove.push_back(record.Order);
        }

        ActorOps->Send(DataShard.ActorId, new TEvChangeExchange::TEvRemoveRecords(std::move(remove)));
        sender.Pending.clear();
    }

    SendRecords();
}

void TBaseChangeSender::OnGone(ui64 partitionId) {
    auto it = Senders.find(partitionId);
    if (it == Senders.end()) {
        return;
    }

    const auto& sender = it->second;
    if (sender.Pending) {
        Enqueued.insert(sender.Pending.begin(), sender.Pending.end());
    }

    Senders.erase(it);

    if (Resolver->IsResolving()) {
        return;
    }

    Resolver->Resolve();
}

void TBaseChangeSender::RemoveRecords() {
    ui64 pendingStatus = 0;
    for (const auto& [_, sender] : Senders) {
        pendingStatus += sender.Pending.size();
    }

    TVector<ui64> remove(Reserve(Enqueued.size() + PendingBody.size() + PendingSent.size() + pendingStatus));

    for (const auto& record : Enqueued) {
        remove.push_back(record.Order);
    }

    for (const auto& record : PendingBody) {
        remove.push_back(record.Order);
    }

    for (const auto& [order, _] : PendingSent) {
        remove.push_back(order);
    }

    for (const auto& [_, sender] : Senders) {
        for (const auto& record : sender.Pending) {
            remove.push_back(record.Order);
        }
    }

    if (remove) {
        ActorOps->Send(DataShard.ActorId, new TEvChangeExchange::TEvRemoveRecords(std::move(remove)));
    }
}

TBaseChangeSender::TBaseChangeSender(IActorOps* actorOps, IChangeSenderResolver* resolver,
        const TDataShardId& dataShard, const TPathId& pathId)
    : ActorOps(actorOps)
    , Resolver(resolver)
    , DataShard(dataShard)
    , PathId(pathId)
    , MemLimit(192_KB)
    , MemUsage(0)
{
}

void TBaseChangeSender::RenderHtmlPage(TEvChangeExchange::ESenderType type, NMon::TEvRemoteHttpInfo::TPtr& ev,
        const TActorContext& ctx)
{
    const auto& cgi = ev->Get()->Cgi();
    if (const auto& str = cgi.Get("partitionId")) {
        ui64 partitionId = 0;
        if (TryFromString(str, partitionId)) {
            auto it = Senders.find(partitionId);
            if (it != Senders.end()) {
                if (const auto& to = it->second.ActorId) {
                    ctx.Send(ev->Forward(to));
                } else {
                    ActorOps->Send(ev->Sender, new NMon::TEvRemoteHttpInfoRes(TStringBuilder()
                        << "Change sender '" << PathId << ":" << partitionId << "' is not running"));
                }
            } else {
                ActorOps->Send(ev->Sender, new NMon::TEvRemoteBinaryInfoRes(NMonitoring::HTTPNOTFOUND));
            }
        } else {
            ActorOps->Send(ev->Sender, new NMon::TEvRemoteHttpInfoRes("Invalid partitionId"));
        }

        return;
    }

    TStringStream html;

    HTML(html) {
        Header(html, TStringBuilder() << type << " change sender", DataShard.TabletId);

        SimplePanel(html, "Partition senders", [this](IOutputStream& html) {
            HTML(html) {
                TABLE_CLASS("table table-hover") {
                    TABLEHEAD() {
                        TABLER() {
                            TABLEH() { html << "#"; }
                            TABLEH() { html << "PartitionId"; }
                            TABLEH() { html << "Ready"; }
                            TABLEH() { html << "Pending"; }
                            TABLEH() { html << "Actor"; }
                        }
                    }
                    TABLEBODY() {
                        ui32 i = 0;
                        for (const auto& [partitionId, sender] : Senders) {
                            TABLER() {
                                TABLED() { html << ++i; }
                                TABLED() { html << partitionId; }
                                TABLED() { html << sender.Ready; }
                                TABLED() { html << sender.Pending.size(); }
                                TABLED() { ActorLink(html, DataShard.TabletId, PathId, partitionId); }
                            }
                        }
                    }
                }
            }
        });

        CollapsedPanel(html, "Enqueued", "enqueued", [this](IOutputStream& html) {
            HTML(html) {
                TABLE_CLASS("table table-hover") {
                    TABLEHEAD() {
                        TABLER() {
                            TABLEH() { html << "#"; }
                            TABLEH() { html << "Order"; }
                            TABLEH() { html << "BodySize"; }
                        }
                    }
                    TABLEBODY() {
                        ui32 i = 0;
                        for (const auto& record : Enqueued) {
                            TABLER() {
                                TABLED() { html << ++i; }
                                TABLED() { html << record.Order; }
                                TABLED() { html << record.BodySize; }
                            }
                        }
                    }
                }
            }
        });

        CollapsedPanel(html, "PendingBody", "pendingBody", [this](IOutputStream& html) {
            HTML(html) {
                TABLE_CLASS("table table-hover") {
                    TABLEHEAD() {
                        TABLER() {
                            TABLEH() { html << "#"; }
                            TABLEH() { html << "Order"; }
                            TABLEH() { html << "BodySize"; }
                        }
                    }
                    TABLEBODY() {
                        ui32 i = 0;
                        for (const auto& record : PendingBody) {
                            TABLER() {
                                TABLED() { html << ++i; }
                                TABLED() { html << record.Order; }
                                TABLED() { html << record.BodySize; }
                            }
                        }
                    }
                }
            }
        });

        CollapsedPanel(html, "PendingSent", "pendingSent", [this](IOutputStream& html) {
            HTML(html) {
                TABLE_CLASS("table table-hover") {
                    TABLEHEAD() {
                        TABLER() {
                            TABLEH() { html << "#"; }
                            TABLEH() { html << "Order"; }
                            TABLEH() { html << "Group"; }
                            TABLEH() { html << "Step"; }
                            TABLEH() { html << "TxId"; }
                            TABLEH() { html << "LockId"; }
                            TABLEH() { html << "LockOffset"; }
                            TABLEH() { html << "PathId"; }
                            TABLEH() { html << "Kind"; }
                            TABLEH() { html << "TableId"; }
                            TABLEH() { html << "SchemaVersion"; }
                        }
                    }
                    TABLEBODY() {
                        ui32 i = 0;
                        for (const auto& [order, record] : PendingSent) {
                            TABLER() {
                                TABLED() { html << ++i; }
                                TABLED() { html << order; }
                                TABLED() { html << record.GetGroup(); }
                                TABLED() { html << record.GetStep(); }
                                TABLED() { html << record.GetTxId(); }
                                TABLED() { html << record.GetLockId(); }
                                TABLED() { html << record.GetLockOffset(); }
                                TABLED() { PathLink(html, record.GetPathId()); }
                                TABLED() { html << record.GetKind(); }
                                TABLED() { PathLink(html, record.GetTableId()); }
                                TABLED() { html << record.GetSchemaVersion(); }
                            }
                        }
                    }
                }
            }
        });
    }

    ActorOps->Send(ev->Sender, new NMon::TEvRemoteHttpInfoRes(html.Str()));
}

}
