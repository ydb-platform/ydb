#pragma once

#include "change_exchange.h"
#include "change_sender_resolver.h"

#include <ydb/core/change_exchange/change_sender_monitoring.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/mon.h>
#include <ydb/library/yverify_stream/yverify_stream.h>

#include <library/cpp/monlib/service/pages/mon_page.h>
#include <library/cpp/monlib/service/pages/templates.h>

#include <util/generic/hash.h>
#include <util/generic/map.h>
#include <util/generic/set.h>
#include <util/string/builder.h>

#include <concepts>

namespace NKikimr::NChangeExchange {

struct TEvChangeExchangePrivate {
    enum EEv {
        EvReady = EventSpaceBegin(TKikimrEvents::ES_PRIVATE),
        EvGone,
        EvEnd,
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_PRIVATE));

    struct TEvReady: public TEventLocal<TEvReady, EvReady> {
        ui64 PartitionId;

        explicit TEvReady(ui64 partiionId)
            : PartitionId(partiionId)
        {
        }

        TString ToString() const override {
            return TStringBuilder() << ToStringHeader() << " {"
                << " PartitionId: " << PartitionId
            << " }";
        }
    };

    struct TEvGone: public TEventLocal<TEvGone, EvGone> {
        ui64 PartitionId;
        bool HardError;

        explicit TEvGone(ui64 partitionId, bool hardError = false)
            : PartitionId(partitionId)
            , HardError(hardError)
        {
        }

        TString ToString() const override {
            return TStringBuilder() << ToStringHeader() << " {"
                << " PartitionId: " << PartitionId
                << " HardError: " << HardError
            << " }";
        }
    };

}; // TEvChangeExchangePrivate

class ISenderFactory {
public:
    virtual ~ISenderFactory() = default;
    virtual IActor* CreateSender(ui64 partitionId) const = 0;
};

template <class TChangeRecord>
class TBaseChangeSender {
    using TIncompleteRecord = TEvChangeExchange::TEvRequestRecords::TRecordInfo;
    // we need this to safely cast and call Out on a container
    static_assert(std::derived_from<TChangeRecordContainer<TChangeRecord>, TBaseChangeRecordContainer>);

    struct TEnqueuedRecord: TIncompleteRecord {
        bool ReEnqueued = false;

        using TIncompleteRecord::TIncompleteRecord;
        explicit TEnqueuedRecord(const TIncompleteRecord& record)
            : TIncompleteRecord(record)
        {
        }
    };

    template <typename... Args>
    static TEnqueuedRecord ReEnqueue(Args&&... args) {
        TEnqueuedRecord record(std::forward<Args>(args)...);
        record.ReEnqueued = true;
        return record;
    }

    struct TSender {
        TActorId ActorId;
        bool Ready = false;
        TVector<TIncompleteRecord> Pending;
        TVector<typename TChangeRecord::TPtr> Prepared;
        TVector<ui64> Broadcasting;
    };

    struct TBroadcast {
        const TIncompleteRecord Record;
        THashSet<ui64> Partitions;
        THashSet<ui64> PendingPartitions;
        THashSet<ui64> CompletedPartitions;
    };

    void LazyCreateSender(THashMap<ui64, TSender>& senders, ui64 partitionId) {
        auto res = senders.emplace(partitionId, TSender{});
        Y_ABORT_UNLESS(res.second);

        for (const auto& [order, broadcast] : Broadcasting) {
            if (AddBroadcastPartition(order, partitionId)) {
                // re-enqueue record to send it in the correct order
                Enqueued.insert(ReEnqueue(broadcast.Record));
            }
        }
    }

    void RegisterSender(ui64 partitionId) {
        Y_ABORT_UNLESS(Senders.contains(partitionId));
        auto& sender = Senders.at(partitionId);

        Y_ABORT_UNLESS(!sender.ActorId);
        sender.ActorId = ActorOps->RegisterWithSameMailbox(SenderFactory->CreateSender(partitionId));
    }

    void CreateMissingSenders(const TVector<ui64>& partitionIds) {
        THashMap<ui64, TSender> senders;

        for (const auto& partitionId : partitionIds) {
            auto it = Senders.find(partitionId);
            if (it != Senders.end()) {
                senders.emplace(partitionId, std::move(it->second));
                Senders.erase(it);
            } else {
                LazyCreateSender(senders, partitionId);
            }
        }

        for (const auto& [partitionId, sender] : Senders) {
            ReEnqueueRecords(sender);
            ProcessBroadcasting(&TBaseChangeSender::RemoveBroadcastPartition,
                partitionId, sender.Broadcasting);
            if (sender.ActorId) {
                ActorOps->Send(sender.ActorId, new TEvents::TEvPoisonPill());
            }
        }

        Senders = std::move(senders);
    }

    void RecreateSenders(const TVector<ui64>& partitionIds) {
        for (const auto& partitionId : partitionIds) {
            LazyCreateSender(Senders, partitionId);
        }
    }

    bool RequestRecords() {
        if (!Enqueued) {
            return false;
        }

        auto it = Enqueued.begin();
        TVector<TIncompleteRecord> records;

        bool exceeded = false;
        while (it != Enqueued.end()) {
            if (MemUsage && (MemUsage + it->BodySize) > MemLimit) {
                if (!it->ReEnqueued || exceeded) {
                    break;
                }

                exceeded = true;
            }

            MemUsage += it->BodySize;

            records.emplace_back(it->Order, it->BodySize);
            PendingBody.emplace(it->Order, it->BodySize);
            it = Enqueued.erase(it);
        }

        if (!records) {
            return false;
        }

        ActorOps->Send(GetChangeServer(), new TEvChangeExchange::TEvRequestRecords(std::move(records)));
        return true;
    }

    void SendRecords() {
        if (!Resolver->IsResolved()) {
            return;
        }

        if (!PendingSent) {
            return;
        }

        auto it = PendingSent.begin();
        THashSet<ui64> sendTo;
        THashSet<ui64> registrations;
        bool needToResolve = false;

        while (it != PendingSent.end()) {
            if (Enqueued && Enqueued.begin()->Order <= it->first) {
                break;
            }

            if (PendingBody && PendingBody.begin()->Order <= it->first) {
                break;
            }

            if (!it->second->IsBroadcast()) {
                const ui64 partitionId = it->second->ResolvePartitionId(Resolver);
                if (!Senders.contains(partitionId)) {
                    needToResolve = true;
                    ++it;
                    continue;
                }

                auto& sender = Senders.at(partitionId);
                sender.Prepared.push_back(std::move(it->second));
                if (!sender.ActorId) {
                    Y_ABORT_UNLESS(!sender.Ready);
                    registrations.insert(partitionId);
                }
                if (sender.Ready) {
                    sendTo.insert(partitionId);
                }
            } else {
                auto& broadcast = EnsureBroadcast(it->second);
                EraseNodesIf(broadcast.PendingPartitions, [&](ui64 partitionId) {
                    if (Senders.contains(partitionId)) {
                        auto& sender = Senders.at(partitionId);
                        sender.Prepared.push_back(it->second);
                        if (!sender.ActorId) {
                            Y_ABORT_UNLESS(!sender.Ready);
                            registrations.insert(partitionId);
                        }
                        if (sender.Ready) {
                            sendTo.insert(partitionId);
                        }

                        return true;
                    }

                    return false;
                });
            }

            it = PendingSent.erase(it);
        }

        for (const auto partitionId : registrations) {
            RegisterSender(partitionId);
        }

        for (const auto partitionId : sendTo) {
            SendPreparedRecords(partitionId);
        }

        if (needToResolve && !Resolver->IsResolving()) {
            Resolver->Resolve();
        }

        RequestRecords();
    }

    void SendPreparedRecords(ui64 partitionId) {
        Y_ABORT_UNLESS(Senders.contains(partitionId));
        auto& sender = Senders.at(partitionId);

        Y_ABORT_UNLESS(sender.Ready);
        sender.Ready = false;

        sender.Pending.reserve(sender.Prepared.size());
        for (const auto& record : sender.Prepared) {
            if (!record->IsBroadcast()) {
                sender.Pending.emplace_back(record->GetOrder(), record->GetBody().size());
                MemUsage -= record->GetBody().size();
            } else {
                sender.Broadcasting.push_back(record->GetOrder());
            }
        }

        Y_ABORT_UNLESS(sender.ActorId);
        ActorOps->Send(sender.ActorId, new TEvChangeExchange::TEvRecords(std::make_shared<TChangeRecordContainer<TChangeRecord>>(std::exchange(sender.Prepared, {}))));
    }

    void ReEnqueueRecords(const TSender& sender) {
        for (const auto& record : sender.Pending) {
            Enqueued.insert(ReEnqueue(record));
        }

        for (const auto& record : sender.Prepared) {
            if (!record->IsBroadcast()) {
                Enqueued.insert(ReEnqueue(record->GetOrder(), record->GetBody().size()));
                MemUsage -= record->GetBody().size();
            }
        }
    }

    TBroadcast& EnsureBroadcast(IChangeRecord::TPtr record) {
        Y_ABORT_UNLESS(record->IsBroadcast());

        auto it = Broadcasting.find(record->GetOrder());
        if (it != Broadcasting.end()) {
            return it->second;
        }

        THashSet<ui64> partitionIds;
        for (const auto& [partitionId, _] : Senders) {
            partitionIds.insert(partitionId);
        }
        for (const auto partitionId : GonePartitions) {
            partitionIds.insert(partitionId);
        }

        auto res = Broadcasting.emplace(record->GetOrder(), TBroadcast{
            .Record = {record->GetOrder(), record->GetBody().size()},
            .Partitions = partitionIds,
            .PendingPartitions = partitionIds,
        });

        return res.first->second;
    }

    bool AddBroadcastPartition(ui64 order, ui64 partitionId) {
        auto it = Broadcasting.find(order);
        Y_ABORT_UNLESS(it != Broadcasting.end());

        auto& broadcast = it->second;
        if (broadcast.CompletedPartitions.contains(partitionId)) {
            return false;
        }

        broadcast.Partitions.insert(partitionId);
        broadcast.PendingPartitions.insert(partitionId);

        return true;
    }

    bool RemoveBroadcastPartition(ui64 order, ui64 partitionId) {
        auto it = Broadcasting.find(order);
        Y_ABORT_UNLESS(it != Broadcasting.end());

        auto& broadcast = it->second;
        broadcast.Partitions.erase(partitionId);
        broadcast.PendingPartitions.erase(partitionId);
        broadcast.CompletedPartitions.erase(partitionId);

        return MaybeCompleteBroadcast(order);
    }

    bool CompleteBroadcastPartition(ui64 order, ui64 partitionId)  {
        auto it = Broadcasting.find(order);
        Y_ABORT_UNLESS(it != Broadcasting.end());

        auto& broadcast = it->second;
        broadcast.CompletedPartitions.insert(partitionId);

        return MaybeCompleteBroadcast(order);
    }

    bool MaybeCompleteBroadcast(ui64 order) {
        auto it = Broadcasting.find(order);
        Y_ABORT_UNLESS(it != Broadcasting.end());

        auto& broadcast = it->second;
        if (broadcast.PendingPartitions || broadcast.Partitions.size() != broadcast.CompletedPartitions.size()) {
            return false;
        }

        Broadcasting.erase(it);
        return true;
    }

    void ProcessBroadcasting(std::function<bool(TBaseChangeSender*, ui64, ui64)> f,
        ui64 partitionId, const TVector<ui64>& broadcasting)
    {
        TVector<ui64> remove;
        for (const auto order : broadcasting) {
            if (std::invoke(f, this, order, partitionId)) {
                remove.push_back(order);
            }
        }

        if (remove) {
            RemoveRecords(std::move(remove));
        }
    }

protected:
    template <typename T>
    void RemoveRecords(TVector<T>&& records) {
        TVector<ui64> remove(Reserve(records.size()));
        for (const auto& record : records) {
            remove.push_back(record.Order);
        }

        ActorOps->Send(GetChangeServer(), new TEvChangeExchange::TEvRemoveRecords(std::move(remove)));
    }

    template <>
    void RemoveRecords(TVector<ui64>&& records) {
        ActorOps->Send(GetChangeServer(), new TEvChangeExchange::TEvRemoveRecords(std::move(records)));
    }

    TActorId GetChangeServer() const { return ChangeServer; }
    void CreateSenders(const TVector<ui64>& partitionIds, bool partitioningChanged = true) {
        if (partitioningChanged) {
            CreateMissingSenders(partitionIds);
        } else {
            RecreateSenders(GonePartitions);
        }

        GonePartitions.clear();

        if (!Enqueued || !RequestRecords()) {
            SendRecords();
        }
    }

    void KillSenders() {
        for (const auto& [_, sender] : std::exchange(Senders, {})) {
            if (sender.ActorId) {
                ActorOps->Send(sender.ActorId, new TEvents::TEvPoisonPill());
            }
        }
    }

    void RemoveRecords() {
        THashSet<ui64> remove;

        for (const auto& record : std::exchange(Enqueued, {})) {
            remove.insert(record.Order);
        }

        for (const auto& record : std::exchange(PendingBody, {})) {
            remove.insert(record.Order);
        }

        for (const auto& [order, _] : std::exchange(PendingSent, {})) {
            remove.insert(order);
        }

        for (const auto& [order, _] : std::exchange(Broadcasting, {})) {
            remove.insert(order);
        }

        for (const auto& [_, sender] : Senders) {
            for (const auto& record : sender.Pending) {
                remove.insert(record.Order);
            }

            for (const auto& record : sender.Prepared) {
                remove.insert(record->GetOrder());
            }
        }

        if (remove) {
            RemoveRecords(TVector<ui64>(remove.begin(), remove.end()));
        }
    }

    void EnqueueRecords(TVector<TEvChangeExchange::TEvEnqueueRecords::TRecordInfo>&& records) {
        for (auto& record : records) {
            Y_VERIFY_S(PathId == record.PathId, "Unexpected record's path id"
                << ": expected# " << PathId
                << ", got# " << record.PathId);
            Enqueued.emplace(record.Order, record.BodySize);
        }

        RequestRecords();
    }

    void ProcessRecords(TVector<typename TChangeRecord::TPtr>&& records) {
        for (auto& record : records) {
            auto it = PendingBody.find(record->GetOrder());
            if (it == PendingBody.end()) {
                continue;
            }

            if (it->BodySize != record->GetBody().size()) {
                MemUsage -= it->BodySize;
                MemUsage += record->GetBody().size();
            }

            if (record->IsBroadcast()) {
                // assume that broadcast records are too small to affect memory consumption
                MemUsage -= record->GetBody().size();
            }

            PendingSent.emplace(record->GetOrder(), std::move(record));
            PendingBody.erase(it);
        }

        SendRecords();
    }

    void ForgetRecords(TVector<ui64>&& records) {
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

    void OnReady(ui64 partitionId) {
        auto it = Senders.find(partitionId);
        if (it == Senders.end()) {
            return;
        }

        auto& sender = it->second;
        sender.Ready = true;

        if (sender.Pending) {
            RemoveRecords(std::exchange(sender.Pending, {}));
        }

        if (sender.Broadcasting) {
            ProcessBroadcasting(&TBaseChangeSender::CompleteBroadcastPartition,
                partitionId, std::exchange(sender.Broadcasting, {}));
        }

        if (sender.Prepared) {
            SendPreparedRecords(partitionId);
        }

        RequestRecords();
    }

    void OnGone(ui64 partitionId) {
        auto it = Senders.find(partitionId);
        if (it == Senders.end()) {
            return;
        }

        ReEnqueueRecords(it->second);
        Senders.erase(it);
        GonePartitions.push_back(partitionId);

        if (Resolver->IsResolving()) {
            return;
        }

        Resolver->Resolve();
    }

    explicit TBaseChangeSender(
        IActorOps* const actorOps,
        IChangeSenderResolver* const resolver,
        ISenderFactory* const senderFactory,
        const TActorId changeServer,
        const TPathId& pathId)
            : ActorOps(actorOps)
            , Resolver(resolver)
            , SenderFactory(senderFactory)
            , ChangeServer(changeServer)
            , PathId(pathId)
            , MemLimit(192_KB)
            , MemUsage(0)
    {}

    void RenderHtmlPage(ui64 tabletId, NMon::TEvRemoteHttpInfo::TPtr& ev, const TActorContext& ctx) {
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
            Header(html, "Change sender", tabletId);

            SimplePanel(html, "Info", [this](IOutputStream& html) {
                HTML(html) {
                    DL_CLASS("dl-horizontal") {
                        TermDesc(html, "MemLimit", MemLimit);
                        TermDesc(html, "MemUsage", MemUsage);
                    }
                }
            });

            SimplePanel(html, "Partition senders", [this, tabletId](IOutputStream& html) {
                HTML(html) {
                    TABLE_CLASS("table table-hover") {
                        TABLEHEAD() {
                            TABLER() {
                                TABLEH() { html << "#"; }
                                TABLEH() { html << "PartitionId"; }
                                TABLEH() { html << "Ready"; }
                                TABLEH() { html << "Pending"; }
                                TABLEH() { html << "Prepared"; }
                                TABLEH() { html << "Broadcasting"; }
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
                                    TABLED() { html << sender.Prepared.size(); }
                                    TABLED() { html << sender.Broadcasting.size(); }
                                    TABLED() { ActorLink(html, tabletId, PathId, partitionId); }
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
                                TABLEH() { html << "Kind"; }
                                TABLEH() { html << "Source"; }
                            }
                        }
                        TABLEBODY() {
                            ui32 i = 0;
                            for (const auto& [order, record] : PendingSent) {
                                TABLER() {
                                    TABLED() { html << ++i; }
                                    TABLED() { html << order; }
                                    TABLED() { html << record->GetGroup(); }
                                    TABLED() { html << record->GetStep(); }
                                    TABLED() { html << record->GetTxId(); }
                                    TABLED() { html << record->GetKind(); }
                                    TABLED() { html << record->GetSource(); }
                                }
                            }
                        }
                    }
                }
            });

            CollapsedPanel(html, "Broadcasting", "broadcasting", [this](IOutputStream& html) {
                HTML(html) {
                    TABLE_CLASS("table table-hover") {
                        TABLEHEAD() {
                            TABLER() {
                                TABLEH() { html << "#"; }
                                TABLEH() { html << "Order"; }
                                TABLEH() { html << "BodySize"; }
                                TABLEH() { html << "Partitions"; }
                                TABLEH() { html << "PendingPartitions"; }
                                TABLEH() { html << "CompletedPartitions"; }
                            }
                        }
                        TABLEBODY() {
                            ui32 i = 0;
                            for (const auto& [order, broadcast] : Broadcasting) {
                                TABLER() {
                                    TABLED() { html << ++i; }
                                    TABLED() { html << order; }
                                    TABLED() { html << broadcast.Record.BodySize; }
                                    TABLED() { html << broadcast.Partitions.size(); }
                                    TABLED() { html << broadcast.PendingPartitions.size(); }
                                    TABLED() { html << broadcast.CompletedPartitions.size(); }
                                }
                            }
                        }
                    }
                }
            });
        }

        ActorOps->Send(ev->Sender, new NMon::TEvRemoteHttpInfoRes(html.Str()));
    }

private:
    IActorOps* const ActorOps;
    IChangeSenderResolver* const Resolver;
    ISenderFactory* const SenderFactory;
protected:
    TActorId ChangeServer;
    const TPathId PathId;

private:
    const ui64 MemLimit;
    ui64 MemUsage;

    THashMap<ui64, TSender> Senders; // ui64 is partition id
    TSet<TEnqueuedRecord> Enqueued;
    TSet<TIncompleteRecord> PendingBody;
    TMap<ui64, typename TChangeRecord::TPtr> PendingSent; // ui64 is order
    THashMap<ui64, TBroadcast> Broadcasting; // ui64 is order

    TVector<ui64> GonePartitions;
}; // TBaseChangeSender

}
