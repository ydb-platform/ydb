#pragma once

#include "change_exchange.h"

#include <ydb/core/tx/replication/service/json_change_record.h>
#include <ydb/core/tx/datashard/change_record.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/mon.h>

#include <util/generic/hash.h>
#include <util/generic/map.h>
#include <util/generic/set.h>
#include <util/string/builder.h>

#include <variant>

namespace NKikimr::NChangeExchange {

using TChangeRecordPtr = std::variant<NDataShard::TChangeRecord::TPtr, NReplication::NService::TChangeRecord::TPtr>;
using TChangeRecordVector = std::variant<TVector<NDataShard::TChangeRecord::TPtr>, TVector<NReplication::NService::TChangeRecord::TPtr>>;
using TChangeRecordMap = std::variant<TMap<ui64, NDataShard::TChangeRecord::TPtr>, TMap<ui64, NReplication::NService::TChangeRecord::TPtr>>;

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

class TBaseChangeSender {
    using TIncompleteRecord = TEvChangeExchange::TEvRequestRecords::TRecordInfo;

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
        TChangeRecordVector Prepared;
        TVector<ui64> Broadcasting;
    };

    struct TBroadcast {
        const TIncompleteRecord Record;
        THashSet<ui64> Partitions;
        THashSet<ui64> PendingPartitions;
        THashSet<ui64> CompletedPartitions;
    };

private:
    void LazyCreateSender(THashMap<ui64, TSender>& senders, ui64 partitionId);
    void RegisterSender(ui64 partitionId);
    void CreateMissingSenders(const TVector<ui64>& partitionIds);
    void RecreateSenders(const TVector<ui64>& partitionIds);

    bool RequestRecords();
    void SendRecords();

    void SendPreparedRecords(ui64 partitionId);
    void ReEnqueueRecords(const TSender& sender);

    TBroadcast& EnsureBroadcast(IChangeRecord::TPtr record);
    bool AddBroadcastPartition(ui64 order, ui64 partitionId);
    bool RemoveBroadcastPartition(ui64 order, ui64 partitionId);
    bool CompleteBroadcastPartition(ui64 order, ui64 partitionId);
    bool MaybeCompleteBroadcast(ui64 order);
    void ProcessBroadcasting(std::function<bool(TBaseChangeSender*, ui64, ui64)> f,
        ui64 partitionId, const TVector<ui64>& broadcasting);

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

    TActorId GetChangeServer() const { return ChangeServer; };
    void CreateSenders(const TVector<ui64>& partitionIds, bool partitioningChanged = true);
    void KillSenders();
    void RemoveRecords();

    void EnqueueRecords(TVector<TEvChangeExchange::TEvEnqueueRecords::TRecordInfo>&& records);
    void ProcessRecords(TChangeRecordVector&& records);
    void ForgetRecords(TVector<ui64>&& records);
    void OnReady(ui64 partitionId);
    void OnGone(ui64 partitionId);

    explicit TBaseChangeSender(
        IActorOps* const actorOps,
        IChangeSenderResolver* const resolver,
        ISenderFactory* const senderFactory,
        const TActorId changeServer,
        const TPathId& pathId);

    void RenderHtmlPage(ui64 tabletId, NMon::TEvRemoteHttpInfo::TPtr& ev, const TActorContext& ctx);

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
    TChangeRecordMap PendingSent; // ui64 is order
    THashMap<ui64, TBroadcast> Broadcasting; // ui64 is order

    TVector<ui64> GonePartitions;

}; // TBaseChangeSender

}
