#pragma once

#include "change_exchange.h"
#include "change_exchange_helpers.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

#include <library/cpp/actors/core/actor.h>
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/actors/core/mon.h>

#include <util/generic/hash.h>
#include <util/generic/map.h>
#include <util/generic/set.h>
#include <util/string/builder.h>

namespace NKikimr {
namespace NDataShard {

struct TEvChangeExchangePrivate {
    enum EEv {
        EvReady = EventSpaceBegin(TKikimrEvents::ES_PRIVATE),
        EvGone,
        EvEnd,
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_PRIVATE));

    template <typename TEv, ui32 TEventType>
    struct TEvWithPartitionId: public TEventLocal<TEv, TEventType> {
        ui64 PartitionId;

        explicit TEvWithPartitionId(ui64 partiionId)
            : PartitionId(partiionId)
        {
        }

        TString ToString() const override {
            return TStringBuilder() << TEventLocal<TEv, TEventType>::ToStringHeader() << " {"
                << " PartitionId: " << PartitionId
            << " }";
        }
    };

    struct TEvReady: public TEvWithPartitionId<TEvReady, EvReady> {
        using TEvWithPartitionId::TEvWithPartitionId;
    };

    struct TEvGone: public TEvWithPartitionId<TEvGone, EvGone> {
        using TEvWithPartitionId::TEvWithPartitionId;
    };

}; // TEvChangeExchangePrivate

class IChangeSender {
public:
    virtual ~IChangeSender() = default;

    virtual void CreateSenders(const TVector<ui64>& partitionIds, bool partitioningChanged = true) = 0;
    virtual void KillSenders() = 0;
    virtual IActor* CreateSender(ui64 partitionId) = 0;
    virtual void RemoveRecords() = 0;

    virtual void EnqueueRecords(TVector<TEvChangeExchange::TEvEnqueueRecords::TRecordInfo>&& records) = 0;
    virtual void ProcessRecords(TVector<TChangeRecord>&& records) = 0;
    virtual void ForgetRecords(TVector<ui64>&& records) = 0;
    virtual void OnReady(ui64 partitionId) = 0;
    virtual void OnGone(ui64 partitionId) = 0;
};

class IChangeSenderResolver {
public:
    virtual ~IChangeSenderResolver() = default;

    virtual void Resolve() = 0;
    virtual bool IsResolving() const = 0;
    virtual bool IsResolved() const = 0;
    virtual ui64 GetPartitionId(const TChangeRecord& record) const = 0;
};

class TBaseChangeSender: public IChangeSender {
    using TEnqueuedRecord = TEvChangeExchange::TEvRequestRecords::TRecordInfo;
    using TRequestedRecord = TEvChangeExchange::TEvRequestRecords::TRecordInfo;

    struct TSender {
        TActorId ActorId;
        bool Ready = false;
        TVector<TEnqueuedRecord> Pending;
        TVector<TChangeRecord> Prepared;
        TVector<ui64> Broadcasting;
    };

    struct TBroadcast {
        const TEnqueuedRecord Record;
        THashSet<ui64> Partitions;
        THashSet<ui64> PendingPartitions;
        THashSet<ui64> CompletedPartitions;
    };

    void RegisterSender(THashMap<ui64, TSender>& senders, ui64 partitionId);
    void CreateMissingSenders(const TVector<ui64>& partitionIds);
    void RecreateSenders(const TVector<ui64>& partitionIds);

    bool RequestRecords();
    void SendRecords();

    void SendPreparedRecords(ui64 partitionId);
    void ReEnqueueRecords(const TSender& sender);

    TBroadcast& EnsureBroadcast(const TChangeRecord& record);
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

        ActorOps->Send(DataShard.ActorId, new TEvChangeExchange::TEvRemoveRecords(std::move(remove)));
    }

    template <>
    void RemoveRecords(TVector<ui64>&& records) {
        ActorOps->Send(DataShard.ActorId, new TEvChangeExchange::TEvRemoveRecords(std::move(records)));
    }

    void CreateSenders(const TVector<ui64>& partitionIds, bool partitioningChanged = true) override;
    void KillSenders() override;
    void RemoveRecords() override;

    void EnqueueRecords(TVector<TEvChangeExchange::TEvEnqueueRecords::TRecordInfo>&& records) override;
    void ProcessRecords(TVector<TChangeRecord>&& records) override;
    void ForgetRecords(TVector<ui64>&& records) override;
    void OnReady(ui64 partitionId) override;
    void OnGone(ui64 partitionId) override;

    explicit TBaseChangeSender(IActorOps* actorOps, IChangeSenderResolver* resolver,
        const TDataShardId& dataShard, const TPathId& pathId);

    void RenderHtmlPage(TEvChangeExchange::ESenderType type, NMon::TEvRemoteHttpInfo::TPtr& ev, const TActorContext& ctx);

private:
    IActorOps* const ActorOps;
    IChangeSenderResolver* const Resolver;

protected:
    const TDataShardId DataShard;
    const TPathId PathId;

private:
    const ui64 MemLimit;
    ui64 MemUsage;

    THashMap<ui64, TSender> Senders; // ui64 is partition id
    TSet<TEnqueuedRecord> Enqueued;
    TSet<TRequestedRecord> PendingBody;
    TMap<ui64, TChangeRecord> PendingSent; // ui64 is order
    THashMap<ui64, TBroadcast> Broadcasting; // ui64 is order

    TVector<ui64> GonePartitions;

}; // TBaseChangeSender

struct TSchemeCacheHelpers {
    using TNavigate = NSchemeCache::TSchemeCacheNavigate;
    using TEvNavigate = TEvTxProxySchemeCache::TEvNavigateKeySet;
    using TResolve = NSchemeCache::TSchemeCacheRequest;
    using TEvResolve = TEvTxProxySchemeCache::TEvResolveKeySet;
    using TCheckFailFunc = std::function<void(const TString&)>;

    inline static TNavigate::TEntry MakeNavigateEntry(const TTableId& tableId, TNavigate::EOp op) {
        TNavigate::TEntry entry;
        entry.RequestType = TNavigate::TEntry::ERequestType::ByTableId;
        entry.TableId = tableId;
        entry.Operation = op;
        entry.ShowPrivatePath = true;
        return entry;
    }

    template <typename T>
    static bool CheckNotEmpty(const TStringBuf marker, const TAutoPtr<T>& result, TCheckFailFunc onFailure) {
        if (result) {
            return true;
        }

        onFailure(TStringBuilder() << "Empty result at '" << marker << "'");
        return false;
    }

    template <typename T>
    static bool CheckEntriesCount(const TStringBuf marker, const TAutoPtr<T>& result, ui32 expected, TCheckFailFunc onFailure) {
        if (result->ResultSet.size() == expected) {
            return true;
        }

        onFailure(TStringBuilder() << "Unexpected entries count at '" << marker << "'"
            << ": expected# " << expected
            << ", got# " << result->ResultSet.size()
            << ", result# " << result->ToString(*AppData()->TypeRegistry));
        return false;
    }

    inline static const TTableId& GetTableId(const TNavigate::TEntry& entry) {
        return entry.TableId;
    }

    inline static const TTableId& GetTableId(const TResolve::TEntry& entry) {
        return entry.KeyDescription->TableId;
    }

    template <typename T>
    static bool CheckTableId(const TStringBuf marker, const T& entry, const TTableId& expected, TCheckFailFunc onFailure) {
        if (GetTableId(entry).HasSamePath(expected)) {
            return true;
        }

        onFailure(TStringBuilder() << "Unexpected table id at '" << marker << "'"
            << ": expected# " << expected
            << ", got# " << GetTableId(entry)
            << ", entry# " << entry.ToString());
        return false;
    }

    inline static bool IsSucceeded(TNavigate::EStatus status) {
        return status == TNavigate::EStatus::Ok;
    }

    inline static bool IsSucceeded(TResolve::EStatus status) {
        return status == TResolve::EStatus::OkData;
    }

    template <typename T>
    static bool CheckEntrySucceeded(const TStringBuf marker, const T& entry, TCheckFailFunc onFailure) {
        if (IsSucceeded(entry.Status)) {
            return true;
        }

        onFailure(TStringBuilder() << "Failed entry at '" << marker << "'"
            << ": entry# " << entry.ToString());
        return false;
    }

    template <typename T>
    static bool CheckEntryKind(const TStringBuf marker, const T& entry, TNavigate::EKind expected, TCheckFailFunc onFailure) {
        if (entry.Kind == expected) {
            return true;
        }

        onFailure(TStringBuilder() << "Unexpected entry kind at '" << marker << "'"
            << ", expected# " << static_cast<ui32>(expected)
            << ", got# " << static_cast<ui32>(entry.Kind)
            << ", entry# " << entry.ToString());
        return false;
    }

}; // TSchemeCacheHelpers

} // NDataShard
} // NKikimr
