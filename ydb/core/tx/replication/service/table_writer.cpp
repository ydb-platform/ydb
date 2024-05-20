#include "json_change_record.h"
#include "logging.h"
#include "table_writer.h"
#include "worker.h"

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/change_exchange/change_sender_common_ops.h>
#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/core/tablet_flat/flat_row_eggs.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/scheme_cache/helpers.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/services/services.pb.h>

#include <util/generic/map.h>
#include <util/generic/maybe.h>
#include <util/string/builder.h>

namespace NKikimr::NReplication::NService {

class TTablePartitionWriter: public TActorBootstrapped<TTablePartitionWriter> {
    TStringBuf GetLogPrefix() const {
        if (!LogPrefix) {
            LogPrefix = TStringBuilder()
                << "[TablePartitionWriter]"
                << TableId
                << "[" << TabletId << "]"
                << SelfId() << " ";
        }

        return LogPrefix.GetRef();
    }

    void GetProxyServices() {
        Send(MakeTxProxyID(), new TEvTxUserProxy::TEvGetProxyServicesRequest());
        Become(&TThis::StateGetProxyServices);
    }

    STATEFN(StateGetProxyServices) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxUserProxy::TEvGetProxyServicesResponse, Handle);
        default:
            return StateBase(ev);
        }
    }

    void Handle(TEvTxUserProxy::TEvGetProxyServicesResponse::TPtr& ev) {
        LOG_D("Handle " << ev->Get()->ToString());

        LeaderPipeCache = ev->Get()->Services.LeaderPipeCache;
        Ready();
    }

    void Ready() {
        Send(Parent, new NChangeExchange::TEvChangeExchangePrivate::TEvReady(TabletId));
        Become(&TThis::StateWaitingRecords);
    }

    STATEFN(StateWaitingRecords) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NChangeExchange::TEvChangeExchange::TEvRecords, Handle);
        default:
            return StateBase(ev);
        }
    }

    void Handle(NChangeExchange::TEvChangeExchange::TEvRecords::TPtr& ev) {
        LOG_D("Handle " << ev->Get()->ToString());

        auto event = MakeHolder<TEvDataShard::TEvApplyReplicationChanges>();
        auto& tableId = *event->Record.MutableTableId();
        tableId.SetOwnerId(TableId.PathId.OwnerId);
        tableId.SetTableId(TableId.PathId.LocalPathId);
        tableId.SetSchemaVersion(TableId.SchemaVersion);

        TString source;
        for (auto recordPtr : ev->Get()->Records) {
            MemoryPool.Clear();
            const auto& record = *recordPtr->Get<TChangeRecord>();
            record.Serialize(*event->Record.AddChanges(), MemoryPool);

            if (!source) {
                source = record.GetSourceId();
            } else {
                Y_ABORT_UNLESS(source == record.GetSourceId());
            }
        }

        if (source) {
            event->Record.SetSource(source);
        }

        Send(LeaderPipeCache, new TEvPipeCache::TEvForward(event.Release(), TabletId, false));
        Become(&TThis::StateWaitingStatus);
    }

    STATEFN(StateWaitingStatus) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvDataShard::TEvApplyReplicationChangesResult, Handle);
        default:
            return StateBase(ev);
        }
    }

    void Handle(TEvDataShard::TEvApplyReplicationChangesResult::TPtr& ev) {
        LOG_D("Handle " << ev->Get()->ToString());

        const auto& record = ev->Get()->Record;
        switch (record.GetStatus()) {
        case NKikimrTxDataShard::TEvApplyReplicationChangesResult::STATUS_OK:
            return Ready();
        default:
            LOG_E("Apply result"
                << ": status# " << static_cast<ui32>(record.GetStatus())
                << ", reason# " << static_cast<ui32>(record.GetReason())
                << ", error# " << record.GetErrorDescription());
            return Leave(IsHardError(record.GetReason()));
        }
    }

    static bool IsHardError(NKikimrTxDataShard::TEvApplyReplicationChangesResult::EReason reason) {
        switch (reason) {
        case NKikimrTxDataShard::TEvApplyReplicationChangesResult::REASON_SCHEME_ERROR:
        case NKikimrTxDataShard::TEvApplyReplicationChangesResult::REASON_BAD_REQUEST:
        case NKikimrTxDataShard::TEvApplyReplicationChangesResult::REASON_UNEXPECTED_ROW_OPERATION:
            return true;
        default:
            return false;
        }
    }

    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
        if (TabletId == ev->Get()->TabletId) {
            Leave();
        }
    }

    void Leave(bool hardError = false) {
        Send(Parent, new NChangeExchange::TEvChangeExchangePrivate::TEvGone(TabletId, hardError));
        PassAway();
    }

    void Unlink() {
        if (LeaderPipeCache) {
            Send(LeaderPipeCache, new TEvPipeCache::TEvUnlink(TabletId));
        }
    }

    void PassAway() override {
        Unlink();
        TActorBootstrapped::PassAway();
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::REPLICATION_TABLE_PARTITION_WRITER;
    }

    explicit TTablePartitionWriter(const TActorId& parent, ui64 tabletId, const TTableId& tableId)
        : Parent(parent)
        , TabletId(tabletId)
        , TableId(tableId)
        , MemoryPool(256)
    {
    }

    void Bootstrap() {
        GetProxyServices();
    }

    STATEFN(StateBase) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

private:
    const TActorId Parent;
    const ui64 TabletId;
    const TTableId TableId;
    mutable TMaybe<TString> LogPrefix;

    TActorId LeaderPipeCache;
    TMemoryPool MemoryPool;

}; // TTablePartitionWriter

class TLocalTableWriter
    : public TActor<TLocalTableWriter>
    , public NChangeExchange::TBaseChangeSender
    , public NChangeExchange::IChangeSenderResolver
    , private NSchemeCache::TSchemeCacheHelpers
{
    TStringBuf GetLogPrefix() const {
        if (!LogPrefix) {
            LogPrefix = TStringBuilder()
                << "[LocalTableWriter]"
                << PathId
                << SelfId() << " ";
        }

        return LogPrefix.GetRef();
    }

    static TSerializedTableRange GetFullRange(ui32 keyColumnsCount) {
        TVector<TCell> fromValues(keyColumnsCount);
        TVector<TCell> toValues;
        return TSerializedTableRange(fromValues, true, toValues, false);
    }

    void LogCritAndLeave(const TString& error) {
        LOG_C(error);
        Leave(TEvWorker::TEvGone::SCHEME_ERROR);
    }

    void LogWarnAndRetry(const TString& error) {
        LOG_W(error);
        Retry();
    }

    template <typename CheckFunc, typename FailFunc, typename T, typename... Args>
    bool Check(CheckFunc checkFunc, FailFunc failFunc, const T& subject, Args&&... args) {
        return checkFunc("writer", subject, std::forward<Args>(args)..., std::bind(failFunc, this, std::placeholders::_1));
    }

    template <typename T>
    bool CheckNotEmpty(const TAutoPtr<T>& result) {
        return Check(&TSchemeCacheHelpers::CheckNotEmpty<T>, &TThis::LogCritAndLeave, result);
    }

    template <typename T>
    bool CheckEntriesCount(const TAutoPtr<T>& result, ui32 expected) {
        return Check(&TSchemeCacheHelpers::CheckEntriesCount<T>, &TThis::LogCritAndLeave, result, expected);
    }

    template <typename T>
    bool CheckTableId(const T& entry, const TTableId& expected) {
        return Check(&TSchemeCacheHelpers::CheckTableId<T>, &TThis::LogCritAndLeave, entry, expected);
    }

    template <typename T>
    bool CheckEntrySucceeded(const T& entry) {
        return Check(&TSchemeCacheHelpers::CheckEntrySucceeded<T>, &TThis::LogWarnAndRetry, entry);
    }

    template <typename T>
    bool CheckEntryKind(const T& entry, TNavigate::EKind expected) {
        return Check(&TSchemeCacheHelpers::CheckEntryKind<T>, &TThis::LogCritAndLeave, entry, expected);
    }

    static TVector<ui64> MakePartitionIds(const TVector<TKeyDesc::TPartitionInfo>& partitions) {
        TVector<ui64> result(::Reserve(partitions.size()));

        for (const auto& partition : partitions) {
            result.push_back(partition.ShardId);
        }

        return result;
    }

    TActorId GetChangeServer() const override {
        return SelfId();
    }

    void Resolve() override {
        ResolveTable();
    }

    bool IsResolving() const override {
        return Resolving;
    }

    bool IsResolved() const override {
        return KeyDesc && KeyDesc->GetPartitions();
    }

    void Handle(TEvWorker::TEvHandshake::TPtr& ev) {
        Worker = ev->Sender;
        LOG_D("Handshake"
            << ": worker# " << Worker);

        ResolveTable();
    }

    void ResolveTable() {
        Resolving = true;

        auto request = MakeHolder<TNavigate>();
        request->ResultSet.emplace_back(MakeNavigateEntry(PathId, TNavigate::OpTable));
        Send(MakeSchemeCacheID(), new TEvNavigate(request.Release()));
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const auto& result = ev->Get()->Request;

        LOG_D("Handle TEvTxProxySchemeCache::TEvNavigateKeySetResult"
            << ": result# " << (result ? result->ToString(*AppData()->TypeRegistry) : "nullptr"));

        if (!CheckNotEmpty(result)) {
            return;
        }

        if (!CheckEntriesCount(result, 1)) {
            return;
        }

        const auto& entry = result->ResultSet.at(0);

        if (!CheckTableId(entry, PathId)) {
            return;
        }

        if (!CheckEntrySucceeded(entry)) {
            return;
        }

        if (!CheckEntryKind(entry, TNavigate::KindTable)) {
            return;
        }

        auto schema = MakeIntrusive<TLightweightSchema>();
        if (entry.Self && entry.Self->Info.HasVersion()) {
            schema->Version = entry.Self->Info.GetVersion().GetTableSchemaVersion();
        }

        for (const auto& [_, column] : entry.Columns) {
            if (column.KeyOrder >= 0) {
                if (schema->KeyColumns.size() <= static_cast<ui32>(column.KeyOrder)) {
                    schema->KeyColumns.resize(column.KeyOrder + 1);
                }

                schema->KeyColumns[column.KeyOrder] = column.PType;
            } else {
                auto res = schema->ValueColumns.emplace(column.Name, TLightweightSchema::TColumn{
                    .Tag = column.Id,
                    .Type = column.PType,
                });
                Y_ABORT_UNLESS(res.second);
            }
        }

        Schema = schema;
        KeyDesc = MakeHolder<TKeyDesc>(
            entry.TableId,
            GetFullRange(schema->KeyColumns.size()).ToTableRange(),
            TKeyDesc::ERowOperation::Update,
            schema->KeyColumns,
            TVector<TKeyDesc::TColumnOp>()
        );

        ResolveKeys();
    }

    void ResolveKeys() {
        auto request = MakeHolder<TResolve>();
        request->ResultSet.emplace_back(std::move(KeyDesc));
        Send(MakeSchemeCacheID(), new TEvResolve(request.Release()));
    }

    void Handle(TEvTxProxySchemeCache::TEvResolveKeySetResult::TPtr& ev) {
        const auto& result = ev->Get()->Request;

        LOG_D("Handle TEvTxProxySchemeCache::TEvResolveKeySetResult"
            << ": result# " << (result ? result->ToString(*AppData()->TypeRegistry) : "nullptr"));

        if (!CheckNotEmpty(result)) {
            return;
        }

        if (!CheckEntriesCount(result, 1)) {
            return;
        }

        auto& entry = result->ResultSet.at(0);

        if (!CheckTableId(entry, PathId)) {
            return;
        }

        if (!CheckEntrySucceeded(entry)) {
            return;
        }

        if (!entry.KeyDescription->GetPartitions()) {
            return LogWarnAndRetry("Empty partitions");
        }

        const bool versionChanged = !TableVersion || TableVersion != entry.GeneralVersion;
        TableVersion = entry.GeneralVersion;

        KeyDesc = std::move(entry.KeyDescription);
        CreateSenders(MakePartitionIds(KeyDesc->GetPartitions()), versionChanged);

        Send(Worker, new TEvWorker::TEvHandshake());
        Resolving = false;
    }

    IActor* CreateSender(ui64 partitionId) override {
        return new TTablePartitionWriter(SelfId(), partitionId, TTableId(PathId, Schema->Version));
    }

    ui64 GetPartitionId(NChangeExchange::IChangeRecord::TPtr record) const override {
        Y_ABORT_UNLESS(KeyDesc);
        Y_ABORT_UNLESS(KeyDesc->GetPartitions());

        MemoryPool.Clear();
        const auto range = TTableRange(record->Get<TChangeRecord>()->GetKey(MemoryPool));
        Y_ABORT_UNLESS(range.Point);

        TVector<TKeyDesc::TPartitionInfo>::const_iterator it = LowerBound(
            KeyDesc->GetPartitions().begin(), KeyDesc->GetPartitions().end(), true,
            [&](const TKeyDesc::TPartitionInfo& partition, bool) {
                const int compares = CompareBorders<true, false>(
                    partition.Range->EndKeyPrefix.GetCells(), range.From,
                    partition.Range->IsInclusive || partition.Range->IsPoint,
                    range.InclusiveFrom || range.Point, KeyDesc->KeyColumnTypes
                );

                return (compares < 0);
            }
        );

        Y_ABORT_UNLESS(it != KeyDesc->GetPartitions().end());
        return it->ShardId;
    }

    void Handle(TEvWorker::TEvData::TPtr& ev) {
        LOG_D("Handle " << ev->Get()->ToString());

        Y_ABORT_UNLESS(PendingRecords.empty());
        TVector<NChangeExchange::TEvChangeExchange::TEvEnqueueRecords::TRecordInfo> records(::Reserve(ev->Get()->Records.size()));

        for (auto& record : ev->Get()->Records) {
            records.emplace_back(record.Offset, PathId, record.Data.size());
            auto res = PendingRecords.emplace(record.Offset, TChangeRecordBuilder()
                .WithSourceId(ev->Get()->Source)
                .WithOrder(record.Offset)
                .WithBody(std::move(record.Data))
                .WithSchema(Schema)
                .Build()
            );
            Y_ABORT_UNLESS(res.second);
        }

        EnqueueRecords(std::move(records));
    }

    void Handle(NChangeExchange::TEvChangeExchange::TEvRequestRecords::TPtr& ev) {
        LOG_D("Handle " << ev->Get()->ToString());

        TVector<NChangeExchange::IChangeRecord::TPtr> records(::Reserve(ev->Get()->Records.size()));

        for (const auto& record : ev->Get()->Records) {
            auto it = PendingRecords.find(record.Order);
            Y_ABORT_UNLESS(it != PendingRecords.end());
            records.emplace_back(it->second);
        }

        ProcessRecords(std::move(records));
    }

    void Handle(NChangeExchange::TEvChangeExchange::TEvRemoveRecords::TPtr& ev) {
        LOG_D("Handle " << ev->Get()->ToString());

        for (const auto& record : ev->Get()->Records) {
            PendingRecords.erase(record);
        }

        if (PendingRecords.empty()) {
            Send(Worker, new TEvWorker::TEvPoll());
        }
    }

    void Handle(NChangeExchange::TEvChangeExchangePrivate::TEvReady::TPtr& ev) {
        LOG_D("Handle " << ev->Get()->ToString());
        OnReady(ev->Get()->PartitionId);
    }

    void Handle(NChangeExchange::TEvChangeExchangePrivate::TEvGone::TPtr& ev) {
        LOG_D("Handle " << ev->Get()->ToString());

        if (ev->Get()->HardError) {
            Leave(TEvWorker::TEvGone::SCHEME_ERROR);
        } else {
            OnGone(ev->Get()->PartitionId);
        }
    }

    void Retry() {
        Schedule(TDuration::Seconds(1), new TEvents::TEvWakeup());
    }

    void Leave(TEvWorker::TEvGone::EStatus status) {
        Send(Worker, new TEvWorker::TEvGone(status));
        PassAway();
    }

    void PassAway() override {
        KillSenders();
        TActor::PassAway();
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::REPLICATION_LOCAL_TABLE_WRITER;
    }

    explicit TLocalTableWriter(const TPathId& tablePathId)
        : TActor(&TThis::StateWork)
        , TBaseChangeSender(this, this, tablePathId)
        , MemoryPool(256)
    {
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvWorker::TEvHandshake, Handle);
            hFunc(TEvWorker::TEvData, Handle);
            hFunc(NChangeExchange::TEvChangeExchange::TEvRequestRecords, Handle);
            hFunc(NChangeExchange::TEvChangeExchange::TEvRemoveRecords, Handle);
            hFunc(NChangeExchange::TEvChangeExchangePrivate::TEvReady, Handle);
            hFunc(NChangeExchange::TEvChangeExchangePrivate::TEvGone, Handle);
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            hFunc(TEvTxProxySchemeCache::TEvResolveKeySetResult, Handle);
            sFunc(TEvents::TEvWakeup, ResolveTable);
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

private:
    mutable TMaybe<TString> LogPrefix;
    mutable TMemoryPool MemoryPool;

    TActorId Worker;
    ui64 TableVersion = 0;
    THolder<TKeyDesc> KeyDesc;
    TLightweightSchema::TCPtr Schema;
    bool Resolving = false;

    TMap<ui64, NChangeExchange::IChangeRecord::TPtr> PendingRecords;

}; // TLocalTableWriter

IActor* CreateLocalTableWriter(const TPathId& tablePathId) {
    return new TLocalTableWriter(tablePathId);
}

}
