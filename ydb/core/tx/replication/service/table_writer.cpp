#include "json_change_record.h"
#include "table_writer.h"
#include "worker.h"

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/change_exchange/change_sender_common_ops.h>
#include <ydb/core/tablet_flat/flat_row_eggs.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/scheme_cache/helpers.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/hfunc.h>

#include <util/generic/map.h>

namespace NKikimr::NReplication::NService {

class TTablePartitionWriter: public TActorBootstrapped<TTablePartitionWriter> {
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
        auto event = MakeHolder<TEvDataShard::TEvApplyReplicationChanges>();

        auto& tableId = *event->Record.MutableTableId();
        tableId.SetOwnerId(TablePathId.OwnerId);
        tableId.SetTableId(TablePathId.LocalPathId);
        // TODO: SetSchemaVersion?

        for (auto recordPtr : ev->Get()->Records) {
            const auto& record = *recordPtr->Get<TChangeRecord>();
            record.Serialize(*event->Record.AddChanges());
            // TODO: set WriteTxId, Source
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

    void Handle(TEvDataShard::TEvApplyReplicationChangesResult::TPtr&) {
        // TODO: handle result
    }

    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
        if (TabletId == ev->Get()->TabletId) {
            Leave();
        }
    }

    void Leave() {
        Send(Parent, new NChangeExchange::TEvChangeExchangePrivate::TEvGone(TabletId));
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
    explicit TTablePartitionWriter(const TActorId& parent, ui64 tabletId, const TPathId& tablePathId)
        : Parent(parent)
        , TabletId(tabletId)
        , TablePathId(tablePathId)
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
    const TPathId TablePathId;

    TActorId LeaderPipeCache;

}; // TTablePartitionWriter

class TLocalTableWriter
    : public TActor<TLocalTableWriter>
    , public NChangeExchange::TBaseChangeSender
    , public NChangeExchange::IChangeSenderResolver
    , private NSchemeCache::TSchemeCacheHelpers
{
    static TSerializedTableRange GetFullRange(ui32 keyColumnsCount) {
        TVector<TCell> fromValues(keyColumnsCount);
        TVector<TCell> toValues;
        return TSerializedTableRange(fromValues, true, toValues, false);
    }

    void LogCritAndLeave(const TString& error) {
        Y_UNUSED(error);
        Leave();
    }

    void LogWarnAndRetry(const TString& error) {
        Y_UNUSED(error);
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
        return new TTablePartitionWriter(SelfId(), partitionId, PathId);
    }

    ui64 GetPartitionId(NChangeExchange::IChangeRecord::TPtr record) const override {
        Y_ABORT_UNLESS(KeyDesc);
        Y_ABORT_UNLESS(KeyDesc->GetPartitions());

        const auto range = TTableRange(record->Get<TChangeRecord>()->GetKey());
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
        Y_ABORT_UNLESS(PendingRecords.empty());

        TVector<NChangeExchange::TEvChangeExchange::TEvEnqueueRecords::TRecordInfo> records(::Reserve(ev->Get()->Records.size()));
        for (auto& record : ev->Get()->Records) {
            records.emplace_back(record.Offset, PathId, record.Data.size());
            auto res = PendingRecords.emplace(record.Offset, TChangeRecordBuilder()
                .WithBody(std::move(record.Data))
                .WithSchema(Schema)
                .Build()
            );
            Y_ABORT_UNLESS(res.second);
        }

        EnqueueRecords(std::move(records));
    }

    void Handle(NChangeExchange::TEvChangeExchange::TEvRequestRecords::TPtr& ev) {
        TVector<NChangeExchange::IChangeRecord::TPtr> records(::Reserve(ev->Get()->Records.size()));
        for (const auto& record : ev->Get()->Records) {
            auto it = PendingRecords.find(record.Order);
            Y_ABORT_UNLESS(it != PendingRecords.end());
            records.emplace_back(it->second);
        }

        ProcessRecords(std::move(records));
    }

    void Handle(NChangeExchange::TEvChangeExchange::TEvRemoveRecords::TPtr& ev) {
        for (const auto& record : ev->Get()->Records) {
            PendingRecords.erase(record);
        }

        if (PendingRecords.empty()) {
            Send(Worker, new TEvWorker::TEvPoll());
        }
    }

    void Handle(NChangeExchange::TEvChangeExchangePrivate::TEvReady::TPtr& ev) {
        OnReady(ev->Get()->PartitionId);
    }

    void Handle(NChangeExchange::TEvChangeExchangePrivate::TEvGone::TPtr& ev) {
        OnGone(ev->Get()->PartitionId);
    }

    void Retry() {
        Schedule(TDuration::Seconds(1), new TEvents::TEvWakeup());
    }

    void Leave() {
        Send(Worker, new TEvents::TEvGone());
        PassAway();
    }

    void PassAway() override {
        KillSenders();
        TActor::PassAway();
    }

public:
    explicit TLocalTableWriter(const TPathId& tablePathId)
        : TActor(&TThis::StateWork)
        , TBaseChangeSender(this, this, tablePathId)
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
