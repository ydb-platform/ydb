#include "base_table_writer.h"
#include "logging.h"
#include "service.h"
#include "worker.h"

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/change_exchange/change_sender.h>
#include <ydb/core/change_exchange/util.h>
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
#include <util/generic/set.h>
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

        for (auto record : ev->Get()->Records) {
            Serializer->Serialize(record, *event->Record.AddChanges());

            if (!source) {
                source = record->GetSourceId();
            } else {
                Y_ABORT_UNLESS(source == record->GetSourceId());
            }
        }

        if (source) {
            event->Record.SetSource(source);
        }

        Send(LeaderPipeCache, new TEvPipeCache::TEvForward(event.Release(), TabletId, true, ++SubscribeCookie));
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
            if (IsHardError(record.GetReason())) {
                return Leave(true);
            } else {
                return DelayedLeave();
            }
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
        if (TabletId == ev->Get()->TabletId && ev->Cookie == SubscribeCookie) {
            DelayedLeave();
        }
    }

    void DelayedLeave() {
        static constexpr TDuration delay = TDuration::MilliSeconds(50);
        Schedule(delay, new TEvents::TEvWakeup());
    }

    void Leave(bool hardError = false) {
        LOG_I("Leave"
            << ": hard error# " << hardError);

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

    explicit TTablePartitionWriter(
            const TActorId& parent,
            ui64 tabletId,
            const TTableId& tableId,
            THolder<IChangeRecordSerializer>&& serializer)
        : Parent(parent)
        , TabletId(tabletId)
        , TableId(tableId)
        , Serializer(std::move(serializer))
    {}

    void Bootstrap() {
        GetProxyServices();
    }

    STATEFN(StateBase) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
            sFunc(TEvents::TEvWakeup, Leave);
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

private:
    const TActorId Parent;
    const ui64 TabletId;
    const TTableId TableId;
    mutable TMaybe<TString> LogPrefix;

    TActorId LeaderPipeCache;
    ui64 SubscribeCookie = 0;
    THolder<IChangeRecordSerializer> Serializer;

}; // TTablePartitionWriter

class TLocalTableWriter
    : public TActor<TLocalTableWriter>
    , public NChangeExchange::TChangeSender
    , public NChangeExchange::IChangeSenderIdentity
    , public NChangeExchange::IChangeSenderPathResolver
    , public NChangeExchange::IChangeSenderFactory
    , private NSchemeCache::TSchemeCacheHelpers
{
    TStringBuf GetLogPrefix() const {
        if (!LogPrefix) {
            LogPrefix = TStringBuilder()
                << "[LocalTableWriter]"
                << TablePathId
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
        Leave(TEvWorker::TEvGone::SCHEME_ERROR, error);
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

    void Registered(TActorSystem*, const TActorId&) override {
        ChangeServer = SelfId();
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
        request->ResultSet.emplace_back(MakeNavigateEntry(TablePathId, TNavigate::OpTable));
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

        if (!CheckTableId(entry, TablePathId)) {
            return;
        }

        if (!CheckEntrySucceeded(entry)) {
            return;
        }

        if (!CheckEntryKind(entry, TNavigate::KindTable)) {
            return;
        }

        if (TableVersion && TableVersion == entry.Self->Info.GetVersion().GetGeneralVersion()) {
            Y_ABORT_UNLESS(Initialized);
            Resolving = false;
            return CreateSenders();
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
        Parser->SetSchema(Schema);
        KeyDesc = MakeHolder<TKeyDesc>(
            entry.TableId,
            GetFullRange(schema->KeyColumns.size()).ToTableRange(),
            TKeyDesc::ERowOperation::Update,
            schema->KeyColumns,
            TVector<TKeyDesc::TColumnOp>()
        );

        TChangeSender::SetPartitionResolver(CreateResolverFn(*KeyDesc.Get()));
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

        if (!CheckTableId(entry, TablePathId)) {
            return;
        }

        if (!CheckEntrySucceeded(entry)) {
            return;
        }

        if (!entry.KeyDescription->GetPartitions()) {
            return LogWarnAndRetry("Empty partitions");
        }

        TableVersion = entry.GeneralVersion;
        KeyDesc = std::move(entry.KeyDescription);
        CreateSenders(NChangeExchange::MakePartitionIds(KeyDesc->GetPartitions()));

        if (!Initialized) {
            LOG_D("Send handshake"
                << ": worker# " << Worker);
            Send(Worker, new TEvWorker::TEvHandshake());
            Initialized = true;
        }

        Resolving = false;
    }

    IActor* CreateSender(ui64 partitionId) const override {
        const auto tableId = TTableId(TablePathId, Schema->Version);
        return new TTablePartitionWriter(SelfId(), partitionId, tableId, Serializer->Clone());
    }

    void Handle(TEvWorker::TEvData::TPtr& ev) {
        LOG_D("Handle " << ev->Get()->ToString());

        Y_ABORT_UNLESS(PendingRecords.empty());
        TVector<NChangeExchange::TEvChangeExchange::TEvEnqueueRecords::TRecordInfo> records(::Reserve(ev->Get()->Records.size()));
        TSet<TRowVersion> versionsWithoutTxId;

        for (auto& [offset, data, _] : ev->Get()->Records) {
            auto record = Parser->Parse(ev->Get()->Source, offset, std::move(data));

            if (Mode == EWriteMode::Consistent) {
                const auto version = TRowVersion(record->GetStep(), record->GetTxId());

                if (record->GetKind() == NChangeExchange::IChangeRecord::EKind::CdcHeartbeat) {
                    TxIds.erase(TxIds.begin(), TxIds.upper_bound(version));
                    Send(Worker, new TEvService::TEvHeartbeat(version));
                    continue;
                } else if (record->GetKind() != NChangeExchange::IChangeRecord::EKind::CdcDataChange) {
                    Y_ABORT("Unexpected record kind");
                }

                if (auto it = TxIds.upper_bound(version); it != TxIds.end()) {
                    record->RewriteTxId(it->second);
                } else {
                    versionsWithoutTxId.insert(version);
                    PendingTxId[version].push_back(std::move(record));
                    continue;
                }
            }

            records.emplace_back(record->GetOrder(), TablePathId, record->GetBody().size());
            Y_ABORT_UNLESS(PendingRecords.emplace(record->GetOrder(), std::move(record)).second);
        }

        if (versionsWithoutTxId) {
            Send(Worker, new TEvService::TEvGetTxId(versionsWithoutTxId));
        }

        if (records) {
            EnqueueRecords(std::move(records));
        } else if (PendingTxId.empty()) {
            Y_ABORT_UNLESS(PendingRecords.empty());
            Send(Worker, new TEvWorker::TEvPoll());
        }
    }

    void Handle(TEvService::TEvTxIdResult::TPtr& ev) {
        LOG_D("Handle " << ev->Get()->ToString());

        TVector<NChangeExchange::TEvChangeExchange::TEvEnqueueRecords::TRecordInfo> records;

        for (const auto& kv : ev->Get()->Record.GetVersionTxIds()) {
            const auto version = TRowVersion::FromProto(kv.GetVersion());
            TxIds.emplace(version, kv.GetTxId());

            for (auto it = PendingTxId.begin(); it != PendingTxId.end();) {
                if (it->first >= version) {
                    break;
                }

                for (auto& record : it->second) {
                    record->RewriteTxId(kv.GetTxId());
                    records.emplace_back(record->GetOrder(), TablePathId, record->GetBody().size());
                    Y_ABORT_UNLESS(PendingRecords.emplace(record->GetOrder(), std::move(record)).second);
                }

                PendingTxId.erase(it++);
            }
        }

        if (records) {
            EnqueueRecords(std::move(records));
        }
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
            Leave(TEvWorker::TEvGone::SCHEME_ERROR, "Cannot apply changes");
        } else {
            OnGone(ev->Get()->PartitionId);
        }
    }

    void Retry() {
        Schedule(TDuration::Seconds(1), new TEvents::TEvWakeup());
    }

    template <typename... Args>
    void Leave(Args&&... args) {
        LOG_I("Leave");

        Send(Worker, new TEvWorker::TEvGone(std::forward<Args>(args)...));
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

    explicit TLocalTableWriter(
            EWriteMode mode,
            const TPathId& tablePathId,
            THolder<IChangeRecordParser>&& parser,
            THolder<IChangeRecordSerializer>&& serializer,
            std::function<NChangeExchange::IPartitionResolverVisitor*(const NKikimr::TKeyDesc&)>&& createResolverFn)
        : TActor(&TThis::StateWork)
        , TChangeSender(this, this, this, this, TActorId())
        , Mode(mode)
        , TablePathId(tablePathId)
        , Parser(std::move(parser))
        , Serializer(std::move(serializer))
        , CreateResolverFn(std::move(createResolverFn))
    {
    }

    TPathId GetChangeSenderIdentity() const override final {
        return TablePathId;
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvWorker::TEvHandshake, Handle);
            hFunc(TEvWorker::TEvData, Handle);
            hFunc(TEvService::TEvTxIdResult, Handle);
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
    const EWriteMode Mode;
    const TPathId TablePathId;
    THolder<IChangeRecordParser> Parser;
    THolder<IChangeRecordSerializer> Serializer;
    std::function<NChangeExchange::IPartitionResolverVisitor*(const NKikimr::TKeyDesc&)> CreateResolverFn;

    TActorId Worker;
    ui64 TableVersion = 0;
    THolder<TKeyDesc> KeyDesc;
    TLightweightSchema::TCPtr Schema;
    bool Resolving = false;
    bool Initialized = false;

    TMap<ui64, NChangeExchange::IChangeRecord::TPtr> PendingRecords;
    TMap<TRowVersion, ui64> TxIds; // key is non-inclusive right hand edge
    TMap<TRowVersion, TVector<NChangeExchange::IChangeRecord::TPtr>> PendingTxId;

}; // TLocalTableWriter

IActor* CreateLocalTableWriter(
        const TPathId& tablePathId,
        THolder<IChangeRecordParser>&& parser,
        THolder<IChangeRecordSerializer>&& serializer,
        std::function<NChangeExchange::IPartitionResolverVisitor*(const NKikimr::TKeyDesc&)>&& createResolverFn,
        EWriteMode mode)
{
    return new TLocalTableWriter(mode, tablePathId, std::move(parser), std::move(serializer), std::move(createResolverFn));
}

} // namespace NKikimr::NReplication::NService
