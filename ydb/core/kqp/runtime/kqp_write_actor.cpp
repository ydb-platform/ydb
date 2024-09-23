#include "kqp_write_actor.h"

#include "kqp_write_table.h"
#include "kqp_write_actor_settings.h"

#include <util/generic/singleton.h>
#include <ydb/core/actorlib_impl/long_timer.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/engine/minikql/minikql_engine_host.h>
#include <ydb/core/formats/arrow/arrow_batch_builder.h>
#include <ydb/core/kqp/common/buffer/buffer.h>
#include <ydb/core/kqp/common/kqp_tx_manager.h>
#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/common/simple/kqp_event_ids.h>
#include <ydb/core/protos/kqp_physical.pb.h>
#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/core/tx/data_events/events.h>
#include <ydb/core/tx/data_events/payload_helper.h>
#include <ydb/core/tx/data_events/shards_splitter.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/tx.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_impl.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>


namespace {
    TDuration CalculateNextAttemptDelay(const NKikimr::NKqp::TWriteActorSettings& settings, ui64 attempt) {
        auto delay = settings.StartRetryDelay;
        for (ui64 index = 0; index < attempt; ++index) {
            delay *= settings.Multiplier;
        }

        delay *= 1 + settings.UnsertaintyRatio * (1 - 2 * RandomNumber<double>());
        delay = Min(delay, settings.MaxRetryDelay);

        return delay;
    }

    struct TLockInfo {
        bool AddAndCheckLock(const NKikimrDataEvents::TLock& lock) {
            if (!Lock) {
                Lock = lock;
                return true;
            } else {
                return lock.GetLockId() == Lock->GetLockId()
                    && lock.GetDataShard() == Lock->GetDataShard()
                    && lock.GetSchemeShard() == Lock->GetSchemeShard()
                    && lock.GetPathId() == Lock->GetPathId()
                    && lock.GetGeneration() == Lock->GetGeneration()
                    && lock.GetCounter() == Lock->GetCounter();
            }
        }

        const std::optional<NKikimrDataEvents::TLock>& GetLock() const {
            return Lock;
        }

    private:
        std::optional<NKikimrDataEvents::TLock> Lock;
    };

    class TLocksManager {
    public:
        bool AddLock(ui64 shardId, const NKikimrDataEvents::TLock& lock) {
            return Locks[shardId].AddAndCheckLock(lock);
        }

        const std::optional<NKikimrDataEvents::TLock>& GetLock(ui64 shardId) {
            return Locks[shardId].GetLock();
        }

        const THashMap<ui64, TLockInfo>& GetLocks() const {
            return Locks;
        }

    private:
        THashMap<ui64, TLockInfo> Locks;
    };

    NKikimrDataEvents::TEvWrite::TOperation::EOperationType GetOperation(NKikimrKqp::TKqpTableSinkSettings::EType type) {
        switch (type) {
        case NKikimrKqp::TKqpTableSinkSettings::MODE_REPLACE:
            return NKikimrDataEvents::TEvWrite::TOperation::OPERATION_REPLACE;
        case NKikimrKqp::TKqpTableSinkSettings::MODE_UPSERT:
            return NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT;
        case NKikimrKqp::TKqpTableSinkSettings::MODE_INSERT:
            return NKikimrDataEvents::TEvWrite::TOperation::OPERATION_INSERT;
        case NKikimrKqp::TKqpTableSinkSettings::MODE_DELETE:
            return NKikimrDataEvents::TEvWrite::TOperation::OPERATION_DELETE;
        case NKikimrKqp::TKqpTableSinkSettings::MODE_UPDATE:
            return NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPDATE;
        default:
            return NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UNSPECIFIED;
        }
    }
}


namespace NKikimr {
namespace NKqp {

struct IKqpTableWriterCallbacks {
    virtual ~IKqpTableWriterCallbacks() = default;

    // Ready to accept writes
    virtual void OnReady() = 0;

    // EvWrite statuses
    virtual void OnPrepared(TPreparedInfo&& preparedInfo, ui64 dataSize) = 0;
    virtual void OnCommitted(ui64 shardId, ui64 dataSize) = 0;
    virtual void OnMessageAcknowledged(ui64 shardId, TTableId tableId, ui64 dataSize, bool hasRead) = 0;

    virtual void OnError(const TString& message, NYql::NDqProto::StatusIds::StatusCode statusCode, const NYql::TIssues& subIssues) = 0;
};

class TKqpTableWriteActor : public TActorBootstrapped<TKqpTableWriteActor> {
    using TBase = TActorBootstrapped<TKqpTableWriteActor>;

    struct TEvPrivate {
        enum EEv {
            EvShardRequestTimeout = EventSpaceBegin(TKikimrEvents::ES_PRIVATE),
            EvResolveRequestPlanned,
            EvTerminate,
        };

        struct TEvShardRequestTimeout : public TEventLocal<TEvShardRequestTimeout, EvShardRequestTimeout> {
            ui64 ShardId;

            TEvShardRequestTimeout(ui64 shardId)
                : ShardId(shardId) {
            }
        };

        struct TEvResolveRequestPlanned : public TEventLocal<TEvResolveRequestPlanned, EvResolveRequestPlanned> {
        };

        struct TEvTerminate : public TEventLocal<TEvTerminate, EvTerminate> {
        };
    };

    enum class EMode {
        WRITE,
        PREPARE,
        COMMIT,
        IMMEDIATE_COMMIT,
    };

public:
    TKqpTableWriteActor(
        IKqpTableWriterCallbacks* callbacks,
        const TTableId& tableId,
        const TStringBuf tablePath,
        const ui64 lockTxId,
        const ui64 lockNodeId,
        const bool inconsistentTx,
        const NMiniKQL::TTypeEnvironment& typeEnv,
        std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> alloc)
        : TypeEnv(typeEnv)
        , Alloc(alloc)
        , TableId(tableId)
        , TablePath(tablePath)
        , LockTxId(lockTxId)
        , LockNodeId(lockNodeId)
        , InconsistentTx(inconsistentTx)
        , Callbacks(callbacks)
    {
        try {
            ShardedWriteController = CreateShardedWriteController(
                TShardedWriteControllerSettings {
                    .MemoryLimitTotal = MessageSettings.InFlightMemoryLimitPerActorBytes,
                    .MemoryLimitPerMessage = MessageSettings.MemoryLimitPerMessageBytes,
                    .MaxBatchesPerMessage = (SchemeEntry->Kind == NSchemeCache::TSchemeCacheNavigate::KindColumnTable
                        ? 1
                        : MessageSettings.MaxBatchesPerMessage),
                },
                TypeEnv,
                Alloc);
        } catch (...) {
            RuntimeError(
                CurrentExceptionMessage(),
                NYql::NDqProto::StatusIds::INTERNAL_ERROR);
        }
    }

    void Bootstrap() {
        LogPrefix = TStringBuilder() << "SelfId: " << this->SelfId() << ", " << LogPrefix;
        ResolveTable();
        Become(&TKqpTableWriteActor::StateProcessing);
    }

    static constexpr char ActorName[] = "KQP_TABLE_WRITE_ACTOR";

    i64 GetMemory() const {
        return IsReady()
            ? ShardedWriteController->GetMemory()
            : 0;
    }

    bool IsReady() const {
        return ShardedWriteController->IsReady();
    }

    bool IsEmpty() const {
        return ShardedWriteController->IsEmpty();
    }

    bool IsOlap() const {
        YQL_ENSURE(SchemeEntry);
        return SchemeEntry->Kind == NSchemeCache::TSchemeCacheNavigate::KindColumnTable;
    }

    const TString& GetTablePath() const {
        return TablePath;
    }

    const THashMap<ui64, TLockInfo>& GetLocks() const {
        return LocksManager.GetLocks();
    }

    TVector<ui64> GetShardsIds() const {
        return ShardedWriteController->GetShardsIds();
    }

    std::optional<size_t> GetShardsCount() const {
        return InconsistentTx
            ? std::nullopt
            : std::optional<size_t>(ShardedWriteController->GetShardsCount());
    }

    using TWriteToken = IShardedWriteController::TWriteToken;

    TWriteToken Open(
        NKikimrDataEvents::TEvWrite::TOperation::EOperationType operationType,
        TVector<NKikimrKqp::TKqpColumnMetadataProto>&& columnsMetadata) {
        YQL_ENSURE(!Closed);
        auto token = ShardedWriteController->Open(
            TableId,
            operationType,
            std::move(columnsMetadata));
        return token;
    }

    void Write(TWriteToken token, const NMiniKQL::TUnboxedValueBatch& data) {
        YQL_ENSURE(!data.IsWide(), "Wide stream is not supported yet");
        YQL_ENSURE(!Closed);
        YQL_ENSURE(ShardedWriteController);
        try {
            ShardedWriteController->Write(token, data);
        } catch (...) {
            RuntimeError(
                CurrentExceptionMessage(),
                NYql::NDqProto::StatusIds::INTERNAL_ERROR);
        }
    }

    void Close(TWriteToken token) {
        YQL_ENSURE(!Closed);
        YQL_ENSURE(ShardedWriteController);
        try {
            ShardedWriteController->Close(token);
        } catch (...) {
            RuntimeError(
                CurrentExceptionMessage(),
                NYql::NDqProto::StatusIds::INTERNAL_ERROR);
        }
    }

    void Close() {
        YQL_ENSURE(!Closed);
        YQL_ENSURE(ShardedWriteController);
        YQL_ENSURE(ShardedWriteController->IsAllWritesClosed());
        Closed = true;
        ShardedWriteController->Close();
    }

    bool IsClosed() const {
        return Closed;
    }

    bool IsFinished() const {
        return IsClosed() && ShardedWriteController->IsAllWritesFinished();
    }

    STFUNC(StateProcessing) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(NKikimr::NEvents::TDataEvents::TEvWriteResult, Handle);
                hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
                hFunc(TEvTxProxySchemeCache::TEvResolveKeySetResult, Handle);
                hFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
                hFunc(TEvPrivate::TEvShardRequestTimeout, Handle);
                hFunc(TEvPrivate::TEvTerminate, Handle);
                hFunc(TEvPrivate::TEvResolveRequestPlanned, Handle);
                IgnoreFunc(TEvInterconnect::TEvNodeConnected);
                IgnoreFunc(TEvTxProxySchemeCache::TEvInvalidateTableResult);
            }
        } catch (const yexception& e) {
            RuntimeError(e.what(), NYql::NDqProto::StatusIds::INTERNAL_ERROR);
        }
    }

    STFUNC(StateTerminating) {
        Y_UNUSED(ev);
    }

    bool IsResolving() const {
        return ResolveAttempts > 0;
    }

    void RetryResolveTable() {
        if (!IsResolving()) {
            ResolveTable();
        }
    }

    void PlanResolveTable() {
        CA_LOG_D("Plan resolve with delay " << CalculateNextAttemptDelay(MessageSettings, ResolveAttempts));
        TlsActivationContext->Schedule(
            CalculateNextAttemptDelay(MessageSettings, ResolveAttempts),
            new IEventHandle(SelfId(), SelfId(), new TEvPrivate::TEvResolveRequestPlanned{}, 0, 0));   
    }

    void Handle(TEvPrivate::TEvResolveRequestPlanned::TPtr&) {
        ResolveTable();
    }

    void ResolveTable() {
        SchemeEntry.reset();
        SchemeRequest.reset();

        if (ResolveAttempts++ >= MessageSettings.MaxResolveAttempts) {
            CA_LOG_E(TStringBuilder()
                << "Too many table resolve attempts for table " << TableId << ".");
            RuntimeError(
                TStringBuilder()
                << "Too many table resolve attempts for table `" << TablePath << "`.",
                NYql::NDqProto::StatusIds::SCHEME_ERROR);
            return;
        }

        CA_LOG_D("Resolve TableId=" << TableId);
        TAutoPtr<NSchemeCache::TSchemeCacheNavigate> request(new NSchemeCache::TSchemeCacheNavigate());
        NSchemeCache::TSchemeCacheNavigate::TEntry entry;
        entry.TableId = TableId;
        entry.RequestType = NSchemeCache::TSchemeCacheNavigate::TEntry::ERequestType::ByTableId;
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpTable;
        entry.SyncVersion = false;
        entry.ShowPrivatePath = true;
        request->ResultSet.emplace_back(entry);

        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvInvalidateTable(TableId, {}));
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request));
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        auto& resultSet = ev->Get()->Request->ResultSet;
        YQL_ENSURE(resultSet.size() == 1);

        if (ev->Get()->Request->ErrorCount > 0) {
            CA_LOG_E(TStringBuilder() << "Failed to get table: "
                << TableId << "'. Entry: " << resultSet[0].ToString());
            PlanResolveTable();
            return;
        }

        SchemeEntry = resultSet[0];

        CA_LOG_D("Resolved TableId=" << TableId << " ("
            << SchemeEntry->TableId.PathId.ToString() << " "
            << SchemeEntry->TableId.SchemaVersion << ")");

        if (SchemeEntry->TableId.SchemaVersion != TableId.SchemaVersion) {
            RuntimeError(TStringBuilder() << "Schema was updated.", NYql::NDqProto::StatusIds::SCHEME_ERROR);
            return;
        }

        if (SchemeEntry->Kind == NSchemeCache::TSchemeCacheNavigate::KindColumnTable) {
            Prepare();
        } else {
            ResolveShards();
        }
    }

    void ResolveShards() {
        YQL_ENSURE(!SchemeRequest || InconsistentTx);
        YQL_ENSURE(SchemeEntry);
        CA_LOG_D("Resolve shards for TableId=" << TableId);

        TVector<TKeyDesc::TColumnOp> columns;
        TVector<NScheme::TTypeInfo> keyColumnTypes;
        for (const auto& [_, column] : SchemeEntry->Columns) {
            TKeyDesc::TColumnOp op = { column.Id, TKeyDesc::EColumnOperation::Set, column.PType, 0, 0 };
            columns.push_back(op);

            if (column.KeyOrder >= 0) {
                keyColumnTypes.resize(Max<size_t>(keyColumnTypes.size(), column.KeyOrder + 1));
                keyColumnTypes[column.KeyOrder] = column.PType;
            }
        }

        const TVector<TCell> minKey(keyColumnTypes.size());
        const TTableRange range(minKey, true, {}, false, false);
        YQL_ENSURE(range.IsFullRange(keyColumnTypes.size()));
        auto keyRange = MakeHolder<TKeyDesc>(SchemeEntry->TableId, range, TKeyDesc::ERowOperation::Update, keyColumnTypes, columns);

        TAutoPtr<NSchemeCache::TSchemeCacheRequest> request(new NSchemeCache::TSchemeCacheRequest());
        request->ResultSet.emplace_back(std::move(keyRange));

        TAutoPtr<TEvTxProxySchemeCache::TEvResolveKeySet> resolveReq(new TEvTxProxySchemeCache::TEvResolveKeySet(request));
        Send(MakeSchemeCacheID(), resolveReq.Release(), 0, 0);
    }

    void Handle(TEvTxProxySchemeCache::TEvResolveKeySetResult::TPtr& ev) {
        YQL_ENSURE(!SchemeRequest || InconsistentTx);
        auto* request = ev->Get()->Request.Get();

        if (request->ErrorCount > 0) {
            CA_LOG_E(TStringBuilder() << "Failed to get table: "
                << TableId << "'");
            PlanResolveTable();
            return;
        }

        YQL_ENSURE(request->ResultSet.size() == 1);
        SchemeRequest = std::move(request->ResultSet[0]);

        CA_LOG_D("Resolved shards for TableId=" << TableId << ". PartitionsCount=" << SchemeRequest->KeyDescription->GetPartitions().size() << ".");

        Prepare();
    }

    void Handle(NKikimr::NEvents::TDataEvents::TEvWriteResult::TPtr& ev) {
        auto getIssues = [&ev]() {
            NYql::TIssues issues;
            NYql::IssuesFromMessage(ev->Get()->Record.GetIssues(), issues);
            return issues;
        };

        CA_LOG_D("Recv EvWriteResult from ShardID=" << ev->Get()->Record.GetOrigin()
            << ", Status=" << NKikimrDataEvents::TEvWriteResult::EStatus_Name(ev->Get()->GetStatus())
            << ", TxId=" << ev->Get()->Record.GetTxId()
            << ", Locks= " << [&]() {
                TStringBuilder builder;
                for (const auto& lock : ev->Get()->Record.GetTxLocks()) {
                    builder << lock.ShortDebugString();
                }
                return builder;
            }()
            << ", Cookie=" << ev->Cookie);

        switch (ev->Get()->GetStatus()) {
        case NKikimrDataEvents::TEvWriteResult::STATUS_UNSPECIFIED: {
            CA_LOG_E("Got UNSPECIFIED for table `"
                    << SchemeEntry->TableId.PathId.ToString() << "`."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << "."
                    << getIssues().ToOneLineString());
            RuntimeError(
                TStringBuilder() << "Unspecified error for table `"
                    << SchemeEntry->TableId.PathId.ToString() << "`. "
                    << getIssues().ToOneLineString(),
                NYql::NDqProto::StatusIds::UNSPECIFIED,
                getIssues());
            return;
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_PREPARED: {
            ProcessWritePreparedShard(ev);
            return;
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED: {
            ProcessWriteCompletedShard(ev);
            return;
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_ABORTED: {
            CA_LOG_E("Got ABORTED for table `"
                    << SchemeEntry->TableId.PathId.ToString() << "`."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << "."
                    << getIssues().ToOneLineString());
            RuntimeError(
                TStringBuilder() << "Aborted for table `"
                    << SchemeEntry->TableId.PathId.ToString() << "`. "
                    << getIssues().ToOneLineString(),
                NYql::NDqProto::StatusIds::ABORTED,
                getIssues());
            return;
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_INTERNAL_ERROR: {
            CA_LOG_E("Got INTERNAL ERROR for table `"
                    << SchemeEntry->TableId.PathId.ToString() << "`."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << "."
                    << getIssues().ToOneLineString());
            
            // TODO: Add new status for splits in datashard. This is tmp solution.
            if (getIssues().ToOneLineString().Contains("in a pre/offline state assuming this is due to a finished split (wrong shard state)")) {
                ResetShardRetries(ev->Get()->Record.GetOrigin(), ev->Cookie);
                RetryResolveTable();
            } else {
                RuntimeError(
                    TStringBuilder() << "Internal error for table `"
                        << SchemeEntry->TableId.PathId.ToString() << "`. "
                        << getIssues().ToOneLineString(),
                    NYql::NDqProto::StatusIds::INTERNAL_ERROR,
                    getIssues());
            }
            return;
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_DISK_SPACE_EXHAUSTED: {
            CA_LOG_E("Got DISK_SPACE_EXHAUSTED for table `"
                    << SchemeEntry->TableId.PathId.ToString() << "`."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << "."
                    << getIssues().ToOneLineString());
            
        RuntimeError(
            TStringBuilder() << "Disk space exhausted for table `"
                << SchemeEntry->TableId.PathId.ToString() << "`. "
                << getIssues().ToOneLineString(),
            NYql::NDqProto::StatusIds::PRECONDITION_FAILED,
            getIssues());
            return;
        }        
        case NKikimrDataEvents::TEvWriteResult::STATUS_OVERLOADED: {
            CA_LOG_W("Got OVERLOADED for table `"
                << SchemeEntry->TableId.PathId.ToString() << "`."
                << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                << " Sink=" << this->SelfId() << "."
                << " Ignored this error."
                << getIssues().ToOneLineString());
            // TODO: support waiting
            if (!InconsistentTx)  {
                RuntimeError(
                    TStringBuilder() << "Tablet " << ev->Get()->Record.GetOrigin() << " is overloaded. Table `"
                        << SchemeEntry->TableId.PathId.ToString() << "`. "
                        << getIssues().ToOneLineString(),
                    NYql::NDqProto::StatusIds::OVERLOADED,
                    getIssues());
            }
            return;
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_CANCELLED: {
            CA_LOG_E("Got CANCELLED for table `"
                    << SchemeEntry->TableId.PathId.ToString() << "`."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << "."
                    << getIssues().ToOneLineString());
            RuntimeError(
                TStringBuilder() << "Cancelled request to table `"
                    << SchemeEntry->TableId.PathId.ToString() << "`."
                    << getIssues().ToOneLineString(),
                NYql::NDqProto::StatusIds::CANCELLED,
                getIssues());
            return;
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST: {
            CA_LOG_E("Got BAD REQUEST for table `"
                    << SchemeEntry->TableId.PathId.ToString() << "`."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << "."
                    << getIssues().ToOneLineString());
            RuntimeError(
                TStringBuilder() << "Bad request. Table `"
                    << SchemeEntry->TableId.PathId.ToString() << "`. "
                    << getIssues().ToOneLineString(),
                NYql::NDqProto::StatusIds::BAD_REQUEST,
                getIssues());
            return;
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_SCHEME_CHANGED: {
            CA_LOG_E("Got SCHEME CHANGED for table `"
                    << SchemeEntry->TableId.PathId.ToString() << "`."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << "."
                    << getIssues().ToOneLineString());
            if (InconsistentTx) {
                ResetShardRetries(ev->Get()->Record.GetOrigin(), ev->Cookie);
                RetryResolveTable();
            } else {
                RuntimeError(
                    TStringBuilder() << "Scheme changed. Table `"
                        << SchemeEntry->TableId.PathId.ToString() << "`. "
                        << getIssues().ToOneLineString(),
                    NYql::NDqProto::StatusIds::SCHEME_ERROR,
                    getIssues());
            }
            return;
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_LOCKS_BROKEN: {
            CA_LOG_E("Got LOCKS BROKEN for table `"
                    << SchemeEntry->TableId.PathId.ToString() << "`."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << "."
                    << getIssues().ToOneLineString());
            RuntimeError(
                TStringBuilder() << "Transaction locks invalidated. Table `"
                    << SchemeEntry->TableId.PathId.ToString() << "`. "
                    << getIssues().ToOneLineString(),
                NYql::NDqProto::StatusIds::ABORTED,
                getIssues());
            return;
        }
        }
    }

    void ProcessWritePreparedShard(NKikimr::NEvents::TDataEvents::TEvWriteResult::TPtr& ev) {
        YQL_ENSURE(Mode == EMode::PREPARE);
        const auto& record = ev->Get()->Record;
        TPreparedInfo preparedInfo;
        preparedInfo.ShardId = record.GetOrigin();
        preparedInfo.MinStep = record.GetMinStep();
        preparedInfo.MaxStep = record.GetMaxStep();
        preparedInfo.Coordinators = TVector<ui64>(record.GetDomainCoordinators().begin(),
                                                                record.GetDomainCoordinators().end());
        const auto result = ShardedWriteController->OnMessageAcknowledged(
                ev->Get()->Record.GetOrigin(), ev->Cookie);
        if (result) {
            Callbacks->OnPrepared(std::move(preparedInfo), result->DataSize);
        }
    }

    void ProcessWriteCompletedShard(NKikimr::NEvents::TDataEvents::TEvWriteResult::TPtr& ev) {
        YQL_ENSURE(SchemeEntry);
        CA_LOG_D("Got completed result TxId=" << ev->Get()->Record.GetTxId()
            << ", TabletId=" << ev->Get()->Record.GetOrigin()
            << ", Cookie=" << ev->Cookie
            << ", Locks=" << [&]() {
                TStringBuilder builder;
                for (const auto& lock : ev->Get()->Record.GetTxLocks()) {
                    builder << lock.ShortDebugString();
                }
                return builder;
            }());

        for (const auto& lock : ev->Get()->Record.GetTxLocks()) {
            if (!LocksManager.AddLock(ev->Get()->Record.GetOrigin(), lock)) {
                RuntimeError(
                    TStringBuilder() << "Transaction locks invalidated. Table `"
                        << SchemeEntry->TableId.PathId.ToString() << "`.",
                    NYql::NDqProto::StatusIds::ABORTED,
                    NYql::TIssues{});
            }
        }

        const auto result = ShardedWriteController->OnMessageAcknowledged(
                ev->Get()->Record.GetOrigin(), ev->Cookie);
        if (result && (Mode == EMode::COMMIT || Mode == EMode::IMMEDIATE_COMMIT)) {
            Callbacks->OnCommitted(ev->Get()->Record.GetOrigin(), result->DataSize);
        } else if (result) {
            Callbacks->OnMessageAcknowledged(ev->Get()->Record.GetOrigin(), SchemeEntry->TableId, result->DataSize, result->HasRead);
        }
    }

    void SetPrepare(const std::shared_ptr<TPrepareSettings>& prepareSettings) {
        YQL_ENSURE(Mode == EMode::WRITE);
        Mode = EMode::PREPARE;
        PrepareSettings = prepareSettings;
        ShardedWriteController->AddCoveringMessages();
    }

    //void SetCommit() {
    //    //TODO: do we need it?
    //    YQL_ENSURE(Mode == EMode::PREPARE);
    //    Mode = EMode::COMMIT;
    //}

    void SetImmediateCommit() {
        YQL_ENSURE(Mode == EMode::WRITE);
        Mode = EMode::IMMEDIATE_COMMIT;

        // TODO: check only one shard
        ShardedWriteController->AddCoveringMessages();
    }

    void Flush() {
        for (const size_t shardId : ShardedWriteController->GetPendingShards()) {
            SendDataToShard(shardId);
        }
    }

    void SendDataToShard(const ui64 shardId) {
        YQL_ENSURE(Mode != EMode::COMMIT);

        const auto metadata = ShardedWriteController->GetMessageMetadata(shardId);
        YQL_ENSURE(metadata);
        if (metadata->SendAttempts >= MessageSettings.MaxWriteAttempts) {
            CA_LOG_E("ShardId=" << shardId
                    << " for table '" << TablePath
                    << "': retry limit exceeded."
                    << " Sink=" << this->SelfId() << ".");
            RuntimeError(
                TStringBuilder()
                    << "ShardId=" << shardId
                    << " for table '" << TablePath
                    << "': retry limit exceeded.",
                NYql::NDqProto::StatusIds::UNAVAILABLE);
            return;
        }

        const bool isPrepare = metadata->IsFinal && Mode == EMode::PREPARE;
        const bool isImmediateCommit = metadata->IsFinal && Mode == EMode::IMMEDIATE_COMMIT;

        auto evWrite = std::make_unique<NKikimr::NEvents::TDataEvents::TEvWrite>();

        evWrite->Record.SetTxMode(isPrepare
            ? NKikimrDataEvents::TEvWrite::MODE_PREPARE
            : NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE);
        
        if (isImmediateCommit) {
            const auto lock = LocksManager.GetLock(shardId);
            if (lock) {
                auto* locks = evWrite->Record.MutableLocks();
                locks->SetOp(NKikimrDataEvents::TKqpLocks::Commit);
                locks->AddSendingShards(shardId);
                locks->AddReceivingShards(shardId);
                if (lock) {
                    *locks->AddLocks() = *lock;
                }
            }
        } else if (isPrepare) {
            evWrite->Record.SetTxId(PrepareSettings->TxId);
            auto* locks = evWrite->Record.MutableLocks();
            locks->SetOp(NKikimrDataEvents::TKqpLocks::Commit);
            
            if (!PrepareSettings->ArbiterColumnShard) {
                for (const ui64 sendingShardId : PrepareSettings->SendingShards) {
                    locks->AddSendingShards(sendingShardId);
                }
                for (const ui64 receivingShardId : PrepareSettings->ReceivingShards) {
                    locks->AddReceivingShards(receivingShardId);
                }
                if (PrepareSettings->ArbiterShard) {
                    locks->SetArbiterShard(*PrepareSettings->ArbiterShard);
                }
            } else if (PrepareSettings->ArbiterColumnShard == shardId) {
                locks->SetArbiterColumnShard(*PrepareSettings->ArbiterColumnShard);
                for (const ui64 sendingShardId : PrepareSettings->SendingShards) {
                    locks->AddSendingShards(sendingShardId);
                }
                for (const ui64 receivingShardId : PrepareSettings->ReceivingShards) {
                    locks->AddReceivingShards(receivingShardId);
                }
            } else {
                locks->SetArbiterColumnShard(*PrepareSettings->ArbiterColumnShard);
                locks->AddSendingShards(*PrepareSettings->ArbiterColumnShard);
                locks->AddReceivingShards(*PrepareSettings->ArbiterColumnShard);
                if (PrepareSettings->SendingShards.contains(shardId)) {
                    locks->AddSendingShards(shardId);
                }
                if (PrepareSettings->ReceivingShards.contains(shardId)) {
                    locks->AddReceivingShards(shardId);
                }
            }

            // TODO: multi locks (for tablestore support)
            const auto lock = LocksManager.GetLock(shardId);
            if (lock) {
                *locks->AddLocks() = *lock;
            }
        } else if (!InconsistentTx) {
            evWrite->SetLockId(LockTxId, LockNodeId);
        }

        const auto serializationResult = ShardedWriteController->SerializeMessageToPayload(shardId, *evWrite);
        YQL_ENSURE(isPrepare || isImmediateCommit || serializationResult.TotalDataSize > 0);

        CA_LOG_D("Send EvWrite to ShardID=" << shardId << ", isPrepare=" << isPrepare << ", isImmediateCommit=" << isImmediateCommit << ", TxId=" << evWrite->Record.GetTxId()
            << ", LockTxId=" << evWrite->Record.GetLockTxId() << ", LockNodeId=" << evWrite->Record.GetLockNodeId()
            << ", Locks= " << [&]() {
                TStringBuilder builder;
                for (const auto& lock : evWrite->Record.GetLocks().GetLocks()) {
                    builder << lock.ShortDebugString();
                }
                return builder;
            }()
            << ", Size=" << serializationResult.TotalDataSize << ", Cookie=" << metadata->Cookie
            << ", OperationsCount=" << metadata->OperationsCount << ", IsFinal=" << metadata->IsFinal
            << ", Attempts=" << metadata->SendAttempts);
        Send(
            PipeCacheId,
            new TEvPipeCache::TEvForward(evWrite.release(), shardId, true),
            0,
            metadata->Cookie);

        ShardedWriteController->OnMessageSent(shardId, metadata->Cookie);

        if (InconsistentTx) {
            TlsActivationContext->Schedule(
                CalculateNextAttemptDelay(MessageSettings, metadata->SendAttempts),
                new IEventHandle(
                    SelfId(),
                    SelfId(),
                    new TEvPrivate::TEvShardRequestTimeout(shardId),
                    0,
                    metadata->Cookie));
        }
    }

    void RetryShard(const ui64 shardId, const std::optional<ui64> ifCookieEqual) {
        const auto metadata = ShardedWriteController->GetMessageMetadata(shardId);
        if (!metadata || (ifCookieEqual && metadata->Cookie != ifCookieEqual)) {
            CA_LOG_D("Retry failed: not found ShardID=" << shardId << " with Cookie=" << ifCookieEqual.value_or(0));
            return;
        }

        CA_LOG_D("Retry ShardID=" << shardId << " with Cookie=" << ifCookieEqual.value_or(0));
        SendDataToShard(shardId);
    }

    void ResetShardRetries(const ui64 shardId, const ui64 cookie) {
        ShardedWriteController->ResetRetries(shardId, cookie);
    }

    void Handle(TEvPrivate::TEvShardRequestTimeout::TPtr& ev) {
        CA_LOG_W("Timeout shardID=" << ev->Get()->ShardId);
        YQL_ENSURE(InconsistentTx);
        RetryShard(ev->Get()->ShardId, ev->Cookie);
    }

    void Handle(TEvPrivate::TEvTerminate::TPtr&) {
        Become(&TKqpTableWriteActor::StateTerminating);
        PassAway();
    }

    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
        CA_LOG_W("TEvDeliveryProblem was received from tablet: " << ev->Get()->TabletId);
        if (InconsistentTx) {
            RetryShard(ev->Get()->TabletId, std::nullopt);
        } else {
            RuntimeError(
                TStringBuilder()
                    << "Error writing to table `" << SchemeEntry->TableId.PathId.ToString() << "`"
                    << ": can't deliver message to tablet " << ev->Get()->TabletId << ".",
                NYql::NDqProto::StatusIds::UNAVAILABLE);
        }
    }

    void Prepare() {
        YQL_ENSURE(SchemeEntry);
        ResolveAttempts = 0;

        try {
            if (SchemeEntry->Kind == NSchemeCache::TSchemeCacheNavigate::KindColumnTable) {
                ShardedWriteController->OnPartitioningChanged(*SchemeEntry);
            } else {
                ShardedWriteController->OnPartitioningChanged(*SchemeEntry, std::move(*SchemeRequest));
                SchemeRequest.reset();
            }
        } catch (...) {
            RuntimeError(
                CurrentExceptionMessage(),
                NYql::NDqProto::StatusIds::INTERNAL_ERROR);
        }

        Callbacks->OnReady();
    }

    void RuntimeError(const TString& message, NYql::NDqProto::StatusIds::StatusCode statusCode, const NYql::TIssues& subIssues = {}) {
        Callbacks->OnError(message, statusCode, subIssues);
    }

    void PassAway() override {;
        Send(PipeCacheId, new TEvPipeCache::TEvUnlink(0));
        TActorBootstrapped<TKqpTableWriteActor>::PassAway();
    }

    void Terminate() {
        Send(this->SelfId(), new TEvPrivate::TEvTerminate{});
    }

    NActors::TActorId PipeCacheId = NKikimr::MakePipePerNodeCacheID(false);

    TString LogPrefix;
    TWriteActorSettings MessageSettings; // TODO: fill it
    const NMiniKQL::TTypeEnvironment& TypeEnv;
    std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> Alloc;

    const TTableId TableId;
    const TString TablePath;

    const ui64 LockTxId;
    const ui64 LockNodeId;
    const bool InconsistentTx;

    IKqpTableWriterCallbacks* Callbacks;

    std::optional<NSchemeCache::TSchemeCacheNavigate::TEntry> SchemeEntry;
    std::optional<NSchemeCache::TSchemeCacheRequest::TEntry> SchemeRequest;
    ui64 ResolveAttempts = 0;

    TLocksManager LocksManager;
    bool Closed = false;
    EMode Mode = EMode::WRITE;
    
    std::shared_ptr<TPrepareSettings> PrepareSettings;

    IShardedWriteControllerPtr ShardedWriteController = nullptr;
};

class TKqpDirectWriteActor : public TActorBootstrapped<TKqpDirectWriteActor>, public NYql::NDq::IDqComputeActorAsyncOutput, public IKqpTableWriterCallbacks {
    using TBase = TActorBootstrapped<TKqpDirectWriteActor>;

public:
    TKqpDirectWriteActor(
        NKikimrKqp::TKqpTableSinkSettings&& settings,
        NYql::NDq::TDqAsyncIoFactory::TSinkArguments&& args,
        TIntrusivePtr<TKqpCounters> counters)
        : LogPrefix(TStringBuilder() << "TxId: " << args.TxId << ", task: " << args.TaskId << ". ")
        , Settings(std::move(settings))
        , MessageSettings(GetWriteActorSettings())
        , OutputIndex(args.OutputIndex)
        , Callbacks(args.Callback)
        , Counters(counters)
        , TypeEnv(args.TypeEnv)
        , Alloc(args.Alloc)
        , TxId(std::get<ui64>(args.TxId))
        , TableId(
            Settings.GetTable().GetOwnerId(),
            Settings.GetTable().GetTableId(),
            Settings.GetTable().GetVersion())
    {
        EgressStats.Level = args.StatsLevel;
    }

    void Bootstrap() {
        LogPrefix = TStringBuilder() << "SelfId: " << this->SelfId() << ", " << LogPrefix;

        WriteTableActor = new TKqpTableWriteActor(
            this,
            TableId,
            Settings.GetTable().GetPath(),
            Settings.GetLockTxId(),
            Settings.GetLockNodeId(),
            Settings.GetInconsistentTx(),
            TypeEnv,
            Alloc);

        WriteTableActorId = RegisterWithSameMailbox(WriteTableActor);

        TVector<NKikimrKqp::TKqpColumnMetadataProto> columnsMetadata;
        columnsMetadata.reserve(Settings.GetColumns().size());
        for (const auto & column : Settings.GetColumns()) {
            columnsMetadata.push_back(column);
        }
        WriteToken = WriteTableActor->Open(GetOperation(Settings.GetType()), std::move(columnsMetadata));
        WaitingForTableActor = true;
    }

    static constexpr char ActorName[] = "KQP_DIRECT_WRITE_ACTOR";

private:
    virtual ~TKqpDirectWriteActor() {
    }

    void CommitState(const NYql::NDqProto::TCheckpoint&) final {};
    void LoadState(const NYql::NDq::TSinkState&) final {};

    ui64 GetOutputIndex() const final {
        return OutputIndex;
    }

    const NYql::NDq::TDqAsyncStats& GetEgressStats() const final {
        return EgressStats;
    }

    i64 GetFreeSpace() const final {
        return (WriteTableActor && WriteTableActor->IsReady())
            ? MessageSettings.InFlightMemoryLimitPerActorBytes - GetMemory()
            : std::numeric_limits<i64>::min(); // Can't use zero here because compute can use overcommit!
    }

    i64 GetMemory() const {
        return (WriteTableActor && WriteTableActor->IsReady())
            ? WriteTableActor->GetMemory()
            : 0;
    }

    TMaybe<google::protobuf::Any> ExtraData() override {
        NKikimrKqp::TEvKqpOutputActorResultInfo resultInfo;
        for (const auto& [_, lockInfo] : WriteTableActor->GetLocks()) {
            if (const auto lock = lockInfo.GetLock(); lock) {
                resultInfo.AddLocks()->CopyFrom(*lock);
            }
        }
        google::protobuf::Any result;
        result.PackFrom(resultInfo);
        return result;
    }

    void SendData(NMiniKQL::TUnboxedValueBatch&& data, i64 size, const TMaybe<NYql::NDqProto::TCheckpoint>&, bool finished) final {
        YQL_ENSURE(!data.IsWide(), "Wide stream is not supported yet");
        YQL_ENSURE(!Closed);
        Closed = finished;
        EgressStats.Resume();
        Y_UNUSED(size);

        YQL_ENSURE(WriteTableActor);
        WriteTableActor->Write(*WriteToken, data);
        if (Closed) {
            WriteTableActor->Close(*WriteToken);
            WriteTableActor->Close();
        }
        Process();
    }

    void Process() {
        if (GetFreeSpace() <= 0) {
            WaitingForTableActor = true;
        } else if (WaitingForTableActor && GetFreeSpace() > MessageSettings.InFlightMemoryLimitPerActorBytes / 2) {
            ResumeExecution();
        }

        if (Closed || GetFreeSpace() <= 0) {
            WriteTableActor->Flush();
        }

        if (Closed && WriteTableActor->IsFinished()) {
            CA_LOG_D("Write actor finished");
            Callbacks->OnAsyncOutputFinished(GetOutputIndex());
        }
    }

    void RuntimeError(const TString& message, NYql::NDqProto::StatusIds::StatusCode statusCode, const NYql::TIssues& subIssues = {}) {
        NYql::TIssue issue(message);
        for (const auto& i : subIssues) {
            issue.AddSubIssue(MakeIntrusive<NYql::TIssue>(i));
        }

        NYql::TIssues issues;
        issues.AddIssue(std::move(issue));

        Callbacks->OnAsyncOutputError(OutputIndex, std::move(issues), statusCode);
    }

    void PassAway() override {
        WriteTableActor->Terminate();
        //TODO: wait for writer actors?
        TActorBootstrapped<TKqpDirectWriteActor>::PassAway();
    }

    void ResumeExecution() {
        CA_LOG_D("Resuming execution.");
        WaitingForTableActor = false;
        Callbacks->ResumeExecution();
    }

    void OnReady() override {
        Process();
    }

    void OnPrepared(TPreparedInfo&&, ui64) override {
        AFL_ENSURE(false);
    }

    void OnCommitted(ui64, ui64) override {
        AFL_ENSURE(false);
    }

    void OnMessageAcknowledged(ui64 shardId, TTableId tableId, ui64 dataSize, bool hasRead) override {
        Y_UNUSED(shardId, tableId, hasRead);
        EgressStats.Bytes += dataSize;
        EgressStats.Chunks++;
        EgressStats.Splits++;
        EgressStats.Resume();
        Process();
    }

    void OnError(const TString& message, NYql::NDqProto::StatusIds::StatusCode statusCode, const NYql::TIssues& subIssues) override {
        RuntimeError(message, statusCode, subIssues);
    }

    TString LogPrefix;
    const NKikimrKqp::TKqpTableSinkSettings Settings;
    TWriteActorSettings MessageSettings;
    const ui64 OutputIndex;
    NYql::NDq::TDqAsyncStats EgressStats;
    NYql::NDq::IDqComputeActorAsyncOutput::ICallbacks * Callbacks = nullptr;
    TIntrusivePtr<TKqpCounters> Counters;
    const NMiniKQL::TTypeEnvironment& TypeEnv;
    std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> Alloc;

    const ui64 TxId;
    const TTableId TableId;
    TKqpTableWriteActor* WriteTableActor = nullptr;
    TActorId WriteTableActorId;

    std::optional<TKqpTableWriteActor::TWriteToken> WriteToken;

    bool Closed = false;

    bool WaitingForTableActor = false;
};


namespace {

struct TWriteToken {
    TTableId TableId;
    ui64 Cookie;

    bool IsEmpty() const {
        return !TableId;
    }
};

struct TTransactionSettings {
    ui64 TxId = 0;
    ui64 LockTxId = 0;
    ui64 LockNodeId = 0;
    bool InconsistentTx = false;
};

struct TWriteSettings {
    TTableId TableId;
    TString TablePath; // for error messages
    NKikimrDataEvents::TEvWrite::TOperation::EOperationType OperationType;
    TVector<NKikimrKqp::TKqpColumnMetadataProto> Columns;
    TTransactionSettings TransactionSettings;
};

struct TBufferWriteMessage {
    TActorId From;
    TWriteToken Token;
    bool Close = false;
    // TODO: move to serialized data
    std::shared_ptr<TVector<NMiniKQL::TUnboxedValueBatch>> Data;
    std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> Alloc;
};

struct TEvBufferWrite : public TEventLocal<TEvBufferWrite, TKqpEvents::EvBufferWrite> {
    bool Close = false;
    std::optional<TWriteToken> Token;
    std::optional<TWriteSettings> Settings;
    std::shared_ptr<TVector<NMiniKQL::TUnboxedValueBatch>> Data;
    std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> Alloc;
};

struct TEvBufferWriteResult : public TEventLocal<TEvBufferWriteResult, TKqpEvents::EvBufferWriteResult> {
    TWriteToken Token;
};

}


class TKqpBufferWriteActor :public TActorBootstrapped<TKqpBufferWriteActor>, public IKqpTableWriterCallbacks {
    using TBase = TActorBootstrapped<TKqpBufferWriteActor>;

public:
    enum class EState {
        WRITING, // Allow to write data to buffer.
        FLUSHING, // Force flush (for uncommitted changes visibility). Can't accept any writes in this state.
        PREPARING, // Do preparation for commit. All writers are closed. New writes wouldn't be accepted.
        COMMITTING, // Do immediate commit (single shard). All writers are closed. New writes wouldn't be accepted.
        ROLLINGBACK, // Do rollback. New writes wouldn't be accepted.
    };

public:
    TKqpBufferWriteActor(
        TKqpBufferWriterSettings&& settings)
        : SessionActorId(settings.SessionActorId)
        , MessageSettings(GetWriteActorSettings())
        , TxManager(settings.TxManager)
        , Alloc(std::make_shared<NKikimr::NMiniKQL::TScopedAlloc>(__LOCATION__))
        , TypeEnv(*Alloc)
    {
        State = EState::WRITING;
        Alloc->Release();
    }

    void Bootstrap() {
        LogPrefix = TStringBuilder() << "SelfId: " << this->SelfId() << ", " << LogPrefix;
        Become(&TKqpBufferWriteActor::StateFunc);
    }

    static constexpr char ActorName[] = "KQP_BUFFER_WRITE_ACTOR";

    STFUNC(StateFunc) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvKqpBuffer::TEvTerminate, Handle);
                hFunc(TEvKqpBuffer::TEvFlush, Handle);
                hFunc(TEvKqpBuffer::TEvCommit, Handle);
                hFunc(TEvBufferWrite, Handle);
            default:
                AFL_ENSURE(false)("unknown message", ev->GetTypeRewrite());
            }
        } catch (const yexception& e) {
            ReplyErrorAndDie(e.what(), NYql::NDqProto::StatusIds::INTERNAL_ERROR, {});
        }
    }

    void Handle(TEvBufferWrite::TPtr& ev) {
        TWriteToken token;
        if (!ev->Get()->Token) {
            AFL_ENSURE(ev->Get()->Settings);
            auto& settings = *ev->Get()->Settings;
            if (!WriteInfos.empty()) {
                AFL_ENSURE(LockTxId == settings.TransactionSettings.LockTxId);
                AFL_ENSURE(LockNodeId == settings.TransactionSettings.LockNodeId);
                AFL_ENSURE(InconsistentTx == settings.TransactionSettings.InconsistentTx);
            } else {
                LockTxId = settings.TransactionSettings.LockTxId;
                LockNodeId = settings.TransactionSettings.LockNodeId;
                InconsistentTx = settings.TransactionSettings.InconsistentTx;
            }

            auto& writeInfo = WriteInfos[settings.TableId];
            if (!writeInfo.WriteTableActor) {
                writeInfo.WriteTableActor = new TKqpTableWriteActor(
                    this,
                    settings.TableId,
                    settings.TablePath,
                    LockTxId,
                    LockNodeId,
                    InconsistentTx,
                    TypeEnv,
                    Alloc);
                writeInfo.WriteTableActorId = RegisterWithSameMailbox(writeInfo.WriteTableActor);
            }

            auto cookie = writeInfo.WriteTableActor->Open(settings.OperationType, std::move(settings.Columns));
            token = TWriteToken{settings.TableId, cookie};
        } else {
            token = *ev->Get()->Token;
        }
        
        auto& queue = DataQueues[token.TableId];
        queue.emplace();
        auto& message = queue.back();

        message.Token = token;
        message.From = ev->Sender;
        message.Close = ev->Get()->Close;
        message.Data = ev->Get()->Data;
        message.Alloc = ev->Get()->Alloc;
        
        Process();
    }

    void Process() {
        ProcessRequestQueue();
        ProcessWrite();
        ProcessAckQueue();
    }

    void ProcessRequestQueue() {
        for (auto& [tableId, queue] : DataQueues) {
            auto& writeInfo = WriteInfos.at(tableId);

            if (!writeInfo.WriteTableActor->IsReady()) {
                return;
            }

            while (!queue.empty()) {
                auto& message = queue.front();

                if (!message.Data->empty()) {
                    for (const auto& data : *message.Data) {
                        writeInfo.WriteTableActor->Write(message.Token.Cookie, data);
                    }
                }
                if (message.Close) {
                    writeInfo.WriteTableActor->Close(message.Token.Cookie);
                }

                AckQueue.push(TAckMessage{
                    .ForwardActorId = message.From,
                    .Token = message.Token,
                    .DataSize = 0,
                });

                {
                    TGuard guard(*message.Alloc);
                    message.Data = nullptr;
                }
                queue.pop();
            }
        }
    }

    void ProcessAckQueue() {
        while (!AckQueue.empty()) {
            const auto& item = AckQueue.front();
            if (GetTotalFreeSpace() >= item.DataSize) {
                auto result = std::make_unique<TEvBufferWriteResult>();
                result->Token = AckQueue.front().Token;
                Send(AckQueue.front().ForwardActorId, result.release());
                AckQueue.pop();
            } else {
                return;
            }
        }
    }

    void ProcessWrite() {
        const bool needToFlush = GetTotalFreeSpace() <= 0
            || State == EState::FLUSHING
            || State == EState::PREPARING
            || State == EState::COMMITTING
            || State == EState::ROLLINGBACK;

        if (needToFlush) {
            for (auto& [_, info] : WriteInfos) {
                if (info.WriteTableActor->IsReady()) {
                    info.WriteTableActor->Flush();
                }
            }
        }

        /*if (State == EState::PREPARING) {
            bool isFinished = true;
            for (auto& [_, info] : WriteInfos) {
                isFinished &= info.WriteTableActor->IsFinished();
            }
            if (isFinished) {
                OnFinished();
            }
        }*/
        if (State == EState::FLUSHING) {
            bool isEmpty = true;
            for (auto& [_, info] : WriteInfos) {
                isEmpty &= info.WriteTableActor->IsEmpty();
            }
            if (isEmpty) {
                OnFlushed();
            }
        }
    }

    /*THashMap<ui64, NKikimrDataEvents::TLock> GetLocks(TWriteToken token) const {
        auto& info = WriteInfos.at(token.TableId);
        THashMap<ui64, NKikimrDataEvents::TLock> result;
        for (const auto& [shardId, lockInfo] : info.WriteTableActor->GetLocks()) {
            if (const auto lock = lockInfo.GetLock(); lock) {
                result.emplace(shardId, *lock);
            }
        }
        return result;
    }

    THashMap<ui64, NKikimrDataEvents::TLock> GetLocks() const {
        THashMap<ui64, NKikimrDataEvents::TLock> result;
        for (const auto& [_, info] : WriteInfos) {
            for (const auto& [shardId, lockInfo] : info.WriteTableActor->GetLocks()) {
                if (const auto lock = lockInfo.GetLock(); lock) {
                    result.emplace(shardId, *lock);
                }
            }
        }
        return result;
    }*/

    void Flush() {
        YQL_ENSURE(State == EState::WRITING);
        State = EState::FLUSHING;
        Process();
    }

    void Prepare(const std::shared_ptr<TPrepareSettings>& prepareSettings) {
        YQL_ENSURE(State == EState::WRITING);
        State = EState::PREPARING;
        for (auto& [_, info] : WriteInfos) {
            info.WriteTableActor->SetPrepare(prepareSettings);
        }
        Close();
        Process();
    }

    //void OnCommit() {
    //    YQL_ENSURE(State == EState::PREPARING);
    //    // TODO: need it?
    //}

    void ImmediateCommit() {
        YQL_ENSURE(State == EState::WRITING);
        State = EState::COMMITTING;
        for (auto& [_, info] : WriteInfos) {
            info.WriteTableActor->SetImmediateCommit();
        }
        Close();
        Process();
    }

    void Close() {
        for (auto& [_, info] : WriteInfos) {
            if (!info.WriteTableActor->IsClosed()) {
                info.WriteTableActor->Close();
            }
        }
    }

    i64 GetFreeSpace(TWriteToken token) const {
        auto& info = WriteInfos.at(token.TableId);
        return info.WriteTableActor->IsReady()
            ? MessageSettings.InFlightMemoryLimitPerActorBytes - info.WriteTableActor->GetMemory()
            : std::numeric_limits<i64>::min(); // Can't use zero here because compute can use overcommit!
    }

    i64 GetTotalFreeSpace() const {
        return MessageSettings.InFlightMemoryLimitPerActorBytes - GetTotalMemory();
    }

    i64 GetTotalMemory() const {
        i64 totalMemory = 0;
        for (auto& [_, info] : WriteInfos) {
            totalMemory += info.WriteTableActor->IsReady()
                ? info.WriteTableActor->GetMemory()
                : 0;
        }
        return totalMemory;
    }

    THashSet<ui64> GetShardsIds() const {
        THashSet<ui64> shardIds;
        for (auto& [_, info] : WriteInfos) {
            for (const auto& id : info.WriteTableActor->GetShardsIds()) {
                shardIds.insert(id);
            }
        }
        return shardIds;
    }

    void PassAway() override {
        for (auto& [_, queue] : DataQueues) {
            while (!queue.empty()) {
                auto& message = queue.front();
                {
                    TGuard guard(*message.Alloc);
                    message.Data = nullptr;
                }
                queue.pop();
            }
        }
        
        for (auto& [_, info] : WriteInfos) {
            if (info.WriteTableActor) {
                info.WriteTableActor->Terminate();
            }
        }
        TActorBootstrapped<TKqpBufferWriteActor>::PassAway();
    }

    void Handle(TEvKqpBuffer::TEvTerminate::TPtr&) {
        PassAway();
    }

    void Handle(TEvKqpBuffer::TEvFlush::TPtr& ev) {
        ExecuterActorId = ev->Get()->ExecuterActorId;
        Flush();
    }

    void Handle(TEvKqpBuffer::TEvCommit::TPtr& ev) {
        ExecuterActorId = ev->Get()->ExecuterActorId;
        ImmediateCommit();
    }

    void OnReady() override {
        Process();
    }

    void OnPrepared(TPreparedInfo&& preparedInfo, ui64 dataSize) override {
        AFL_ENSURE(State == EState::PREPARING);
        Y_UNUSED(preparedInfo, dataSize);
        // TODO: collect info for commit
        Process();
    }

    void OnCommitted(ui64 shardId, ui64 dataSize) override {
        AFL_ENSURE(State == EState::COMMITTING);
        Y_UNUSED(shardId, dataSize);
        // TODO: check if everything is committed
        if (true) {
            Send(ExecuterActorId, new TEvKqpBuffer::TEvResult{});
            ExecuterActorId = {};
        }
        // Process(); // Don't need it?
    }

    void OnMessageAcknowledged(ui64 shardId, TTableId tableId, ui64 dataSize, bool hasRead) override {
        Y_UNUSED(dataSize);
        auto& info = WriteInfos.at(tableId);
        const auto& lockInfo = info.WriteTableActor->GetLocks().at(shardId);

        const auto lock = lockInfo.GetLock();
        YQL_ENSURE(lock);
        YQL_ENSURE(shardId == lock->GetDataShard());
        TxManager->AddShard(shardId, info.WriteTableActor->IsOlap(), info.WriteTableActor->GetTablePath());
        TxManager->AddAction(shardId, IKqpTransactionManager::EAction::WRITE);
        if (hasRead) {
            TxManager->AddAction(shardId, IKqpTransactionManager::EAction::READ);
        }
        //TxManager->AddLock(lock->GetDataShard(), lock);

        Process();
    }

    void OnFinished() {
        // TODO: send collected data
    }

    void OnFlushed() {
        State = EState::WRITING;
        Send(ExecuterActorId, new TEvKqpBuffer::TEvResult{});
        ExecuterActorId = {};
    }

    void OnError(const TString& message, NYql::NDqProto::StatusIds::StatusCode statusCode, const NYql::TIssues& subIssues) override {
        ReplyErrorAndDie(message, statusCode, subIssues);
    }

    void ReplyErrorAndDie(const TString& message, NYql::NDqProto::StatusIds::StatusCode statusCode, const NYql::TIssues& subIssues) {
        CA_LOG_E(message << ". statusCode=" << NYql::NDqProto::StatusIds_StatusCode_Name(statusCode) << ". subIssues=" << subIssues.ToString());
        Send(SessionActorId, new TEvKqpBuffer::TEvError{
            message,
            statusCode,
            subIssues,
        });
        PassAway();
    }

private:
    TString LogPrefix;
    const TActorId SessionActorId;
    TWriteActorSettings MessageSettings;

    TActorId ExecuterActorId;
    IKqpTransactionManagerPtr TxManager;

    ui64 LockTxId = 0;
    ui64 LockNodeId = 0;
    bool InconsistentTx = false;

    std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> Alloc;
    NMiniKQL::TTypeEnvironment TypeEnv;

    struct TWriteInfo {
        TKqpTableWriteActor* WriteTableActor = nullptr;
        TActorId WriteTableActorId;
    };

    THashMap<TTableId, TWriteInfo> WriteInfos;

    EState State;
    THashMap<TTableId, std::queue<TBufferWriteMessage>> DataQueues;

    struct TAckMessage {
        TActorId ForwardActorId;
        TWriteToken Token;
        i64 DataSize;
    };
    std::queue<TAckMessage> AckQueue;

    IShardedWriteControllerPtr ShardedWriteController = nullptr;
};

class TKqpForwardWriteActor : public TActorBootstrapped<TKqpForwardWriteActor>, public NYql::NDq::IDqComputeActorAsyncOutput {
    using TBase = TActorBootstrapped<TKqpForwardWriteActor>;

public:
    TKqpForwardWriteActor(
        NKikimrKqp::TKqpTableSinkSettings&& settings,
        NYql::NDq::TDqAsyncIoFactory::TSinkArguments&& args,
        TIntrusivePtr<TKqpCounters> counters)
        : LogPrefix(TStringBuilder() << "TxId: " << args.TxId << ", task: " << args.TaskId << ". ")
        , Settings(std::move(settings))
        , MessageSettings(GetWriteActorSettings())
        , OutputIndex(args.OutputIndex)
        , Callbacks(args.Callback)
        , Counters(counters)
        , TypeEnv(args.TypeEnv)
        , Alloc(args.Alloc)
        , BufferActorId(ActorIdFromProto(Settings.GetBufferActorId()))
        , TxId(std::get<ui64>(args.TxId))
        , TableId(
            Settings.GetTable().GetOwnerId(),
            Settings.GetTable().GetTableId(),
            Settings.GetTable().GetVersion())
    {
        EgressStats.Level = args.StatsLevel;
    }

    void Bootstrap() {
        LogPrefix = TStringBuilder() << "SelfId: " << this->SelfId() << ", " << LogPrefix;
        Become(&TKqpForwardWriteActor::StateFuncFwd);
    }

    static constexpr char ActorName[] = "KQP_FORWARD_WRITE_ACTOR";

private:
    STFUNC(StateFuncFwd) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvBufferWriteResult, Handle);
            default:
                AFL_ENSURE(false)("unknown message", ev->GetTypeRewrite());
            }
        } catch (const yexception& e) {
            RuntimeError(e.what(), NYql::NDqProto::StatusIds::INTERNAL_ERROR);
        }
    }

    void Handle(TEvBufferWriteResult::TPtr& result) {
        EgressStats.Bytes += DataSize;
        EgressStats.Chunks++;
        EgressStats.Splits++;
        EgressStats.Resume();

        WriteToken = result->Get()->Token;
        DataSize = 0;
        {
            auto alloc = TypeEnv.BindAllocator();
            Data = nullptr;
        }

        if (Closed) {
            Callbacks->OnAsyncOutputFinished(GetOutputIndex());
        }
        Callbacks->ResumeExecution();
    }

    void WriteToBuffer() {
        auto ev = std::make_unique<TEvBufferWrite>();

        ev->Data = Data;
        ev->Close = Closed;
        ev->Alloc = Alloc;

        if (!WriteToken.IsEmpty()) {
            ev->Token = WriteToken;
        } else {
            TVector<NKikimrKqp::TKqpColumnMetadataProto> columnsMetadata;
            columnsMetadata.reserve(Settings.GetColumns().size());
            for (const auto & column : Settings.GetColumns()) {
                columnsMetadata.push_back(column);
            }

            ev->Settings = TWriteSettings{
                .TableId = TableId,
                .TablePath = Settings.GetTable().GetPath(),
                .OperationType = GetOperation(Settings.GetType()),
                .Columns = std::move(columnsMetadata),
                .TransactionSettings = TTransactionSettings{
                    .TxId = TxId,
                    .LockTxId = Settings.GetLockTxId(),
                    .LockNodeId = Settings.GetLockNodeId(),
                    .InconsistentTx = Settings.GetInconsistentTx(),
                },
            };
        }

        AFL_ENSURE(Send(BufferActorId, ev.release()));
    }

    void CommitState(const NYql::NDqProto::TCheckpoint&) final {};
    void LoadState(const NYql::NDq::TSinkState&) final {};

    ui64 GetOutputIndex() const final {
        return OutputIndex;
    }

    const NYql::NDq::TDqAsyncStats& GetEgressStats() const final {
        return EgressStats;
    }

    i64 GetFreeSpace() const final {
        return MessageSettings.MaxForwardedSize - DataSize > 0
            ? MessageSettings.MaxForwardedSize - DataSize
            : std::numeric_limits<i64>::min();
    }

    TMaybe<google::protobuf::Any> ExtraData() override {
        return {};
    }

    void SendData(NMiniKQL::TUnboxedValueBatch&& data, i64 size, const TMaybe<NYql::NDqProto::TCheckpoint>&, bool finished) final {
        YQL_ENSURE(!data.IsWide(), "Wide stream is not supported yet");
        Closed |= finished;
        if (!Data) {
            Data = std::make_shared<TVector<NMiniKQL::TUnboxedValueBatch>>();
        }

        Data->emplace_back(std::move(data));
        DataSize += size;

        if (Closed || GetFreeSpace() <= 0) {
            WriteToBuffer();
        }
    }

    void RuntimeError(const TString& message, NYql::NDqProto::StatusIds::StatusCode statusCode, const NYql::TIssues& subIssues = {}) {
        NYql::TIssue issue(message);
        for (const auto& i : subIssues) {
            issue.AddSubIssue(MakeIntrusive<NYql::TIssue>(i));
        }

        NYql::TIssues issues;
        issues.AddIssue(std::move(issue));

        Callbacks->OnAsyncOutputError(OutputIndex, std::move(issues), statusCode);
    }

    ~TKqpForwardWriteActor() {
        {
            TGuard guard(*Alloc);
            Data = nullptr;
        }
    }

    void PassAway() override {
        TActorBootstrapped<TKqpForwardWriteActor>::PassAway();
    }

    TString LogPrefix;
    const NKikimrKqp::TKqpTableSinkSettings Settings;
    TWriteActorSettings MessageSettings;
    const ui64 OutputIndex;
    NYql::NDq::TDqAsyncStats EgressStats;
    NYql::NDq::IDqComputeActorAsyncOutput::ICallbacks * Callbacks = nullptr;
    TIntrusivePtr<TKqpCounters> Counters;
    const NMiniKQL::TTypeEnvironment& TypeEnv;
    std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> Alloc;

    TActorId BufferActorId;

    std::shared_ptr<TVector<NMiniKQL::TUnboxedValueBatch>> Data;
    i64 DataSize = 0;
    bool Closed = false;

    const ui64 TxId;
    const TTableId TableId;

    TWriteToken WriteToken;
};

NActors::IActor* CreateKqpBufferWriterActor(TKqpBufferWriterSettings&& settings) {
    return new TKqpBufferWriteActor(std::move(settings));
}


void RegisterKqpWriteActor(NYql::NDq::TDqAsyncIoFactory& factory, TIntrusivePtr<TKqpCounters> counters) {
    factory.RegisterSink<NKikimrKqp::TKqpTableSinkSettings>(
        TString(NYql::KqpTableSinkName),
        [counters] (NKikimrKqp::TKqpTableSinkSettings&& settings, NYql::NDq::TDqAsyncIoFactory::TSinkArguments&& args) {
            if (!ActorIdFromProto(settings.GetBufferActorId())) {
                auto* actor = new TKqpDirectWriteActor(std::move(settings), std::move(args), counters);
                return std::make_pair<NYql::NDq::IDqComputeActorAsyncOutput*, NActors::IActor*>(actor, actor);
            } else {
                auto* actor = new TKqpForwardWriteActor(std::move(settings), std::move(args), counters);
                return std::make_pair<NYql::NDq::IDqComputeActorAsyncOutput*, NActors::IActor*>(actor, actor);
            }
        });
}

}
}
