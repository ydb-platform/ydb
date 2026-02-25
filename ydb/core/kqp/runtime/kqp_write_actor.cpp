#include "kqp_write_actor.h"

#include "kqp_buffer_lookup_actor.h"
#include "kqp_write_actor_settings.h"
#include "kqp_write_table.h"

#include <util/generic/singleton.h>
#include <ydb/core/actorlib_impl/long_timer.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/engine/minikql/minikql_engine_host.h>
#include <ydb/core/formats/arrow/arrow_batch_builder.h>
#include <ydb/core/kqp/common/buffer/buffer.h>
#include <ydb/core/kqp/common/kqp_data_integrity_trails.h>
#include <ydb/core/kqp/common/kqp_locks_tli_helpers.h>
#include <ydb/core/kqp/common/kqp_tx_manager.h>
#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/common/simple/kqp_event_ids.h>
#include <ydb/core/protos/kqp_physical.pb.h>
#include <ydb/core/protos/kqp_stats.pb.h>
#include <ydb/core/protos/query_stats.pb.h>
#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/core/tx/data_events/events.h>
#include <ydb/core/tx/data_events/payload_helper.h>
#include <ydb/core/tx/data_events/shards_splitter.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/tx.h>
#include <ydb/core/persqueue/events/global.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/wilson_ids/wilson.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_impl.h>
#include <yql/essentials/public/issue/yql_issue_message.h>


namespace {
    TDuration CalculateNextAttemptDelay(const NKikimr::NKqp::TWriteActorSettings& settings, ui64 attempt) {
        auto delay = settings.StartRetryDelay;
        for (ui64 index = 0; index < attempt && delay * (1 - settings.UnsertaintyRatio) <= settings.MaxRetryDelay; ++index) {
            delay *= settings.Multiplier;
        }

        delay *= 1 + settings.UnsertaintyRatio * (1 - 2 * RandomNumber<double>());
        delay = Min(delay, settings.MaxRetryDelay);

        return delay;
    }

    NKikimrDataEvents::TEvWrite::TOperation::EOperationType GetOperation(NKikimrKqp::TKqpTableSinkSettings::EType type) {
        switch (type) {
        case NKikimrKqp::TKqpTableSinkSettings::MODE_FILL:
        case NKikimrKqp::TKqpTableSinkSettings::MODE_REPLACE:
            return NKikimrDataEvents::TEvWrite::TOperation::OPERATION_REPLACE;
        case NKikimrKqp::TKqpTableSinkSettings::MODE_UPSERT:
        case NKikimrKqp::TKqpTableSinkSettings::MODE_UPDATE_CONDITIONAL: // Rows were already checked during WHERE execution.
            return NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT;
        case NKikimrKqp::TKqpTableSinkSettings::MODE_UPSERT_INCREMENT:
            return NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT_INCREMENT;
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

    void FillEvWritePrepare(NKikimr::NEvents::TDataEvents::TEvWrite* evWrite,
        ui64 shardId, ui64 txId, const NKikimr::NKqp::IKqpTransactionManagerPtr& txManager)
    {
        evWrite->Record.SetTxId(txId);
        auto* protoLocks = evWrite->Record.MutableLocks();
        protoLocks->SetOp(NKikimrDataEvents::TKqpLocks::Commit);

        const auto prepareSettings = txManager->GetPrepareTransactionInfo();
        if (!prepareSettings.ArbiterColumnShard) {
            for (const ui64 sendingShardId : prepareSettings.SendingShards) {
                protoLocks->AddSendingShards(sendingShardId);
            }
            for (const ui64 receivingShardId : prepareSettings.ReceivingShards) {
                protoLocks->AddReceivingShards(receivingShardId);
            }
            if (prepareSettings.Arbiter) {
                protoLocks->SetArbiterShard(*prepareSettings.Arbiter);
            }
        } else if (prepareSettings.ArbiterColumnShard == shardId
                    && !prepareSettings.SendingShards.empty()
                    && !prepareSettings.ReceivingShards.empty()) {
            protoLocks->SetArbiterColumnShard(*prepareSettings.ArbiterColumnShard);
            for (const ui64 sendingShardId : prepareSettings.SendingShards) {
                protoLocks->AddSendingShards(sendingShardId);
            }
            for (const ui64 receivingShardId : prepareSettings.ReceivingShards) {
                protoLocks->AddReceivingShards(receivingShardId);
            }
        } else if (!prepareSettings.SendingShards.empty()
                    && !prepareSettings.ReceivingShards.empty()) {
            protoLocks->SetArbiterColumnShard(*prepareSettings.ArbiterColumnShard);
            protoLocks->AddSendingShards(*prepareSettings.ArbiterColumnShard);
            protoLocks->AddReceivingShards(*prepareSettings.ArbiterColumnShard);
            if (prepareSettings.SendingShards.contains(shardId)) {
                protoLocks->AddSendingShards(shardId);
            }
            if (prepareSettings.ReceivingShards.contains(shardId)) {
                protoLocks->AddReceivingShards(shardId);
            }
            std::sort(
                std::begin(*protoLocks->MutableSendingShards()),
                std::end(*protoLocks->MutableSendingShards()));
            std::sort(
                std::begin(*protoLocks->MutableReceivingShards()),
                std::end(*protoLocks->MutableReceivingShards()));
        }

        const auto locks = txManager->GetLocks(shardId);
        for (const auto& lock : locks) {
            *protoLocks->AddLocks() = lock;
        }
    }

    void FillEvWriteRollback(NKikimr::NEvents::TDataEvents::TEvWrite* evWrite, ui64 shardId, const NKikimr::NKqp::IKqpTransactionManagerPtr& txManager) {
        auto* protoLocks = evWrite->Record.MutableLocks();
        protoLocks->SetOp(NKikimrDataEvents::TKqpLocks::Rollback);

        const auto locks = txManager->GetLocks(shardId);
        for (const auto& lock : locks) {
            *protoLocks->AddLocks() = lock;
        }
    }

    void FillTopicsCommit(NKikimrPQ::TDataTransaction& transaction, const NKikimr::NKqp::IKqpTransactionManagerPtr& txManager) {
        transaction.SetOp(NKikimrPQ::TDataTransaction::Commit);
        const auto prepareSettings = txManager->GetPrepareTransactionInfo();

        if (!prepareSettings.ArbiterColumnShard) {
            for (const ui64 sendingShardId : prepareSettings.SendingShards) {
                transaction.AddSendingShards(sendingShardId);
            }
            for (const ui64 receivingShardId : prepareSettings.ReceivingShards) {
                transaction.AddReceivingShards(receivingShardId);
            }
        } else {
            transaction.AddSendingShards(*prepareSettings.ArbiterColumnShard);
            transaction.AddReceivingShards(*prepareSettings.ArbiterColumnShard);
        }
    }

    std::optional<NKikimrDataEvents::TMvccSnapshot> GetOptionalMvccSnapshot(const NKikimrKqp::TKqpTableSinkSettings& settings) {
        if (settings.HasMvccSnapshot()) {
            return settings.GetMvccSnapshot();
        } else {
            return std::nullopt;
        }
    }

    std::optional<bool> HandleAttachResult(NKikimr::NKqp::IKqpTransactionManagerPtr& txManager, NKikimr::TEvDataShard::TEvProposeTransactionAttachResult::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        const ui64 shardId = record.GetTabletId();

        const auto& reattachState = txManager->GetReattachState(shardId);
        if (reattachState.Cookie != ev->Cookie) {
            return std::nullopt;
        }

        const auto shardState = txManager->GetState(shardId);
        switch (shardState) {
            case NKikimr::NKqp::IKqpTransactionManager::EXECUTING:
            case NKikimr::NKqp::IKqpTransactionManager::PREPARED:
                break;
            case NKikimr::NKqp::IKqpTransactionManager::FINISHED:
            case NKikimr::NKqp::IKqpTransactionManager::ERROR:
                return std::nullopt;
            case NKikimr::NKqp::IKqpTransactionManager::PREPARING:
            case NKikimr::NKqp::IKqpTransactionManager::PROCESSING:
                YQL_ENSURE(false);
        }

        if (record.GetStatus() == NKikimrProto::OK) {
            // Transaction still exists at this shard
            txManager->Reattached(shardId);
        }
        return record.GetStatus() == NKikimrProto::OK;
    }

    bool HandleTransactionRestart(NKikimr::NKqp::IKqpTransactionManagerPtr& txManager, NKikimr::TEvDataShard::TEvProposeTransactionRestart::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        const ui64 shardId = record.GetTabletId();

        switch (txManager->GetState(shardId)) {
            case NKikimr::NKqp::IKqpTransactionManager::PREPARED:
            case NKikimr::NKqp::IKqpTransactionManager::EXECUTING: {
                txManager->SetRestarting(shardId);
                return true;
            }
            case NKikimr::NKqp::IKqpTransactionManager::PREPARING: {
                return false;
            }
            case NKikimr::NKqp::IKqpTransactionManager::FINISHED:
            case NKikimr::NKqp::IKqpTransactionManager::ERROR: {
                return true;
            }
            case NKikimr::NKqp::IKqpTransactionManager::PROCESSING: {
                YQL_ENSURE(false);
            }
        }
        return true;
    }

    const TStringBuf ConflictWithExistingKeyErrorText = "Conflict with existing key.";
    const TStringBuf DuplicateKeyErrorText = "Duplicated keys found.";
    constexpr ui64 RollbackMessageCookie = 1;
}


namespace NKikimr {
namespace NKqp {

struct IKqpTableWriterCallbacks {
    virtual ~IKqpTableWriterCallbacks() = default;

    // Ready to accept writes
    virtual void OnReady() = 0;

    // EvWrite statuses
    virtual void OnPrepared(IKqpTransactionManager::TPrepareResult&& preparedInfo, ui64 dataSize) = 0;
    virtual void OnCommitted(ui64 shardId, ui64 dataSize) = 0;
    virtual void OnMessageAcknowledged(ui64 dataSize) = 0;

    virtual void OnError(NYql::NDqProto::StatusIds::StatusCode statusCode, NYql::EYqlIssueCode id, const TString& message, const NYql::TIssues& subIssues) = 0;
    virtual void OnError(NYql::NDqProto::StatusIds::StatusCode statusCode, NYql::TIssues&& issues) = 0;

    virtual void OnLocksBrokenError(ui64 shardId, NYql::NDqProto::StatusIds::StatusCode statusCode, NYql::TIssues&& issues) {
        Y_UNUSED(shardId);
        OnError(statusCode, std::move(issues));
    }
};

struct TKqpTableWriterStatistics {
    ui64 ReadRows = 0;
    ui64 ReadBytes = 0;
    ui64 WriteRows = 0;
    ui64 WriteBytes = 0;
    ui64 EraseRows = 0;
    ui64 EraseBytes = 0;
    ui64 LocksBrokenAsBreaker = 0;
    ui64 LocksBrokenAsVictim = 0;
    TVector<ui64> BreakerQuerySpanIds;
    TVector<ui64> DeferredBreakerQuerySpanIds;
    TVector<ui32> DeferredBreakerNodeIds;

    THashSet<ui64> AffectedPartitions;


    void UpdateStats(const NKikimrQueryStats::TTxStats& txStats, const TTableId& tableId) {
        for (const auto& tableAccessStats : txStats.GetTableAccessStats()) {
            YQL_ENSURE(tableAccessStats.GetTableInfo().GetPathId() == tableId.PathId.LocalPathId);
            ReadRows += tableAccessStats.GetSelectRow().GetRows();
            ReadRows += tableAccessStats.GetSelectRange().GetRows();
            ReadBytes += tableAccessStats.GetSelectRow().GetBytes();
            ReadBytes += tableAccessStats.GetSelectRange().GetBytes();
            WriteRows += tableAccessStats.GetUpdateRow().GetRows();
            WriteBytes += tableAccessStats.GetUpdateRow().GetBytes();
            EraseRows += tableAccessStats.GetEraseRow().GetRows();
            EraseBytes += tableAccessStats.GetEraseRow().GetBytes();
        }

        for (const auto& perShardStats : txStats.GetPerShardStats()) {
            AffectedPartitions.insert(perShardStats.GetShardId());
        }

        LocksBrokenAsBreaker += txStats.GetLocksBrokenAsBreaker();
        LocksBrokenAsVictim += txStats.GetLocksBrokenAsVictim();
        if (txStats.GetLocksBrokenAsBreaker() > 0 && txStats.BreakerQuerySpanIdsSize() > 0) {
            for (ui64 id : txStats.GetBreakerQuerySpanIds()) {
                BreakerQuerySpanIds.push_back(id);
            }
        }
        if (txStats.DeferredBreakerQuerySpanIdsSize() > 0) {
            for (size_t i = 0; i < static_cast<size_t>(txStats.DeferredBreakerQuerySpanIdsSize()); ++i) {
                DeferredBreakerQuerySpanIds.push_back(txStats.GetDeferredBreakerQuerySpanIds(i));
                DeferredBreakerNodeIds.push_back(
                    i < static_cast<size_t>(txStats.DeferredBreakerNodeIdsSize()) ? txStats.GetDeferredBreakerNodeIds(i) : 0u);
            }
        }
    }

    static void AddLockStats(NYql::NDqProto::TDqTaskStats* stats, ui64 brokenAsBreaker, ui64 brokenAsVictim,
        const TVector<ui64>& breakerQuerySpanIds = {},
        const TVector<ui64>& deferredBreakerQuerySpanIds = {},
        const TVector<ui32>& deferredBreakerNodeIds = {})
    {
        NKqpProto::TKqpTaskExtraStats extraStats;
        if (stats->HasExtra()) {
            stats->GetExtra().UnpackTo(&extraStats);
        }
        extraStats.MutableLockStats()->SetBrokenAsBreaker(
            extraStats.GetLockStats().GetBrokenAsBreaker() + brokenAsBreaker);
        extraStats.MutableLockStats()->SetBrokenAsVictim(
            extraStats.GetLockStats().GetBrokenAsVictim() + brokenAsVictim);
        for (ui64 id : breakerQuerySpanIds) {
            extraStats.MutableLockStats()->AddBreakerQuerySpanIds(id);
        }
        for (size_t i = 0; i < deferredBreakerQuerySpanIds.size(); ++i) {
            extraStats.MutableLockStats()->AddDeferredBreakerQuerySpanIds(deferredBreakerQuerySpanIds[i]);
            extraStats.MutableLockStats()->AddDeferredBreakerNodeIds(
                i < deferredBreakerNodeIds.size() ? deferredBreakerNodeIds[i] : 0u);
        }
        stats->MutableExtra()->PackFrom(extraStats);
    }

    void FillStats(NYql::NDqProto::TDqTaskStats* stats, const TString& tablePath) {
        AddLockStats(stats, LocksBrokenAsBreaker, LocksBrokenAsVictim, BreakerQuerySpanIds,
                     DeferredBreakerQuerySpanIds, DeferredBreakerNodeIds);
        LocksBrokenAsBreaker = 0;
        LocksBrokenAsVictim = 0;
        BreakerQuerySpanIds.clear();
        DeferredBreakerQuerySpanIds.clear();
        DeferredBreakerNodeIds.clear();

        if (ReadRows + WriteRows + EraseRows == 0) {
            // Avoid empty table_access stats
            return;
        }
        NYql::NDqProto::TDqTableStats* tableStats = nullptr;
        for (size_t i = 0; i < stats->TablesSize(); ++i) {
            auto* table = stats->MutableTables(i);
            if (table->GetTablePath() == tablePath) {
                tableStats = table;
            }
        }
        if (!tableStats) {
            tableStats = stats->AddTables();
            tableStats->SetTablePath(tablePath);
        }

        tableStats->SetReadRows(tableStats->GetReadRows() + ReadRows);
        tableStats->SetReadBytes(tableStats->GetReadBytes() + ReadBytes);
        tableStats->SetWriteRows(tableStats->GetWriteRows() + WriteRows);
        tableStats->SetWriteBytes(tableStats->GetWriteBytes() + WriteBytes);
        tableStats->SetEraseRows(tableStats->GetEraseRows() + EraseRows);
        tableStats->SetEraseBytes(tableStats->GetEraseBytes() + EraseBytes);

        ReadRows = 0;
        ReadBytes = 0;
        WriteRows = 0;
        WriteBytes = 0;
        EraseRows = 0;
        EraseBytes = 0;

        tableStats->SetAffectedPartitions(
            tableStats->GetAffectedPartitions() + AffectedPartitions.size());
        AffectedPartitions.clear();
    }
};

class TKqpTableWriteActor : public TActorBootstrapped<TKqpTableWriteActor> {
    using TBase = TActorBootstrapped<TKqpTableWriteActor>;

    struct TEvPrivate {
        enum EEv {
            EvShardRequestTimeout = EventSpaceBegin(TKikimrEvents::ES_PRIVATE),
            EvResolveRequestPlanned,
            EvReattachToShard,
        };

        struct TEvShardRequestTimeout : public TEventLocal<TEvShardRequestTimeout, EvShardRequestTimeout> {
            ui64 ShardId;

            TEvShardRequestTimeout(ui64 shardId)
                : ShardId(shardId) {
            }
        };

        struct TEvResolveRequestPlanned : public TEventLocal<TEvResolveRequestPlanned, EvResolveRequestPlanned> {
        };

        struct TEvReattachToShard : public TEventLocal<TEvReattachToShard, EvReattachToShard> {
            const ui64 TabletId;

            explicit TEvReattachToShard(ui64 tabletId)
                : TabletId(tabletId) {}
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
        const TString& database,
        const TTableId& tableId,
        const TStringBuf tablePath,
        const ui64 lockTxId,
        const ui64 lockNodeId,
        const bool inconsistentTx,
        const bool isOlap,
        TVector<NScheme::TTypeInfo> keyColumnTypes,
        std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> alloc,
        const std::optional<NKikimrDataEvents::TMvccSnapshot>& mvccSnapshot,
        const NKikimrDataEvents::ELockMode lockMode,
        const IKqpTransactionManagerPtr& txManager,
        const TActorId sessionActorId,
        TIntrusivePtr<TKqpCounters> counters,
        const TString& userSID)
        : MessageSettings(GetWriteActorSettings())
        , Alloc(alloc)
        , MvccSnapshot(mvccSnapshot)
        , LockMode(lockMode)
        , Database(database)
        , TableId(tableId)
        , TablePath(tablePath)
        , LockTxId(lockTxId)
        , LockNodeId(lockNodeId)
        , InconsistentTx(inconsistentTx)
        , IsOlap(isOlap)
        , KeyColumnTypes(std::move(keyColumnTypes))
        , Callbacks(callbacks)
        , TxManager(txManager ? txManager : CreateKqpTransactionManager(/* collectOnly= */ true))
        , Counters(counters)
        , UserSID(userSID)
    {
        LogPrefix = TStringBuilder() << "Table: `" << TablePath << "` (" << TableId << "), " << "SessionActorId: " << sessionActorId;
        ShardedWriteController = CreateShardedWriteController(
            TShardedWriteControllerSettings {
                .MemoryLimitTotal = MessageSettings.InFlightMemoryLimitPerActorBytes,
                .Inconsistent = InconsistentTx,
            },
            Alloc);

        Counters->WriteActorsCount->Inc();
    }

    ~TKqpTableWriteActor() {
        ClearMkqlData();
    }

    // Set the current query's SpanId before processing each batch.
    // This ensures UpdateShards/AddAction passes the correct per-query SpanId to TxManager.
    void SetCurrentQuerySpanId(ui64 querySpanId) {
        CurrentQuerySpanId = querySpanId;
    }

    void Bootstrap() {
        LogPrefix = TStringBuilder() << "SelfId: " << this->SelfId() << ", " << LogPrefix;
        try {
            const auto partitioning = TxManager->GetPartitioning(TableId);
            if (!partitioning) {
                Resolve();
            } else {
                Partitioning = partitioning;
                Prepare();
            }
        } catch (const TMemoryLimitExceededException&) {
            RuntimeError(
                NYql::NDqProto::StatusIds::PRECONDITION_FAILED,
                NYql::TIssuesIds::KIKIMR_PRECONDITION_FAILED,
                TStringBuilder() << "Memory limit exception"
                    << ", current limit is " << Alloc->GetLimit() << " bytes.");
            return;
        } catch (...) {
            RuntimeError(
                NYql::NDqProto::StatusIds::INTERNAL_ERROR,
                NYql::TIssuesIds::KIKIMR_INTERNAL_ERROR,
                CurrentExceptionMessage());
            return;
        }

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

    const TTableId& GetTableId() const {
        return TableId;
    }

    TVector<NKikimrDataEvents::TLock> GetLocks() const {
        return TxManager->GetLocks();
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

    void Open(
        const TWriteToken token,
        const NKikimrDataEvents::TEvWrite::TOperation::EOperationType operationType,
        TVector<NKikimrKqp::TKqpColumnMetadataProto> keyColumnsMetadata,
        TVector<NKikimrKqp::TKqpColumnMetadataProto> columnsMetadata,
        ui32 defaultColumnsCount,
        i64 priority) {
        YQL_ENSURE(!Closed);
        ShardedWriteController->Open(
            token,
            TableId,
            operationType,
            std::move(keyColumnsMetadata),
            std::move(columnsMetadata),
            defaultColumnsCount,
            priority);

        // At current time only insert operation can fail.
        NeedToFlushBeforeCommit |= (operationType == NKikimrDataEvents::TEvWrite::TOperation::OPERATION_INSERT);

        // Associate this token with the current query's SpanId for TLI lock-break attribution.
        // This ensures each batch created from this token carries the correct QuerySpanId,
        // even when batches from multiple queries are later combined into a single EvWrite.
        ShardedWriteController->SetTokenQuerySpanId(token, CurrentQuerySpanId);

        CA_LOG_D("Open: token=" << token);
    }

    void Write(
            const TWriteToken token,
            IDataBatchPtr data) {
        YQL_ENSURE(!Closed);
        YQL_ENSURE(ShardedWriteController);
        CA_LOG_D("Write: token=" << token);
        ShardedWriteController->Write(token, std::move(data));
    }

    void Close(TWriteToken token) {
        YQL_ENSURE(!Closed);
        YQL_ENSURE(ShardedWriteController);
        CA_LOG_D("Close: token=" << token);

        ShardedWriteController->Close(token);
    }

    void Close() {
        YQL_ENSURE(!Closed);
        YQL_ENSURE(ShardedWriteController);
        YQL_ENSURE(ShardedWriteController->IsAllWritesClosed());
        Closed = true;
        ShardedWriteController->Close();
    }

    void CleanupClosedTokens() {
        YQL_ENSURE(ShardedWriteController);
        ShardedWriteController->CleanupClosedTokens();
        NeedToFlushBeforeCommit = false;
    }

    void SetParentTraceId(NWilson::TTraceId traceId) {
        ParentTraceId = std::move(traceId);
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
                hFunc(TEvDataShard::TEvProposeTransactionAttachResult, Handle);
                hFunc(TEvPrivate::TEvReattachToShard, Handle);
                hFunc(TEvDataShard::TEvProposeTransactionRestart, Handle);
                hFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
                hFunc(TEvPrivate::TEvShardRequestTimeout, Handle);
                hFunc(TEvPrivate::TEvResolveRequestPlanned, Handle);
                hFunc(TEvDataShard::TEvOverloadReady, Handle);
                hFunc(TEvColumnShard::TEvOverloadReady, Handle);
                IgnoreFunc(TEvInterconnect::TEvNodeConnected);
                IgnoreFunc(TEvTxProxySchemeCache::TEvInvalidateTableResult);
            default:
                AFL_ENSURE(false)("unknown message", ev->GetTypeRewrite());
            }
        } catch (const TMemoryLimitExceededException&) {
            RuntimeError(
                NYql::NDqProto::StatusIds::PRECONDITION_FAILED,
                NYql::TIssuesIds::KIKIMR_PRECONDITION_FAILED,
                TStringBuilder() << "Memory limit exception"
                    << ", current limit is " << Alloc->GetLimit() << " bytes.");
            return;
        } catch (...) {
            RuntimeError(
                NYql::NDqProto::StatusIds::INTERNAL_ERROR,
                NYql::TIssuesIds::KIKIMR_INTERNAL_ERROR,
                CurrentExceptionMessage());
            return;
        }
    }

    bool IsResolving() const {
        return ResolveAttempts > 0;
    }

    void RetryResolve() {
        if (!IsResolving()) {
            Resolve();
        }
    }

    void Resolve() {
        AFL_ENSURE(InconsistentTx || IsOlap);
        TableWriteActorSpan = NWilson::TSpan(TWilsonKqp::TableWriteActor, NWilson::TTraceId(ParentTraceId),
            "WaitForTableResolve", NWilson::EFlags::AUTO_END);

        if (IsOlap) {
            ResolveTable();
        } else {
            ResolveShards();
        }
    }

    void PlanResolve() {
        CA_LOG_D("Plan resolve with delay " << CalculateNextAttemptDelay(MessageSettings, ResolveAttempts));
        TlsActivationContext->Schedule(
            CalculateNextAttemptDelay(MessageSettings, ResolveAttempts),
            new IEventHandle(SelfId(), SelfId(), new TEvPrivate::TEvResolveRequestPlanned{}, 0, 0));
    }

    void Handle(TEvPrivate::TEvResolveRequestPlanned::TPtr&) {
        RetryResolve();
    }

    void ResolveTable() {
        Counters->WriteActorsShardResolve->Inc();
        SchemeEntry.reset();
        Partitioning.reset();

        if (ResolveAttempts++ >= MessageSettings.MaxResolveAttempts) {
            CA_LOG_E(TStringBuilder()
                << "Too many table resolve attempts for table `" << TablePath << "` (" << TableId << ").");
            RuntimeError(
                NYql::NDqProto::StatusIds::SCHEME_ERROR,
                NYql::TIssuesIds::KIKIMR_SCHEME_ERROR,
                TStringBuilder()
                << "Too many table resolve attempts for table `" << TablePath << "`.");
            return;
        }

        CA_LOG_D("Resolve TableId=" << TableId);
        TAutoPtr<NSchemeCache::TSchemeCacheNavigate> request(new NSchemeCache::TSchemeCacheNavigate());
        request->DatabaseName = Database;
        NSchemeCache::TSchemeCacheNavigate::TEntry entry;
        entry.TableId = TableId;
        entry.RequestType = NSchemeCache::TSchemeCacheNavigate::TEntry::ERequestType::ByTableId;
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpTable;
        entry.SyncVersion = false;
        entry.ShowPrivatePath = true;
        request->ResultSet.emplace_back(entry);

        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request), 0, 0, TableWriteActorSpan.GetTraceId());
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        auto& resultSet = ev->Get()->Request->ResultSet;
        YQL_ENSURE(resultSet.size() == 1);

        if (ev->Get()->Request->ErrorCount > 0) {
            CA_LOG_E(TStringBuilder() << "Failed to get table: "
                << TableId << "'. Entry: " << resultSet[0].ToString());
            PlanResolve();
            return;
        }

        SchemeEntry = resultSet[0];

        CA_LOG_D("Resolved TableId=" << TableId << " ("
            << TablePath << " "
            << TableId.SchemaVersion << ")");

        if (TableId.SchemaVersion != SchemeEntry->TableId.SchemaVersion) {
            RuntimeError(
                NYql::NDqProto::StatusIds::SCHEME_ERROR,
                NYql::TIssuesIds::KIKIMR_SCHEME_ERROR,
                TStringBuilder() << "Schema was updated.");
            return;
        }

        YQL_ENSURE(IsOlap && (SchemeEntry->Kind == NSchemeCache::TSchemeCacheNavigate::KindColumnTable));

        Prepare();
    }

    void ResolveShards() {
        YQL_ENSURE(!KeyColumnTypes.empty());
        CA_LOG_D("Resolve shards for TableId=" << TableId);

        AFL_ENSURE(InconsistentTx); // Only for CTAS

        const TVector<TCell> minKey(KeyColumnTypes.size());
        const TTableRange range(minKey, true, {}, false, false);
        YQL_ENSURE(range.IsFullRange(KeyColumnTypes.size()));
        auto keyRange = MakeHolder<TKeyDesc>(
            TableId,
            range,
            TKeyDesc::ERowOperation::Update, // Only for CTAS
            KeyColumnTypes,
            TVector<TKeyDesc::TColumnOp>{});

        TAutoPtr<NSchemeCache::TSchemeCacheRequest> request(new NSchemeCache::TSchemeCacheRequest());
        request->DatabaseName = Database;
        request->ResultSet.emplace_back(std::move(keyRange));

        TAutoPtr<TEvTxProxySchemeCache::TEvResolveKeySet> resolveReq(new TEvTxProxySchemeCache::TEvResolveKeySet(request));
        Send(MakeSchemeCacheID(), resolveReq.Release(), 0, 0, TableWriteActorSpan.GetTraceId());
    }

    void Handle(TEvTxProxySchemeCache::TEvResolveKeySetResult::TPtr& ev) {
        auto* request = ev->Get()->Request.Get();

        if (request->ErrorCount > 0) {
            CA_LOG_E(TStringBuilder() << "Failed to get table: "
                << TableId << "'");
            PlanResolve();
            return;
        }

        YQL_ENSURE(request->ResultSet.size() == 1);
        Partitioning = std::move(request->ResultSet[0].KeyDescription->Partitioning);

        CA_LOG_D("Resolved shards for TableId=" << TableId << ". PartitionsCount=" << Partitioning->size() << ".");

        Prepare();
    }

    void OnOverloadReady(const ui64 shardId, const ui64 seqNo) {
        const auto metadata = ShardedWriteController->GetMessageMetadata(shardId);
        if (metadata && seqNo + 1 == metadata->NextOverloadSeqNo) {
            CA_LOG_D("Retry Overloaded ShardID=" << shardId);
            ResetShardRetries(shardId, metadata->Cookie);
            SendDataToShard(shardId);
        }
    }

    void Handle(TEvDataShard::TEvOverloadReady::TPtr& ev) {
        auto& record = ev->Get()->Record;
        const ui64 shardId = record.GetTabletID();
        const ui64 seqNo = record.GetSeqNo();

        OnOverloadReady(shardId, seqNo);
    }

    void Handle(TEvColumnShard::TEvOverloadReady::TPtr& ev) {

        auto& record = ev->Get()->Record;
        const ui64 shardId = record.GetTabletID();
        const ui64 seqNo = record.GetSeqNo();

        OnOverloadReady(shardId, seqNo);
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

        TxManager->AddParticipantNode(ev->Sender.NodeId());

        const bool handleOverload = ev->Get()->GetStatus() == NKikimrDataEvents::TEvWriteResult::STATUS_DISK_GROUP_OUT_OF_SPACE
                    || ev->Get()->GetStatus() == NKikimrDataEvents::TEvWriteResult::STATUS_OVERLOADED;

        if (ev->Get()->Record.HasOverloadSubscribed() && handleOverload) {
            CA_LOG_I("Got OverloadSubscribed for table `"
                << TablePath << "`."
                << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                << " Sink=" << this->SelfId() << "."
                << getIssues().ToOneLineString());

            const auto metadata = ShardedWriteController->GetMessageMetadata(ev->Get()->Record.GetOrigin());
            if (metadata && ev->Get()->Record.GetOverloadSubscribed() + 1 == metadata->NextOverloadSeqNo) {
                ResetShardRetries(ev->Get()->Record.GetOrigin(), ev->Cookie);
            }

            return;
        }

        switch (ev->Get()->GetStatus()) {
        case NKikimrDataEvents::TEvWriteResult::STATUS_UNSPECIFIED: {
            CA_LOG_E("Got UNSPECIFIED for table `"
                    << TablePath << "`."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << "."
                    << getIssues().ToOneLineString());
            UpdateStats(ev->Get()->Record.GetTxStats());
            TxManager->SetError(ev->Get()->Record.GetOrigin());
            RuntimeError(
                NYql::NDqProto::StatusIds::UNSPECIFIED,
                NYql::TIssuesIds::DEFAULT_ERROR,
                TStringBuilder() << "Unspecified error. Table `"
                    << TablePath << "`.",
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
                    << TablePath << "`."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << "."
                    << getIssues().ToOneLineString());
            UpdateStats(ev->Get()->Record.GetTxStats());
            TxManager->SetError(ev->Get()->Record.GetOrigin());
            RuntimeError(
                NYql::NDqProto::StatusIds::ABORTED,
                NYql::TIssuesIds::KIKIMR_OPERATION_ABORTED,
                TStringBuilder() << "Operation aborted.",
                getIssues());
            return;
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_WRONG_SHARD_STATE:
            CA_LOG_E("Got WRONG SHARD STATE for table `"
                    << TablePath << "`."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << "."
                    << getIssues().ToOneLineString());

            if (InconsistentTx) {
                ResetShardRetries(ev->Get()->Record.GetOrigin(), ev->Cookie);
                RetryResolve();
            } else {
                UpdateStats(ev->Get()->Record.GetTxStats());
                TxManager->SetError(ev->Get()->Record.GetOrigin());
                RuntimeError(
                    NYql::NDqProto::StatusIds::UNAVAILABLE,
                    NYql::TIssuesIds::KIKIMR_TEMPORARILY_UNAVAILABLE,
                    TStringBuilder() << "Wrong shard state. Table `"
                        << TablePath << "`.",
                    getIssues());
            }
            return;
        case NKikimrDataEvents::TEvWriteResult::STATUS_INTERNAL_ERROR: {
            CA_LOG_E("Got INTERNAL ERROR for table `"
                    << TablePath << "`."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << "."
                    << getIssues().ToOneLineString());
            UpdateStats(ev->Get()->Record.GetTxStats());
            TxManager->SetError(ev->Get()->Record.GetOrigin());
            RuntimeError(
                NYql::NDqProto::StatusIds::INTERNAL_ERROR,
                NYql::TIssuesIds::KIKIMR_INTERNAL_ERROR,
                TStringBuilder() << "Internal error while executing transaction.",
                getIssues());
            return;
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_DATABASE_DISK_SPACE_QUOTA_EXCEEDED: {
            CA_LOG_E("Got DATABASE_DISK_SPACE_QUOTA_EXCEEDED for table `"
                    << TablePath << "`."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << "."
                    << getIssues().ToOneLineString());
            UpdateStats(ev->Get()->Record.GetTxStats());
            TxManager->SetError(ev->Get()->Record.GetOrigin());
            RuntimeError(
                NYql::NDqProto::StatusIds::UNAVAILABLE,
                NYql::TIssuesIds::KIKIMR_DATABASE_DISK_SPACE_QUOTA_EXCEEDED,
                TStringBuilder() << "Disk space exhausted. Table `"
                    << TablePath << "`.",
                getIssues());
            return;
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_DISK_GROUP_OUT_OF_SPACE: {
            CA_LOG_W("Got DISK_GROUP_OUT_OF_SPACE for table `"
                << TablePath << "`."
                << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                << " Sink=" << this->SelfId() << "."
                << " Ignored this error."
                << getIssues().ToOneLineString());
            // TODO: support waiting
            if (!InconsistentTx)  {
                UpdateStats(ev->Get()->Record.GetTxStats());
                TxManager->SetError(ev->Get()->Record.GetOrigin());
                RuntimeError(
                    NYql::NDqProto::StatusIds::UNAVAILABLE,
                    NYql::TIssuesIds::KIKIMR_DISK_GROUP_OUT_OF_SPACE,
                    TStringBuilder() << "Tablet " << ev->Get()->Record.GetOrigin() << " is out of space. Table `"
                        << TablePath << "`.",
                    getIssues());
            }
            return;
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_OVERLOADED: {
            CA_LOG_W("Got OVERLOADED for table `"
                << TablePath << "`."
                << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                << " Sink=" << this->SelfId() << "."
                << " Ignored this error."
                << getIssues().ToOneLineString());
            // TODO: support waiting
            if (!InconsistentTx)  {
                UpdateStats(ev->Get()->Record.GetTxStats());
                TxManager->SetError(ev->Get()->Record.GetOrigin());
                RuntimeError(
                    NYql::NDqProto::StatusIds::OVERLOADED,
                    NYql::TIssuesIds::KIKIMR_OVERLOADED,
                    TStringBuilder() << "Kikimr cluster or one of its subsystems is overloaded."
                        << " Tablet " << ev->Get()->Record.GetOrigin() << " is overloaded. Table `"
                        << TablePath << "`.",
                    getIssues());
            }
            return;
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_CANCELLED: {
            CA_LOG_E("Got CANCELLED for table `"
                    << TablePath << "`."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << "."
                    << getIssues().ToOneLineString());
            UpdateStats(ev->Get()->Record.GetTxStats());
            TxManager->SetError(ev->Get()->Record.GetOrigin());
            RuntimeError(
                NYql::NDqProto::StatusIds::CANCELLED,
                NYql::TIssuesIds::KIKIMR_OPERATION_CANCELLED,
                TStringBuilder() << "Operation cancelled.",
                getIssues());
            return;
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST: {
            CA_LOG_E("Got BAD REQUEST for table `"
                    << TablePath << "`."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << "."
                    << getIssues().ToOneLineString());
            UpdateStats(ev->Get()->Record.GetTxStats());
            TxManager->SetError(ev->Get()->Record.GetOrigin());
            RuntimeError(
                NYql::NDqProto::StatusIds::BAD_REQUEST,
                NYql::TIssuesIds::KIKIMR_BAD_REQUEST,
                TStringBuilder() << "Bad request. Table: `"
                    << TablePath << "`.",
                getIssues());
            return;
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_SCHEME_CHANGED: {
            CA_LOG_E("Got SCHEME CHANGED for table `"
                    << TablePath << "`."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << "."
                    << getIssues().ToOneLineString());
            if (InconsistentTx) {
                ResetShardRetries(ev->Get()->Record.GetOrigin(), ev->Cookie);
                RetryResolve();
            } else {
                UpdateStats(ev->Get()->Record.GetTxStats());
                TxManager->SetError(ev->Get()->Record.GetOrigin());
                RuntimeError(
                    NYql::NDqProto::StatusIds::SCHEME_ERROR,
                    NYql::TIssuesIds::KIKIMR_SCHEME_MISMATCH,
                    TStringBuilder() << "Scheme changed. Table: `"
                        << TablePath << "`.",
                    getIssues());
            }
            return;
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_LOCKS_BROKEN: {
            CA_LOG_E("Got LOCKS BROKEN for table `"
                    << TablePath << "`."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << "."
                    << getIssues().ToOneLineString());

            const ui64 brokenShardId = ev->Get()->Record.GetOrigin();

            UpdateStats(ev->Get()->Record.GetTxStats());
            TxManager->BreakLock(brokenShardId);
            YQL_ENSURE(TxManager->BrokenLocks());

            SetVictimQuerySpanIdFromBrokenLocks(brokenShardId, ev->Get()->Record.GetTxLocks(), TxManager);
            if (TableWriteActorSpan) {
                TableWriteActorSpan.EndError("LOCKS_BROKEN");
            }
            Callbacks->OnLocksBrokenError(brokenShardId, NYql::NDqProto::StatusIds::ABORTED, MakeLockIssues(TxManager, getIssues()));
            return;
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_CONSTRAINT_VIOLATION: {
            CA_LOG_E("Got CONSTRAINT VIOLATION for table `" << TablePath << "`."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << "."
                    << getIssues().ToOneLineString());
            UpdateStats(ev->Get()->Record.GetTxStats());
            TxManager->SetError(ev->Get()->Record.GetOrigin());
            RuntimeError(
                NYql::NDqProto::StatusIds::PRECONDITION_FAILED,
                NYql::TIssuesIds::KIKIMR_CONSTRAINT_VIOLATION,
                TStringBuilder() << "Constraint violated. Table: `"
                    << TablePath << "`.",
                getIssues());
            return;
        }
        }
    }

    void ProcessWritePreparedShard(NKikimr::NEvents::TDataEvents::TEvWriteResult::TPtr& ev) {
        YQL_ENSURE(Mode == EMode::PREPARE);
        const auto& record = ev->Get()->Record;
        AFL_ENSURE(record.GetTxLocks().empty());

        UpdateStats(record.GetTxStats());

        IKqpTransactionManager::TPrepareResult preparedInfo;
        preparedInfo.ShardId = record.GetOrigin();
        preparedInfo.MinStep = record.GetMinStep();
        preparedInfo.MaxStep = record.GetMaxStep();

        preparedInfo.Coordinator = 0;
        if (record.DomainCoordinatorsSize()) {
            auto domainCoordinators = TCoordinators(TVector<ui64>(record.GetDomainCoordinators().begin(),
                                                                  record.GetDomainCoordinators().end()));
            preparedInfo.Coordinator = domainCoordinators.Select(*TxId);
        }

        OnMessageReceived(ev->Get()->Record.GetOrigin());
        const auto result = ShardedWriteController->OnMessageAcknowledged(
                ev->Get()->Record.GetOrigin(), ev->Cookie);
        if (result) {
            YQL_ENSURE(result->IsShardEmpty);
            Callbacks->OnPrepared(std::move(preparedInfo), result->DataSize);
        }
    }

    void ProcessWriteCompletedShard(NKikimr::NEvents::TDataEvents::TEvWriteResult::TPtr& ev) {
        CA_LOG_D("Got completed result TxId=" << ev->Get()->Record.GetTxId()
            << ", TabletId=" << ev->Get()->Record.GetOrigin()
            << ", Cookie=" << ev->Cookie
            << ", Mode=" << static_cast<int>(Mode)
            << ", Locks=" << [&]() {
                TStringBuilder builder;
                for (const auto& lock : ev->Get()->Record.GetTxLocks()) {
                    builder << lock.ShortDebugString();
                }
                return builder;
            }());

        // Only collect locks in WRITE mode (COLLECTING state required by AddLock)
        if (Mode == EMode::WRITE) {
            for (const auto& lock : ev->Get()->Record.GetTxLocks()) {
                if (!TxManager->AddLock(ev->Get()->Record.GetOrigin(), lock, CurrentQuerySpanId)) {
                    UpdateStats(ev->Get()->Record.GetTxStats());
                    Stats.LocksBrokenAsVictim += 1;
                    YQL_ENSURE(TxManager->BrokenLocks());
                    RuntimeError(NYql::NDqProto::StatusIds::ABORTED, MakeLockIssues(TxManager, {}));
                    return;
                }
            }
        }

        if (Mode == EMode::COMMIT) {
            UpdateStats(ev->Get()->Record.GetTxStats());
            Callbacks->OnCommitted(ev->Get()->Record.GetOrigin(), 0);
            return;
        }

        OnMessageReceived(ev->Get()->Record.GetOrigin());
        const auto result = ShardedWriteController->OnMessageAcknowledged(
                ev->Get()->Record.GetOrigin(), ev->Cookie);
        if (result && result->IsShardEmpty && Mode == EMode::IMMEDIATE_COMMIT) {
            UpdateStats(ev->Get()->Record.GetTxStats());
            Callbacks->OnCommitted(ev->Get()->Record.GetOrigin(), result->DataSize);
        } else if (result) {
            AFL_ENSURE(Mode == EMode::WRITE);
            UpdateStats(ev->Get()->Record.GetTxStats());
            Callbacks->OnMessageAcknowledged(result->DataSize);
        }
    }

    void OnMessageReceived(const ui64 shardId) {
        if (auto it = SendTime.find(shardId); it != std::end(SendTime)) {
            Counters->WriteActorWritesLatencyHistogram->Collect((TInstant::Now() - it->second).MicroSeconds());
            SendTime.erase(it);
        }
    }

    void SetPrepare(ui64 txId) {
        CA_LOG_D("SetPrepare; txId=" << txId);
        YQL_ENSURE(Mode == EMode::WRITE);
        Mode = EMode::PREPARE;
        TxId = txId;
        ShardedWriteController->AddCoveringMessages();
    }

    void SetDistributedCommit() {
        CA_LOG_D("SetDistributedCommit; txId=" << *TxId);
        YQL_ENSURE(Mode == EMode::PREPARE);
        Mode = EMode::COMMIT;
    }

    void SetImmediateCommit() {
        CA_LOG_D("SetImmediateCommit");
        YQL_ENSURE(Mode == EMode::WRITE);
        Mode = EMode::IMMEDIATE_COMMIT;

        if (ShardedWriteController->GetShardsCount() == 1) {
            ShardedWriteController->AddCoveringMessages();
        } else {
            YQL_ENSURE(ShardedWriteController->GetShardsCount() == 0);
        }
    }

    void UpdateShards() {
        for (const auto& shardInfo : ShardedWriteController->ExtractShardUpdates()) {
            TxManager->AddShard(shardInfo.ShardId, IsOlap, TablePath);
            IKqpTransactionManager::TActionFlags flags = IKqpTransactionManager::EAction::WRITE;
            if (shardInfo.HasRead) {
                flags |= IKqpTransactionManager::EAction::READ;
            }
            // Use per-batch QuerySpanId when available; fall back to CurrentQuerySpanId.
            // This ensures TxManager tracks the correct per-query SpanId even when
            // FlushBuffers() flushes batches from multiple queries at once.
            const ui64 spanId = shardInfo.QuerySpanId != 0 ? shardInfo.QuerySpanId : CurrentQuerySpanId;
            TxManager->AddAction(shardInfo.ShardId, flags, spanId);
        }
    }

    void FlushBuffer(const TWriteToken token) {
        ShardedWriteController->FlushBuffer(token);
        UpdateShards();
    }

    void FlushBuffers() {
        ShardedWriteController->FlushBuffers();
        UpdateShards();
    }

    bool FlushToShards() {
        bool ok = true;
        ShardedWriteController->ForEachPendingShard([&](const auto& shardInfo) {
            if (ok && !SendDataToShard(shardInfo.ShardId)) {
                ok = false;
            }
        });
        return ok;
    }

    bool SendDataToShard(const ui64 shardId) {
        YQL_ENSURE(Mode != EMode::COMMIT);

        const auto metadata = ShardedWriteController->GetMessageMetadata(shardId);
        YQL_ENSURE(metadata);
        YQL_ENSURE(metadata->SendAttempts == 0 || InconsistentTx);
        if (metadata->SendAttempts >= MessageSettings.MaxWriteAttempts) {
            CA_LOG_W("ShardId=" << shardId
                    << " for table '" << TablePath
                    << "': retry limit exceeded."
                    << " Sink=" << this->SelfId() << ".");
            RetryResolve();
            return false;
        }

        // BreakerQuerySpanId is set in AddAction during write phase, not here

        const bool isPrepare = metadata->IsFinal && Mode == EMode::PREPARE;
        const bool isImmediateCommit = metadata->IsFinal && Mode == EMode::IMMEDIATE_COMMIT;

        auto evWrite = std::make_unique<NKikimr::NEvents::TDataEvents::TEvWrite>();

        evWrite->Record.SetTxMode(isPrepare
            ? (TxManager->IsVolatile()
                ? NKikimrDataEvents::TEvWrite::MODE_VOLATILE_PREPARE
                : NKikimrDataEvents::TEvWrite::MODE_PREPARE)
            : NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE);

        evWrite->Record.SetUserSID(UserSID);

        if (isImmediateCommit) {
            const auto locks = TxManager->GetLocks(shardId);
            if (!locks.empty()) {
                auto* protoLocks = evWrite->Record.MutableLocks();
                protoLocks->SetOp(NKikimrDataEvents::TKqpLocks::Commit);
                protoLocks->AddSendingShards(shardId);
                protoLocks->AddReceivingShards(shardId);
                for (const auto& lock : locks) {
                    *protoLocks->AddLocks() = lock;
                }
            }
        } else if (isPrepare) {
            YQL_ENSURE(TxId);
            FillEvWritePrepare(evWrite.get(), shardId, *TxId, TxManager);
        } else if (!InconsistentTx) {
            evWrite->SetLockId(LockTxId, LockNodeId);

            if (MvccSnapshot) {
                *evWrite->Record.MutableMvccSnapshot() = *MvccSnapshot;
            }
        }

        if (LockMode != NKikimrDataEvents::OPTIMISTIC) {
            evWrite->Record.SetLockMode(LockMode);
        }

        evWrite->Record.SetOverloadSubscribe(metadata->NextOverloadSeqNo);

        const auto serializationResult = ShardedWriteController->SerializeMessageToPayload(shardId, *evWrite);
        YQL_ENSURE(isPrepare || isImmediateCommit || serializationResult.TotalDataSize > 0);

        if (metadata->SendAttempts == 0) {
            if (!isPrepare) {
                Counters->WriteActorImmediateWrites->Inc();
            } else {
                Counters->WriteActorPrepareWrites->Inc();
            }
            Counters->WriteActorWritesSizeHistogram->Collect(serializationResult.TotalDataSize);
            Counters->WriteActorWritesOperationsHistogram->Collect(metadata->OperationsCount);

            for (const auto& operation : evWrite->Record.GetOperations()) {
                if (operation.GetType() == NKikimrDataEvents::TEvWrite::TOperation::OPERATION_INSERT
                       || operation.GetType() == NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPDATE) {
                    Counters->WriteActorReadWriteOperations->Inc();
                } else {
                    Counters->WriteActorWriteOnlyOperations->Inc();
                }
            }

            SendTime[shardId] = TInstant::Now();
        } else {
            YQL_ENSURE(!isPrepare);
            Counters->WriteActorImmediateWritesRetries->Inc();
        }

        if (MvccSnapshot && (isPrepare || isImmediateCommit)) {
            // Commit in snapshot isolation must validate writes against a snapshot
            bool needMvccSnapshot = LockMode == NKikimrDataEvents::OPTIMISTIC_SNAPSHOT_ISOLATION;
            if (!needMvccSnapshot && isPrepare) {
                for (const auto& operation : evWrite->Record.GetOperations()) {
                    if (operation.GetType() == NKikimrDataEvents::TEvWrite::TOperation::OPERATION_INSERT) {
                        // This operation may fail with an incorrect unique constraint violation otherwise
                        needMvccSnapshot = true;
                        break;
                    }
                }
            }
            if (needMvccSnapshot) {
                *evWrite->Record.MutableMvccSnapshot() = *MvccSnapshot;
            }
        }

        NDataIntegrity::LogIntegrityTrails("EvWriteTx", evWrite->Record.GetTxId(), shardId, TlsActivationContext->AsActorContext(), "WriteActor");

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
            << ", OperationsCount=" << evWrite->Record.OperationsSize() << ", IsFinal=" << metadata->IsFinal
            << ", Attempts=" << metadata->SendAttempts << ", Mode=" << static_cast<int>(Mode)
            << ", BufferMemory=" << GetMemory());

        AFL_ENSURE(Mode == EMode::WRITE || metadata->IsFinal);

        LinkedPipeCache = true;
        Send(
            PipeCacheId,
            new TEvPipeCache::TEvForward(evWrite.release(), shardId, /* subscribe */ true),
            0,
            metadata->Cookie,
            NWilson::TTraceId(ParentTraceId));

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

        return true;
    }

    void RetryShard(const ui64 shardId, const std::optional<ui64> ifCookieEqual) {
        const auto metadata = ShardedWriteController->GetMessageMetadata(shardId);
        if (!metadata || (ifCookieEqual && metadata->Cookie != ifCookieEqual)) {
            CA_LOG_I("Retry failed: not found ShardID=" << shardId << " with Cookie=" << ifCookieEqual.value_or(0));
            return;
        }

        CA_LOG_D("Retry ShardID=" << shardId
            << ", Cookie=" << ifCookieEqual.value_or(0)
            << ", Attempt=" << metadata->SendAttempts
            << ", Next Delay=" << CalculateNextAttemptDelay(MessageSettings, metadata->SendAttempts));
        SendDataToShard(shardId);
    }

    void ResetShardRetries(const ui64 shardId, const ui64 cookie) {
        ShardedWriteController->ResetRetries(shardId, cookie);
    }

    void Handle(TEvPrivate::TEvShardRequestTimeout::TPtr& ev) {
        CA_LOG_I("Timeout shardID=" << ev->Get()->ShardId);
        YQL_ENSURE(InconsistentTx);
        RetryShard(ev->Get()->ShardId, ev->Cookie);
    }

    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
        CA_LOG_W("TEvDeliveryProblem was received from tablet: " << ev->Get()->TabletId);
        if (InconsistentTx) {
            RetryShard(ev->Get()->TabletId, std::nullopt);
            return;
        }

        const auto& reattachState = TxManager->GetReattachState(ev->Get()->TabletId);

        const auto state = TxManager->GetState(ev->Get()->TabletId);
        if ((state == IKqpTransactionManager::PREPARED
                    || state == IKqpTransactionManager::EXECUTING)
                && TxManager->ShouldReattach(ev->Get()->TabletId, TlsActivationContext->Now())) {
            // Disconnected while waiting for other shards to prepare
            CA_LOG_N("Shard " << ev->Get()->TabletId << " delivery problem (reattaching in "
                        << reattachState.ReattachInfo.Delay << ")");

            Schedule(reattachState.ReattachInfo.Delay, new TEvPrivate::TEvReattachToShard(ev->Get()->TabletId));
        } else if (state == IKqpTransactionManager::EXECUTING && (!ev->Get()->NotDelivered || reattachState.Cookie != 0)) {
            TxManager->SetError(ev->Get()->TabletId);
            RuntimeError(
                NYql::NDqProto::StatusIds::UNDETERMINED,
                NYql::TIssuesIds::KIKIMR_OPERATION_STATE_UNKNOWN,
                TStringBuilder()
                    << "State of operation is unknown. "
                    << "Error writing to table `" << TablePath << "`"
                    << ". Transaction state unknown for tablet " << ev->Get()->TabletId << ".");
            return;
        } else if (state == IKqpTransactionManager::PROCESSING
                || state == IKqpTransactionManager::PREPARING
                || state == IKqpTransactionManager::PREPARED
                || (state == IKqpTransactionManager::EXECUTING && (ev->Get()->NotDelivered && reattachState.Cookie == 0))) {
            TxManager->SetError(ev->Get()->TabletId);
            RuntimeError(
                NYql::NDqProto::StatusIds::UNAVAILABLE,
                NYql::TIssuesIds::KIKIMR_TEMPORARILY_UNAVAILABLE,
                TStringBuilder()
                    << "Kikimr cluster or one of its subsystems was unavailable. "
                    << "Error writing to table `" << TablePath << "`"
                    << ": can't deliver message to tablet " << ev->Get()->TabletId << ".");
            return;
        } else {
            AFL_ENSURE(state == IKqpTransactionManager::FINISHED || state == IKqpTransactionManager::ERROR);
        }
    }

    void Handle(TEvDataShard::TEvProposeTransactionAttachResult::TPtr& ev) {
        const auto result = HandleAttachResult(TxManager, ev);
        if (!result) {
            return;
        }
        if (*result) {
            CA_LOG_D("Reattached to shard " << ev->Get()->Record.GetTabletId());
            return;
        }

        if (Mode == EMode::PREPARE) {
            RuntimeError(
                NYql::NDqProto::StatusIds::UNAVAILABLE,
                NYql::TIssuesIds::KIKIMR_TEMPORARILY_UNAVAILABLE,
                TStringBuilder()
                    << "Disconnected from shard " << ev->Get()->Record.GetTabletId() << "."
                    << "Table: `" << TablePath << "`.");
            return;
        } else {
            RuntimeError(
                NYql::NDqProto::StatusIds::UNDETERMINED,
                NYql::TIssuesIds::KIKIMR_OPERATION_STATE_UNKNOWN,
                TStringBuilder()
                    << "Disconnected from shard " << ev->Get()->Record.GetTabletId() << "."
                    << "Table: `" << TablePath << "`.");
            return;
        }
    }

    void Handle(TEvDataShard::TEvProposeTransactionRestart::TPtr& ev) {
        CA_LOG_D("Got transaction restart event from tabletId: " << ev->Get()->Record.GetTabletId());
        if (!HandleTransactionRestart(TxManager, ev)) {
            RuntimeError(
                NYql::NDqProto::StatusIds::UNAVAILABLE,
                NYql::TIssuesIds::KIKIMR_TEMPORARILY_UNAVAILABLE,
                TStringBuilder()
                    << "Disconnected from shard " << ev->Get()->Record.GetTabletId() << "."
                    << "Table: `" << TablePath << "`.");
        }
    }

    void Handle(TEvPrivate::TEvReattachToShard::TPtr& ev) {
        const ui64 tabletId = ev->Get()->TabletId;
        auto& state = TxManager->GetReattachState(tabletId);

        CA_LOG_D("Reattach to shard " << tabletId);

        YQL_ENSURE(TxId);
        Send(MakePipePerNodeCacheID(false), new TEvPipeCache::TEvForward(
            new TEvDataShard::TEvProposeTransactionAttach(tabletId, *TxId),
            tabletId, /* subscribe */ true), 0, ++state.Cookie);
    }

    void Prepare() {
        if (TableWriteActorSpan) {
            TableWriteActorSpan.EndOk(); // Resolve finished
        }

        ResolveAttempts = 0;

        if (IsOlap) {
            YQL_ENSURE(SchemeEntry);
            ShardedWriteController->OnPartitioningChanged(*SchemeEntry);
        } else {
            ShardedWriteController->OnPartitioningChanged(Partitioning);
            Partitioning.reset();
        }

        if (InconsistentTx && Closed) {
            FlushBuffers();
            YQL_ENSURE(ShardedWriteController);
            YQL_ENSURE(ShardedWriteController->IsAllWritesClosed());
            ShardedWriteController->Close();
        }

        Callbacks->OnReady();
    }

    void RuntimeError(NYql::NDqProto::StatusIds::StatusCode statusCode, NYql::EYqlIssueCode id, const TString& message, const NYql::TIssues& subIssues = {}) {
        if (TableWriteActorSpan) {
            TableWriteActorSpan.EndError(message);
        }

        Callbacks->OnError(statusCode, id, message, subIssues);
    }

    void RuntimeError(NYql::NDqProto::StatusIds::StatusCode statusCode, NYql::TIssues&& issues) {
        if (TableWriteActorSpan) {
            TableWriteActorSpan.EndError(issues.ToOneLineString());
        }

        Callbacks->OnError(statusCode, std::move(issues));
    }

    void Unlink() {
        if (LinkedPipeCache) {
            Send(PipeCacheId, new TEvPipeCache::TEvUnlink(0));
            LinkedPipeCache = false;
        }
    }

    void PassAway() override {
        Y_ABORT_UNLESS(Alloc);
        ClearMkqlData();
        Counters->WriteActorsCount->Dec();
        Unlink();
        TActorBootstrapped<TKqpTableWriteActor>::PassAway();
    }

    void Terminate() {
        PassAway();
    }

    void UpdateStats(const NKikimrQueryStats::TTxStats& txStats) {
        Stats.UpdateStats(txStats, TableId);
    }

    void FillStats(NYql::NDqProto::TDqTaskStats* stats) {
        Stats.FillStats(stats, TablePath);
    }

    bool FlushBeforeCommit() const {
        return NeedToFlushBeforeCommit;
    }

private:
    void ClearMkqlData() {
        if (Alloc && ShardedWriteController) {
            TGuard<NMiniKQL::TScopedAlloc> allocGuard(*Alloc);
            ShardedWriteController.Reset();
        }
    }

    NActors::TActorId PipeCacheId = NKikimr::MakePipePerNodeCacheID(false);
    bool LinkedPipeCache = false;

    TString LogPrefix;
    TWriteActorSettings MessageSettings;
    std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> Alloc;

    const std::optional<NKikimrDataEvents::TMvccSnapshot> MvccSnapshot;
    const NKikimrDataEvents::ELockMode LockMode;

    const TString Database;
    const TTableId TableId;
    const TString TablePath;

    std::optional<ui64> TxId;
    const ui64 LockTxId;
    const ui64 LockNodeId;
    const bool InconsistentTx;
    const bool IsOlap;
    const TVector<NScheme::TTypeInfo> KeyColumnTypes;

    IKqpTableWriterCallbacks* Callbacks;

    std::optional<NSchemeCache::TSchemeCacheNavigate::TEntry> SchemeEntry;
    std::shared_ptr<const TVector<TKeyDesc::TPartitionInfo>> Partitioning;
    ui64 ResolveAttempts = 0;

    IKqpTransactionManagerPtr TxManager;
    bool Closed = false;
    bool NeedToFlushBeforeCommit = false;
    EMode Mode = EMode::WRITE;
    THashMap<ui64, TInstant> SendTime;

    IShardedWriteControllerPtr ShardedWriteController = nullptr;

    TIntrusivePtr<TKqpCounters> Counters;
    const TString UserSID;

    TKqpTableWriterStatistics Stats;

    NWilson::TTraceId ParentTraceId;
    NWilson::TSpan TableWriteActorSpan;
    // Current query's SpanId, set before each batch via SetCurrentQuerySpanId.
    ui64 CurrentQuerySpanId = 0;
};


class TKqpWriteTask {
public:
    struct TPathWriteInfo {
        std::vector<ui32> DeleteKeysIndexes;
        std::vector<ui32> NewColumnsIndexes;
        std::vector<ui32> OldColumnsIndexes;
        TKqpTableWriteActor* WriteActor = nullptr;
        std::vector<NScheme::TTypeInfo> ColumnTypes;
        bool NeedWriteProjection = true;
    };

    struct TPathLookupInfo {
        std::vector<ui32> KeyIndexes; // Secondary key
        std::vector<ui32> FullKeyIndexes; // Secondary table key (includes primary key columns)
        std::vector<ui32> PrimaryInFullKeyIndexes; // Primary key in secondary table key
        std::vector<ui32> OldKeyIndexes; // Old secondary key

        IKqpBufferTableLookup* Lookup = nullptr;
    };

    class IKqpReturningConsumer {
    public:
        virtual ~IKqpReturningConsumer() = default;

        virtual void Consume(IDataBatchPtr data) = 0;
    };

    struct TReturningInfo {
        std::vector<ui32> ColumnsIndexes;

        IKqpReturningConsumer* Consumer = nullptr;
    };

private:
    enum class EState {
        BLOCKED,
        BUFFERING,
        LOOKUP_MAIN_TABLE,
        LOOKUP_UNIQUE_INDEX,
        WRITING,
        CLOSING,
        FINISHED,
    };

public:
    TKqpWriteTask(
            const ui64 cookie,
            const ui64 deleteCookie,
            const i64 priority,
            const TPathId pathId,
            const NKikimrKqp::TKqpTableSinkSettings::EType operationType,
            std::vector<TPathWriteInfo> writes,
            std::vector<TPathLookupInfo> lookups,
            std::optional<TReturningInfo> returning,
            TConstArrayRef<NKikimrKqp::TKqpColumnMetadataProto> keyColumns,
            std::vector<ui32> defaultMap,
            std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> alloc)
        : Cookie(cookie)
        , DeleteCookie(deleteCookie)
        , Priority(priority)
        , PathId(pathId)
        , OperationType(operationType)
        , DefaultMap(defaultMap)
        , Alloc(std::move(alloc)) {

        AFL_ENSURE(!keyColumns.empty());
        for (const auto& keyColumn : keyColumns) {
            NScheme::TTypeInfo typeInfo = NScheme::TypeInfoFromProto(keyColumn.GetTypeId(), keyColumn.GetTypeInfo());
            KeyColumnTypes.push_back(typeInfo);
        }

        for (const auto& write : writes) {
            AFL_ENSURE(write.NewColumnsIndexes.size() == write.OldColumnsIndexes.size());
            AFL_ENSURE(write.NewColumnsIndexes.empty() || write.NewColumnsIndexes.size() == write.ColumnTypes.size());
            AFL_ENSURE(write.OldColumnsIndexes.empty() || write.OldColumnsIndexes.size() == write.ColumnTypes.size());
            PathWriteInfo[write.WriteActor->GetTableId().PathId] = write;
        }
        for (const auto& lookup : lookups) {
            PathLookupInfo[lookup.Lookup->GetTableId().PathId] = lookup;

            if (lookup.Lookup->GetTableId().PathId == PathId) {
                AFL_ENSURE(lookup.Lookup->GetKeyColumnTypes().size() == KeyColumnTypes.size());
                AFL_ENSURE(lookup.KeyIndexes.empty());
                AFL_ENSURE(lookup.OldKeyIndexes.empty());
                AFL_ENSURE(lookup.FullKeyIndexes.empty());
                AFL_ENSURE(lookup.PrimaryInFullKeyIndexes.empty());
            } else {
                AFL_ENSURE(lookup.OldKeyIndexes.size() == lookup.KeyIndexes.size());
                AFL_ENSURE(lookup.PrimaryInFullKeyIndexes.size() == KeyColumnTypes.size());
                AFL_ENSURE(lookup.FullKeyIndexes.size() >= KeyColumnTypes.size());
            }
        }

        ReturningInfo = returning;
    }

    void Write(IDataBatchPtr data) {
        AFL_ENSURE(!Closed);
        AFL_ENSURE(!IsError());

        AFL_ENSURE(BufferedBatches.empty()); // At current time fwd<->buffer inflight = 1
        if (!data->IsEmpty()) {
            Memory += data->GetMemory();
            BufferedBatches.push_back(std::move(data));
        }
    }

    void Close() {
        Closed = true;
    }

    void Start() {
        State = EState::BUFFERING;
    }

    void Process(bool forceFlush) {
        AFL_ENSURE(!IsFinished());

        auto stateIteration = [&]() -> bool {
            switch (State) {
                case EState::BLOCKED:
                    return false;
                case EState::BUFFERING:
                    return ProcessBuffering(forceFlush);
                case EState::LOOKUP_MAIN_TABLE:
                    return ProcessLookupMainTable();
                case EState::LOOKUP_UNIQUE_INDEX:
                    return ProcessLookupUniqueIndex();
                case EState::WRITING:
                    return ProcessWriting();
                case EState::CLOSING:
                    return ProcessClosing();
                case EState::FINISHED:
                    return false;
            }
        };

        while (stateIteration());
    }

    i64 GetMemory() const {
        return Memory;
    }

    bool IsEmpty() const {
        return BufferedBatches.empty() && ProcessBatches.empty() && Writes.empty();
    }

    bool IsClosed() const {
        return Closed;
    }

    bool IsFinished() const {
        return State == EState::FINISHED;
    }

    bool IsBlocked() const {
        return State == EState::BLOCKED;
    }

    bool HasReturning() const {
        return ReturningInfo.has_value();
    }

    i64 GetPriority() const {
        return Priority;
    }

    ui64 GetCookie() const {
        return Cookie;
    }

    const THashMap<TPathId, TPathWriteInfo>& GetPathWriteInfo() const {
        return PathWriteInfo;
    }

    const THashMap<TPathId, TPathLookupInfo>& GetPathLookupInfo() const {
        return PathLookupInfo;
    }

    bool IsError() const {
        return Error.has_value();
    }

    TString GetError() const {
        AFL_ENSURE(Error);
        return *Error;
    }

private:
    bool ProcessBuffering(bool forceFlush) {
        AFL_ENSURE(!IsError());
        AFL_ENSURE(ProcessBatches.empty());
        AFL_ENSURE(ProcessCells.empty());
        AFL_ENSURE(Writes.empty());

        if (IsClosed()) {
            if (BufferedBatches.empty()) {
                AFL_ENSURE(IsEmpty());
                AFL_ENSURE(!IsError());
                AFL_ENSURE(GetMemory() == 0);
                State = EState::CLOSING;
                return true;
            }
        } else {
            if (!forceFlush || BufferedBatches.empty()) {
                return false;
            }
        }

        if (auto lookupInfoIt = PathLookupInfo.find(PathId); lookupInfoIt != PathLookupInfo.end()) {
            // Need to lookup main table
            AFL_ENSURE(OperationType != NKikimrKqp::TKqpTableSinkSettings::MODE_INSERT);

            std::swap(BufferedBatches, ProcessBatches);

            auto& lookupInfo = lookupInfoIt->second;
            AFL_ENSURE(lookupInfo.KeyIndexes.empty());

            THashSet<TConstArrayRef<TCell>, NKikimr::TCellVectorsHash, NKikimr::TCellVectorsEquals> primaryKeysSet;
            size_t index = 0;
            for (const auto& batch : ProcessBatches) {
                for (const auto& row : GetRows(batch)) {
                    ProcessCells.push_back(row);
                    const auto& key = ProcessCells.back().first(KeyColumnTypes.size());
                    primaryKeysSet.insert(key);
                    KeyToIndexes[key].push_back(index++);
                }
            }

            AFL_ENSURE(!ProcessCells.empty());
            lookupInfo.Lookup->AddLookupTask(
                Cookie, std::vector<TConstArrayRef<TCell>>(primaryKeysSet.begin(), primaryKeysSet.end()));

            State = EState::LOOKUP_MAIN_TABLE;
            return true;
        }

        // TODO: remove !PathLookupInfo.empty() after full error support
        if (OperationType == NKikimrKqp::TKqpTableSinkSettings::MODE_INSERT && PathWriteInfo.size() > 1) {
            THashSet<TConstArrayRef<TCell>, NKikimr::TCellVectorsHash, NKikimr::TCellVectorsEquals> primaryKeysSet;
            for (const auto& batch : BufferedBatches) {
                for (const auto& row : GetRows(batch)) {
                    const auto& key = row.first(KeyColumnTypes.size());
                    if (!primaryKeysSet.insert(key).second) {
                        Error = DuplicateKeyErrorText;
                        return false;
                    }
                }
            }
        }

        Writes.reserve(BufferedBatches.size());
        for (auto& batch : BufferedBatches) {
            const auto rowsCount = batch->GetRowsCount();
            Writes.push_back(TWrite{
                .Batch = std::move(batch),
                .ExistsMask = std::vector<bool>(rowsCount, true),
            });
        }
        BufferedBatches.clear();

        if (!PathLookupInfo.empty()) {
            // Need to lookup unique indexes.
            // In this case unique indexes keys are subsets of main table key or operation is INSERT.
            AFL_ENSURE(OperationType != NKikimrKqp::TKqpTableSinkSettings::MODE_DELETE);

            for (auto& [pathId, lookupInfo] : PathLookupInfo) {
                AFL_ENSURE(pathId != PathId);

                TUniqueSecondaryKeyCollector collector(
                    KeyColumnTypes,
                    lookupInfo.Lookup->GetKeyColumnTypes(),
                    lookupInfo.KeyIndexes,
                    lookupInfo.FullKeyIndexes,
                    lookupInfo.PrimaryInFullKeyIndexes);
                for (const auto& write : Writes) {
                    for (const auto& row : GetRows(write.Batch)) {
                        if (!collector.AddRow(row)) {
                            Error = DuplicateKeyErrorText;
                            return false;
                        }
                    }
                }

                const auto uniqueSecondaryKeys = std::move(collector).BuildUniqueSecondaryKeys();

                lookupInfo.Lookup->AddUniqueCheckTask(
                    Cookie,
                    std::vector<TConstArrayRef<TCell>>{uniqueSecondaryKeys.begin(), uniqueSecondaryKeys.end()},
                    /* fail on existing row*/
                    OperationType == NKikimrKqp::TKqpTableSinkSettings::MODE_INSERT);
            }

            State = EState::LOOKUP_UNIQUE_INDEX;
        } else {
            State = EState::WRITING;
        }
        return true;
    }

    bool ProcessLookupMainTable() {
        AFL_ENSURE(!IsError());
        AFL_ENSURE(!ProcessBatches.empty());
        AFL_ENSURE(!ProcessCells.empty());
        AFL_ENSURE(OperationType != NKikimrKqp::TKqpTableSinkSettings::MODE_INSERT);

        auto& lookupInfo = PathLookupInfo.at(PathId);
        if (!lookupInfo.Lookup->HasResult(Cookie) && !lookupInfo.Lookup->IsEmpty(Cookie)) {
            return false;
        }

        const size_t lookupColumnsCount = lookupInfo.Lookup->LookupColumnsCount(Cookie);
        auto rowsBatcher = CreateRowsBatcher(ProcessCells[0].size() + lookupColumnsCount, Alloc);

        std::vector<TOwnedCellVec> readCells;
        THashMap<TConstArrayRef<TCell>, size_t, NKikimr::TCellVectorsHash, NKikimr::TCellVectorsEquals> keyToReadCellsIndex;

        lookupInfo.Lookup->ExtractResult(Cookie, [&](TConstArrayRef<TCell> cells) {
            AFL_ENSURE(cells.size() > KeyColumnTypes.size());

            readCells.emplace_back(cells);
            const auto key = readCells.back().first(KeyColumnTypes.size());
            AFL_ENSURE(keyToReadCellsIndex.emplace(key, readCells.size() - 1).second);
        });

        std::vector<bool> existsMask;
        existsMask.reserve(ProcessCells.size());
        for (size_t index = 0; index < ProcessCells.size(); ++index) {
            const auto& processCells = ProcessCells[index];
            const auto& key = processCells.first(KeyColumnTypes.size());

            const auto keyIt = keyToReadCellsIndex.find(key);
            if (keyIt != keyToReadCellsIndex.end()) {
                const auto& newCells = TConstArrayRef<TCell>(readCells[keyIt->second]).last(readCells[keyIt->second].size() - KeyColumnTypes.size());
                AFL_ENSURE(lookupColumnsCount == newCells.size());

                if (DefaultMap.empty()) {
                    for (const auto& cell : processCells) {
                        rowsBatcher->AddCell(cell);
                    }
                } else {
                    Memory -= EstimateSize(processCells);
                    AFL_ENSURE(DefaultMap.size() == processCells.size());
                    for (size_t index = 0; index < processCells.size(); ++index) {
                        AFL_ENSURE(DefaultMap[index] < processCells.size() + newCells.size());
                        AFL_ENSURE(DefaultMap[index] == 0 || DefaultMap[index] >= processCells.size());

                        const auto& cell = DefaultMap[index] == 0
                            ? processCells[index]
                            : newCells[DefaultMap[index] - processCells.size()];

                        Memory += EstimateSize(TConstArrayRef<TCell>(&cell, 1));
                        rowsBatcher->AddCell(cell);
                    }
                }

                Memory += EstimateSize(newCells);
                for (const auto& cell : newCells) {
                    rowsBatcher->AddCell(cell);
                }

                rowsBatcher->AddRow();
                existsMask.push_back(true);
            } else if (OperationType == NKikimrKqp::TKqpTableSinkSettings::MODE_UPDATE
                    || OperationType == NKikimrKqp::TKqpTableSinkSettings::MODE_DELETE) {
                // Skip updates and deletes for non-existing rows.
                Memory -= EstimateSize(processCells);
            } else {
                // For UPDATE WHERE all rows must exist.
                AFL_ENSURE(OperationType != NKikimrKqp::TKqpTableSinkSettings::MODE_UPDATE_CONDITIONAL);

                for (const auto& cell : processCells) {
                    rowsBatcher->AddCell(cell);
                }
                for (size_t i = 0; i < lookupColumnsCount; ++i) {
                    rowsBatcher->AddCell(TCell{});
                    Memory += sizeof(TCell);
                }
                rowsBatcher->AddRow();
                existsMask.push_back(false);
            }
        }

        readCells.clear();
        keyToReadCellsIndex.clear();
        KeyToIndexes.clear();
        ProcessBatches.clear();
        ProcessCells.clear();

        Writes.push_back(TWrite{
            .Batch = rowsBatcher->Flush(),
            .ExistsMask = std::move(existsMask),
        });
        AFL_ENSURE(rowsBatcher->IsEmpty());

        if (PathLookupInfo.size() > 1) {
            // Lookup unique indexes
            AFL_ENSURE(OperationType != NKikimrKqp::TKqpTableSinkSettings::MODE_DELETE);

            AFL_ENSURE(Writes.size() == 1);
            const auto writeRows = GetRows(Writes[0].Batch);
            const auto& existsMask = Writes[0].ExistsMask;
            AFL_ENSURE(writeRows.size() == existsMask.size());

            for (auto& [pathId, lookupInfo] : PathLookupInfo) {
                if (pathId == PathId) {
                    continue;
                }

                TUniqueSecondaryKeyCollector collector(
                        KeyColumnTypes,
                        lookupInfo.Lookup->GetKeyColumnTypes(),
                        lookupInfo.KeyIndexes,
                        lookupInfo.FullKeyIndexes,
                        lookupInfo.PrimaryInFullKeyIndexes);

                AFL_ENSURE(lookupInfo.KeyIndexes.size() == lookupInfo.OldKeyIndexes.size());
                AFL_ENSURE(lookupInfo.KeyIndexes.size() <= lookupInfo.Lookup->GetKeyColumnTypes().size());
                for (size_t index = 0; index < writeRows.size(); ++index) {
                    // Only UPSERT/REPLACE/UPDATE here
                    const auto& row = writeRows[index];
                    if (existsMask[index]
                            && IsEqual(
                                row,
                                lookupInfo.KeyIndexes,
                                lookupInfo.OldKeyIndexes,
                                TConstArrayRef<NScheme::TTypeInfo>(lookupInfo.Lookup->GetKeyColumnTypes())
                                    .first(lookupInfo.KeyIndexes.size()))) {
                        // skip unchanged keys
                        continue;
                    }
                    if (!collector.AddRow(row)) {
                        Error = DuplicateKeyErrorText;
                        return false;
                    }
                }

                const auto uniqueSecondaryKeys = std::move(collector).BuildUniqueSecondaryKeys();

                lookupInfo.Lookup->AddUniqueCheckTask(
                    Cookie,
                    std::vector<TConstArrayRef<TCell>>{uniqueSecondaryKeys.begin(), uniqueSecondaryKeys.end()},
                    /* fail on existing row*/
                    false);
            }

            State = EState::LOOKUP_UNIQUE_INDEX;
        } else {
            State = EState::WRITING;
        }
        return true;
    }

    bool ProcessLookupUniqueIndex() {
        AFL_ENSURE(!IsError());
        AFL_ENSURE(ProcessBatches.empty());
        AFL_ENSURE(ProcessCells.empty());
        AFL_ENSURE(!Writes.empty());
        AFL_ENSURE(OperationType != NKikimrKqp::TKqpTableSinkSettings::MODE_DELETE);

        for (auto& [pathId, lookupInfo] : PathLookupInfo) {
            if (pathId != PathId && !lookupInfo.Lookup->HasResult(Cookie) && !lookupInfo.Lookup->IsEmpty(Cookie)) {
                return false;
            }
        }

        for (auto& [pathId, lookupInfo] : PathLookupInfo) {
            if (pathId != PathId) {
                TUniqueSecondaryKeyCollector collector(
                        KeyColumnTypes,
                        lookupInfo.Lookup->GetKeyColumnTypes(),
                        lookupInfo.KeyIndexes,
                        lookupInfo.FullKeyIndexes,
                        lookupInfo.PrimaryInFullKeyIndexes);
                std::vector<TOwnedCellVec> extractedRows;
                lookupInfo.Lookup->ExtractResult(Cookie, [&](TConstArrayRef<TCell> cells) {
                    extractedRows.emplace_back(cells);
                    AFL_ENSURE(collector.AddSecondaryTableRow(extractedRows.back()));
                });

                for (const auto& write : Writes) {
                    for (const auto& row : GetRows(write.Batch)) {
                        if (!collector.AddRow(row)) {
                            Error = ConflictWithExistingKeyErrorText;
                            return false;
                        }
                    }
                }
            }
        }

        State = EState::WRITING;
        return true;
    }

    bool ProcessWriting() {
        AFL_ENSURE(State == EState::WRITING);
        AFL_ENSURE(!IsError());

        if (!AllOf(PathWriteInfo, [](const auto& writeInfo) {
                return writeInfo.second.WriteActor->IsReady();
            })) {
            return false;
        }

        FlushWritesToActors();
        State = EState::BUFFERING;
        return true;
    }

    bool ProcessClosing() {
        AFL_ENSURE(State == EState::CLOSING);
        AFL_ENSURE(!IsError());
        if (!AllOf(PathWriteInfo, [](const auto& writeInfo) {
                return writeInfo.second.WriteActor->IsReady();
            })) {
            return false;
        }
        CloseWrite();
        State = EState::FINISHED;
        return true;
    }

    void FlushWritesToActors() {
        AFL_ENSURE(!IsError());

        if (PathWriteInfo.contains(PathId) ? PathWriteInfo.size() > 1 : PathWriteInfo.size() > 0) {
            // Secondary index exists
            THashMap<
                TConstArrayRef<TCell>,
                std::pair<TConstArrayRef<TCell>, bool>,
                NKikimr::TCellVectorsHash,
                NKikimr::TCellVectorsEquals> keyToRow;

            for (int batchIndex = static_cast<int>(Writes.size()) - 1; batchIndex >= 0; --batchIndex) {
                const auto& write = Writes[batchIndex];
                const auto rows = GetRows(write.Batch);
                AFL_ENSURE(rows.size() == write.ExistsMask.size());
                for (int index = static_cast<int>(rows.size()) - 1; index >= 0; --index) {
                    keyToRow.emplace(
                        rows[index].first(KeyColumnTypes.size()),
                        std::make_pair(rows[index], write.ExistsMask[index]));
                }
            }

            const bool hasMainTableLookup = PathLookupInfo.contains(PathId);

            for (auto& [actorPathId, actorInfo] : PathWriteInfo) {
                auto rowPossiblyChanged = [&actorInfo, &hasMainTableLookup, this](const TConstArrayRef<TCell> row) {
                    return OperationType == NKikimrKqp::TKqpTableSinkSettings::MODE_DELETE
                        || OperationType == NKikimrKqp::TKqpTableSinkSettings::MODE_INSERT
                        || !hasMainTableLookup
                        || !IsEqual(
                            row,
                            actorInfo.NewColumnsIndexes,
                            actorInfo.OldColumnsIndexes,
                            actorInfo.ColumnTypes);
                };

                // At first, write to indexes
                if (PathId != actorPathId) {
                    const bool hasAdditionalDelete = !actorInfo.DeleteKeysIndexes.empty();
                    if (hasAdditionalDelete) {
                        AFL_ENSURE(OperationType != NKikimrKqp::TKqpTableSinkSettings::MODE_DELETE
                            && OperationType != NKikimrKqp::TKqpTableSinkSettings::MODE_INSERT);
                        {
                            auto deleteProjection = CreateDataBatchProjection(
                                actorInfo.DeleteKeysIndexes, Alloc);
                            for (const auto& [key, rowAndExists] : keyToRow) {
                                const auto& [row, exists] = rowAndExists;
                                if (exists && rowPossiblyChanged(row)) {
                                    deleteProjection->AddRow(row);
                                }
                            }
                            auto preparedKeyBatch = deleteProjection->Flush();
                            actorInfo.WriteActor->Write(
                                DeleteCookie,
                                std::move(preparedKeyBatch));
                        }
                        actorInfo.WriteActor->FlushBuffer(DeleteCookie);
                    }

                    AFL_ENSURE(!actorInfo.NewColumnsIndexes.empty());
                    auto projection = CreateDataBatchProjection(
                                actorInfo.NewColumnsIndexes, Alloc);

                    for (const auto& [key, rowAndExists] : keyToRow) {
                        const auto& [row, exists] = rowAndExists;
                        if (!exists || rowPossiblyChanged(row)) {
                            projection->AddRow(row);
                        }
                    }
                    auto preparedBatch = projection->Flush();
                    actorInfo.WriteActor->Write(
                        Cookie,
                        preparedBatch);
                    actorInfo.WriteActor->FlushBuffer(Cookie);
                }
            }
        }

        if (ReturningInfo) {
            for (auto& write : Writes) {
                const auto& batch = write.Batch;

                auto projection = CreateDataBatchProjection(
                    ReturningInfo->ColumnsIndexes,
                    Alloc);
                for (const auto& row : GetRows(batch)) {
                    projection->AddRow(row);
                }
                auto returningBatch = projection->Flush();

                ReturningInfo->Consumer->Consume(std::move(returningBatch));
            }
        }

        for (auto& write : Writes) {
            auto& batch = write.Batch;
            Memory -= batch->GetMemory();

            auto& actorInfo = PathWriteInfo.at(PathId);
            AFL_ENSURE(actorInfo.DeleteKeysIndexes.empty());

            if (actorInfo.NeedWriteProjection) {
                AFL_ENSURE(!actorInfo.NewColumnsIndexes.empty());
                auto projection = CreateDataBatchProjection(
                    actorInfo.NewColumnsIndexes,
                    Alloc);
                for (const auto& row : GetRows(batch)) {
                    projection->AddRow(row);
                }
                batch = projection->Flush();
            }
            PathWriteInfo.at(PathId).WriteActor->Write(
                Cookie,
                std::move(batch));
        }

        Writes.clear();
    }

    void CloseWrite() {
        AFL_ENSURE(!IsError());
        for (auto& [pathId, actorInfo] : PathWriteInfo) {
            actorInfo.WriteActor->Close(Cookie);
            if (!actorInfo.DeleteKeysIndexes.empty()) {
                AFL_ENSURE(pathId != PathId);
                actorInfo.WriteActor->Close(DeleteCookie);
            }
        }
    }

    const ui64 Cookie;
    const ui64 DeleteCookie;
    const i64 Priority;
    const TPathId PathId;
    const NKikimrKqp::TKqpTableSinkSettings::EType OperationType;
    std::vector<NScheme::TTypeInfo> KeyColumnTypes;
    std::vector<ui32> DefaultMap;
    std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> Alloc;

    EState State = EState::BLOCKED;

    THashMap<TPathId, TPathWriteInfo> PathWriteInfo;
    THashMap<TPathId, TPathLookupInfo> PathLookupInfo;
    std::optional<TReturningInfo> ReturningInfo;

    bool Closed = false;
    i64 Memory = 0;

    std::optional<TString> Error;

    std::vector<IDataBatchPtr> BufferedBatches;
    std::vector<IDataBatchPtr> ProcessBatches;

    struct TWrite {
        IDataBatchPtr Batch;
        std::vector<bool> ExistsMask;
    };
    std::vector<TWrite> Writes;
    std::vector<TConstArrayRef<TCell>> ProcessCells;
    THashMap<TConstArrayRef<TCell>, std::vector<ui32>, NKikimr::TCellVectorsHash, NKikimr::TCellVectorsEquals> KeyToIndexes;
};

class TWriteTasksPlanner {
public:
    void AddTask(TKqpWriteTask& task) {
        AFL_ENSURE(!task.IsFinished());
        AFL_ENSURE(PriorityToBlockedTask.emplace(task.GetPriority(), task).second);
        while (PriorityToBlockedTask.contains(FirstUnknownPriority)) {
            ++FirstUnknownPriority;
        }
    }

    void ReleaseTask(TKqpWriteTask& task) {
        AFL_ENSURE(task.IsFinished());
        for (const auto& [pathId, _] : task.GetPathWriteInfo()) {
            TableLocks.at(pathId).Release(task.GetCookie());
        }
        for (const auto& [pathId, _] : task.GetPathLookupInfo()) {
            TableLocks.at(pathId).Release(task.GetCookie());
        }
    }

    bool StartUnblockedTasks() {
        bool startedTasks = false;
        THashSet<TPathId> failedLocks;
        for (auto it = PriorityToBlockedTask.begin(); it != PriorityToBlockedTask.end() && it->first < FirstUnknownPriority;) {
            auto& task = it->second.get();
            bool canAquireLocks = true;
            // If some task failed to lock a path, then next tasks also can't lock this path.
            for (const auto& [pathId, _] : task.GetPathWriteInfo()) {
                if (failedLocks.contains(pathId)) {
                    canAquireLocks = false;
                } else if (!TableLocks[pathId].CheckAcquire(
                        task.GetCookie(),
                        TTableLock::ETableLockMode::EXCLUSIVE)) {
                    canAquireLocks = false;
                    failedLocks.insert(pathId);
                }
            }
            for (const auto& [pathId, _] : task.GetPathLookupInfo()) {
                if (failedLocks.contains(pathId)) {
                    canAquireLocks = false;
                } else if (!TableLocks[pathId].CheckAcquire(
                        task.GetCookie(),
                        TTableLock::ETableLockMode::SHARED)) {
                    canAquireLocks = false;
                    failedLocks.insert(pathId);
                }
            }
            if (canAquireLocks) {
                for (const auto& [pathId, _] : task.GetPathWriteInfo()) {
                    TableLocks.at(pathId).Acquire(
                        task.GetCookie(),
                        TTableLock::ETableLockMode::EXCLUSIVE);
                }
                for (const auto& [pathId, _] : task.GetPathLookupInfo()) {
                    TableLocks.at(pathId).Acquire(
                        task.GetCookie(),
                        TTableLock::ETableLockMode::SHARED);
                }
                task.Start();
                it = PriorityToBlockedTask.erase(it);
                startedTasks = true;
            } else {
                ++it;
            }
        }

        return startedTasks;
    }

private:
    class TTableLock {
    public:
        enum class ETableLockMode {
            NONE,
            SHARED,
            EXCLUSIVE,
        };

        bool CheckAcquire(ui64 taskCookie, ETableLockMode mode) const {
            AFL_ENSURE(mode != ETableLockMode::NONE);
            if (Mode == ETableLockMode::NONE) {
                AFL_ENSURE(Owners.empty());
                return true;
            } else if (Mode == ETableLockMode::SHARED) {
                AFL_ENSURE(!Owners.empty());
                if (mode == ETableLockMode::SHARED) {
                    return true;
                } else {
                    return Owners.size() == 1 && Owners.contains(taskCookie);
                }
            } else {
                AFL_ENSURE(Owners.size() == 1);
                return Owners.contains(taskCookie);
            }
        }

        void Acquire(ui64 taskCookie, ETableLockMode mode) {
            AFL_ENSURE(CheckAcquire(taskCookie, mode));
            if (Mode < mode) {
                Mode = mode;
            }
            Owners.insert(taskCookie);
        }

        void Release(ui64 taskCookie) {
            Owners.erase(taskCookie);
            if (Owners.empty()) {
                Mode = ETableLockMode::NONE;
            }
        }

    private:
        ETableLockMode Mode = ETableLockMode::NONE;
        THashSet<ui64> Owners;
    };

    THashMap<TPathId, TTableLock> TableLocks;
    TMap<i64, std::reference_wrapper<TKqpWriteTask>> PriorityToBlockedTask;
    i64 FirstUnknownPriority = 0;
};

class TKqpDirectWriteActor : public TActorBootstrapped<TKqpDirectWriteActor>, public NYql::NDq::IDqComputeActorAsyncOutput, public IKqpTableWriterCallbacks {
    using TBase = TActorBootstrapped<TKqpDirectWriteActor>;

public:
    TKqpDirectWriteActor(
        NKikimrKqp::TKqpTableSinkSettings&& settings,
        NYql::NDq::TDqAsyncIoFactory::TSinkArguments&& args,
        TIntrusivePtr<TKqpCounters> counters,
        const TString& userSID)
        : LogPrefix(TStringBuilder() << "TxId: " << args.TxId << ", task: " << args.TaskId << ". ")
        , Settings(std::move(settings))
        , MessageSettings(GetWriteActorSettings())
        , OutputIndex(args.OutputIndex)
        , Callbacks(args.Callback)
        , Counters(counters)
        , Alloc(args.Alloc)
        , TxId(std::get<ui64>(args.TxId))
        , TableId(
            Settings.GetTable().GetOwnerId(),
            Settings.GetTable().GetTableId(),
            Settings.GetTable().GetVersion())
        , DirectWriteActorSpan(TWilsonKqp::DirectWriteActor, NWilson::TTraceId(args.TraceId), "TKqpDirectWriteActor")
        , UserSID(userSID)
    {
        EgressStats.Level = args.StatsLevel;

        TVector<NKikimrKqp::TKqpColumnMetadataProto> columnsMetadata(
            Settings.GetColumns().begin(),
            Settings.GetColumns().end());
        std::vector<ui32> writeIndex(
            Settings.GetWriteIndexes().begin(),
            Settings.GetWriteIndexes().end());

        TGuard guard(*Alloc);
        if (Settings.GetIsOlap()) {
            Batcher = CreateColumnDataBatcher(columnsMetadata, std::move(writeIndex), Alloc);
        } else {
            Batcher = CreateRowDataBatcher(columnsMetadata, std::move(writeIndex), Alloc);
        }
    }

    void Bootstrap() {
        LogPrefix = TStringBuilder() << "SelfId: " << this->SelfId() << ", " << LogPrefix;

        try {
            TVector<NScheme::TTypeInfo> keyColumnTypes;
            keyColumnTypes.reserve(Settings.GetKeyColumns().size());
            for (const auto& column : Settings.GetKeyColumns()) {
                auto typeInfoMod = NScheme::TypeInfoModFromProtoColumnType(column.GetTypeId(),
                    column.HasTypeInfo() ? &column.GetTypeInfo() : nullptr);
                keyColumnTypes.push_back(typeInfoMod.TypeInfo);
            }

            WriteTableActor = new TKqpTableWriteActor(
                this,
                Settings.GetDatabase(),
                TableId,
                Settings.GetTable().GetPath(),
                Settings.GetLockTxId(),
                Settings.GetLockNodeId(),
                Settings.GetInconsistentTx(),
                Settings.GetIsOlap(),
                std::move(keyColumnTypes),
                Alloc,
                GetOptionalMvccSnapshot(Settings),
                Settings.GetLockMode(),
                nullptr,
                TActorId{},
                Counters,
                UserSID);
            // Set initial QuerySpanId for direct write actor
            WriteTableActor->SetCurrentQuerySpanId(Settings.GetQuerySpanId());

            WriteTableActor->SetParentTraceId(DirectWriteActorSpan.GetTraceId());
            WriteTableActorId = RegisterWithSameMailbox(WriteTableActor);

            TVector<NKikimrKqp::TKqpColumnMetadataProto> keyColumnsMetadata(
                Settings.GetKeyColumns().begin(),
                Settings.GetKeyColumns().end());
            TVector<NKikimrKqp::TKqpColumnMetadataProto> columnsMetadata(Settings.GetColumns().size());
            AFL_ENSURE(Settings.GetColumns().size() == Settings.GetWriteIndexes().size());
            for (int index = 0; index < Settings.GetColumns().size(); ++index) {
                columnsMetadata[Settings.GetWriteIndexes()[index]] = Settings.GetColumns()[index];
            }
            YQL_ENSURE(Settings.GetPriority() == 0);
            WriteTableActor->Open(
                WriteToken,
                GetOperation(Settings.GetType()),
                std::move(keyColumnsMetadata),
                std::move(columnsMetadata),
                0,
                Settings.GetPriority());
            WaitingForTableActor = true;
        } catch (const TMemoryLimitExceededException&) {
            RuntimeError(
                NYql::NDqProto::StatusIds::PRECONDITION_FAILED,
                NYql::TIssuesIds::KIKIMR_PRECONDITION_FAILED,
                TStringBuilder() << "Memory limit exception"
                    << ", current limit is " << Alloc->GetLimit() << " bytes.",
                {});
            return;
        } catch (...) {
            RuntimeError(
                NYql::NDqProto::StatusIds::INTERNAL_ERROR,
                NYql::TIssuesIds::KIKIMR_INTERNAL_ERROR,
                CurrentExceptionMessage(),
                {});
            return;
        }
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
        return DataBufferMemory + ((WriteTableActor && WriteTableActor->IsReady())
            ? WriteTableActor->GetMemory()
            : 0);
    }

    TMaybe<google::protobuf::Any> ExtraData() override {
        if (!WriteTableActor) {
            return {};
        }
        NKikimrKqp::TEvKqpOutputActorResultInfo resultInfo;
        for (const auto& lock : WriteTableActor->GetLocks()) {
            resultInfo.AddLocks()->CopyFrom(lock);
        }
        resultInfo.SetHasRead(
            GetOperation(Settings.GetType()) == NKikimrDataEvents::TEvWrite::TOperation::OPERATION_INSERT ||
            GetOperation(Settings.GetType()) == NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPDATE);
        google::protobuf::Any result;
        result.PackFrom(resultInfo);
        return result;
    }

    void SendData(NMiniKQL::TUnboxedValueBatch&& data, i64 size, const TMaybe<NYql::NDqProto::TCheckpoint>& checkpoint, bool finished) final {
        YQL_ENSURE(!data.IsWide(), "Wide stream is not supported yet");
        YQL_ENSURE(!Closed || data.empty());
        Closed = finished;
        EgressStats.Resume();
        Y_UNUSED(size);

        try {
            if (!data.empty()) {
                Batcher->AddData(data);
                auto batch = Batcher->Build();
                DataBufferMemory += batch->GetMemory();
                DataBuffer.emplace(std::move(batch));
            }

            if (checkpoint) {
                DataBuffer.emplace(*checkpoint);
            }
        } catch (const TMemoryLimitExceededException&) {
            RuntimeError(
                NYql::NDqProto::StatusIds::PRECONDITION_FAILED,
                NYql::TIssuesIds::KIKIMR_PRECONDITION_FAILED,
                TStringBuilder() << "Memory limit exception"
                    << ", current limit is " << Alloc->GetLimit() << " bytes.",
                {});
            return;
        } catch (...) {
            RuntimeError(
                NYql::NDqProto::StatusIds::INTERNAL_ERROR,
                NYql::TIssuesIds::KIKIMR_INTERNAL_ERROR,
                CurrentExceptionMessage(),
                {});
            return;
        }

        Process();
    }

    void Process() {
        try {
            YQL_ENSURE(WriteTableActor);
            if (CheckpointInProgress && WriteTableActor->IsEmpty()) {
                DoCheckpoint();
            }

            while (!DataBuffer.empty() && !CheckpointInProgress) {
                auto variant = std::move(DataBuffer.front());
                DataBuffer.pop();

                if (std::holds_alternative<IDataBatchPtr>(variant)) {
                    auto data = std::get<IDataBatchPtr>(std::move(variant));
                    DataBufferMemory -= data->GetMemory();
                    WriteTableActor->Write(WriteToken, std::move(data));
                } else if (std::holds_alternative<NYql::NDqProto::TCheckpoint>(variant)) {
                    CheckpointInProgress = std::get<NYql::NDqProto::TCheckpoint>(std::move(variant));
                    WriteTableActor->FlushBuffers();
                    if (WriteTableActor->IsEmpty()) {
                        DoCheckpoint();
                    }
                }
            }

            if (Closed && DataBuffer.empty() && !WriteTableActor->IsClosed()) {
                WriteTableActor->Close(WriteToken);
                WriteTableActor->FlushBuffers();
                WriteTableActor->Close();
            }

            const bool outOfMemory = GetFreeSpace() <= 0;
            if (outOfMemory) {
                WaitingForTableActor = true;
            } else if (WaitingForTableActor) {
                ResumeExecution();
            }

            if (outOfMemory && !Settings.GetEnableStreamWrite()) {
                RuntimeError(
                    NYql::NDqProto::StatusIds::PRECONDITION_FAILED,
                    NYql::TIssuesIds::KIKIMR_PRECONDITION_FAILED,
                    TStringBuilder() << "Out of buffer memory. Used " << GetMemory()
                        << " bytes of " << MessageSettings.InFlightMemoryLimitPerActorBytes << " bytes.",
                    {});
                return;
            }

            if (!Closed && outOfMemory) {
                WriteTableActor->FlushBuffers();
            }

            if (Closed || outOfMemory || CheckpointInProgress) {
                if (!WriteTableActor->FlushToShards()) {
                    return;
                }
            }

            if (Closed && WriteTableActor->IsFinished()) {
                CA_LOG_D("Write actor finished");
                Callbacks->OnAsyncOutputFinished(GetOutputIndex());
            }
        } catch (const TMemoryLimitExceededException&) {
            RuntimeError(
                NYql::NDqProto::StatusIds::PRECONDITION_FAILED,
                NYql::TIssuesIds::KIKIMR_PRECONDITION_FAILED,
                TStringBuilder() << "Memory limit exception"
                    << ", current limit is " << Alloc->GetLimit() << " bytes.",
                {});
            return;
        } catch (...) {
            RuntimeError(
                NYql::NDqProto::StatusIds::INTERNAL_ERROR,
                NYql::TIssuesIds::KIKIMR_INTERNAL_ERROR,
                CurrentExceptionMessage(),
                {});
            return;
        }
    }

    void RuntimeError(NYql::NDqProto::StatusIds::StatusCode statusCode, NYql::EYqlIssueCode id, const TString& message, const NYql::TIssues& subIssues) {
        DirectWriteActorSpan.EndError(message);

        NYql::TIssue issue(message);
        SetIssueCode(id, issue);
        for (const auto& i : subIssues) {
            issue.AddSubIssue(MakeIntrusive<NYql::TIssue>(i));
        }

        NYql::TIssues issues;
        issues.AddIssue(std::move(issue));

        Callbacks->OnAsyncOutputError(OutputIndex, std::move(issues), statusCode);
    }

    void RuntimeError(NYql::NDqProto::StatusIds::StatusCode statusCode, NYql::TIssues&& issues) {
        DirectWriteActorSpan.EndError(issues.ToOneLineString());

        Callbacks->OnAsyncOutputError(OutputIndex, std::move(issues), statusCode);
    }

    void PassAway() override {
        if (WriteTableActor) {
            WriteTableActor->Terminate();
        }
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

    void OnPrepared(IKqpTransactionManager::TPrepareResult&&, ui64) override {
        AFL_ENSURE(false);
    }

    void OnCommitted(ui64, ui64) override {
        AFL_ENSURE(false);
    }

    void OnMessageAcknowledged(ui64 dataSize) override {
        EgressStats.Bytes += dataSize;
        EgressStats.Chunks++;
        EgressStats.Splits++;
        EgressStats.Resume();
        Process();
    }

    void OnError(NYql::NDqProto::StatusIds::StatusCode statusCode, NYql::EYqlIssueCode id, const TString& message, const NYql::TIssues& subIssues) override {
        RuntimeError(statusCode, id, message, subIssues);
    }

    void OnError(NYql::NDqProto::StatusIds::StatusCode statusCode, NYql::TIssues&& issues) override {
        RuntimeError(statusCode, std::move(issues));
    }

    void FillExtraStats(NYql::NDqProto::TDqTaskStats* stats, bool last, const NYql::NDq::TDqMeteringStats*) override {
        if (last && WriteTableActor) {
            WriteTableActor->FillStats(stats);
        }
    }

    void DoCheckpoint() {
        YQL_ENSURE(CheckpointInProgress);
        Callbacks->OnAsyncOutputStateSaved({}, OutputIndex, *CheckpointInProgress);
        CheckpointInProgress = std::nullopt;
    }

    TString LogPrefix;
    const NKikimrKqp::TKqpTableSinkSettings Settings;
    TWriteActorSettings MessageSettings;
    const ui64 OutputIndex;
    NYql::NDq::TDqAsyncStats EgressStats;
    NYql::NDq::IDqComputeActorAsyncOutput::ICallbacks * Callbacks = nullptr;
    TIntrusivePtr<TKqpCounters> Counters;
    std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> Alloc;
    IDataBatcherPtr Batcher;

    const ui64 TxId;
    const TTableId TableId;
    TKqpTableWriteActor* WriteTableActor = nullptr;
    TActorId WriteTableActorId;

    TKqpTableWriteActor::TWriteToken WriteToken = 0;
    std::queue<std::variant<IDataBatchPtr, NYql::NDqProto::TCheckpoint>> DataBuffer;
    std::optional<NYql::NDqProto::TCheckpoint> CheckpointInProgress;
    i64 DataBufferMemory = 0;

    bool Closed = false;
    bool WaitingForTableActor = false;

    NWilson::TSpan DirectWriteActorSpan;
    const TString UserSID;
};


namespace {

struct TWriteToken {
    TPathId PathId;
    ui64 Cookie;

    bool IsEmpty() const {
        return !PathId;
    }
};

struct TTransactionSettings {
    ui64 TxId = 0;
    ui64 LockTxId = 0;
    ui64 LockNodeId = 0;
    bool InconsistentTx = false;
    std::optional<NKikimrDataEvents::TMvccSnapshot> MvccSnapshot;
    NKikimrDataEvents::ELockMode LockMode;
};

struct TWriteSettings {
    TString Database;
    TTableId TableId;
    TString TablePath; // for error messages
    NKikimrKqp::TKqpTableSinkSettings::EType OperationType;
    TVector<NKikimrKqp::TKqpColumnMetadataProto> KeyColumns;
    TVector<NKikimrKqp::TKqpColumnMetadataProto> Columns;
    TVector<NKikimrKqp::TKqpColumnMetadataProto> LookupColumns;
    TVector<NKikimrKqp::TKqpColumnMetadataProto> ReturningColumns;
    TTransactionSettings TransactionSettings;
    i64 Priority;
    bool IsOlap;
    THashSet<TStringBuf> DefaultColumns;

    struct TIndex {
        TTableId TableId;
        TString TablePath;
        TVector<NKikimrKqp::TKqpColumnMetadataProto> KeyColumns;
        ui32 KeyPrefixSize;
        TVector<NKikimrKqp::TKqpColumnMetadataProto> Columns;
        bool IsUniq;
    };

    std::vector<TIndex> Indexes;

    bool EnableStreamWrite;
    ui64 QuerySpanId = 0;
};

struct TBufferWriteMessage {
    TActorId From;
    TWriteToken Token;
    bool Close = false;
    IDataBatchPtr Data;
};

struct TEvBufferWrite : public TEventLocal<TEvBufferWrite, TKqpEvents::EvBufferWrite> {
    bool Close = false;
    std::optional<TWriteToken> Token;
    std::optional<TWriteSettings> Settings;
    IDataBatchPtr Data;

    TInstant SendTime;
};

struct TEvBufferWriteResult : public TEventLocal<TEvBufferWriteResult, TKqpEvents::EvBufferWriteResult> {
    TWriteToken Token;
    std::vector<IDataBatchPtr> Data;
};

}


class TKqpBufferWriteActor : public TActorBootstrapped<TKqpBufferWriteActor>, public IKqpTableWriterCallbacks, public IKqpBufferTableLookupCallbacks {
    using TBase = TActorBootstrapped<TKqpBufferWriteActor>;
    using TTopicTabletTxs = NTopic::TTopicOperationTransactions;

    struct TEvPrivate {
        enum EEv {
            EvReattachToShard = EventSpaceBegin(TKikimrEvents::ES_PRIVATE),
        };

        struct TEvReattachToShard : public TEventLocal<TEvReattachToShard, EvReattachToShard> {
            const ui64 TabletId;

            explicit TEvReattachToShard(ui64 tabletId)
                : TabletId(tabletId) {}
        };
    };

public:
    TKqpBufferWriteActor(
        TKqpBufferWriterSettings&& settings)
        : SessionActorId(settings.SessionActorId)
        , MessageSettings(GetWriteActorSettings())
        , TxManager(settings.TxManager)
        , Alloc(settings.Alloc)
        , TypeEnv(std::make_shared<NKikimr::NMiniKQL::TTypeEnvironment>(*Alloc))
        , MemInfo("TKqpBufferWriteActor")
        , HolderFactory(std::make_shared<NKikimr::NMiniKQL::THolderFactory>(
            Alloc->Ref(), MemInfo, AppData()->FunctionRegistry))
        , Counters(settings.Counters)
        , TxProxyMon(settings.TxProxyMon)
        , BufferWriteActorSpan(TWilsonKqp::BufferWriteActor, NWilson::TTraceId(settings.TraceId), "BufferWriteActor", NWilson::EFlags::AUTO_END)
        , UserSID(settings.UserSID)
        , QuerySpanId(settings.QuerySpanId)
    {
        Counters->BufferActorsCount->Inc();
        UpdateTracingState("Write", BufferWriteActorSpan.GetTraceId());
    }

    void Bootstrap() {
        LogPrefix = TStringBuilder() << "SelfId: " << this->SelfId() << ", SessionActorId: " << SessionActorId << ", " << LogPrefix;
        Become(&TThis::StateWrite);
    }

    static constexpr char ActorName[] = "KQP_BUFFER_WRITE_ACTOR";

    STFUNC(StateWrite) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvKqpBuffer::TEvTerminate, Handle);
                hFunc(TEvKqpBuffer::TEvFlush, Handle);
                hFunc(TEvKqpBuffer::TEvCommit, Handle);
                hFunc(TEvKqpBuffer::TEvRollback, Handle);
                hFunc(TEvBufferWrite, Handle);
            default:
                AFL_ENSURE(false)("StateWrite: unknown message", ev->GetTypeRewrite());
            }
        } catch (const TMemoryLimitExceededException&) {
            ReplyMemoryLimitError();
        } catch (...) {
            ReplyCurrentExceptionError();
        }
    }

    STFUNC(StateWaitTasks) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvKqpBuffer::TEvTerminate, Handle);
                hFunc(TEvKqpBuffer::TEvRollback, Handle);
            default:
                AFL_ENSURE(false)("StateWaitTasks: unknown message", ev->GetTypeRewrite());
            }
        } catch (const TMemoryLimitExceededException&) {
            ReplyMemoryLimitError();
        } catch (...) {
            ReplyCurrentExceptionError();
        }
    }

    STFUNC(StateFlush) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvKqpBuffer::TEvTerminate, Handle);
                hFunc(TEvKqpBuffer::TEvRollback, Handle);
            default:
                AFL_ENSURE(false)("StateFlush: unknown message", ev->GetTypeRewrite());
            }
        } catch (const TMemoryLimitExceededException&) {
            ReplyMemoryLimitError();
        } catch (...) {
            ReplyCurrentExceptionError();
        }
    }

    STFUNC(StatePrepare) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvKqpBuffer::TEvTerminate, Handle);
                hFunc(TEvKqpBuffer::TEvRollback, Handle);
                hFunc(TEvPersQueue::TEvProposeTransactionResult, HandlePrepare);
                hFunc(NKikimr::NEvents::TDataEvents::TEvWriteResult, HandlePrepare);
                hFunc(TEvPipeCache::TEvDeliveryProblem, HandlePrepare);
                hFunc(TEvDataShard::TEvOverloadReady, HandlePrepare);
                hFunc(TEvColumnShard::TEvOverloadReady, HandlePrepare);

                hFunc(TEvDataShard::TEvProposeTransactionAttachResult, HandlePrepare);
                hFunc(TEvPrivate::TEvReattachToShard, Handle);
                hFunc(TEvDataShard::TEvProposeTransactionRestart, Handle);
            default:
                AFL_ENSURE(false)("StatePrepare: unknown message", ev->GetTypeRewrite());
            }
        } catch (const TMemoryLimitExceededException&) {
            ReplyMemoryLimitError();
        } catch (...) {
            ReplyCurrentExceptionError();
        }
    }

    STFUNC(StateCommit) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvKqpBuffer::TEvTerminate, Handle);
                hFunc(TEvKqpBuffer::TEvRollback, Handle);
                hFunc(TEvTxProxy::TEvProposeTransactionStatus, Handle);
                hFunc(TEvPersQueue::TEvProposeTransactionResult, HandleCommit);
                hFunc(NKikimr::NEvents::TDataEvents::TEvWriteResult, HandleCommit);
                hFunc(TEvPipeCache::TEvDeliveryProblem, HandleCommit);

                hFunc(TEvDataShard::TEvProposeTransactionAttachResult, HandleCommit);
                hFunc(TEvPrivate::TEvReattachToShard, Handle);
                hFunc(TEvDataShard::TEvProposeTransactionRestart, Handle);
            default:
                AFL_ENSURE(false)("StateCommit: unknown message", ev->GetTypeRewrite());
            }
        } catch (const TMemoryLimitExceededException&) {
            ReplyMemoryLimitError();
        } catch (...) {
            ReplyCurrentExceptionError();
        }
    }

    STFUNC(StateRollback) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvKqpBuffer::TEvTerminate, Handle);
                hFunc(NKikimr::NEvents::TDataEvents::TEvWriteResult, HandleRollback);
                hFunc(TEvPipeCache::TEvDeliveryProblem, HandleRollback);

            default:
                CA_LOG_W("StateRollback: unknown message " << ev->GetTypeRewrite());
            }
        } catch (const TMemoryLimitExceededException&) {
            ReplyMemoryLimitError();
        } catch (...) {
            ReplyCurrentExceptionError();
        }
    }

    STFUNC(StateError) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvKqpBuffer::TEvTerminate, Handle);
                hFunc(TEvKqpBuffer::TEvRollback, Handle);

            default:
                CA_LOG_W("StateRollback: unknown message " << ev->GetTypeRewrite());
            }
        } catch (...) {
            ReplyCurrentExceptionError();
        }
    }

    void ReplyMemoryLimitError() {
        ReplyError(
            NYql::NDqProto::StatusIds::PRECONDITION_FAILED,
            NYql::TIssuesIds::KIKIMR_PRECONDITION_FAILED,
            TStringBuilder() << "Memory limit exception"
                << ", current limit is " << Alloc->GetLimit() << " bytes.",
            {});
    }

    void ReplyCurrentExceptionError() {
        ReplyError(
            NYql::NDqProto::StatusIds::INTERNAL_ERROR,
            NYql::TIssuesIds::KIKIMR_INTERNAL_ERROR,
            CurrentExceptionMessage(),
            {});
    }

    void Handle(TEvBufferWrite::TPtr& ev) {
        Counters->ForwardActorWritesLatencyHistogram->Collect((TInstant::Now() - ev->Get()->SendTime).MicroSeconds());
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

            // Can't have CTAS here
            AFL_ENSURE(settings.OperationType != NKikimrKqp::TKqpTableSinkSettings::MODE_FILL);

            auto createWriteActor = [&](const TTableId tableId, const TString& tablePath, const TVector<NKikimrKqp::TKqpColumnMetadataProto>& keyColumns) -> std::pair<TKqpTableWriteActor*, TActorId> {
                TVector<NScheme::TTypeInfo> keyColumnTypes;
                keyColumnTypes.reserve(keyColumns.size());
                for (const auto& column : keyColumns) {
                    auto typeInfoMod = NScheme::TypeInfoModFromProtoColumnType(column.GetTypeId(),
                        column.HasTypeInfo() ? &column.GetTypeInfo() : nullptr);
                    keyColumnTypes.push_back(typeInfoMod.TypeInfo);
                }
                TKqpTableWriteActor* ptr = new TKqpTableWriteActor(
                    this,
                    settings.Database,
                    tableId,
                    tablePath,
                    LockTxId,
                    LockNodeId,
                    InconsistentTx,
                    settings.IsOlap,
                    std::move(keyColumnTypes),
                    Alloc,
                    settings.TransactionSettings.MvccSnapshot,
                    settings.TransactionSettings.LockMode,
                    TxManager,
                    SessionActorId,
                    Counters,
                    UserSID);
                ptr->SetParentTraceId(BufferWriteActorStateSpan.GetTraceId());
                TActorId id = RegisterWithSameMailbox(ptr);
                CA_LOG_D("Create new TableWriteActor for table `" << tablePath << "` (" << tableId << "). lockId=" << LockTxId << ". ActorId=" << id);

                return {ptr, id};
            };

            auto checkSchemaVersion = [&](auto* actor, const TTableId tableId, const TString& tablePath) -> bool {
                if (actor->GetTableId().SchemaVersion != tableId.SchemaVersion) {
                    CA_LOG_E("Scheme changed for table `"
                        << tablePath << "`.");
                    ReplyError(
                        NYql::NDqProto::StatusIds::SCHEME_ERROR,
                        NYql::TIssuesIds::KIKIMR_SCHEME_MISMATCH,
                        TStringBuilder() << "Scheme changed. Table: `"
                            << tablePath << "`.",
                        {});
                    return false;
                }
                AFL_ENSURE(actor->GetTableId() == tableId);
                return true;
            };

            auto createLookupActor = [&](const TTableId tableId, const TString& tablePath) -> std::pair<IKqpBufferTableLookup*, TActorId> {
                auto [ptr, actor] = CreateKqpBufferTableLookup(TKqpBufferTableLookupSettings{
                    .Callbacks = this,

                    .TableId = tableId,
                    .TablePath = tablePath,

                    .LockTxId = LockTxId,
                    .LockNodeId = LockNodeId,
                    .LockMode = settings.TransactionSettings.LockMode,
                    .QuerySpanId = QuerySpanId,
                    .MvccSnapshot = settings.TransactionSettings.MvccSnapshot,

                    .TxManager = TxManager,
                    .Alloc = Alloc,
                    .TypeEnv = *TypeEnv,
                    .HolderFactory = *HolderFactory,
                    .SessionActorId = SessionActorId,
                    .Counters = Counters,

                    .ParentTraceId = BufferWriteActorStateSpan.GetTraceId(),
                });

                TActorId id = RegisterWithSameMailbox(actor);
                CA_LOG_D("Create new KqpBufferTableLookup for table `" << tablePath << "` (" << tableId << "). lockId=" << LockTxId << ". ActorId=" << id);

                return {ptr, id};
            };


            auto& writeInfo = WriteInfos[settings.TableId.PathId];
            if (!writeInfo.Actors.contains(settings.TableId.PathId)) {
                AFL_ENSURE(writeInfo.Actors.empty());
                const auto [ptr, id] = createWriteActor(settings.TableId, settings.TablePath, settings.KeyColumns);
                writeInfo.Actors.emplace(settings.TableId.PathId, TWriteInfo::TActorInfo{
                    .WriteActor = ptr,
                    .Id = id,
                });
            } else {
                if (!checkSchemaVersion(
                        writeInfo.Actors.at(settings.TableId.PathId).WriteActor,
                        settings.TableId,
                        settings.TablePath)) {
                    return;
                }
            }
            // Set the current query's SpanId on all write actors for this table.
            // TxManager::AddAction (called from UpdateShards) will collect per-shard SpanIds.
            for (auto& [pathId, actorInfo] : writeInfo.Actors) {
                actorInfo.WriteActor->SetCurrentQuerySpanId(settings.QuerySpanId);
            }

            if (!settings.LookupColumns.empty()) {
                AFL_ENSURE(!settings.IsOlap);
                auto& lookupInfo = LookupInfos[settings.TableId.PathId];
                if (!lookupInfo.Actors.contains(settings.TableId.PathId)) {
                    const auto [ptr, id] = createLookupActor(settings.TableId, settings.TablePath);
                    AFL_ENSURE(lookupInfo.Actors.emplace(settings.TableId.PathId, TLookupInfo::TActorInfo{
                        .LookupActor = ptr,
                        .Id = id,
                    }).second);
                } else {
                    if (!checkSchemaVersion(
                            lookupInfo.Actors.at(settings.TableId.PathId).LookupActor,
                            settings.TableId,
                            settings.TablePath)) {
                        return;
                    }
                }
            }

            THashSet<TStringBuf> primaryKeyColumnsSet;
            for (const auto& column : settings.KeyColumns) {
                primaryKeyColumnsSet.insert(column.GetName());
            }
            const auto isSubsetOfPrimaryKeyColumns = [&](const TVector<NKikimrKqp::TKqpColumnMetadataProto>& columns) {
                return std::all_of(columns.begin(), columns.end(), [&](const NKikimrKqp::TKqpColumnMetadataProto& column) {
                    return primaryKeyColumnsSet.contains(column.GetName());
                });
            };

            for (const auto& indexSettings : settings.Indexes) {
                AFL_ENSURE(!settings.IsOlap);
                if (!writeInfo.Actors.contains(indexSettings.TableId.PathId)) {
                    const auto [ptr, id] = createWriteActor(indexSettings.TableId, indexSettings.TablePath, indexSettings.KeyColumns);
                    writeInfo.Actors.emplace(indexSettings.TableId.PathId, TWriteInfo::TActorInfo{
                        .WriteActor = ptr,
                        .Id = id,
                    });
                } else {
                    if (!checkSchemaVersion(
                            writeInfo.Actors.at(indexSettings.TableId.PathId).WriteActor,
                            indexSettings.TableId,
                            indexSettings.TablePath)) {
                        return;
                    }
                }

                if (indexSettings.IsUniq) {
                    auto& lookupInfo = LookupInfos[indexSettings.TableId.PathId];
                    if (!lookupInfo.Actors.contains(indexSettings.TableId.PathId)) {
                        const auto [ptr, id] = createLookupActor(indexSettings.TableId, indexSettings.TablePath);
                        AFL_ENSURE(lookupInfo.Actors.emplace(indexSettings.TableId.PathId, TLookupInfo::TActorInfo{
                            .LookupActor = ptr,
                            .Id = id,
                        }).second);
                    } else {
                        if (!checkSchemaVersion(
                                lookupInfo.Actors.at(indexSettings.TableId.PathId).LookupActor,
                                indexSettings.TableId,
                                indexSettings.TablePath)) {
                            return;
                        }
                    }
                }
            }

            EnableStreamWrite &= settings.EnableStreamWrite;

            token = TWriteToken{settings.TableId.PathId, CurrentWriteToken};
            CurrentWriteToken += 2;
            // Cookie -- for operations with main table and writes to indexes
            // Cookie+1 -- for deletes from indexes
            const auto writeCookie = token.Cookie;
            const auto deleteCookie = token.Cookie + 1;

            std::vector<TKqpWriteTask::TPathWriteInfo> writes;
            std::vector<TKqpWriteTask::TPathLookupInfo> lookups;

            AFL_ENSURE(writeInfo.Actors.size() > settings.Indexes.size());
            for (auto& indexSettings : settings.Indexes) {
                AFL_ENSURE(!settings.IsOlap);
                // Flag for the case of UPDATE ON been processed without doing lookup,
                // so no secondary index key columns are touched.
                // In this case we must use UPDATE operation at shards for all table,
                // because we don't know if updated rows exist.
                // No lookup means that secondary key is subset of primary key.
                const bool updateOnWithoutLookup = (
                    settings.OperationType == NKikimrKqp::TKqpTableSinkSettings::MODE_UPDATE
                    && settings.LookupColumns.empty());

                writeInfo.Actors.at(indexSettings.TableId.PathId).WriteActor->Open(
                    writeCookie,
                    settings.OperationType == NKikimrKqp::TKqpTableSinkSettings::MODE_DELETE
                        ? NKikimrDataEvents::TEvWrite::TOperation::OPERATION_DELETE
                        : (updateOnWithoutLookup
                            ? NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPDATE
                            : NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT),
                    indexSettings.KeyColumns,
                    indexSettings.Columns,
                    CountLocalDefaults(
                        settings.DefaultColumns,
                        indexSettings.Columns,
                        settings.LookupColumns),
                    settings.Priority);

                const bool needAdditionalDelete = !isSubsetOfPrimaryKeyColumns(indexSettings.KeyColumns)
                    && settings.OperationType != NKikimrKqp::TKqpTableSinkSettings::MODE_DELETE
                    && settings.OperationType != NKikimrKqp::TKqpTableSinkSettings::MODE_INSERT;

                if (needAdditionalDelete) {
                    writeInfo.Actors.at(indexSettings.TableId.PathId).WriteActor->Open(
                        deleteCookie,
                        NKikimrDataEvents::TEvWrite::TOperation::OPERATION_DELETE,
                        indexSettings.KeyColumns,
                        indexSettings.KeyColumns,
                        0, // DELETE doesn't need DEFAULT values
                        settings.Priority);
                }

                writes.emplace_back(TKqpWriteTask::TPathWriteInfo{
                    .DeleteKeysIndexes = needAdditionalDelete
                                ? GetIndexes(
                                    settings.Columns,
                                    settings.LookupColumns,
                                    indexSettings.KeyColumns,
                                    /* preferAdditionalInputColumns */ true)
                                : std::vector<ui32>{},
                    .NewColumnsIndexes = GetIndexes(
                                settings.Columns,
                                settings.LookupColumns,
                                indexSettings.Columns,
                                /* preferAdditionalInputColumns */ false),
                    .OldColumnsIndexes = GetIndexes(
                                settings.Columns,
                                settings.LookupColumns,
                                indexSettings.Columns,
                                /* preferAdditionalInputColumns */ true),
                    .WriteActor = writeInfo.Actors.at(indexSettings.TableId.PathId).WriteActor,
                    .ColumnTypes = [&]() {
                        std::vector<NScheme::TTypeInfo> result(indexSettings.Columns.size());
                        for (ui32 index = 0; index < indexSettings.Columns.size(); ++index) {
                            const auto& column = indexSettings.Columns[index];
                            result[index] = NScheme::TypeInfoFromProto(
                                column.GetTypeId(), column.GetTypeInfo());
                        }
                        return result;
                    }(),
                    .NeedWriteProjection = true,
                });

                if (indexSettings.IsUniq) {
                    auto lookupInfo = LookupInfos.at(indexSettings.TableId.PathId);
                    auto lookupActor = lookupInfo.Actors.at(indexSettings.TableId.PathId).LookupActor;
                    lookups.emplace_back(TKqpWriteTask::TPathLookupInfo{
                        .KeyIndexes = GetIndexes( // inserted secondary keys
                            settings.Columns,
                            settings.LookupColumns,
                            TConstArrayRef{
                                indexSettings.KeyColumns.data(),
                                indexSettings.KeyPrefixSize},
                            /* preferAdditionalInputColumns */ false),
                        .FullKeyIndexes = GetIndexes( // full secondary table keys
                            settings.Columns,
                            settings.LookupColumns,
                            indexSettings.KeyColumns,
                            /* preferAdditionalInputColumns */ false),
                        .PrimaryInFullKeyIndexes = [&](){ // primary key in full secondary table keys
                            THashMap<TStringBuf, ui32> ColumnNameToIndex;
                            for (ui32 index = 0; index < indexSettings.KeyColumns.size(); ++index) {
                                ColumnNameToIndex[indexSettings.KeyColumns[index].GetName()] = index;
                            }
                            std::vector<ui32> result(settings.KeyColumns.size());
                            for (ui32 index = 0; index < settings.KeyColumns.size(); ++index) {
                                result[index] = ColumnNameToIndex[settings.KeyColumns[index].GetName()];
                            }
                            return result;
                        }(),
                        .OldKeyIndexes = GetIndexes( // old secondary keys
                                settings.Columns,
                                settings.LookupColumns,
                                TConstArrayRef{
                                    indexSettings.KeyColumns.data(),
                                    indexSettings.KeyPrefixSize},
                                /* preferAdditionalInputColumns */ true),
                        .Lookup = lookupActor,
                    });

                    lookupActor->SetLookupSettings(
                        token.Cookie,
                        indexSettings.KeyPrefixSize,
                        indexSettings.KeyColumns,
                        {});
                }
            }

            if (!settings.LookupColumns.empty()) {
                AFL_ENSURE(!settings.IsOlap);
                auto lookupInfo = LookupInfos.at(settings.TableId.PathId);
                auto lookupActor = lookupInfo.Actors.at(settings.TableId.PathId).LookupActor;
                lookups.emplace_back(TKqpWriteTask::TPathLookupInfo{
                    .KeyIndexes = {},
                    .FullKeyIndexes = {},
                    .PrimaryInFullKeyIndexes = {},
                    .OldKeyIndexes = {},
                    .Lookup = lookupActor,
                });

                lookupActor->SetLookupSettings(
                    token.Cookie,
                    settings.KeyColumns.size(),
                    settings.KeyColumns,
                    settings.LookupColumns);
            }

            writeInfo.Actors.at(settings.TableId.PathId).WriteActor->Open(
                token.Cookie,
                (settings.OperationType == NKikimrKqp::TKqpTableSinkSettings::MODE_UPDATE_CONDITIONAL
                    || (settings.OperationType == NKikimrKqp::TKqpTableSinkSettings::MODE_UPDATE && !settings.LookupColumns.empty()))
                    ? NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT // Avoid unnecessary reads at datashard if we have already done lookup
                    : GetOperation(settings.OperationType),
                settings.KeyColumns,
                settings.Columns,
                CountLocalDefaults(
                    settings.DefaultColumns,
                    settings.Columns,
                    settings.LookupColumns),
                settings.Priority);

            AFL_ENSURE(settings.KeyColumns.size() <= settings.Columns.size());
            writes.emplace_back(TKqpWriteTask::TPathWriteInfo{
                .DeleteKeysIndexes = {},
                .NewColumnsIndexes = GetIndexes(
                            settings.Columns,
                            settings.LookupColumns,
                            settings.Columns,
                            /* preferAdditionalInputColumns */ false),
                .OldColumnsIndexes = GetIndexes(
                            settings.Columns,
                            settings.LookupColumns,
                            settings.Columns,
                            /* preferAdditionalInputColumns */ true),
                .WriteActor = writeInfo.Actors.at(settings.TableId.PathId).WriteActor,
                .ColumnTypes = [&]() {
                    std::vector<NScheme::TTypeInfo> result(settings.Columns.size());
                    for (ui32 index = 0; index < settings.Columns.size(); ++index) {
                        const auto& column = settings.Columns[index];
                        result[index] = NScheme::TypeInfoFromProto(
                            column.GetTypeId(), column.GetTypeInfo());
                    }
                    return result;
                }(),
                .NeedWriteProjection = !settings.LookupColumns.empty(),
            });

            std::optional<TKqpWriteTask::TReturningInfo> returningInfo;
            if (!settings.ReturningColumns.empty()) {
                returningInfo.emplace();
                returningInfo->Consumer = &ReturningConsumers[token.Cookie];
                returningInfo->ColumnsIndexes = GetIndexes(
                    settings.Columns,
                    settings.LookupColumns,
                    settings.ReturningColumns,
                    /* preferAdditionalInputColumns */ false);
            }

            auto [taskIter, _] = WriteTasks.emplace(
                token.Cookie,
                TKqpWriteTask{
                    writeCookie,
                    deleteCookie,
                    settings.Priority,
                    settings.TableId.PathId,
                    settings.OperationType,
                    std::move(writes),
                    std::move(lookups),
                    returningInfo,
                    settings.KeyColumns,
                    (settings.LookupColumns.empty() || settings.DefaultColumns.empty())
                        ? std::vector<ui32>{}
                        : BuildDefaultMap(
                            settings.DefaultColumns,
                            settings.Columns,
                            settings.LookupColumns),
                    Alloc
                });

            TasksPlanner.AddTask(taskIter->second);
        } else {
            token = *ev->Get()->Token;
        }

        auto& queue = RequestQueues[token.PathId];
        queue.emplace();
        auto& message = queue.back();

        message.Token = token;
        message.From = ev->Sender;
        message.Close = ev->Get()->Close;
        message.Data = ev->Get()->Data;

        Process();
    }

    bool NeedToFlush() {
        const bool outOfMemory = GetTotalFreeSpace() <= 0;
        const bool needToFlush = outOfMemory
            || CurrentStateFunc() == &TThis::StateFlush
            || CurrentStateFunc() == &TThis::StatePrepare
            || CurrentStateFunc() == &TThis::StateCommit;
        return needToFlush;
    }

    bool NeedToFlushActor(const TKqpTableWriteActor* actor) {
        return NeedToFlush()
            && (CurrentStateFunc() != &TThis::StateFlush
                || !TxId // Flush between queries
                || actor->FlushBeforeCommit()); // Flush before commit
    }

    bool Process() {
        if (CurrentStateFunc() == &TThis::StateError || CurrentStateFunc() == &TThis::StateRollback) {
            return false;
        }

        ProcessRequestQueue();
        if (!ProcessTasks(/* forceFlush */ EnableStreamWrite && GetTotalFreeSpace() <= 0)) {
            return false;
        }
        if (!ProcessFlush()) {
            return false;
        }
        ProcessAckQueue();

        if (CurrentStateFunc() == &TThis::StateWaitTasks && WriteTasks.empty()) {
            OnAllTasksFinised();
        } else if (CurrentStateFunc() == &TThis::StateFlush) {
            bool isEmpty = true;
            ForEachWriteActor([&](const TKqpTableWriteActor* actor, const TActorId) {
                if (NeedToFlushActor(actor)) {
                    isEmpty &= actor->IsReady() && actor->IsEmpty();
                }
            });
            if (isEmpty) {
                OnFlushed();
            }
        }
        return true;
    }

    void ProcessRequestQueue() {
        for (auto& [pathId, queue] : RequestQueues) {
            while (!queue.empty()) {
                auto& message = queue.front();

                AFL_ENSURE(message.Token.PathId == pathId);
                auto& writeTask = WriteTasks.at(message.Token.Cookie);
                writeTask.Write(std::move(message.Data));
                if (message.Close) {
                    writeTask.Close();
                }

                if (!writeTask.HasReturning()) {
                    // TODO: For stream write must send AckMessage only for working tasks.
                    AckQueue.push(TAckMessage{
                        .ForwardActorId = message.From,
                        .Token = message.Token,
                        .InputDataSize = 0,
                        .OutputData = {},
                    });
                } else {
                    ReturningConsumers.at(message.Token.Cookie).PushDelayedAck(TAckMessage{
                        .ForwardActorId = message.From,
                        .Token = message.Token,
                        .InputDataSize = 0,
                        .OutputData = {},
                    });
                }

                queue.pop();
            }
        }
    }

    bool ProcessTasks(bool forceFlush) {
        TVector<ui64> finishedCookies;
        do {
            for (auto& [cookie, writeTask] : WriteTasks) {
                writeTask.Process(forceFlush);
                if (writeTask.IsError()) {
                    ReplyError(
                        NYql::NDqProto::StatusIds::PRECONDITION_FAILED,
                        NYql::TIssuesIds::KIKIMR_PRECONDITION_FAILED,
                        TStringBuilder() << writeTask.GetError(),
                        {});
                    return false;
                }
                if (writeTask.IsFinished()) {
                    TasksPlanner.ReleaseTask(writeTask);
                    finishedCookies.push_back(cookie);
                }
            }

            for (auto& [cookie, returningConsumer] : ReturningConsumers) {
                if (!returningConsumer.IsEmpty()) {
                    for (auto& ack : returningConsumer.FlushDelayedAcks()) {
                        AckQueue.push(std::move(ack));
                    }
                }
            }

            for (const auto& cookie : finishedCookies) {
                WriteTasks.erase(cookie);
                if (const auto iterReturningConsumers = ReturningConsumers.find(cookie);
                        iterReturningConsumers != ReturningConsumers.end()) {
                    AFL_ENSURE(iterReturningConsumers->second.IsEmpty());
                    if (!iterReturningConsumers->second.IsDelayedAcksEmpty()) {
                        for (auto& ack : iterReturningConsumers->second.FlushDelayedAcks()) {
                            AckQueue.push(std::move(ack));
                        }
                    }
                    ReturningConsumers.erase(iterReturningConsumers);
                }
            }
        } while (TasksPlanner.StartUnblockedTasks());

        return true;
    }

    void ProcessAckQueue() {
        while (!AckQueue.empty()) {
            const auto& item = AckQueue.front();
            if (GetTotalFreeSpace() >= item.InputDataSize) {
                auto result = std::make_unique<TEvBufferWriteResult>();
                result->Token = AckQueue.front().Token;
                result->Data = std::move(item.OutputData);
                Send(AckQueue.front().ForwardActorId, result.release());
                AckQueue.pop();
            } else {
                return;
            }
        }
    }

    bool ProcessFlush() {
        if (!EnableStreamWrite && GetTotalFreeSpace() <= 0) {
            ReplyError(
                NYql::NDqProto::StatusIds::PRECONDITION_FAILED,
                NYql::TIssuesIds::KIKIMR_PRECONDITION_FAILED,
                TStringBuilder() << "Out of buffer memory. Used " << GetTotalMemory()
                        << " bytes of " << MessageSettings.InFlightMemoryLimitPerActorBytes << " bytes.",
                {});
            return false;
        }

        if (NeedToFlush()) {
            CA_LOG_D("Flush data");

            bool flushFailed = false;
            ForEachWriteActor([&](TKqpTableWriteActor* actor, const TActorId) {
                if (!flushFailed && actor->IsReady() && NeedToFlushActor(actor)) {
                    actor->FlushBuffers();
                    if (!actor->FlushToShards()) {
                        flushFailed = true;
                    }
                }
            });

            if (flushFailed) {
                return false;
            }
        }
        return true;
    }

    bool Flush(NWilson::TTraceId traceId) {
        AFL_ENSURE(WriteTasks.empty());
        YQL_ENSURE(CurrentStateFunc() == &TThis::StateWaitTasks);
        Become(&TThis::StateFlush);

        Counters->BufferActorFlushes->Inc();
        UpdateTracingState("Flush", std::move(traceId));
        OperationStartTime = TInstant::Now();

        ForEachWriteActor([](TKqpTableWriteActor* actor, const TActorId) {
            actor->FlushBuffers();
        });

        CA_LOG_D("Start flush");
        CheckQueuesEmpty();
        return Process();
    }

    void Commit(ui64 txId, NWilson::TTraceId traceId) {
        AFL_ENSURE(WriteTasks.empty());
        ForEachWriteActor([&](TKqpTableWriteActor* actor, const TActorId) {
            actor->FlushBuffers();
        });

        if (!TxManager->NeedCommit()) {
            Rollback(std::move(traceId), /* waitForResult */ true);
        } else if (TxManager->BrokenLocks()) {
            NYql::TIssues issues;
            issues.AddIssue(*TxManager->GetLockIssue());
            ReplyError(
                NYql::NDqProto::StatusIds::ABORTED,
                std::move(issues));
            return;
        } else if (TxManager->IsSingleShard() && !TxManager->HasOlapTable() && (!WriteInfos.empty() || TxManager->HasTopics())) {
            TxManager->StartExecute();
            ImmediateCommit(std::move(traceId));
        } else {
            AFL_ENSURE(txId);
            TxId = txId;

            bool needToFlushBeforeCommit = false;
            ForEachWriteActor([&](TKqpTableWriteActor* actor, const TActorId) {
                needToFlushBeforeCommit |= actor->FlushBeforeCommit();
            });

            if (needToFlushBeforeCommit) {
                Flush(std::move(traceId));
            } else {
                TxManager->StartPrepare();
                Prepare(std::move(traceId));
            }
        }
    }

    bool Prepare(std::optional<NWilson::TTraceId> traceId) {
        UpdateTracingState("Commit", std::move(traceId));
        OperationStartTime = TInstant::Now();

        CA_LOG_D("Start prepare for distributed commit");
        AFL_ENSURE(CurrentStateFunc() == &TThis::StateWaitTasks
            || CurrentStateFunc() == &TThis::StateFlush);
        Become(&TThis::StatePrepare);

        PendingPrepareShards = TxManager->GetShardsCount();

        CheckQueuesEmpty();
        AFL_ENSURE(TxId);
        ForEachWriteActor([&](TKqpTableWriteActor* actor, const TActorId) {
            AFL_ENSURE(!actor->FlushBeforeCommit());
            actor->SetPrepare(*TxId);
        });
        Close();
        if (!Process()) {
            return false;
        }
        SendToExternalShards();
        SendToTopics(false);
        return true;
    }

    bool ImmediateCommit(NWilson::TTraceId traceId) {
        Counters->BufferActorImmediateCommits->Inc();
        UpdateTracingState("Commit", std::move(traceId));
        OperationStartTime = TInstant::Now();

        CA_LOG_D("Start immediate commit");
        YQL_ENSURE(CurrentStateFunc() == &TThis::StateWaitTasks);
        Become(&TThis::StateCommit);
        PendingCommitShards = TxManager->GetShardsCount();

        IsImmediateCommit = true;
        CheckQueuesEmpty();
        ForEachWriteActor([](TKqpTableWriteActor* actor, const TActorId) {
            actor->SetImmediateCommit();
        });
        Close();
        if (!Process()) {
            return false;
        }
        SendToTopics(true);
        return true;
    }

    void DistributedCommit() {
        Counters->BufferActorDistributedCommits->Inc();
        OperationStartTime = TInstant::Now();

        CA_LOG_D("Start distributed commit with TxId=" << *TxId);
        YQL_ENSURE(CurrentStateFunc() == &TThis::StatePrepare);
        Become(&TThis::StateCommit);
        PendingCommitShards = TxManager->GetShardsCount();
        CheckQueuesEmpty();
        ForEachWriteActor([](TKqpTableWriteActor* actor, const TActorId) {
            actor->SetDistributedCommit();
        });
        SendCommitToCoordinator();
    }

    void Rollback(NWilson::TTraceId traceId, bool waitForResult) noexcept {
        AFL_ENSURE(CurrentStateFunc() != &TThis::StateRollback);
        AFL_ENSURE(!TxManager->IsRollBack());
        Become(&TThis::StateRollback);
        try {
            Counters->BufferActorRollbacks->Inc();
            UpdateTracingState("RollBack", std::move(traceId));

            CA_LOG_D("Start rollback");
            const auto& shardsToRollback = TxManager->StartRollback();

            if (shardsToRollback.empty() && waitForResult) {
                OnRollbackFinished();
                return;
            }

            for (const ui64 shardId : shardsToRollback) {
                SendToExternalShard(shardId, /* isRollback */ true);
            }
        } catch (...) {
            CA_LOG_E("Failed to rollback transaction. Error: " << CurrentExceptionMessage() << ".");
        }
    }

    void CheckQueuesEmpty() {
        for (const auto& [_, queue] : RequestQueues) {
            AFL_ENSURE(queue.empty());
        }
    }

    void SendToExternalShards() {
        auto shards = TxManager->GetShards();

        // Exclude shards prepared by write actors
        ForEachWriteActor([&](const TKqpTableWriteActor* actor, const TActorId) {
            for (const auto& shardId : actor->GetShardsIds()) {
                shards.erase(shardId);
            }
        });

        for (const ui64 shardId : shards) {
            if (TxManager->GetLocks(shardId).empty()) {
                continue;
            }
            SendToExternalShard(shardId, /* isRollback */ false);
        }
    }

    void SendToExternalShard(const ui64 shardId, const bool isRollback) {
        auto evWrite = std::make_unique<NKikimr::NEvents::TDataEvents::TEvWrite>(isRollback
            ? NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE
            : (TxManager->IsVolatile()
                ? NKikimrDataEvents::TEvWrite::MODE_VOLATILE_PREPARE
                : NKikimrDataEvents::TEvWrite::MODE_PREPARE));
        evWrite ->SetUserSID(UserSID);

        if (isRollback) {
            FillEvWriteRollback(evWrite.get(), shardId, TxManager);
        } else {
            YQL_ENSURE(TxId);
            // BreakerQuerySpanId comes from TxManager (set by per-table write actors), not this actor
            FillEvWritePrepare(evWrite.get(), shardId, *TxId, TxManager);
            evWrite->Record.SetOverloadSubscribe(++ExternalShardIdToOverloadSeqNo[shardId]);
        }

        NDataIntegrity::LogIntegrityTrails("EvWriteTx", evWrite->Record.GetTxId(), shardId, TlsActivationContext->AsActorContext(), "BufferActor");

        const auto rollbackCookie = isRollback ? RollbackMessageCookie : 0;
        SendTime[shardId] = TInstant::Now();
        CA_LOG_D("Send EvWrite (external) to ShardID=" << shardId << ", isPrepare=" << !isRollback << ", isRollback=" << isRollback << ", TxId=" << evWrite->Record.GetTxId()
            << ", LockTxId=" << evWrite->Record.GetLockTxId() << ", LockNodeId=" << evWrite->Record.GetLockNodeId()
            << ", Locks= " << [&]() {
                TStringBuilder builder;
                for (const auto& lock : evWrite->Record.GetLocks().GetLocks()) {
                    builder << lock.ShortDebugString();
                }
                return builder;
            }()
            << ", Size=" << 0 << ", Cookie=" << rollbackCookie
            << ", OperationsCount=" << 0 << ", IsFinal=" << 1
            << ", Attempts=" << 0);

        Send(
            NKikimr::MakePipePerNodeCacheID(false),
            new TEvPipeCache::TEvForward(evWrite.release(), shardId, /* subscribe */ true),
            0,
            isRollback ? RollbackMessageCookie : 0,
            BufferWriteActorStateSpan.GetTraceId());
    }

    void SendToTopics(bool isImmediateCommit) {
        if (!TxManager->HasTopics()) {
            return;
        }

        TTopicTabletTxs topicTxs;
        TxManager->BuildTopicTxs(topicTxs);

        TMaybe<ui64> writeId;
        if (TxManager->GetTopicOperations().HasWriteId()) {
            writeId = TxManager->GetTopicOperations().GetWriteId();
        }
        bool kafkaTransaction = TxManager->GetTopicOperations().HasKafkaOperations();

        for (auto& [tabletId, t] : topicTxs) {
            auto& transaction = t.tx;

            if (!isImmediateCommit) {
                FillTopicsCommit(transaction, TxManager);
            }

            if (t.hasWrite && writeId.Defined() && !kafkaTransaction) {
                auto* w = transaction.MutableWriteId();
                w->SetNodeId(SelfId().NodeId());
                w->SetKeyId(*writeId);
            } else if (t.hasWrite && kafkaTransaction) {
                auto* w = transaction.MutableWriteId();
                w->SetKafkaTransaction(true);
                w->MutableKafkaProducerInstanceId()->SetId(TxManager->GetTopicOperations().GetKafkaProducerInstanceId().Id);
                w->MutableKafkaProducerInstanceId()->SetEpoch(TxManager->GetTopicOperations().GetKafkaProducerInstanceId().Epoch);
            }
            transaction.SetImmediate(isImmediateCommit);

            auto ev = std::make_unique<TEvPersQueue::TEvProposeTransactionBuilder>();

            ActorIdToProto(SelfId(), ev->Record.MutableSourceActor());
            ev->Record.MutableData()->Swap(&transaction);

            if (!isImmediateCommit) {
                YQL_ENSURE(TxId);
                ev->Record.SetTxId(*TxId);
            }

            SendTime[tabletId] = TInstant::Now();

            CA_LOG_D("Executing KQP transaction on topic tablet: " << tabletId
            << ", writeId: " << writeId << ", isImmediateCommit: " << isImmediateCommit);

            Send(
                MakePipePerNodeCacheID(false),
                new TEvPipeCache::TEvForward(ev.release(), tabletId, /* subscribe */ true),
                0,
                0,
                BufferWriteActorStateSpan.GetTraceId());
        }
    }

    void SendCommitToCoordinator() {
        const auto commitInfo = TxManager->GetCommitInfo();

        auto ev = MakeHolder<TEvTxProxy::TEvProposeTransaction>();

        YQL_ENSURE(commitInfo.Coordinator);
        Coordinator = commitInfo.Coordinator;
        ev->Record.SetCoordinatorID(*Coordinator);

        auto& transaction = *ev->Record.MutableTransaction();
        auto& affectedSet = *transaction.MutableAffectedSet();
        affectedSet.Reserve(commitInfo.ShardsInfo.size());

        YQL_ENSURE(TxId);
        transaction.SetTxId(*TxId);
        transaction.SetMinStep(commitInfo.MinStep);
        transaction.SetMaxStep(commitInfo.MaxStep);
        if (TxManager->IsVolatile()) {
            transaction.SetFlags(TEvTxProxy::TEvProposeTransaction::FlagVolatile);
        }

        for (const auto& shardInfo : commitInfo.ShardsInfo) {
            auto& item = *affectedSet.Add();
            item.SetTabletId(shardInfo.ShardId);
            AFL_ENSURE(shardInfo.AffectedFlags != 0);
            item.SetFlags(shardInfo.AffectedFlags);
        }

        NDataIntegrity::LogIntegrityTrails("PlannedTx", *TxId, {}, TlsActivationContext->AsActorContext(), "BufferActor");

        CA_LOG_D("Execute planned transaction, coordinator: " << *Coordinator
            << ", volitale: " << ((transaction.GetFlags() & TEvTxProxy::TEvProposeTransaction::FlagVolatile) != 0)
            << ", shards: " << affectedSet.size());
        Send(
            MakePipePerNodeCacheID(false),
            new TEvPipeCache::TEvForward(ev.Release(), *Coordinator, /* subscribe */ true),
            0,
            0,
            BufferWriteActorStateSpan.GetTraceId());
    }

    void Close() {
        ForEachWriteActor([&](TKqpTableWriteActor* actor, const TActorId) {
            actor->Close();
        });
    }

    i64 GetTotalFreeSpace() const {
        return MessageSettings.InFlightMemoryLimitPerActorBytes - GetTotalMemory();
    }

    i64 GetTotalMemory() const {
        i64 totalMemory = 0;
        ForEachWriteActor([&](const TKqpTableWriteActor* actor, const TActorId) {
            totalMemory += actor->GetMemory();
        });
        for (auto& [_, writeTask] : WriteTasks) {
            totalMemory += writeTask.GetMemory();
        }
        return totalMemory;
    }

    THashSet<ui64> GetShardsIds() const {
        THashSet<ui64> shardIds;
        ForEachWriteActor([&](const TKqpTableWriteActor* actor, const TActorId) {
            for (const auto& id : actor->GetShardsIds()) {
                shardIds.insert(id);
            }
        });

        return shardIds;
    }

    void PassAway() override {
        Counters->BufferActorsCount->Dec();
        for (auto& [_, queue] : RequestQueues) {
            while (!queue.empty()) {
                queue.pop();
            }
        }

        Clear();

        Send(MakePipePerNodeCacheID(false), new TEvPipeCache::TEvUnlink(0));
        TActorBootstrapped<TKqpBufferWriteActor>::PassAway();
    }

    void Clear() {
        ForEachLookupActor([](IKqpBufferTableLookup* actor, const TActorId) {
            actor->Terminate();
        });

        ForEachWriteActor([](TKqpTableWriteActor* actor, const TActorId) {
            actor->Terminate();
        });

        {
            Y_ABORT_UNLESS(Alloc);
            TGuard<NMiniKQL::TScopedAlloc> allocGuard(*Alloc);
            WriteInfos.clear();
            LookupInfos.clear();
            WriteTasks.clear();
            HolderFactory.reset();
            TypeEnv.reset();
        }
    }

    void Handle(TEvTxProxy::TEvProposeTransactionStatus::TPtr &ev) {
        TEvTxProxy::TEvProposeTransactionStatus* res = ev->Get();
        CA_LOG_D("Got transaction status, status: " << res->GetStatus());

        switch (res->GetStatus()) {
            case TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusAccepted:
                TxProxyMon->ClientTxStatusAccepted->Inc();
                break;
            case TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusProcessed:
                TxProxyMon->ClientTxStatusProcessed->Inc();
                break;
            case TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusConfirmed:
                TxProxyMon->ClientTxStatusConfirmed->Inc();
                break;

            case TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusPlanned:
                TxProxyMon->ClientTxStatusPlanned->Inc();
                TxPlanned = true;
                break;

            case TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusOutdated:
            case TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusDeclined:
            case TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusDeclinedNoSpace:
            case TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusRestarting:
                TxProxyMon->ClientTxStatusCoordinatorDeclined->Inc();
                ReplyError(
                    NYql::NDqProto::StatusIds::UNAVAILABLE,
                    NYql::TIssuesIds::KIKIMR_TEMPORARILY_UNAVAILABLE,
                    TStringBuilder() << "Failed to plan transaction, status: " << res->GetStatus(),
                    {});
                break;

            case TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusUnknown:
            case TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusAborted:
                TxProxyMon->ClientTxStatusCoordinatorDeclined->Inc();
                ReplyError(
                    NYql::NDqProto::StatusIds::INTERNAL_ERROR,
                    NYql::TIssuesIds::KIKIMR_INTERNAL_ERROR,
                    TStringBuilder() << "Unexpected TEvProposeTransactionStatus status: " << res->GetStatus(),
                    {});
                break;
        }
    }

    void HandlePrepare(TEvPersQueue::TEvProposeTransactionResult::TPtr& ev) {
        auto& event = ev->Get()->Record;
        const ui64 tabletId = event.GetOrigin();

        CA_LOG_D("Got ProposeTransactionResult" <<
              ", PQ tablet: " << tabletId <<
              ", status: " << NKikimrPQ::TEvProposeTransactionResult_EStatus_Name(event.GetStatus()));

        switch (event.GetStatus()) {
        case NKikimrPQ::TEvProposeTransactionResult::PREPARED:
            ProcessPreparedTopic(ev);
            return;
        default:
            HandleError(ev);
        }
    }

    void HandleCommit(TEvPersQueue::TEvProposeTransactionResult::TPtr& ev) {
        auto& event = ev->Get()->Record;
        const ui64 tabletId = event.GetOrigin();

        CA_LOG_D("Got ProposeTransactionResult" <<
              ", PQ tablet: " << tabletId <<
              ", status: " << NKikimrPQ::TEvProposeTransactionResult_EStatus_Name(event.GetStatus()));

        switch (event.GetStatus()) {
        case NKikimrPQ::TEvProposeTransactionResult::COMPLETE:
            ProcessCompletedTopic(ev);
            return;
        default:
            HandleError(ev);
        }
    }

    static TString GetPQErrorMessage(const NKikimrPQ::TEvProposeTransactionResult& event, TStringBuf default_) {
        if (event.ErrorsSize()) {
            return event.GetErrors(0).GetReason();
        }
        return {default_.begin(), default_.end()};
    }

    void HandleError(TEvPersQueue::TEvProposeTransactionResult::TPtr& ev) {
        auto& event = ev->Get()->Record;
        switch (event.GetStatus()) {
        case NKikimrPQ::TEvProposeTransactionResult::PREPARED:
            AFL_ENSURE(false);
        case NKikimrPQ::TEvProposeTransactionResult::COMPLETE:
            AFL_ENSURE(false);
        case NKikimrPQ::TEvProposeTransactionResult::ABORTED:
            CA_LOG_E("Got ABORTED ProposeTransactionResult for PQ."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << ".");
            ReplyError(
                NYql::NDqProto::StatusIds::ABORTED,
                NYql::TIssuesIds::KIKIMR_OPERATION_ABORTED,
                GetPQErrorMessage(event, "Aborted proposal status for PQ. "),
                {});
            return;
        case NKikimrPQ::TEvProposeTransactionResult::BAD_REQUEST:
            CA_LOG_E("Got BAD REQUEST ProposeTransactionResult for PQ."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << ".");
            ReplyError(
                NYql::NDqProto::StatusIds::BAD_REQUEST,
                NYql::TIssuesIds::KIKIMR_BAD_REQUEST,
                GetPQErrorMessage(event, "Bad request proposal status for PQ. "),
                {});
            return;
        case NKikimrPQ::TEvProposeTransactionResult::OVERLOADED:
            CA_LOG_E("Got OVERLOADED ProposeTransactionResult for PQ."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << ".");
            ReplyError(
                NYql::NDqProto::StatusIds::OVERLOADED,
                NYql::TIssuesIds::KIKIMR_OVERLOADED,
                GetPQErrorMessage(event, "Overloaded proposal status for PQ. "),
                {});
            return;
        case NKikimrPQ::TEvProposeTransactionResult::CANCELLED:
            CA_LOG_E("Got CANCELLED ProposeTransactionResult for PQ."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << ".");
            ReplyError(
                NYql::NDqProto::StatusIds::CANCELLED,
                NYql::TIssuesIds::KIKIMR_OPERATION_CANCELLED,
                GetPQErrorMessage(event, "Cancelled proposal status for PQ. "),
                {});
            return;
        default:
            CA_LOG_E("Got undefined ProposeTransactionResult for PQ."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << ".");
            ReplyError(
                NYql::NDqProto::StatusIds::INTERNAL_ERROR,
                NYql::TIssuesIds::KIKIMR_INTERNAL_ERROR,
                GetPQErrorMessage(event, "Undefined proposal status for PQ. "),
                {});
            return;
        }
    }

    void HandlePrepare(TEvDataShard::TEvProposeTransactionAttachResult::TPtr& ev) {
        const auto result = HandleAttachResult(TxManager, ev);
        if (!result) {
            return;
        }
        if (*result) {
            CA_LOG_D("Reattached to shard " << ev->Get()->Record.GetTabletId());
            return;
        }

        ReplyError(
            NYql::NDqProto::StatusIds::UNAVAILABLE,
            NYql::TIssuesIds::KIKIMR_TEMPORARILY_UNAVAILABLE,
            TStringBuilder()
                << "Disconnected from shard " << ev->Get()->Record.GetTabletId() << "."
                << GetPathes(ev->Get()->Record.GetTabletId()) << ".");
    }

    void HandleCommit(TEvDataShard::TEvProposeTransactionAttachResult::TPtr& ev) {
        const auto result = HandleAttachResult(TxManager, ev);
        if (!result) {
            return;
        }
        if (*result) {
            CA_LOG_D("Reattached to shard " << ev->Get()->Record.GetTabletId());
            return;
        }

        ReplyError(
            NYql::NDqProto::StatusIds::UNDETERMINED,
            NYql::TIssuesIds::KIKIMR_OPERATION_STATE_UNKNOWN,
            TStringBuilder()
                << "Disconnected from shard " << ev->Get()->Record.GetTabletId() << "."
                << GetPathes(ev->Get()->Record.GetTabletId()) << ".");
    }

    void Handle(TEvDataShard::TEvProposeTransactionRestart::TPtr& ev) {
        CA_LOG_D("Got transaction restart event from tabletId: " << ev->Get()->Record.GetTabletId());
        if (!HandleTransactionRestart(TxManager, ev)) {
            ReplyError(
                NYql::NDqProto::StatusIds::UNAVAILABLE,
                NYql::TIssuesIds::KIKIMR_TEMPORARILY_UNAVAILABLE,
                TStringBuilder()
                    << "Disconnected from shard " << ev->Get()->Record.GetTabletId() << "."
                    << GetPathes(ev->Get()->Record.GetTabletId()) << ".");
        }
    }

    void Handle(TEvPrivate::TEvReattachToShard::TPtr& ev) {
        const ui64 tabletId = ev->Get()->TabletId;
        auto& state = TxManager->GetReattachState(tabletId);

        CA_LOG_D("Reattach to shard " << tabletId);

        YQL_ENSURE(TxId);
        Send(MakePipePerNodeCacheID(false), new TEvPipeCache::TEvForward(
            new TEvDataShard::TEvProposeTransactionAttach(tabletId, *TxId),
            tabletId, /* subscribe */ true), 0, ++state.Cookie);
    }

    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
        CA_LOG_W("TEvDeliveryProblem was received from tablet: " << ev->Get()->TabletId);
        ReplyError(
            NYql::NDqProto::StatusIds::UNAVAILABLE,
            NYql::TIssuesIds::KIKIMR_TEMPORARILY_UNAVAILABLE,
            TStringBuilder() << "Kikimr cluster or one of its subsystems was unavailable. Failed to deviler message.",
            {});
    }

    void HandlePrepare(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
        CA_LOG_W("TEvDeliveryProblem was received from tablet: " << ev->Get()->TabletId);

        const auto state = TxManager->GetState(ev->Get()->TabletId);
        if (state == IKqpTransactionManager::PREPARED && TxManager->ShouldReattach(ev->Get()->TabletId, TlsActivationContext->Now())) {
            const auto& reattachState = TxManager->GetReattachState(ev->Get()->TabletId);
            CA_LOG_N("Shard " << ev->Get()->TabletId << " delivery problem (reattaching in "
                        << reattachState.ReattachInfo.Delay << ")");

            Schedule(reattachState.ReattachInfo.Delay, new TEvPrivate::TEvReattachToShard(ev->Get()->TabletId));
            return;
        }

        ReplyError(
            NYql::NDqProto::StatusIds::UNAVAILABLE,
            NYql::TIssuesIds::KIKIMR_TEMPORARILY_UNAVAILABLE,
            TStringBuilder() << "Kikimr cluster or one of its subsystems was unavailable. Failed to deviler message.",
            {});
    }

    void HandleCommit(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
        CA_LOG_W("TEvDeliveryProblem was received from tablet: " << ev->Get()->TabletId);

        if (Coordinator == ev->Get()->TabletId) {
            if (ev->Get()->NotDelivered) {
                ReplyError(
                    NYql::NDqProto::StatusIds::UNAVAILABLE,
                    NYql::TIssuesIds::KIKIMR_TEMPORARILY_UNAVAILABLE,
                    TStringBuilder() << "Kikimr cluster or one of its subsystems was unavailable. Failed to deviler message to coordinator.",
                    {});
                return;
            }

            if (TxPlanned) {
                // Already planned
                return;
            }

            ReplyError(
                    NYql::NDqProto::StatusIds::UNDETERMINED,
                    NYql::TIssuesIds::KIKIMR_OPERATION_STATE_UNKNOWN,
                    TStringBuilder() << "State of operation is unknown. Failed to deviler message to coordinator.",
                    {});
            return;
        }

        const auto state = TxManager->GetState(ev->Get()->TabletId);
        if (state == IKqpTransactionManager::EXECUTING && TxManager->ShouldReattach(ev->Get()->TabletId, TlsActivationContext->Now())) {
            const auto& reattachState = TxManager->GetReattachState(ev->Get()->TabletId);
            CA_LOG_N("Shard " << ev->Get()->TabletId << " delivery problem (reattaching in "
                        << reattachState.ReattachInfo.Delay << ")");

            Schedule(reattachState.ReattachInfo.Delay, new TEvPrivate::TEvReattachToShard(ev->Get()->TabletId));
            return;
        }

        ReplyError(
            NYql::NDqProto::StatusIds::UNDETERMINED,
            NYql::TIssuesIds::KIKIMR_OPERATION_STATE_UNKNOWN,
            TStringBuilder() << "State of operation is unknown. Failed to deviler message.",
            {});
    }

    void HandleRollback(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
        CA_LOG_W("TEvDeliveryProblem was received from tablet: " << ev->Get()->TabletId);
        if (Coordinator == ev->Get()->TabletId) {
            return;
        }

        OnRolledback(ev->Get()->TabletId);
    }

    void Handle(TEvKqpBuffer::TEvTerminate::TPtr&) {
        if (!TxManager->IsRollBack()) {
            CancelProposal();
            Rollback(BufferWriteActorSpan.GetTraceId(), /* waitForResult */ false);
        }
        PassAway();
    }

    void Handle(TEvKqpBuffer::TEvFlush::TPtr& ev) {
        ExecuterActorId = ev->Get()->ExecuterActorId;
        for (auto& [_, writeTask] : WriteTasks) {
            AFL_ENSURE(writeTask.IsClosed());
        }
        YQL_ENSURE(CurrentStateFunc() == &TThis::StateWrite);
        Become(&TThis::StateWaitTasks);
        UpdateTracingState("WaitTasks", NWilson::TTraceId(ev->TraceId));

        AfterWaitTasksState = TAfterWaitTasksState{
            .IsCommit = false,
            .TxId = 0,
            .TraceId = std::move(ev->TraceId),
        };
        Process();
    }

    void Handle(TEvKqpBuffer::TEvCommit::TPtr& ev) {
        ExecuterActorId = ev->Get()->ExecuterActorId;
        for (auto& [_, writeTask] : WriteTasks) {
            AFL_ENSURE(writeTask.IsClosed());
        }
        YQL_ENSURE(CurrentStateFunc() == &TThis::StateWrite);
        Become(&TThis::StateWaitTasks);
        UpdateTracingState("WaitTasks", NWilson::TTraceId(ev->TraceId));

        AfterWaitTasksState = TAfterWaitTasksState{
            .IsCommit = true,
            .TxId = ev->Get()->TxId,
            .TraceId = std::move(ev->TraceId),
        };
        Process();
    }

    void Handle(TEvKqpBuffer::TEvRollback::TPtr& ev) {
        ExecuterActorId = ev->Get()->ExecuterActorId;
        Rollback(std::move(ev->TraceId), /* waitForResult */ true);
    }

    void OnAllTasksFinised() {
        AFL_ENSURE(AfterWaitTasksState);
        auto afterWaitTasksState = std::move(*AfterWaitTasksState);
        AfterWaitTasksState = std::nullopt;

        if (afterWaitTasksState.IsCommit) {
            Commit(afterWaitTasksState.TxId, std::move(afterWaitTasksState.TraceId));
        } else {
            Flush(std::move(afterWaitTasksState.TraceId));
        }
    }

    void HandlePrepare(NKikimr::NEvents::TDataEvents::TEvWriteResult::TPtr& ev) {
        CA_LOG_D("Recv EvWriteResult (external) from ShardID=" << ev->Get()->Record.GetOrigin()
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

        TxManager->AddParticipantNode(ev->Sender.NodeId());

        switch (ev->Get()->GetStatus()) {
        case NKikimrDataEvents::TEvWriteResult::STATUS_PREPARED: {
            ProcessWritePreparedShard(ev);
            return;
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_DISK_GROUP_OUT_OF_SPACE:
        case NKikimrDataEvents::TEvWriteResult::STATUS_OVERLOADED: {
            if (ev->Get()->Record.HasOverloadSubscribed()) {
                CA_LOG_D("Shard " << ev->Get()->Record.GetOrigin() << " is overloaded. Waiting.");
                return;
            }
        }
        default:
            if (HandleDeferredLocksBrokenOnPrepare(&ev->Get()->Record)) return;
            HandleError(ev);
        }
    }

    void HandleCommit(NKikimr::NEvents::TDataEvents::TEvWriteResult::TPtr& ev) {
        CA_LOG_D("Recv EvWriteResult (external) from ShardID=" << ev->Get()->Record.GetOrigin()
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

        TxManager->AddParticipantNode(ev->Sender.NodeId());

        switch (ev->Get()->GetStatus()) {
        case NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED: {
            ProcessWriteCompletedShard(ev);
            return;
        }
        default:
            if (HandleDeferredLocksBrokenOnCommit(ev->Get()->Record)) return;
            HandleError(ev);
        }
    }

    void HandleRollback(NKikimr::NEvents::TDataEvents::TEvWriteResult::TPtr& ev) {
        CA_LOG_D("Recv EvWriteResult (external) from ShardID=" << ev->Get()->Record.GetOrigin()
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

        TxManager->AddParticipantNode(ev->Sender.NodeId());

        if (RollbackMessageCookie == ev->Cookie
                || ev->Get()->GetStatus() != NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED) {
            OnRolledback(ev->Get()->Record.GetOrigin());
        }
    }

    void OnOverloadReady(const ui64 shardId, const ui64 seqNo) {
        if (seqNo == ExternalShardIdToOverloadSeqNo.at(shardId)) {
            CA_LOG_D("Retry Overloaded ShardID=" << shardId);
            SendToExternalShard(shardId, false);
        }
    }

    void HandlePrepare(TEvDataShard::TEvOverloadReady::TPtr& ev) {
        auto& record = ev->Get()->Record;
        const ui64 shardId = record.GetTabletID();
        const ui64 seqNo = record.GetSeqNo();

        OnOverloadReady(shardId, seqNo);
    }

    void HandlePrepare(TEvColumnShard::TEvOverloadReady::TPtr& ev) {
        auto& record = ev->Get()->Record;
        const ui64 shardId = record.GetTabletID();
        const ui64 seqNo = record.GetSeqNo();

        OnOverloadReady(shardId, seqNo);
    }

    void HandleError(NKikimr::NEvents::TDataEvents::TEvWriteResult::TPtr& ev) {
        auto getIssues = [&ev]() {
            NYql::TIssues issues;
            NYql::IssuesFromMessage(ev->Get()->Record.GetIssues(), issues);
            return issues;
        };

        switch (ev->Get()->GetStatus()) {
        case NKikimrDataEvents::TEvWriteResult::STATUS_UNSPECIFIED: {
            CA_LOG_E("Got UNSPECIFIED for tables."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << "."
                    << getIssues().ToOneLineString());
            TxManager->SetError(ev->Get()->Record.GetOrigin());
            ReplyError(
                NYql::NDqProto::StatusIds::UNSPECIFIED,
                NYql::TIssuesIds::DEFAULT_ERROR,
                TStringBuilder() << "Unspecified error. " << GetPathes(ev->Get()->Record.GetOrigin()) << ". "
                    << getIssues().ToOneLineString(),
                getIssues());
            return;
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_PREPARED:
        case NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED:
            AFL_ENSURE(false);
        case NKikimrDataEvents::TEvWriteResult::STATUS_ABORTED: {
            CA_LOG_E("Got ABORTED for tables."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << "."
                    << getIssues().ToOneLineString());
            TxManager->SetError(ev->Get()->Record.GetOrigin());
            ReplyError(
                NYql::NDqProto::StatusIds::ABORTED,
                NYql::TIssuesIds::KIKIMR_OPERATION_ABORTED,
                TStringBuilder() << "Operation aborted.",
                getIssues());
            return;
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_WRONG_SHARD_STATE: {
            CA_LOG_E("Got WRONG SHARD STATE for tables."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << "."
                    << getIssues().ToOneLineString());
            TxManager->SetError(ev->Get()->Record.GetOrigin());
            ReplyError(
                NYql::NDqProto::StatusIds::UNAVAILABLE,
                NYql::TIssuesIds::KIKIMR_TEMPORARILY_UNAVAILABLE,
                TStringBuilder() << "Wrong shard state. " << GetPathes(ev->Get()->Record.GetOrigin()) << ".",
                getIssues());
            return;
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_INTERNAL_ERROR: {
            CA_LOG_E("Got INTERNAL ERROR for tables."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << "."
                    << getIssues().ToOneLineString());
            TxManager->SetError(ev->Get()->Record.GetOrigin());
            ReplyError(
                NYql::NDqProto::StatusIds::INTERNAL_ERROR,
                NYql::TIssuesIds::KIKIMR_INTERNAL_ERROR,
                TStringBuilder() << "Internal error while executing transaction.",
                getIssues());
            return;
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_DATABASE_DISK_SPACE_QUOTA_EXCEEDED: {
            CA_LOG_E("Got DATABASE_DISK_SPACE_QUOTA_EXCEEDED for tables."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << "."
                    << getIssues().ToOneLineString());
            TxManager->SetError(ev->Get()->Record.GetOrigin());
            ReplyError(
                NYql::NDqProto::StatusIds::UNAVAILABLE,
                NYql::TIssuesIds::KIKIMR_DATABASE_DISK_SPACE_QUOTA_EXCEEDED,
                TStringBuilder() << "Disk space exhausted. " << GetPathes(ev->Get()->Record.GetOrigin()) << ".",
                getIssues());
            return;
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_DISK_GROUP_OUT_OF_SPACE: {
            CA_LOG_W("Got DISK_GROUP_OUT_OF_SPACE for tables."
                << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                << " Sink=" << this->SelfId() << "."
                << " Ignored this error."
                << getIssues().ToOneLineString());
            TxManager->SetError(ev->Get()->Record.GetOrigin());
            ReplyError(
                NYql::NDqProto::StatusIds::UNAVAILABLE,
                NYql::TIssuesIds::KIKIMR_DISK_GROUP_OUT_OF_SPACE,
                TStringBuilder() << "Tablet " << ev->Get()->Record.GetOrigin() << " is out of space. "
                    << GetPathes(ev->Get()->Record.GetOrigin()) << ".",
                getIssues());
            return;
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_OVERLOADED: {
            CA_LOG_W("Got OVERLOADED for tables."
                << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                << " Sink=" << this->SelfId() << "."
                << " Ignored this error."
                << getIssues().ToOneLineString());
            TxManager->SetError(ev->Get()->Record.GetOrigin());
            ReplyError(
                NYql::NDqProto::StatusIds::OVERLOADED,
                NYql::TIssuesIds::KIKIMR_OVERLOADED,
                TStringBuilder() << "Kikimr cluster or one of its subsystems is overloaded."
                    << " Tablet " << ev->Get()->Record.GetOrigin() << " is overloaded."
                    << " " << GetPathes(ev->Get()->Record.GetOrigin()) << ".",
                getIssues());
            return;
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_CANCELLED: {
            CA_LOG_E("Got CANCELLED for tables."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << "."
                    << getIssues().ToOneLineString());
            TxManager->SetError(ev->Get()->Record.GetOrigin());
            ReplyError(
                NYql::NDqProto::StatusIds::CANCELLED,
                NYql::TIssuesIds::KIKIMR_OPERATION_CANCELLED,
                TStringBuilder() << "Operation cancelled.",
                getIssues());
            return;
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST: {
            CA_LOG_E("Got BAD REQUEST for tables."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << "."
                    << getIssues().ToOneLineString());
            TxManager->SetError(ev->Get()->Record.GetOrigin());
            ReplyError(
                NYql::NDqProto::StatusIds::BAD_REQUEST,
                NYql::TIssuesIds::KIKIMR_BAD_REQUEST,
                TStringBuilder() << "Bad request. " << GetPathes(ev->Get()->Record.GetOrigin()) << ".",
                getIssues());
            return;
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_SCHEME_CHANGED: {
            CA_LOG_E("Got SCHEME CHANGED for table."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << "."
                    << getIssues().ToOneLineString());
            TxManager->SetError(ev->Get()->Record.GetOrigin());
            ReplyError(
                NYql::NDqProto::StatusIds::SCHEME_ERROR,
                NYql::TIssuesIds::KIKIMR_SCHEME_MISMATCH,
                TStringBuilder() << "Scheme changed. " << GetPathes(ev->Get()->Record.GetOrigin()) << ".",
                getIssues());
            return;
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_LOCKS_BROKEN: {
            CA_LOG_E("Got LOCKS BROKEN for table."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << "."
                    << getIssues().ToOneLineString());
            if (CurrentStateFunc() == &TThis::StateCommit && PendingCommitShards > 0) {
                --PendingCommitShards;
            }
            CollectTliStats(ev->Get()->Record);
            TxManager->BreakLock(ev->Get()->Record.GetOrigin());
            YQL_ENSURE(TxManager->BrokenLocks());
            SetVictimQuerySpanIdFromBrokenLocks(ev->Get()->Record.GetOrigin(), ev->Get()->Record.GetTxLocks(), TxManager);
            if (TryDeferLocksBrokenError(ev->Get()->Record.GetOrigin(), MakeLockIssues(TxManager, getIssues()))) {
                return;
            }
            TxManager->SetError(ev->Get()->Record.GetOrigin());
            ReplyError(NYql::NDqProto::StatusIds::ABORTED, MakeLockIssues(TxManager, getIssues()));
            return;
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_CONSTRAINT_VIOLATION: {
            CA_LOG_E("Got CONSTRAINT VIOLATION for table."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << "."
                    << getIssues().ToOneLineString());
            TxManager->SetError(ev->Get()->Record.GetOrigin());
            ReplyError(
                NYql::NDqProto::StatusIds::PRECONDITION_FAILED,
                NYql::TIssuesIds::KIKIMR_CONSTRAINT_VIOLATION,
                TStringBuilder() << "Constraint violated. " << GetPathes(ev->Get()->Record.GetOrigin()) << ".",
                getIssues());
            return;
        }
        }
    }

    void OnMessageReceived(const ui64 shardId) {
        if (auto it = SendTime.find(shardId); it != std::end(SendTime)) {
            Counters->WriteActorWritesLatencyHistogram->Collect((TInstant::Now() - it->second).MicroSeconds());
            SendTime.erase(it);
        }
    }

    void OnOperationFinished(NMonitoring::THistogramPtr latencyHistogramUs) {
        latencyHistogramUs->Collect((TInstant::Now() - OperationStartTime).MicroSeconds());
    }

    void ProcessPreparedTopic(TEvPersQueue::TEvProposeTransactionResult::TPtr& ev) {
        OnMessageReceived(ev->Get()->Record.GetOrigin());
        CA_LOG_D("Got propose prepared result TxId=" << ev->Get()->Record.GetTxId()
            << ", TabletId=" << ev->Get()->Record.GetOrigin()
            << ", Cookie=" << ev->Cookie);

        const auto& record = ev->Get()->Record;
        IKqpTransactionManager::TPrepareResult preparedInfo;
        preparedInfo.ShardId = record.GetOrigin();
        preparedInfo.MinStep = record.GetMinStep();
        preparedInfo.MaxStep = record.GetMaxStep();

        preparedInfo.Coordinator = 0;
        if (record.DomainCoordinatorsSize()) {
            auto domainCoordinators = TCoordinators(TVector<ui64>(record.GetDomainCoordinators().begin(),
                                                                  record.GetDomainCoordinators().end()));
            preparedInfo.Coordinator = domainCoordinators.Select(*TxId);
        }

        OnPrepared(std::move(preparedInfo), 0);
    }

    void ProcessCompletedTopic(TEvPersQueue::TEvProposeTransactionResult::TPtr& ev) {
        NKikimrPQ::TEvProposeTransactionResult& event = ev->Get()->Record;

        OnMessageReceived(event.GetOrigin());
        CA_LOG_D("Got propose completed result" <<
              ", topic tablet: " << event.GetOrigin() <<
              ", status: " << NKikimrPQ::TEvProposeTransactionResult_EStatus_Name(event.GetStatus()));

        OnCommitted(event.GetOrigin(), 0);
    }

    void ProcessWritePreparedShard(NKikimr::NEvents::TDataEvents::TEvWriteResult::TPtr& ev) {
        OnMessageReceived(ev->Get()->Record.GetOrigin());
        CA_LOG_D("Got prepared result TxId=" << ev->Get()->Record.GetTxId()
            << ", TabletId=" << ev->Get()->Record.GetOrigin()
            << ", Cookie=" << ev->Cookie);

        CollectTliStats(ev->Get()->Record);

        const auto& record = ev->Get()->Record;
        IKqpTransactionManager::TPrepareResult preparedInfo;
        preparedInfo.ShardId = record.GetOrigin();
        preparedInfo.MinStep = record.GetMinStep();
        preparedInfo.MaxStep = record.GetMaxStep();

        preparedInfo.Coordinator = 0;
        if (record.DomainCoordinatorsSize()) {
            auto domainCoordinators = TCoordinators(TVector<ui64>(record.GetDomainCoordinators().begin(),
                                                                  record.GetDomainCoordinators().end()));
            preparedInfo.Coordinator = domainCoordinators.Select(*TxId);
        }

        OnPrepared(std::move(preparedInfo), 0);
    }

    void ProcessWriteCompletedShard(NKikimr::NEvents::TDataEvents::TEvWriteResult::TPtr& ev) {
        OnMessageReceived(ev->Get()->Record.GetOrigin());
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

        CollectTliStats(ev->Get()->Record);

        OnCommitted(ev->Get()->Record.GetOrigin(), 0);
    }

    void OnReady() override {
        Process();
    }

    void OnPrepared(IKqpTransactionManager::TPrepareResult&& preparedInfo, ui64) override {
        if (HandleDeferredLocksBrokenOnPrepare()) return;
        if (!preparedInfo.Coordinator || (TxManager->GetCoordinator() && preparedInfo.Coordinator != TxManager->GetCoordinator())) {
            CA_LOG_E("Handle TEvWriteResult: unable to select coordinator. Tx canceled, actorId: " << SelfId()
                << ", previously selected coordinator: " << TxManager->GetCoordinator()
                << ", coordinator selected at propose result: " << preparedInfo.Coordinator);

            TxProxyMon->TxResultAborted->Inc();
            ReplyError(NYql::NDqProto::StatusIds::CANCELLED,
                NKikimrIssues::TIssuesIds::TX_DECLINED_IMPLICIT_COORDINATOR,
                "Unable to choose coordinator.");
            return;
        }
        if (TxManager->ConsumePrepareTransactionResult(std::move(preparedInfo))) {
            OnOperationFinished(Counters->BufferActorPrepareLatencyHistogram);
            TxManager->StartExecute();
            AFL_ENSURE(GetTotalMemory() == 0);
            DistributedCommit();
            return;
        }
        Process();
    }

    void OnCommitted(ui64 shardId, ui64) override {
        if (PendingCommitShards > 0) {
            --PendingCommitShards;
        }
        if (TxManager->ConsumeCommitResult(shardId)) {
            if (FlushDeferredLocksBrokenIfPending()) return;
            CA_LOG_D("Committed TxId=" << TxId.value_or(0));
            OnOperationFinished(Counters->BufferActorCommitLatencyHistogram);
            Send<ESendingType::Tail>(ExecuterActorId, new TEvKqpBuffer::TEvResult{
                BuildStats()
            });
            ExecuterActorId = {};
            AFL_ENSURE(GetTotalMemory() == 0);
            PassAway();
            return;
        }
    }

    void OnRolledback(ui64 shardId) {
        if (TxManager->ConsumeRollbackResult(shardId)) {
            OnRollbackFinished();
        }
    }

    void OnRollbackFinished() {
        CA_LOG_D("RolledBack TxId=" << TxId.value_or(0));
        OnOperationFinished(Counters->BufferActorRollbackLatencyHistogram);
        Send<ESendingType::Tail>(ExecuterActorId, new TEvKqpBuffer::TEvResult{
            BuildStats()
        });
        ExecuterActorId = {};
        PassAway();
    }

    void OnMessageAcknowledged(ui64) override {
        Process();
    }

    void OnFlushed() {
        YQL_ENSURE(CurrentStateFunc() == &TThis::StateFlush);
        UpdateTracingState("Write", BufferWriteActorSpan.GetTraceId());
        OnOperationFinished(Counters->BufferActorFlushLatencyHistogram);

        ForEachWriteActor([&](TKqpTableWriteActor* actor, const TActorId) {
            AFL_ENSURE(TxId || actor->IsEmpty());
            if (actor->IsEmpty()) {
                actor->CleanupClosedTokens();
            }
            if (!TxId) {
                actor->Unlink();
            }

            AFL_ENSURE(!actor->FlushBeforeCommit());
        });

        if (TxId) {
            TxManager->StartPrepare();
            Prepare(std::nullopt);
            return;
        }
        Become(&TKqpBufferWriteActor::StateWrite);

        Send<ESendingType::Tail>(ExecuterActorId, new TEvKqpBuffer::TEvResult{
            BuildStats()
        });
        ExecuterActorId = {};
        AFL_ENSURE(GetTotalMemory() == 0);
    }

    void OnLookupTaskFinished() override {
        Process();
    }

    // --- TLI (Transaction Lineage Information) helpers ---
    // Collect lock-breaking stats from a shard response for TLI attribution.
    void CollectTliStats(const NKikimrDataEvents::TEvWriteResult& record) {
        if (record.HasTxStats()) {
            const auto& txStats = record.GetTxStats();
            LocksBrokenAsBreaker += txStats.GetLocksBrokenAsBreaker();
            LocksBrokenAsVictim += txStats.GetLocksBrokenAsVictim();
            for (ui64 id : txStats.GetBreakerQuerySpanIds()) {
                BreakerQuerySpanIds.push_back(id);
            }
            for (size_t i = 0; i < static_cast<size_t>(txStats.DeferredBreakerQuerySpanIdsSize()); ++i) {
                DeferredBreakerQuerySpanIds.push_back(txStats.GetDeferredBreakerQuerySpanIds(i));
                DeferredBreakerNodeIds.push_back(
                    i < static_cast<size_t>(txStats.DeferredBreakerNodeIdsSize()) ? txStats.GetDeferredBreakerNodeIds(i) : 0u);
            }
        }
    }

    // Defer a LOCKS_BROKEN error during distributed prepare/commit so remaining
    // shards can respond with their TxStats (breaker TLI info) before the error
    // is sent to the session actor.  Returns true if the error was deferred.
    bool TryDeferLocksBrokenError(ui64 shardId, NYql::TIssues&& issues) {
        if (IsImmediateCommit || PendingLocksBrokenError) {
            return false;
        }
        if (!IS_INFO_LOG_ENABLED(NKikimrServices::TLI)) {
            return false;
        }
        PendingLocksBrokenError.emplace(NYql::NDqProto::StatusIds::ABORTED, std::move(issues));
        if (CurrentStateFunc() == &TThis::StateCommit) {
            if (TxManager->ConsumeCommitResult(shardId)) {
                FlushPendingLocksBrokenError();
            }
            return true;
        }
        if (CurrentStateFunc() == &TThis::StatePrepare) {
            if (PendingPrepareShards == 0) {
                FlushPendingLocksBrokenError();
            }
            return true;
        }
        PendingLocksBrokenError.reset();
        return false;
    }

    // Account for a prepare-phase shard response and, when a deferred
    // LOCKS_BROKEN error exists, check whether all shards have responded.
    // The decrement is unconditional so that PREPARED responses arriving
    // before the first LOCKS_BROKEN are counted correctly (otherwise the
    // counter would never reach 0 and the actor would hang).
    bool HandleDeferredLocksBrokenOnPrepare(const NKikimrDataEvents::TEvWriteResult* record = nullptr) {
        if (PendingPrepareShards > 0) {
            --PendingPrepareShards;
        }
        if (!PendingLocksBrokenError) return false;
        if (record) CollectTliStats(*record);
        if (PendingPrepareShards == 0) {
            FlushPendingLocksBrokenError();
        }
        return true;
    }

    bool HandleDeferredLocksBrokenOnCommit(const NKikimrDataEvents::TEvWriteResult& record) {
        if (!PendingLocksBrokenError) return false;
        if (PendingCommitShards > 0) {
            --PendingCommitShards;
        }
        CollectTliStats(record);
        if (TxManager->ConsumeCommitResult(record.GetOrigin())) {
            FlushPendingLocksBrokenError();
        } else if (PendingCommitShards == 0) {
            FlushPendingLocksBrokenError();
        }
        return true;
    }

    // Flush a deferred LOCKS_BROKEN error if one is pending.
    bool FlushDeferredLocksBrokenIfPending() {
        if (!PendingLocksBrokenError) return false;
        FlushPendingLocksBrokenError();
        return true;
    }

    void FlushPendingLocksBrokenError() {
        Y_ABORT_UNLESS(PendingLocksBrokenError);
        auto error = std::move(*PendingLocksBrokenError);
        PendingLocksBrokenError.reset();
        PendingPrepareShards = 0;
        PendingCommitShards = 0;
        ReplyErrorImpl(error.first, std::move(error.second));
    }

    // Common accounting for non-success shard replies while a deferred
    // LOCKS_BROKEN error may already be pending.
    bool HandlePendingLocksBrokenErrorOnShardFailure(std::optional<ui64> commitShardId = std::nullopt) {
        if (CurrentStateFunc() == &TThis::StateCommit && PendingCommitShards > 0) {
            --PendingCommitShards;
        }
        if (!PendingLocksBrokenError) {
            return false;
        }

        if (CurrentStateFunc() == &TThis::StateCommit) {
            const bool commitConsumed = commitShardId && TxManager->ConsumeCommitResult(*commitShardId);
            if (commitConsumed || PendingCommitShards == 0) {
                FlushPendingLocksBrokenError();
            }
        } else if (CurrentStateFunc() == &TThis::StatePrepare) {
            if (PendingPrepareShards > 0) {
                --PendingPrepareShards;
            }
            if (PendingPrepareShards == 0) {
                FlushPendingLocksBrokenError();
            }
        }

        return true;
    }
    // --- End TLI helpers ---

    void OnLocksBrokenError(ui64 shardId, NYql::NDqProto::StatusIds::StatusCode statusCode, NYql::TIssues&& issues) override {
        if (HandlePendingLocksBrokenErrorOnShardFailure(shardId)) {
            return;
        }
        if (CurrentStateFunc() == &TThis::StatePrepare && PendingPrepareShards > 0) {
            --PendingPrepareShards;
        }
        if (TryDeferLocksBrokenError(shardId, std::move(issues))) return;
        TxManager->SetError(shardId);
        ReplyError(statusCode, std::move(issues));
    }

    void OnError(NYql::NDqProto::StatusIds::StatusCode statusCode, NYql::EYqlIssueCode id, const TString& message, const NYql::TIssues& subIssues) override {
        if (HandlePendingLocksBrokenErrorOnShardFailure()) {
            return;
        }
        ReplyError(statusCode, id, message, subIssues);
    }

    void OnError(NYql::NDqProto::StatusIds::StatusCode statusCode, NYql::TIssues&& issues) override {
        if (HandlePendingLocksBrokenErrorOnShardFailure()) {
            return;
        }
        ReplyError(statusCode, std::move(issues));
    }

    void OnLookupError(
            NYql::NDqProto::StatusIds::StatusCode statusCode,
            NYql::EYqlIssueCode id,
            const TString& message,
            const NYql::TIssues& subIssues) override {
        ReplyError(statusCode, id, message, subIssues);
    }

    void ReplyError(NYql::NDqProto::StatusIds::StatusCode statusCode, auto id, const TString& message, const NYql::TIssues& subIssues = {}) {
        BufferWriteActorStateSpan.EndError(message);

        NYql::TIssue issue(message);
        SetIssueCode(id, issue);
        for (const auto& i : subIssues) {
            issue.AddSubIssue(MakeIntrusive<NYql::TIssue>(i));
        }

        NYql::TIssues issues;
        issues.AddIssue(std::move(issue));

        ReplyErrorImpl(statusCode, std::move(issues));
    }

    void ReplyError(NYql::NDqProto::StatusIds::StatusCode statusCode, NYql::TIssues&& issues) {
        BufferWriteActorStateSpan.EndError(issues.ToOneLineString());
        ReplyErrorImpl(statusCode, std::move(issues));
    }

    void UpdateTracingState(const char* name, std::optional<NWilson::TTraceId> traceId) {
        if (!traceId) {
            return;
        }
        if (BufferWriteActorStateSpan) {
            BufferWriteActorStateSpan.EndOk();
        }
        BufferWriteActorStateSpan = NWilson::TSpan(TWilsonKqp::BufferWriteActorState, std::move(*traceId),
            name, NWilson::EFlags::AUTO_END);
        if (BufferWriteActorStateSpan.GetTraceId() != BufferWriteActorSpan.GetTraceId()) {
            BufferWriteActorStateSpan.Link(BufferWriteActorSpan.GetTraceId());
        }
        ForEachWriteActor([&](TKqpTableWriteActor* actor, const TActorId) {
            actor->SetParentTraceId(BufferWriteActorStateSpan.GetTraceId());
        });
    }

    void ReplyErrorImpl(NYql::NDqProto::StatusIds::StatusCode statusCode, NYql::TIssues&& issues) {
        CA_LOG_E("statusCode=" << NYql::NDqProto::StatusIds_StatusCode_Name(statusCode) << ". Issue=" << issues.ToString() << ". sessionActorId=" << SessionActorId << ".");

        TxManager->SetError();
        CancelProposal();
        Become(&TKqpBufferWriteActor::StateError);

        Send<ESendingType::Tail>(SessionActorId, new TEvKqpBuffer::TEvError{
            statusCode,
            std::move(issues),
            BuildStats()
        });

        Clear();
    }

    TString GetPathes(ui64 shardId) const {
        const auto tableInfo = TxManager->GetShardTableInfo(shardId);
        TStringBuilder builder;
        for (const auto& path : tableInfo.Pathes) {
            if (!builder.empty()) {
                builder << ", ";
            }
            builder << "`" << path << "`";
        }
        return (tableInfo.Pathes.size() == 1 ? "Table: " : "Tables: ")  + builder;
    }

    NYql::NDqProto::TDqTaskStats BuildStats() {
        NYql::NDqProto::TDqTaskStats result;
        ForEachWriteActor([&](TKqpTableWriteActor* actor, const TActorId) {
            actor->FillStats(&result);
        });
        ForEachLookupActor([&](IKqpBufferTableLookup* actor, const TActorId) {
            actor->FillStats(&result);
        });
        TKqpTableWriterStatistics::AddLockStats(&result, LocksBrokenAsBreaker, LocksBrokenAsVictim, BreakerQuerySpanIds,
                                                DeferredBreakerQuerySpanIds, DeferredBreakerNodeIds);
        return result;
    }

    void CancelProposal() noexcept {
        try {
            if (!TxId || !(CurrentStateFunc() == &TThis::StatePrepare || CurrentStateFunc() == &TThis::StateCommit)) {
                return;
            }
            for (const auto& shardId : TxManager->GetShards()) {
                const auto state = TxManager->GetState(shardId);

                if (state == IKqpTransactionManager::EShardState::PREPARING
                        || state == IKqpTransactionManager::EShardState::PREPARED
                        || (state == IKqpTransactionManager::EShardState::EXECUTING && IsImmediateCommit)) {
                    TxManager->SetError(shardId);
                    Send(MakePipePerNodeCacheID(false), new TEvPipeCache::TEvForward(
                        new TEvDataShard::TEvCancelTransactionProposal(*TxId), shardId, false));
                }
            }
        } catch (...) {
            CA_LOG_E("Failed to cancel transaction proposals. Error: " << CurrentExceptionMessage() << ".");
        }
    }

    void ForEachWriteActor(std::function<void(TKqpTableWriteActor*, const TActorId)>&& func) {
        for (auto& [_, writeInfo] : WriteInfos) {
            for (auto& [_, actorInfo] : writeInfo.Actors) {
                func(actorInfo.WriteActor, actorInfo.Id);
            }
        }
    }

    void ForEachWriteActor(std::function<void(const TKqpTableWriteActor*, const TActorId)>&& func) const {
        for (const auto& [_, writeInfo] : WriteInfos) {
            for (const auto& [_, actorInfo] : writeInfo.Actors) {
                func(actorInfo.WriteActor, actorInfo.Id);
            }
        }
    }

    void ForEachLookupActor(std::function<void(IKqpBufferTableLookup*, const TActorId)>&& func) {
        for (auto& [_, lookupInfo] : LookupInfos) {
            for (auto& [_, actorInfo] : lookupInfo.Actors) {
                func(actorInfo.LookupActor, actorInfo.Id);
            }
        }
    }

    void ForEachLookupActor(std::function<void(const IKqpBufferTableLookup*, const TActorId)>&& func) const {
        for (auto& [_, lookupInfo] : LookupInfos) {
            for (auto& [_, actorInfo] : lookupInfo.Actors) {
                func(actorInfo.LookupActor, actorInfo.Id);
            }
        }
    }

private:
    TString LogPrefix;
    const TActorId SessionActorId;
    TWriteActorSettings MessageSettings;

    TActorId ExecuterActorId;
    IKqpTransactionManagerPtr TxManager;

    std::optional<ui64> TxId;
    ui64 LockTxId = 0;
    ui64 LockNodeId = 0;
    bool InconsistentTx = false;
    bool EnableStreamWrite = true;

    bool IsImmediateCommit = false;
    bool TxPlanned = false;
    std::optional<ui64> Coordinator;

    ui64 LocksBrokenAsBreaker = 0;
    ui64 LocksBrokenAsVictim = 0;
    TVector<ui64> BreakerQuerySpanIds;
    TVector<ui64> DeferredBreakerQuerySpanIds;
    TVector<ui32> DeferredBreakerNodeIds;

    // Deferred error for STATUS_LOCKS_BROKEN during distributed prepare/commit.
    // Allows remaining shards to respond (with breaker TLI stats) before
    // sending the error to the session actor.
    std::optional<std::pair<NYql::NDqProto::StatusIds::StatusCode, NYql::TIssues>> PendingLocksBrokenError;

    // Number of shard responses still pending during prepare phase.
    // Used together with PendingLocksBrokenError to know when all shards
    // have responded and the deferred error can be flushed.
    ui64 PendingPrepareShards = 0;
    ui64 PendingCommitShards = 0;

    std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> Alloc;
    std::shared_ptr<NMiniKQL::TTypeEnvironment> TypeEnv;
    NKikimr::NMiniKQL::TMemoryUsageInfo MemInfo;
    std::shared_ptr<NMiniKQL::THolderFactory> HolderFactory;

    struct TAckMessage {
        TActorId ForwardActorId;
        TWriteToken Token;
        i64 InputDataSize;
        std::vector<IDataBatchPtr> OutputData;
    };

    std::queue<TAckMessage> AckQueue;

    struct TWriteInfo {
        struct TActorInfo {
            TKqpTableWriteActor* WriteActor = nullptr;
            TActorId Id;
        };

        THashMap<TPathId, TActorInfo> Actors;
    };

    struct TLookupInfo {
        struct TActorInfo {
            IKqpBufferTableLookup* LookupActor = nullptr;
            TActorId Id;
        };

        THashMap<TPathId, TActorInfo> Actors;
    };

    class TReturningConsumer : public TKqpWriteTask::IKqpReturningConsumer  {
    public:
        void Consume(IDataBatchPtr data) override {
            Data.emplace_back(std::move(data));
        }

        void PushDelayedAck(TAckMessage message) {
            AFL_ENSURE(Acks.empty()); // At current time fwd<->buffer inflight = 1
            Acks.push_back(std::move(message));
        }

        std::vector<TAckMessage> FlushDelayedAcks() {
            AFL_ENSURE(!Acks.empty());

            for (auto& data : Acks.back().OutputData) {
                if (data) {
                    data->DetachAlloc();
                }
            }

            std::swap(Acks.back().OutputData, Data);
            std::vector<TAckMessage> result;
            std::swap(result, Acks);

            AFL_ENSURE(IsEmpty());
            return result;
        }

        bool IsEmpty() const {
            return Data.empty();
        }

        bool IsDelayedAcksEmpty() const {
            return Acks.empty();
        }

    private:
        std::vector<IDataBatchPtr> Data;
        std::vector<TAckMessage> Acks;
    };

    THashMap<TPathId, TWriteInfo> WriteInfos;
    THashMap<TPathId, TLookupInfo> LookupInfos;
    THashMap<ui64, TReturningConsumer> ReturningConsumers;
    THashMap<ui64, TKqpWriteTask> WriteTasks;
    TKqpTableWriteActor::TWriteToken CurrentWriteToken = 0;

    THashMap<TPathId, std::queue<TBufferWriteMessage>> RequestQueues;
    TWriteTasksPlanner TasksPlanner;

    TIntrusivePtr<TKqpCounters> Counters;
    TIntrusivePtr<NTxProxy::TTxProxyMon> TxProxyMon;
    THashMap<ui64, TInstant> SendTime;
    TInstant OperationStartTime;

    THashMap<ui64, ui64> ExternalShardIdToOverloadSeqNo;

    struct TAfterWaitTasksState {
        bool IsCommit = false;
        ui64 TxId = 0;
        NWilson::TTraceId TraceId;
    };

    std::optional<TAfterWaitTasksState> AfterWaitTasksState;

    NWilson::TSpan BufferWriteActorSpan;
    NWilson::TSpan BufferWriteActorStateSpan;
    const TString UserSID;
    ui64 QuerySpanId = 0;
};

class TKqpForwardWriteActor : public TActorBootstrapped<TKqpForwardWriteActor>, public NYql::NDq::IDqComputeActorAsyncOutput {
    using TBase = TActorBootstrapped<TKqpForwardWriteActor>;

public:
    template<typename TArgs>
    TKqpForwardWriteActor(
        NKikimrKqp::TKqpTableSinkSettings&& settings,
        TArgs&& args,
        TIntrusivePtr<TKqpCounters> counters,
        const TString& userSID)
        : LogPrefix(TStringBuilder() << "TxId: " << args.TxId << ", task: " << args.TaskId << ". ")
        , Settings(std::move(settings))
        , MessageSettings(GetWriteActorSettings())
        , Alloc(args.Alloc)
        , HolderFactory(args.HolderFactory)
        , OutputIndex(args.OutputIndex)
        , Callbacks(args.Callback)
        , Counters(counters)
        , BufferActorId(ActorIdFromProto(Settings.GetBufferActorId()))
        , TxId(std::get<ui64>(args.TxId))
        , TableId(
            Settings.GetTable().GetOwnerId(),
            Settings.GetTable().GetTableId(),
            Settings.GetTable().GetVersion())
        , ForwardWriteActorSpan(TWilsonKqp::ForwardWriteActor, NWilson::TTraceId(args.TraceId), "ForwardWriteActor",
                NWilson::EFlags::AUTO_END)
        , UserSID(userSID)
        , TransformOutput(ExtractTransformOutput(args))
    {
        EgressStats.Level = args.StatsLevel;

        TVector<NKikimrKqp::TKqpColumnMetadataProto> columnsMetadata(
            Settings.GetColumns().begin(),
            Settings.GetColumns().end());
        std::vector<ui32> writeIndex(
            Settings.GetWriteIndexes().begin(),
            Settings.GetWriteIndexes().end());

        TGuard guard(*Alloc);
        if (Settings.GetIsOlap()) {
            Batcher = CreateColumnDataBatcher(columnsMetadata, std::move(writeIndex), Alloc);
        } else {
            Batcher = CreateRowDataBatcher(columnsMetadata, std::move(writeIndex), Alloc);
        }

        if (TransformOutput) {
            AFL_ENSURE(!Settings.GetReturningColumns().empty());
            ReturningColumnsTypes.reserve(Settings.GetReturningColumns().size());
            for (const auto& column : Settings.GetReturningColumns()) {
                ReturningColumnsTypes.push_back(NScheme::TypeInfoFromProto(
                    column.GetTypeId(), column.GetTypeInfo()));
            }
        } else {
            AFL_ENSURE(Settings.GetReturningColumns().empty());
        }

        Counters->ForwardActorsCount->Inc();
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
        } catch (...) {
            RuntimeError(
                CurrentExceptionMessage(),
                NYql::NDqProto::StatusIds::INTERNAL_ERROR);
            return;
        }
    }

    void Handle(TEvBufferWriteResult::TPtr& result) {
        CA_LOG_D("TKqpForwardWriteActor receive EvBufferWriteResult from " << BufferActorId);

        WriteToken = result->Get()->Token;

        if (TransformOutput) {
            AFL_ENSURE(Alloc);
            TGuard<NMiniKQL::TScopedAlloc> allocGuard(*Alloc);
            for (const auto& batch : result->Get()->Data) {
                for (const auto& row : GetRows(batch)) {
                    AFL_ENSURE(row.size() == ReturningColumnsTypes.size());
                    NUdf::TUnboxedValue* outputRowItems = nullptr;
                    auto outputRow = HolderFactory.CreateDirectArrayHolder(ReturningColumnsTypes.size(), outputRowItems);

                    for (size_t index = 0; index < ReturningColumnsTypes.size(); ++index) {
                        outputRowItems[index] = NMiniKQL::GetCellValue(row[index], ReturningColumnsTypes[index]);
                    }

                    AFL_ENSURE(TransformOutput->GetFillLevel() == NYql::NDq::EDqFillLevel::NoLimit);
                    TransformOutput->Consume(std::move(outputRow));
                }
            }
        }

        OnFlushed();
    }

    void OnFlushed() {
        InFlight = false;

        EgressStats.Bytes += DataSize;
        EgressStats.Chunks++;
        EgressStats.Splits++;
        EgressStats.Resume();

        Counters->ForwardActorWritesSizeHistogram->Collect(DataSize);
        DataSize = 0;

        if (Closed) {
            if (TransformOutput) {
                TransformOutput->Finish();
            }
            CA_LOG_D("Finished");
            Callbacks->OnAsyncOutputFinished(GetOutputIndex());
        } else {
            CA_LOG_D("Resume with freeSpace=" << GetFreeSpace());
            Callbacks->ResumeExecution();
        }
    }

    void WriteToBuffer() {
        InFlight = true;
        auto ev = std::make_unique<TEvBufferWrite>();

        ev->Data = Batcher->Build();
        ev->Data->DetachAlloc();
        ev->Close = Closed;

        if (!WriteToken.IsEmpty()) {
            ev->Token = WriteToken;
        } else {
            TVector<NKikimrKqp::TKqpColumnMetadataProto> keyColumnsMetadata(
                Settings.GetKeyColumns().begin(),
                Settings.GetKeyColumns().end());
            TVector<NKikimrKqp::TKqpColumnMetadataProto> columnsMetadata(Settings.GetColumns().size());
            AFL_ENSURE(Settings.GetColumns().size() == Settings.GetWriteIndexes().size());
            for (int index = 0; index < Settings.GetColumns().size(); ++index) {
                columnsMetadata[Settings.GetWriteIndexes()[index]] = Settings.GetColumns()[index];
            }
            TVector<NKikimrKqp::TKqpColumnMetadataProto> lookupColumnsMetadata(
                Settings.GetLookupColumns().begin(),
                Settings.GetLookupColumns().end());

            THashSet<TStringBuf> defaultColumns;
            {
                THashSet<ui32> defaultColumnIds(
                    Settings.GetDefaultColumnsIds().begin(),
                    Settings.GetDefaultColumnsIds().end());
                for (const auto& column : Settings.GetColumns()) {
                    if (defaultColumnIds.contains(column.GetId())) {
                        defaultColumns.insert(column.GetName());
                    }
                }
            }

            AFL_ENSURE(Settings.GetDefaultColumnsIds().empty()
                || Settings.GetType() == NKikimrKqp::TKqpTableSinkSettings::MODE_UPSERT);

            ev->Settings = TWriteSettings{
                .Database = Settings.GetDatabase(),
                .TableId = TableId,
                .TablePath = Settings.GetTable().GetPath(),
                .OperationType = Settings.GetType(),
                .KeyColumns = std::move(keyColumnsMetadata),
                .Columns = std::move(columnsMetadata),
                .LookupColumns = std::move(lookupColumnsMetadata),
                .ReturningColumns = TVector<NKikimrKqp::TKqpColumnMetadataProto>(
                    Settings.GetReturningColumns().begin(),
                    Settings.GetReturningColumns().end()),
                .TransactionSettings = TTransactionSettings{
                    .TxId = TxId,
                    .LockTxId = Settings.GetLockTxId(),
                    .LockNodeId = Settings.GetLockNodeId(),
                    .InconsistentTx = Settings.GetInconsistentTx(),
                    .MvccSnapshot = GetOptionalMvccSnapshot(Settings),
                    .LockMode = Settings.GetLockMode(),
                },
                .Priority = Settings.GetPriority(),
                .IsOlap = Settings.GetIsOlap(),
                .DefaultColumns = std::move(defaultColumns),

                .EnableStreamWrite = Settings.GetEnableStreamWrite(),
                .QuerySpanId = Settings.GetQuerySpanId(),
            };

            for (const auto& indexSettings : Settings.GetIndexes()) {
                TVector<NKikimrKqp::TKqpColumnMetadataProto> keyColumnsMetadata(
                    indexSettings.GetKeyColumns().begin(),
                    indexSettings.GetKeyColumns().end());
                TVector<NKikimrKqp::TKqpColumnMetadataProto> columnsMetadata(indexSettings.GetColumns().size());
                AFL_ENSURE(indexSettings.GetColumns().size() == indexSettings.GetWriteIndexes().size());
                for (int index = 0; index < indexSettings.GetColumns().size(); ++index) {
                    columnsMetadata[indexSettings.GetWriteIndexes()[index]] = indexSettings.GetColumns()[index];
                }

                ev->Settings->Indexes.push_back(TWriteSettings::TIndex {
                    .TableId = TTableId(indexSettings.GetTable().GetOwnerId(),
                                        indexSettings.GetTable().GetTableId(),
                                        indexSettings.GetTable().GetVersion()),
                    .TablePath = indexSettings.GetTable().GetPath(),
                    .KeyColumns = std::move(keyColumnsMetadata),
                    .KeyPrefixSize = indexSettings.GetKeyPrefixSize(),
                    .Columns = std::move(columnsMetadata),
                    .IsUniq = indexSettings.GetIsUniq(),
                });
            }
        }

        ev->SendTime = TInstant::Now();

        CA_LOG_D("Send data=" << DataSize << ", closed=" << Closed << ", bufferActorId=" << BufferActorId);
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
        return InFlight
            ? std::numeric_limits<i64>::min()
            : MessageSettings.MaxForwardedSize - DataSize;
    }

    TMaybe<google::protobuf::Any> ExtraData() override {
        return {};
    }

    void SendData(NMiniKQL::TUnboxedValueBatch&& data, i64 size, const TMaybe<NYql::NDqProto::TCheckpoint>&, bool finished) final {
        YQL_ENSURE(!data.IsWide(), "Wide stream is not supported yet");
        AFL_ENSURE(!InFlight);
        Closed |= finished;
        Batcher->AddData(data);
        DataSize += size;

        CA_LOG_D("Add data: " << size << " / " << DataSize);
        if (Closed || GetFreeSpace() <= 0) {
            WriteToBuffer();
        }
    }

    void RuntimeError(const TString& message, NYql::NDqProto::StatusIds::StatusCode statusCode, const NYql::TIssues& subIssues = {}) {
        ForwardWriteActorSpan.EndError(message);

        CA_LOG_E("RuntimeError: " << message);
        NYql::TIssue issue(message);
        for (const auto& i : subIssues) {
            issue.AddSubIssue(MakeIntrusive<NYql::TIssue>(i));
        }

        NYql::TIssues issues;
        issues.AddIssue(std::move(issue));

        Callbacks->OnAsyncOutputError(OutputIndex, std::move(issues), statusCode);
    }

    void PassAway() override {
        Counters->ForwardActorsCount->Dec();

        if (TransformOutput) {
            AFL_ENSURE(Alloc);
            TGuard<NMiniKQL::TScopedAlloc> allocGuard(*Alloc);
            TransformOutput.Reset();
        }

        TActorBootstrapped<TKqpForwardWriteActor>::PassAway();
    }

    TString LogPrefix;
    const NKikimrKqp::TKqpTableSinkSettings Settings;
    TWriteActorSettings MessageSettings;
    std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> Alloc;
    const NMiniKQL::THolderFactory& HolderFactory;
    const ui64 OutputIndex;
    NYql::NDq::TDqAsyncStats EgressStats;
    NYql::NDq::IDqComputeActorAsyncOutput::ICallbacks * Callbacks = nullptr;
    TIntrusivePtr<TKqpCounters> Counters;

    TActorId BufferActorId;
    IDataBatcherPtr Batcher;

    i64 DataSize = 0;
    bool Closed = false;
    bool InFlight = false;

    const ui64 TxId;
    const TTableId TableId;
    std::vector<NScheme::TTypeInfo> ReturningColumnsTypes;

    TWriteToken WriteToken;
    NWilson::TSpan ForwardWriteActorSpan;
    const TString UserSID;
    NYql::NDq::IDqOutputConsumer::TPtr TransformOutput;

private:
    template<typename TArgs>
    NYql::NDq::IDqOutputConsumer::TPtr ExtractTransformOutput(TArgs&& args) {
        if constexpr (std::is_same_v<std::decay_t<TArgs>, NYql::NDq::IDqAsyncIoFactory::TOutputTransformArguments>) {
            return args.TransformOutput;
        }
        return nullptr;
    }
};

NActors::IActor* CreateKqpBufferWriterActor(TKqpBufferWriterSettings&& settings) {
    return new TKqpBufferWriteActor(std::move(settings));
}


void RegisterKqpWriteActor(NYql::NDq::TDqAsyncIoFactory& factory, TIntrusivePtr<TKqpCounters> counters) {
    factory.RegisterSink<NKikimrKqp::TKqpTableSinkSettings>(
        TString(NYql::KqpTableSinkName),
        [counters] (NKikimrKqp::TKqpTableSinkSettings&& settings, NYql::NDq::TDqAsyncIoFactory::TSinkArguments&& args) {
            auto userSID = settings.GetUserSID();
            if (!ActorIdFromProto(settings.GetBufferActorId())) {
                auto* actor = new TKqpDirectWriteActor(std::move(settings), std::move(args), counters, userSID);
                return std::make_pair<NYql::NDq::IDqComputeActorAsyncOutput*, NActors::IActor*>(actor, actor);
            } else {
                auto* actor = new TKqpForwardWriteActor(std::move(settings), std::move(args), counters, userSID);
                return std::make_pair<NYql::NDq::IDqComputeActorAsyncOutput*, NActors::IActor*>(actor, actor);
            }
        });

    factory.RegisterOutputTransform<NKikimrKqp::TKqpTableSinkSettings>(
        TString(NYql::KqpTableSinkName),
        [counters] (NKikimrKqp::TKqpTableSinkSettings&& settings, NYql::NDq::TDqAsyncIoFactory::TOutputTransformArguments&& args) {
            AFL_ENSURE(ActorIdFromProto(settings.GetBufferActorId()));
            auto* actor = new TKqpForwardWriteActor(std::move(settings), std::move(args), counters, settings.GetUserSID());
            return std::make_pair<NYql::NDq::IDqComputeActorAsyncOutput*, NActors::IActor*>(actor, actor);
        });
}

}
}
