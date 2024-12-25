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
        for (ui64 index = 0; index < attempt; ++index) {
            delay *= settings.Multiplier;
        }

        delay *= 1 + settings.UnsertaintyRatio * (1 - 2 * RandomNumber<double>());
        delay = Min(delay, settings.MaxRetryDelay);

        return delay;
    }

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

    void FillEvWritePrepare(NKikimr::NEvents::TDataEvents::TEvWrite* evWrite, ui64 shardId, ui64 txId, const NKikimr::NKqp::IKqpTransactionManagerPtr& txManager) {
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
        } else if (prepareSettings.ArbiterColumnShard == shardId) {
            protoLocks->SetArbiterColumnShard(*prepareSettings.ArbiterColumnShard);
            for (const ui64 sendingShardId : prepareSettings.SendingShards) {
                protoLocks->AddSendingShards(sendingShardId);
            }
            for (const ui64 receivingShardId : prepareSettings.ReceivingShards) {
                protoLocks->AddReceivingShards(receivingShardId);
            }
        } else {
            protoLocks->SetArbiterColumnShard(*prepareSettings.ArbiterColumnShard);
            protoLocks->AddSendingShards(*prepareSettings.ArbiterColumnShard);
            protoLocks->AddReceivingShards(*prepareSettings.ArbiterColumnShard);
            if (prepareSettings.SendingShards.contains(shardId)) {
                protoLocks->AddSendingShards(shardId);
            }
            if (prepareSettings.ReceivingShards.contains(shardId)) {
                protoLocks->AddReceivingShards(shardId);
            }
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

    virtual void OnError(const TString& message, NYql::NDqProto::StatusIds::StatusCode statusCode, const NYql::TIssues& subIssues) = 0;
};

struct TKqpTableWriterStatistics {
    ui64 ReadRows = 0;
    ui64 ReadBytes = 0;
    ui64 WriteRows = 0;
    ui64 WriteBytes = 0;
    ui64 EraseRows = 0;
    ui64 EraseBytes = 0;

    THashSet<ui64> AffectedPartitions;
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
        const bool isOlap,
        TVector<NScheme::TTypeInfo> keyColumnTypes,
        const NMiniKQL::TTypeEnvironment& typeEnv,
        std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> alloc,
        const std::optional<NKikimrDataEvents::TMvccSnapshot>& mvccSnapshot,
        const NKikimrDataEvents::ELockMode lockMode,
        const IKqpTransactionManagerPtr& txManager,
        const TActorId sessionActorId,
        TIntrusivePtr<TKqpCounters> counters,
        NWilson::TTraceId traceId)
        : TypeEnv(typeEnv)
        , Alloc(alloc)
        , MvccSnapshot(mvccSnapshot)
        , LockMode(lockMode)
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
        , TableWriteActorSpan(TWilsonKqp::TableWriteActor, NWilson::TTraceId(traceId), "TKqpTableWriteActor")
    {
        LogPrefix = TStringBuilder() << "Table: `" << TablePath << "` (" << TableId << "), " << "SessionActorId: " << sessionActorId;
        try {
            ShardedWriteController = CreateShardedWriteController(
                TShardedWriteControllerSettings {
                    .MemoryLimitTotal = MessageSettings.InFlightMemoryLimitPerActorBytes,
                    .MemoryLimitPerMessage = MessageSettings.MemoryLimitPerMessageBytes,
                    .MaxBatchesPerMessage = MessageSettings.MaxBatchesPerMessage,
                },
                TypeEnv,
                Alloc);
        } catch (...) {
            RuntimeError(
                CurrentExceptionMessage(),
                NYql::NDqProto::StatusIds::INTERNAL_ERROR);
        }

        Counters->WriteActorsCount->Inc();
    }

    void Bootstrap() {
        LogPrefix = TStringBuilder() << "SelfId: " << this->SelfId() << ", " << LogPrefix;
        Resolve();
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

    TWriteToken Open(
        NKikimrDataEvents::TEvWrite::TOperation::EOperationType operationType,
        TVector<NKikimrKqp::TKqpColumnMetadataProto>&& keyColumnsMetadata,
        TVector<NKikimrKqp::TKqpColumnMetadataProto>&& columnsMetadata,
        std::vector<ui32>&& writeIndexes,
        i64 priority) {
        YQL_ENSURE(!Closed);
        auto token = ShardedWriteController->Open(
            TableId,
            operationType,
            std::move(keyColumnsMetadata),
            std::move(columnsMetadata),
            std::move(writeIndexes),
            priority);
        CA_LOG_D("Open: token=" << token);
        return token;
    }

    void Write(TWriteToken token, IDataBatchPtr&& data) {
        YQL_ENSURE(!Closed);
        YQL_ENSURE(ShardedWriteController);
        CA_LOG_D("Write: token=" << token);
        try {
            ShardedWriteController->Write(token, std::move(data));
            UpdateShards();
        } catch (...) {
            RuntimeError(
                CurrentExceptionMessage(),
                NYql::NDqProto::StatusIds::INTERNAL_ERROR);
        }
    }

    void Close(TWriteToken token) {
        YQL_ENSURE(!Closed);
        YQL_ENSURE(ShardedWriteController);
        CA_LOG_D("Close: token=" << token);
        try {
            ShardedWriteController->Close(token);
            UpdateShards();
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

    void UpdateShards() {
        // TODO: Maybe there are better ways to initialize new shards...
        for (const auto& shardInfo : ShardedWriteController->GetPendingShards()) {
            TxManager->AddShard(shardInfo.ShardId, IsOlap, TablePath);
            IKqpTransactionManager::TActionFlags flags = IKqpTransactionManager::EAction::WRITE;
            if (shardInfo.HasRead) {
                flags |= IKqpTransactionManager::EAction::READ;
            }
            TxManager->AddAction(shardInfo.ShardId, flags);
        }
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
                hFunc(TEvPrivate::TEvResolveRequestPlanned, Handle);
                IgnoreFunc(TEvInterconnect::TEvNodeConnected);
                IgnoreFunc(TEvTxProxySchemeCache::TEvInvalidateTableResult);
            }
        } catch (const yexception& e) {
            RuntimeError(e.what(), NYql::NDqProto::StatusIds::INTERNAL_ERROR);
        }
    }

    STFUNC(StateTerminating) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvPrivate::TEvTerminate, Handle);
            }
        } catch (const yexception& e) {
            CA_LOG_W(e.what());
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
        Resolve();
    }

    void ResolveTable() {
        Counters->WriteActorsShardResolve->Inc();
        SchemeEntry.reset();
        KeyDescription.Reset();

        if (ResolveAttempts++ >= MessageSettings.MaxResolveAttempts) {
            CA_LOG_E(TStringBuilder()
                << "Too many table resolve attempts for table `" << TablePath << "` (" << TableId << ").");
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

        TableWriteActorStateSpan = NWilson::TSpan(TWilsonKqp::TableWriteActorTableNavigate, TableWriteActorSpan.GetTraceId(),
            "WaitForShardsResolve", NWilson::EFlags::AUTO_END);

        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request), 0, 0, TableWriteActorStateSpan.GetTraceId());
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
            << TableId.PathId.ToString() << " "
            << TableId.SchemaVersion << ")");

        if (TableId.SchemaVersion != SchemeEntry->TableId.SchemaVersion) {
            RuntimeError(TStringBuilder() << "Schema was updated.", NYql::NDqProto::StatusIds::SCHEME_ERROR);
            return;
        }

        YQL_ENSURE(IsOlap && (SchemeEntry->Kind == NSchemeCache::TSchemeCacheNavigate::KindColumnTable));

        ResolveShards();
    }

    void ResolveShards() {
        YQL_ENSURE(!KeyColumnTypes.empty());
        CA_LOG_D("Resolve shards for TableId=" << TableId);

        const TVector<TCell> minKey(KeyColumnTypes.size());
        const TTableRange range(minKey, true, {}, false, false);
        YQL_ENSURE(range.IsFullRange(KeyColumnTypes.size()));
        auto keyRange = MakeHolder<TKeyDesc>(
            TableId,
            range,
            TKeyDesc::ERowOperation::Update,
            KeyColumnTypes,
            TVector<TKeyDesc::TColumnOp>{});

        TAutoPtr<NSchemeCache::TSchemeCacheRequest> request(new NSchemeCache::TSchemeCacheRequest());
        request->ResultSet.emplace_back(std::move(keyRange));

        TAutoPtr<TEvTxProxySchemeCache::TEvResolveKeySet> resolveReq(new TEvTxProxySchemeCache::TEvResolveKeySet(request));
        Send(MakeSchemeCacheID(), resolveReq.Release(), 0, 0, TableWriteActorStateSpan.GetTraceId());
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
        KeyDescription = std::move(request->ResultSet[0].KeyDescription);

        CA_LOG_D("Resolved shards for TableId=" << TableId << ". PartitionsCount=" << KeyDescription->GetPartitions().size() << ".");

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
        UpdateStats(ev->Get()->Record.GetTxStats());

        switch (ev->Get()->GetStatus()) {
        case NKikimrDataEvents::TEvWriteResult::STATUS_UNSPECIFIED: {
            CA_LOG_E("Got UNSPECIFIED for table `"
                    << TableId.PathId.ToString() << "`."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << "."
                    << getIssues().ToOneLineString());
            RuntimeError(
                TStringBuilder() << "Unspecified error for table `"
                    << TablePath << "`. "
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
                    << TableId.PathId.ToString() << "`."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << "."
                    << getIssues().ToOneLineString());
            RuntimeError(
                TStringBuilder() << "Aborted for table `"
                    << TablePath << "`. "
                    << getIssues().ToOneLineString(),
                NYql::NDqProto::StatusIds::ABORTED,
                getIssues());
            return;
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_WRONG_SHARD_STATE:
            CA_LOG_E("Got WRONG SHARD STATE for table `"
                    << SchemeEntry->TableId.PathId.ToString() << "`."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << "."
                    << getIssues().ToOneLineString());
            
            if (InconsistentTx) {
                ResetShardRetries(ev->Get()->Record.GetOrigin(), ev->Cookie);
                RetryResolve();
            } else {
                RuntimeError(
                    TStringBuilder() << "Wrong shard state for table `"
                        << TablePath << "`. "
                        << getIssues().ToOneLineString(),
                    NYql::NDqProto::StatusIds::PRECONDITION_FAILED,
                    getIssues());
            }
            return;
        case NKikimrDataEvents::TEvWriteResult::STATUS_INTERNAL_ERROR: {
            CA_LOG_E("Got INTERNAL ERROR for table `"
                    << TableId.PathId.ToString() << "`."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << "."
                    << getIssues().ToOneLineString());
            RuntimeError(
                TStringBuilder() << "Internal error for table `"
                    << TablePath << "`. "
                    << getIssues().ToOneLineString(),
                NYql::NDqProto::StatusIds::INTERNAL_ERROR,
                getIssues());
            return;
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_DISK_SPACE_EXHAUSTED: {
            CA_LOG_E("Got DISK_SPACE_EXHAUSTED for table `"
                    << TableId.PathId.ToString() << "`."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << "."
                    << getIssues().ToOneLineString());
            RuntimeError(
                TStringBuilder() << "Disk space exhausted for table `"
                    << TablePath << "`. "
                    << getIssues().ToOneLineString(),
                NYql::NDqProto::StatusIds::UNAVAILABLE,
                getIssues());
            return;
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_OVERLOADED: {
            CA_LOG_W("Got OVERLOADED for table `"
                << TableId.PathId.ToString() << "`."
                << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                << " Sink=" << this->SelfId() << "."
                << " Ignored this error."
                << getIssues().ToOneLineString());
            // TODO: support waiting
            if (!InconsistentTx)  {
                RuntimeError(
                    TStringBuilder() << "Tablet " << ev->Get()->Record.GetOrigin() << " is overloaded. Table `"
                        << TablePath << "`. "
                        << getIssues().ToOneLineString(),
                    NYql::NDqProto::StatusIds::OVERLOADED,
                    getIssues());
            }
            return;
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_CANCELLED: {
            CA_LOG_E("Got CANCELLED for table `"
                    << TableId.PathId.ToString() << "`."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << "."
                    << getIssues().ToOneLineString());
            RuntimeError(
                TStringBuilder() << "Cancelled request to table `"
                    << TablePath << "`."
                    << getIssues().ToOneLineString(),
                NYql::NDqProto::StatusIds::CANCELLED,
                getIssues());
            return;
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST: {
            CA_LOG_E("Got BAD REQUEST for table `"
                    << TableId.PathId.ToString() << "`."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << "."
                    << getIssues().ToOneLineString());
            RuntimeError(
                TStringBuilder() << "Bad request. Table `"
                    << TablePath << "`. "
                    << getIssues().ToOneLineString(),
                NYql::NDqProto::StatusIds::BAD_REQUEST,
                getIssues());
            return;
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_SCHEME_CHANGED: {
            CA_LOG_E("Got SCHEME CHANGED for table `"
                    << TableId.PathId.ToString() << "`."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << "."
                    << getIssues().ToOneLineString());
            if (InconsistentTx) {
                ResetShardRetries(ev->Get()->Record.GetOrigin(), ev->Cookie);
                RetryResolve();
            } else {
                RuntimeError(
                    TStringBuilder() << "Scheme changed. Table `"
                        << TablePath << "`. "
                        << getIssues().ToOneLineString(),
                    NYql::NDqProto::StatusIds::SCHEME_ERROR,
                    getIssues());
            }
            return;
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_LOCKS_BROKEN: {
            CA_LOG_E("Got LOCKS BROKEN for table `"
                    << TableId.PathId.ToString() << "`."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << "."
                    << getIssues().ToOneLineString());

            TxManager->BreakLock(ev->Get()->Record.GetOrigin());
            YQL_ENSURE(TxManager->BrokenLocks());
            RuntimeError(
                TStringBuilder() << "Transaction locks invalidated. Table `"
                    << TablePath << "`. "
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

        for (const auto& lock : ev->Get()->Record.GetTxLocks()) {
            Y_ABORT_UNLESS(Mode == EMode::WRITE);
            if (!TxManager->AddLock(ev->Get()->Record.GetOrigin(), lock)) {
                YQL_ENSURE(TxManager->BrokenLocks());
                NYql::TIssues issues;
                issues.AddIssue(*TxManager->GetLockIssue());
                RuntimeError(
                    TStringBuilder() << "Transaction locks invalidated. Table `"
                        << TablePath << "`.",
                    NYql::NDqProto::StatusIds::ABORTED,
                    issues);
                return;
            }
        }

        if (Mode == EMode::COMMIT) {
            Callbacks->OnCommitted(ev->Get()->Record.GetOrigin(), 0);
            return;
        }

        OnMessageReceived(ev->Get()->Record.GetOrigin());
        const auto result = ShardedWriteController->OnMessageAcknowledged(
                ev->Get()->Record.GetOrigin(), ev->Cookie);
        if (result && result->IsShardEmpty && Mode == EMode::IMMEDIATE_COMMIT) {
            Callbacks->OnCommitted(ev->Get()->Record.GetOrigin(), result->DataSize);
        } else if (result) {
            Callbacks->OnMessageAcknowledged(result->DataSize);
        }
    }

    void OnMessageReceived(const ui64 shardId) {
        if (auto it = SendTime.find(shardId); it != std::end(SendTime)) {
            Counters->WriteActorWritesLatencyHistogram->Collect((TInstant::Now() - it->second).MilliSeconds());
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

    void FlushBuffers() {
        ShardedWriteController->FlushBuffers();
        UpdateShards();
    }

    void Flush() {
        for (const auto& shardInfo : ShardedWriteController->GetPendingShards()) {
            SendDataToShard(shardInfo.ShardId);
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
            ? (TxManager->IsVolatile()
                ? NKikimrDataEvents::TEvWrite::MODE_VOLATILE_PREPARE
                : NKikimrDataEvents::TEvWrite::MODE_PREPARE)
            : NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE);

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
            evWrite->Record.SetLockMode(LockMode);

            if (LockMode == NKikimrDataEvents::OPTIMISTIC_SNAPSHOT_ISOLATION) {
                YQL_ENSURE(MvccSnapshot);
                *evWrite->Record.MutableMvccSnapshot() = *MvccSnapshot;
            }
        }

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

            SendTime[shardId] = TInstant::Now();
        } else {
            YQL_ENSURE(!isPrepare);
            Counters->WriteActorImmediateWritesRetries->Inc();
        }

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
            << ", Attempts=" << metadata->SendAttempts << ", Mode=" << static_cast<int>(Mode));
        Send(
            PipeCacheId,
            new TEvPipeCache::TEvForward(evWrite.release(), shardId, /* subscribe */ true),
            IEventHandle::FlagTrackDelivery,
            metadata->Cookie,
            TableWriteActorSpan.GetTraceId());

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
        PassAway();
    }

    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
        CA_LOG_W("TEvDeliveryProblem was received from tablet: " << ev->Get()->TabletId);
        if (InconsistentTx) {
            RetryShard(ev->Get()->TabletId, std::nullopt);
        } else {
            RuntimeError(
                TStringBuilder()
                    << "Error writing to table `" << TableId.PathId.ToString() << "`"
                    << ": can't deliver message to tablet " << ev->Get()->TabletId << ".",
                NYql::NDqProto::StatusIds::UNAVAILABLE);
        }
    }

    void Prepare() {
        TableWriteActorStateSpan.EndOk();
        ResolveAttempts = 0;

        try {
            if (IsOlap) {
                YQL_ENSURE(SchemeEntry);
                ShardedWriteController->OnPartitioningChanged(*SchemeEntry);
            } else {
                ShardedWriteController->OnPartitioningChanged(std::move(KeyDescription));
                KeyDescription.Reset();
            }
        } catch (...) {
            RuntimeError(
                CurrentExceptionMessage(),
                NYql::NDqProto::StatusIds::INTERNAL_ERROR);
        }

        Callbacks->OnReady();
    }

    void RuntimeError(const TString& message, NYql::NDqProto::StatusIds::StatusCode statusCode, const NYql::TIssues& subIssues = {}) {
        if (TableWriteActorStateSpan) {
            TableWriteActorStateSpan.EndError(message);
        }
        if (TableWriteActorSpan) {
            TableWriteActorSpan.EndError(message);
        }

        Callbacks->OnError(message, statusCode, subIssues);
    }

    void PassAway() override {;
        CA_LOG_D("PassAway");
        Send(PipeCacheId, new TEvPipeCache::TEvUnlink(0));
        TActorBootstrapped<TKqpTableWriteActor>::PassAway();
    }

    void Terminate() {
        Become(&TKqpTableWriteActor::StateTerminating);
        Send(this->SelfId(), new TEvPrivate::TEvTerminate{});
    }

    void UpdateStats(const NKikimrQueryStats::TTxStats& txStats) {
        for (const auto& tableAccessStats : txStats.GetTableAccessStats()) {
            YQL_ENSURE(tableAccessStats.GetTableInfo().GetPathId() == TableId.PathId.LocalPathId);
            Stats.ReadRows += tableAccessStats.GetSelectRow().GetRows();
            Stats.ReadRows += tableAccessStats.GetSelectRange().GetRows();
            Stats.ReadBytes += tableAccessStats.GetSelectRow().GetBytes();
            Stats.ReadBytes += tableAccessStats.GetSelectRange().GetBytes();
            Stats.WriteRows += tableAccessStats.GetUpdateRow().GetRows();
            Stats.WriteBytes += tableAccessStats.GetUpdateRow().GetBytes();
            Stats.EraseRows += tableAccessStats.GetEraseRow().GetRows();
            Stats.EraseBytes += tableAccessStats.GetEraseRow().GetRows();
        }

        for (const auto& perShardStats : txStats.GetPerShardStats()) {
            Stats.AffectedPartitions.insert(perShardStats.GetShardId());
        }
    }

    void FillStats(NYql::NDqProto::TDqTaskStats* stats) {
        NYql::NDqProto::TDqTableStats* tableStats = nullptr;
        for (size_t i = 0; i < stats->TablesSize(); ++i) {
            auto* table = stats->MutableTables(i);
            if (table->GetTablePath() == TablePath) {
                tableStats = table;
            }
        }
        if (!tableStats) {
            tableStats = stats->AddTables();
            tableStats->SetTablePath(TablePath);
        }

        tableStats->SetReadRows(tableStats->GetReadRows() + Stats.ReadRows);
        tableStats->SetReadBytes(tableStats->GetReadBytes() + Stats.ReadBytes);
        tableStats->SetWriteRows(tableStats->GetWriteRows() + Stats.WriteRows);
        tableStats->SetWriteBytes(tableStats->GetWriteBytes() + Stats.WriteBytes);
        tableStats->SetEraseRows(tableStats->GetEraseRows() + Stats.EraseRows);
        tableStats->SetEraseBytes(tableStats->GetEraseBytes() + Stats.EraseBytes);
    
        Stats.ReadRows = 0;
        Stats.ReadBytes = 0;
        Stats.WriteRows = 0;
        Stats.WriteBytes = 0;
        Stats.EraseRows = 0;
        Stats.EraseBytes = 0;

        tableStats->SetAffectedPartitions(
            tableStats->GetAffectedPartitions() + Stats.AffectedPartitions.size());
        Stats.AffectedPartitions.clear();
    }

    NActors::TActorId PipeCacheId = NKikimr::MakePipePerNodeCacheID(false);

    TString LogPrefix;
    TWriteActorSettings MessageSettings;
    const NMiniKQL::TTypeEnvironment& TypeEnv;
    std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> Alloc;

    const std::optional<NKikimrDataEvents::TMvccSnapshot> MvccSnapshot;
    const NKikimrDataEvents::ELockMode LockMode;

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
    THolder<TKeyDesc> KeyDescription;
    ui64 ResolveAttempts = 0;

    IKqpTransactionManagerPtr TxManager;
    bool Closed = false;
    EMode Mode = EMode::WRITE;
    THashMap<ui64, TInstant> SendTime;

    IShardedWriteControllerPtr ShardedWriteController = nullptr;

    TIntrusivePtr<TKqpCounters> Counters;

    TKqpTableWriterStatistics Stats;

    NWilson::TSpan TableWriteActorSpan;
    NWilson::TSpan TableWriteActorStateSpan;
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
        , DirectWriteActorSpan(TWilsonKqp::DirectWriteActor, NWilson::TTraceId(args.TraceId), "TKqpDirectWriteActor")
    {
        EgressStats.Level = args.StatsLevel;

        TVector<NKikimrKqp::TKqpColumnMetadataProto> columnsMetadata(
            Settings.GetColumns().begin(),
            Settings.GetColumns().end());
        std::vector<ui32> writeIndex(
            Settings.GetWriteIndexes().begin(),
            Settings.GetWriteIndexes().end());

        if (Settings.GetIsOlap()) {
            Batcher = CreateColumnDataBatcher(columnsMetadata, std::move(writeIndex));
        } else {
            Batcher = CreateRowDataBatcher(columnsMetadata, std::move(writeIndex));
        }
    }

    void Bootstrap() {
        LogPrefix = TStringBuilder() << "SelfId: " << this->SelfId() << ", " << LogPrefix;

        TVector<NScheme::TTypeInfo> keyColumnTypes;
        keyColumnTypes.reserve(Settings.GetKeyColumns().size());
        for (const auto& column : Settings.GetKeyColumns()) {
            auto typeInfoMod = NScheme::TypeInfoModFromProtoColumnType(column.GetTypeId(),
                column.HasTypeInfo() ? &column.GetTypeInfo() : nullptr);
            keyColumnTypes.push_back(typeInfoMod.TypeInfo);
        }

        WriteTableActor = new TKqpTableWriteActor(
            this,
            TableId,
            Settings.GetTable().GetPath(),
            Settings.GetLockTxId(),
            Settings.GetLockNodeId(),
            Settings.GetInconsistentTx(),
            Settings.GetIsOlap(),
            std::move(keyColumnTypes),
            TypeEnv,
            Alloc,
            Settings.GetMvccSnapshot(),
            Settings.GetLockMode(),
            nullptr,
            TActorId{},
            Counters,
            DirectWriteActorSpan.GetTraceId());

        WriteTableActorId = RegisterWithSameMailbox(WriteTableActor);

        TVector<NKikimrKqp::TKqpColumnMetadataProto> keyColumnsMetadata(
            Settings.GetKeyColumns().begin(),
            Settings.GetKeyColumns().end());
        TVector<NKikimrKqp::TKqpColumnMetadataProto> columnsMetadata(
            Settings.GetColumns().begin(),
            Settings.GetColumns().end());
        std::vector<ui32> writeIndex(
            Settings.GetWriteIndexes().begin(),
            Settings.GetWriteIndexes().end());
        YQL_ENSURE(Settings.GetPriority() == 0);
        WriteToken = WriteTableActor->Open(
            GetOperation(Settings.GetType()),
            std::move(keyColumnsMetadata),
            std::move(columnsMetadata),
            std::move(writeIndex),
            Settings.GetPriority());
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

    void SendData(NMiniKQL::TUnboxedValueBatch&& data, i64 size, const TMaybe<NYql::NDqProto::TCheckpoint>&, bool finished) final {
        YQL_ENSURE(!data.IsWide(), "Wide stream is not supported yet");
        YQL_ENSURE(!Closed);
        Closed = finished;
        EgressStats.Resume();
        Y_UNUSED(size);

        Batcher->AddData(data);
        YQL_ENSURE(WriteTableActor);
        WriteTableActor->Write(*WriteToken, Batcher->Build());
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
        DirectWriteActorSpan.EndError(message);

        NYql::TIssue issue(message);
        for (const auto& i : subIssues) {
            issue.AddSubIssue(MakeIntrusive<NYql::TIssue>(i));
        }

        NYql::TIssues issues;
        issues.AddIssue(std::move(issue));

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

    void OnError(const TString& message, NYql::NDqProto::StatusIds::StatusCode statusCode, const NYql::TIssues& subIssues) override {
        RuntimeError(message, statusCode, subIssues);
    }

    void FillExtraStats(NYql::NDqProto::TDqTaskStats* stats, bool last, const NYql::NDq::TDqMeteringStats*) override {
        if (last && WriteTableActor) {
            WriteTableActor->FillStats(stats);
        }
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
    IDataBatcherPtr Batcher;

    const ui64 TxId;
    const TTableId TableId;
    TKqpTableWriteActor* WriteTableActor = nullptr;
    TActorId WriteTableActorId;

    std::optional<TKqpTableWriteActor::TWriteToken> WriteToken;

    bool Closed = false;

    bool WaitingForTableActor = false;

    NWilson::TSpan DirectWriteActorSpan;
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
    std::optional<NKikimrDataEvents::TMvccSnapshot> MvccSnapshot;
    NKikimrDataEvents::ELockMode LockMode;
};

struct TWriteSettings {
    TTableId TableId;
    TString TablePath; // for error messages
    NKikimrDataEvents::TEvWrite::TOperation::EOperationType OperationType;
    TVector<NKikimrKqp::TKqpColumnMetadataProto> KeyColumns;
    TVector<NKikimrKqp::TKqpColumnMetadataProto> Columns;
    std::vector<ui32> WriteIndex;
    TTransactionSettings TransactionSettings;
    i64 Priority;
    bool IsOlap;
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
};

struct TEvBufferWriteResult : public TEventLocal<TEvBufferWriteResult, TKqpEvents::EvBufferWriteResult> {
    TWriteToken Token;
};

}


class TKqpBufferWriteActor :public TActorBootstrapped<TKqpBufferWriteActor>, public IKqpTableWriterCallbacks {
    using TBase = TActorBootstrapped<TKqpBufferWriteActor>;
    using TTopicTabletTxs = NTopic::TTopicOperationTransactions;

public:
    enum class EState {
        WRITING, // Allow to write data to buffer.
        FLUSHING, // Force flush (for uncommitted changes visibility). Can't accept any writes in this state.
        PREPARING, // Do preparation for commit. All writers are closed. New writes wouldn't be accepted.
        COMMITTING, // Do commit. All writers are closed. New writes wouldn't be accepted.
        ROLLINGBACK, // Do rollback. New writes wouldn't be accepted.
        FINISHED,
    };

public:
    TKqpBufferWriteActor(
        TKqpBufferWriterSettings&& settings)
        : SessionActorId(settings.SessionActorId)
        , MessageSettings(GetWriteActorSettings())
        , TxManager(settings.TxManager)
        , Alloc(std::make_shared<NKikimr::NMiniKQL::TScopedAlloc>(__LOCATION__))
        , TypeEnv(*Alloc)
        , Counters(settings.Counters)
        , TxProxyMon(settings.TxProxyMon)
        , BufferWriteActor(TWilsonKqp::BufferWriteActor, NWilson::TTraceId(settings.TraceId), "TKqpBufferWriteActor", NWilson::EFlags::AUTO_END)
        , BufferWriteActorState(TWilsonKqp::BufferWriteActorState, BufferWriteActor.GetTraceId(),
            "BufferWriteActorState::Writing", NWilson::EFlags::AUTO_END)
    {
        State = EState::WRITING;
        Alloc->Release();
        Counters->BufferActorsCount->Inc();
    }

    void Bootstrap() {
        LogPrefix = TStringBuilder() << "SelfId: " << this->SelfId() << ", SessionActorId: " << SessionActorId << ", " << LogPrefix;
        Become(&TKqpBufferWriteActor::StateWrite);
    }

    static constexpr char ActorName[] = "KQP_BUFFER_WRITE_ACTOR";

    // TODO: split states
    STFUNC(StateWrite) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvKqpBuffer::TEvTerminate, Handle);
                hFunc(TEvKqpBuffer::TEvFlush, Handle);
                hFunc(TEvKqpBuffer::TEvCommit, Handle);
                hFunc(TEvKqpBuffer::TEvRollback, Handle);
                hFunc(TEvBufferWrite, Handle);

                hFunc(TEvTxProxy::TEvProposeTransactionStatus, Handle);
                hFunc(TEvPersQueue::TEvProposeTransactionResult, Handle);
                hFunc(NKikimr::NEvents::TDataEvents::TEvWriteResult, Handle);
                hFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
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
                TVector<NScheme::TTypeInfo> keyColumnTypes;
                keyColumnTypes.reserve(settings.KeyColumns.size());
                for (const auto& column : settings.KeyColumns) {
                    auto typeInfoMod = NScheme::TypeInfoModFromProtoColumnType(column.GetTypeId(),
                        column.HasTypeInfo() ? &column.GetTypeInfo() : nullptr);
                    keyColumnTypes.push_back(typeInfoMod.TypeInfo);
                }
                writeInfo.WriteTableActor = new TKqpTableWriteActor(
                    this,
                    settings.TableId,
                    settings.TablePath,
                    LockTxId,
                    LockNodeId,
                    InconsistentTx,
                    settings.IsOlap,
                    std::move(keyColumnTypes),
                    TypeEnv,
                    Alloc,
                    settings.TransactionSettings.MvccSnapshot,
                    settings.TransactionSettings.LockMode,
                    TxManager,
                    SessionActorId,
                    Counters,
                    BufferWriteActor.GetTraceId());
                writeInfo.WriteTableActorId = RegisterWithSameMailbox(writeInfo.WriteTableActor);
                CA_LOG_D("Create new TableWriteActor for table `" << settings.TablePath << "` (" << settings.TableId << "). lockId=" << LockTxId << " " << writeInfo.WriteTableActorId);
            }

            auto cookie = writeInfo.WriteTableActor->Open(
                settings.OperationType,
                std::move(settings.KeyColumns),
                std::move(settings.Columns),
                std::move(settings.WriteIndex),
                settings.Priority);
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
        
        Process();
    }

    void Process() {
        ProcessRequestQueue();
        ProcessWrite();
        ProcessAckQueue();

        if (State == EState::FLUSHING) {
            bool isEmpty = true;
            for (auto& [_, info] : WriteInfos) {
                isEmpty = isEmpty && info.WriteTableActor->IsReady() && info.WriteTableActor->IsEmpty();
            }
            if (isEmpty) {
                OnFlushed();
            }
        }
    }

    void ProcessRequestQueue() {
        for (auto& [tableId, queue] : DataQueues) {
            auto& writeInfo = WriteInfos.at(tableId);

            if (!writeInfo.WriteTableActor->IsReady()) {
                CA_LOG_D("ProcessRequestQueue " << tableId << " NOT READY queue=" << queue.size());
                return;
            }

            while (!queue.empty()) {
                auto& message = queue.front();

                if (message.Data) {
                    writeInfo.WriteTableActor->Write(message.Token.Cookie, std::move(message.Data));
                }

                if (message.Close) {
                    writeInfo.WriteTableActor->Close(message.Token.Cookie);
                }

                AckQueue.push(TAckMessage{
                    .ForwardActorId = message.From,
                    .Token = message.Token,
                    .DataSize = 0,
                });

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
                YQL_ENSURE(false);
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
            CA_LOG_D("Flush data");
            for (auto& [_, info] : WriteInfos) {
                if (info.WriteTableActor->IsReady()) {
                    info.WriteTableActor->Flush();
                }
            }
        }
    }

    void Flush() {
        Counters->BufferActorFlushes->Inc();
        BufferWriteActorState = NWilson::TSpan(TWilsonKqp::BufferWriteActorState, BufferWriteActor.GetTraceId(),
            "BufferWriteActorState::Flushing", NWilson::EFlags::AUTO_END);

        CA_LOG_D("Start flush");
        YQL_ENSURE(State == EState::WRITING);
        State = EState::FLUSHING;
        for (auto& [_, queue] : DataQueues) {
            YQL_ENSURE(queue.empty());
        }
        Process();
    }

    void Prepare(const ui64 txId) {
        BufferWriteActorState = NWilson::TSpan(TWilsonKqp::BufferWriteActorState, BufferWriteActor.GetTraceId(),
            "BufferWriteActorState::Preparing", NWilson::EFlags::AUTO_END);

        CA_LOG_D("Start prepare for distributed commit");
        YQL_ENSURE(State == EState::WRITING);
        State = EState::PREPARING;
        for (auto& [_, queue] : DataQueues) {
            YQL_ENSURE(queue.empty());
        }
        TxId = txId;
        for (auto& [_, info] : WriteInfos) {
            info.WriteTableActor->SetPrepare(txId);
        }
        Close();
        Process();
        SendToExternalShards(false);
        SendToTopics(false);
    }

    void ImmediateCommit() {
        Counters->BufferActorImmediateCommits->Inc();
        BufferWriteActorState = NWilson::TSpan(TWilsonKqp::BufferWriteActorState, BufferWriteActor.GetTraceId(),
            "BufferWriteActorState::Committing", NWilson::EFlags::AUTO_END);

        CA_LOG_D("Start immediate commit");
        YQL_ENSURE(State == EState::WRITING);
        State = EState::COMMITTING;
        for (auto& [_, queue] : DataQueues) {
            YQL_ENSURE(queue.empty());
        }
        for (auto& [_, info] : WriteInfos) {
            info.WriteTableActor->SetImmediateCommit();
        }
        Close();
        Process();
        SendToTopics(true);
    }

    void DistributedCommit() {
        Counters->BufferActorDistributedCommits->Inc();
        BufferWriteActorState = NWilson::TSpan(TWilsonKqp::BufferWriteActorState, BufferWriteActor.GetTraceId(),
            "BufferWriteActorState::Committing", NWilson::EFlags::AUTO_END);

        CA_LOG_D("Start distributed commit with TxId=" << *TxId);
        YQL_ENSURE(State == EState::PREPARING);
        State = EState::COMMITTING;
        for (auto& [_, queue] : DataQueues) {
            YQL_ENSURE(queue.empty());
        }
        for (auto& [_, info] : WriteInfos) {
            info.WriteTableActor->SetDistributedCommit();
        }
        SendCommitToCoordinator();
    }

    void Rollback() {
        Counters->BufferActorRollbacks->Inc();
        BufferWriteActorState = NWilson::TSpan(TWilsonKqp::BufferWriteActorState, BufferWriteActor.GetTraceId(),
            "BufferWriteActorState::RollingBack", NWilson::EFlags::AUTO_END);

        CA_LOG_D("Start rollback");
        State = EState::ROLLINGBACK;
        SendToExternalShards(true);
        SendToTopics(true);
    }

    void SendToExternalShards(bool isRollback) {
        THashSet<ui64> shards = TxManager->GetShards();
        if (!isRollback) {
            for (auto& [_, info] : WriteInfos) {
                for (const auto& shardId : info.WriteTableActor->GetShardsIds()) {
                    shards.erase(shardId);
                }
            }
        }

        for (const ui64 shardId : shards) {
            if (TxManager->GetLocks(shardId).empty()) {
                continue;
            }
            auto evWrite = std::make_unique<NKikimr::NEvents::TDataEvents::TEvWrite>(isRollback
                ? NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE
                : (TxManager->IsVolatile()
                    ? NKikimrDataEvents::TEvWrite::MODE_VOLATILE_PREPARE
                    : NKikimrDataEvents::TEvWrite::MODE_PREPARE));

            if (isRollback) {
                FillEvWriteRollback(evWrite.get(), shardId, TxManager);
            } else {
                YQL_ENSURE(TxId);
                FillEvWritePrepare(evWrite.get(), shardId, *TxId, TxManager);
            }

            SendTime[shardId] = TInstant::Now();
            CA_LOG_D("Send EvWrite (external) to ShardID=" << shardId << ", isPrepare=" << !isRollback << ", isImmediateCommit=" << isRollback << ", TxId=" << evWrite->Record.GetTxId()
            << ", LockTxId=" << evWrite->Record.GetLockTxId() << ", LockNodeId=" << evWrite->Record.GetLockNodeId()
            << ", Locks= " << [&]() {
                TStringBuilder builder;
                for (const auto& lock : evWrite->Record.GetLocks().GetLocks()) {
                    builder << lock.ShortDebugString();
                }
                return builder;
            }()
            << ", Size=" << 0 << ", Cookie=" << 0
            << ", OperationsCount=" << 0 << ", IsFinal=" << 1
            << ", Attempts=" << 0);

            // TODO: Track latecy
            Send(
                NKikimr::MakePipePerNodeCacheID(false),
                new TEvPipeCache::TEvForward(evWrite.release(), shardId, /* subscribe */ true),
                IEventHandle::FlagTrackDelivery,
                0);
        }
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

        for (auto& [tabletId, t] : topicTxs) {
            auto& transaction = t.tx;
            
            if (!isImmediateCommit) {
                FillTopicsCommit(transaction, TxManager);
            }

            if (t.hasWrite && writeId.Defined()) {
                auto* w = transaction.MutableWriteId();
                w->SetNodeId(SelfId().NodeId());
                w->SetKeyId(*writeId);
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
            auto traceId = BufferWriteActor.GetTraceId();

            CA_LOG_D("Executing KQP transaction on topic tablet: " << tabletId
            << ", writeId: " << writeId << ", isImmediateCommit: " << isImmediateCommit);

            Send(
                MakePipePerNodeCacheID(false),
                new TEvPipeCache::TEvForward(ev.release(), tabletId, /* subscribe */ true),
                IEventHandle::FlagTrackDelivery,
                0,
                std::move(traceId));
        }
    }

    void SendCommitToCoordinator() {
        const auto commitInfo = TxManager->GetCommitInfo();

        auto ev = MakeHolder<TEvTxProxy::TEvProposeTransaction>();

        YQL_ENSURE(commitInfo.Coordinator);
        ev->Record.SetCoordinatorID(commitInfo.Coordinator);

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
            Y_ABORT_UNLESS(shardInfo.AffectedFlags != 0);
            item.SetFlags(shardInfo.AffectedFlags);
        }

        //TODO: NDataIntegrity
        CA_LOG_D("Execute planned transaction, coordinator: " << commitInfo.Coordinator
            << ", volitale: " << ((transaction.GetFlags() & TEvTxProxy::TEvProposeTransaction::FlagVolatile) != 0)
            << ", shards: " << affectedSet.size());
        Send(MakePipePerNodeCacheID(false), new TEvPipeCache::TEvForward(ev.Release(), commitInfo.Coordinator, /* subscribe */ true));
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
                break;

            case TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusOutdated:
            case TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusDeclined:
            case TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusDeclinedNoSpace:
            case TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusRestarting:
                TxProxyMon->ClientTxStatusCoordinatorDeclined->Inc();
                ReplyErrorAndDie(TStringBuilder() << "Failed to plan transaction, status: " << res->GetStatus(), NYql::NDqProto::StatusIds::UNAVAILABLE, {});
                break;

            case TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusUnknown:
            case TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusAborted:
                TxProxyMon->ClientTxStatusCoordinatorDeclined->Inc();
                ReplyErrorAndDie(TStringBuilder() << "Unexpected TEvProposeTransactionStatus status: " << res->GetStatus(), NYql::NDqProto::StatusIds::INTERNAL_ERROR, {});
                break;
        }
    }

    void Handle(TEvPersQueue::TEvProposeTransactionResult::TPtr& ev) {
        auto& event = ev->Get()->Record;
        const ui64 tabletId = event.GetOrigin();
        
        CA_LOG_D("Got ProposeTransactionResult" <<
              ", PQ tablet: " << tabletId <<
              ", status: " << NKikimrPQ::TEvProposeTransactionResult_EStatus_Name(event.GetStatus()));

        switch (event.GetStatus()) {
        case NKikimrPQ::TEvProposeTransactionResult::PREPARED:
            ProcessPreparedTopic(ev);
            return;
        case NKikimrPQ::TEvProposeTransactionResult::COMPLETE:
            ProcessCompletedTopic(ev);
            return;
        case NKikimrPQ::TEvProposeTransactionResult::ABORTED:
            CA_LOG_E("Got ABORTED ProposeTransactionResult for PQ."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << ".");
            ReplyErrorAndDie(
                TStringBuilder() << "Aborted proposal status for PQ. ",
                NYql::NDqProto::StatusIds::ABORTED,
                {});
            return;
        case NKikimrPQ::TEvProposeTransactionResult::BAD_REQUEST:
            CA_LOG_E("Got BAD REQUEST ProposeTransactionResult for PQ."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << ".");
            ReplyErrorAndDie(
                TStringBuilder() << "Bad request proposal status for PQ. ",
                NYql::NDqProto::StatusIds::BAD_REQUEST,
                {});
            return;
        case NKikimrPQ::TEvProposeTransactionResult::OVERLOADED:
            CA_LOG_E("Got OVERLOADED ProposeTransactionResult for PQ."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << ".");
            ReplyErrorAndDie(
                TStringBuilder() << "Overloaded proposal status for PQ. ",
                NYql::NDqProto::StatusIds::OVERLOADED,
                {});
            return;
        case NKikimrPQ::TEvProposeTransactionResult::CANCELLED:
            CA_LOG_E("Got CANCELLED ProposeTransactionResult for PQ."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << ".");
            ReplyErrorAndDie(
                TStringBuilder() << "Cancelled proposal status for PQ. ",
                NYql::NDqProto::StatusIds::CANCELLED,
                {});
            return;
        default:
            CA_LOG_E("Got undefined ProposeTransactionResult for PQ."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << ".");
            ReplyErrorAndDie(
                TStringBuilder() << "Undefined proposal status for PQ. ",
                NYql::NDqProto::StatusIds::INTERNAL_ERROR,
                {});
            return;
        }
    }

    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
        CA_LOG_W("TEvDeliveryProblem was received from tablet: " << ev->Get()->TabletId);
        ReplyErrorAndDie(TStringBuilder() << "Failed to deviler message.", NYql::NDqProto::StatusIds::UNAVAILABLE, {});
    }

    void Handle(TEvKqpBuffer::TEvTerminate::TPtr&) {
        PassAway();
    }

    void Handle(TEvKqpBuffer::TEvFlush::TPtr& ev) {
        ExecuterActorId = ev->Get()->ExecuterActorId;
        for (auto& [_, info] : WriteInfos) {
            info.WriteTableActor->FlushBuffers();
        }
        Flush();
    }

    void Handle(TEvKqpBuffer::TEvCommit::TPtr& ev) {
        ExecuterActorId = ev->Get()->ExecuterActorId;
        for (auto& [_, info] : WriteInfos) {
            info.WriteTableActor->FlushBuffers();
        }

        if (!TxManager->NeedCommit()) {
            Rollback();
            State = EState::FINISHED;
            Send(ExecuterActorId, new TEvKqpBuffer::TEvResult{});
        } else if (TxManager->IsSingleShard() && !TxManager->HasOlapTable() && (!WriteInfos.empty() || TxManager->HasTopics())) {
            TxManager->StartExecute();
            ImmediateCommit();
        } else {
            TxManager->StartPrepare();
            Prepare(ev->Get()->TxId);
        }
    }

    void Handle(TEvKqpBuffer::TEvRollback::TPtr& ev) {
        ExecuterActorId = ev->Get()->ExecuterActorId;
        Rollback();
        State = EState::FINISHED;
        Send(ExecuterActorId, new TEvKqpBuffer::TEvResult{});
    }

    void Handle(NKikimr::NEvents::TDataEvents::TEvWriteResult::TPtr& ev) {
        auto getIssues = [&ev]() {
            NYql::TIssues issues;
            NYql::IssuesFromMessage(ev->Get()->Record.GetIssues(), issues);
            return issues;
        };

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

        // TODO: get rid of copy-paste
        switch (ev->Get()->GetStatus()) {
        case NKikimrDataEvents::TEvWriteResult::STATUS_UNSPECIFIED: {
            CA_LOG_E("Got UNSPECIFIED for table."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << "."
                    << getIssues().ToOneLineString());
            ReplyErrorAndDie(
                TStringBuilder() << "Unspecified error for table. "
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
            CA_LOG_E("Got ABORTED for table."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << "."
                    << getIssues().ToOneLineString());
            ReplyErrorAndDie(
                TStringBuilder() << "Aborted for table. "
                    << getIssues().ToOneLineString(),
                NYql::NDqProto::StatusIds::ABORTED,
                getIssues());
            return;
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_WRONG_SHARD_STATE: {
            CA_LOG_E("Got WRONG SHARD STATE for table."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << "."
                    << getIssues().ToOneLineString());
            ReplyErrorAndDie(
                TStringBuilder() << "Wrong shard state for table. "
                    << getIssues().ToOneLineString(),
                NYql::NDqProto::StatusIds::INTERNAL_ERROR,
                getIssues());
            return;
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_INTERNAL_ERROR: {
            CA_LOG_E("Got INTERNAL ERROR for table."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << "."
                    << getIssues().ToOneLineString());
            ReplyErrorAndDie(
                TStringBuilder() << "Internal error for table. "
                    << getIssues().ToOneLineString(),
                NYql::NDqProto::StatusIds::INTERNAL_ERROR,
                getIssues());
            return;
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_DISK_SPACE_EXHAUSTED: {
            CA_LOG_E("Got DISK_SPACE_EXHAUSTED for table."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << "."
                    << getIssues().ToOneLineString());
            ReplyErrorAndDie(
                TStringBuilder() << "Disk space exhausted for table. "
                    << getIssues().ToOneLineString(),
                NYql::NDqProto::StatusIds::UNAVAILABLE,
                getIssues());
                return;
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_OVERLOADED: {
            CA_LOG_W("Got OVERLOADED for table ."
                << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                << " Sink=" << this->SelfId() << "."
                << " Ignored this error."
                << getIssues().ToOneLineString());
            ReplyErrorAndDie(
                TStringBuilder() << "Tablet " << ev->Get()->Record.GetOrigin() << " is overloaded."
                    << getIssues().ToOneLineString(),
                NYql::NDqProto::StatusIds::OVERLOADED,
                getIssues());
            return;
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_CANCELLED: {
            CA_LOG_E("Got CANCELLED for table."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << "."
                    << getIssues().ToOneLineString());
            ReplyErrorAndDie(
                TStringBuilder() << "Cancelled request to table."
                    << getIssues().ToOneLineString(),
                NYql::NDqProto::StatusIds::CANCELLED,
                getIssues());
            return;
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST: {
            CA_LOG_E("Got BAD REQUEST for table."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << "."
                    << getIssues().ToOneLineString());
            ReplyErrorAndDie(
                TStringBuilder() << "Bad request. "
                    << getIssues().ToOneLineString(),
                NYql::NDqProto::StatusIds::BAD_REQUEST,
                getIssues());
            return;
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_SCHEME_CHANGED: {
            CA_LOG_E("Got SCHEME CHANGED for table."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << "."
                    << getIssues().ToOneLineString());
            ReplyErrorAndDie(
                TStringBuilder() << "Scheme changed. "
                    << getIssues().ToOneLineString(),
                NYql::NDqProto::StatusIds::SCHEME_ERROR,
                getIssues());
            return;
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_LOCKS_BROKEN: {
            CA_LOG_E("Got LOCKS BROKEN for table."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << "."
                    << getIssues().ToOneLineString());

            TxManager->BreakLock(ev->Get()->Record.GetOrigin());
            YQL_ENSURE(TxManager->BrokenLocks());
            ReplyErrorAndDie(
                TStringBuilder() << "Transaction locks invalidated."
                    << getIssues().ToOneLineString(),
                NYql::NDqProto::StatusIds::ABORTED,
                getIssues());
            return;
        }
        }
    }

    void OnMessageReceived(const ui64 shardId) {
        if (auto it = SendTime.find(shardId); it != std::end(SendTime)) {
            Counters->WriteActorWritesLatencyHistogram->Collect((TInstant::Now() - it->second).MilliSeconds());
            SendTime.erase(it);
        }
    }

    void ProcessPreparedTopic(TEvPersQueue::TEvProposeTransactionResult::TPtr& ev) {
        if (State != EState::PREPARING) {
            CA_LOG_D("Ignored topic prepared event.");
            return;
        }
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

        if (State != EState::COMMITTING) {
            CA_LOG_D("Ignored completed event.");
            return;
        }
        OnMessageReceived(event.GetOrigin());
        CA_LOG_D("Got propose completed result" <<
              ", topic tablet: " << event.GetOrigin() <<
              ", status: " << NKikimrPQ::TEvProposeTransactionResult_EStatus_Name(event.GetStatus()));

        OnCommitted(event.GetOrigin(), 0);
    }

    void ProcessWritePreparedShard(NKikimr::NEvents::TDataEvents::TEvWriteResult::TPtr& ev) {
        if (State != EState::PREPARING) {
            CA_LOG_D("Ignored write prepared event.");
            return;
        }
        OnMessageReceived(ev->Get()->Record.GetOrigin());
        CA_LOG_D("Got prepared result TxId=" << ev->Get()->Record.GetTxId()
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

    void ProcessWriteCompletedShard(NKikimr::NEvents::TDataEvents::TEvWriteResult::TPtr& ev) {
        if (State != EState::COMMITTING) {
            CA_LOG_D("Ignored write completed event.");
            return;
        }
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

        OnCommitted(ev->Get()->Record.GetOrigin(), 0);
    }

    void OnReady() override {
        Process();
    }

    void OnPrepared(IKqpTransactionManager::TPrepareResult&& preparedInfo, ui64 dataSize) override {
        if (State != EState::PREPARING) {
            return;
        }
        Y_UNUSED(preparedInfo, dataSize);
        if (TxManager->ConsumePrepareTransactionResult(std::move(preparedInfo))) {
            TxManager->StartExecute();
            Y_ABORT_UNLESS(GetTotalMemory() == 0);
            DistributedCommit();
            return;
        }
        Process();
    }

    void OnCommitted(ui64 shardId, ui64 dataSize) override {
        if (State != EState::COMMITTING) {
            return;
        }
        Y_UNUSED(dataSize);
        if (TxManager->ConsumeCommitResult(shardId)) {
            CA_LOG_D("Committed");
            State = EState::FINISHED;
            Send(ExecuterActorId, new TEvKqpBuffer::TEvResult{
                BuildStats()
            });
            ExecuterActorId = {};
            Y_ABORT_UNLESS(GetTotalMemory() == 0);
            return;
        }
    }

    void OnMessageAcknowledged(ui64 dataSize) override {
        Y_UNUSED(dataSize);
        Process();
    }

    void OnFlushed() {
        BufferWriteActorState = NWilson::TSpan(TWilsonKqp::BufferWriteActorState, BufferWriteActor.GetTraceId(),
            "BufferWriteActorState::Writing", NWilson::EFlags::AUTO_END);
        CA_LOG_D("Flushed");
        State = EState::WRITING;
        Send(ExecuterActorId, new TEvKqpBuffer::TEvResult{
            BuildStats()
        });
        ExecuterActorId = {};
        Y_ABORT_UNLESS(GetTotalMemory() == 0);
    }

    void OnError(const TString& message, NYql::NDqProto::StatusIds::StatusCode statusCode, const NYql::TIssues& subIssues) override {
        ReplyErrorAndDie(message, statusCode, subIssues);
    }

    void ReplyErrorAndDie(const TString& message, NYql::NDqProto::StatusIds::StatusCode statusCode, const NYql::TIssues& subIssues = {}) {
        BufferWriteActorState.EndError(message);
        BufferWriteActor.EndError(message);
        CA_LOG_E(message << ". statusCode=" << NYql::NDqProto::StatusIds_StatusCode_Name(statusCode) << ". subIssues=" << subIssues.ToString() << ". sessionActorId=" << SessionActorId << ". isRollback=" << (State == EState::ROLLINGBACK));

        Y_ABORT_UNLESS(!HasError);
        HasError = true;
        if (State != EState::ROLLINGBACK) {
            // Rollback can't finish with error
            Send(SessionActorId, new TEvKqpBuffer::TEvError{
                message,
                statusCode,
                subIssues,
            });
        }
        PassAway();
    }

    NYql::NDqProto::TDqTaskStats BuildStats() {
        NYql::NDqProto::TDqTaskStats result;
        for (const auto& [_, writeInfo] : WriteInfos) {
            writeInfo.WriteTableActor->FillStats(&result);
        }
        return result;
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

    std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> Alloc;
    NMiniKQL::TTypeEnvironment TypeEnv;

    struct TWriteInfo {
        TKqpTableWriteActor* WriteTableActor = nullptr;
        TActorId WriteTableActorId;
    };

    THashMap<TTableId, TWriteInfo> WriteInfos;

    EState State;
    bool HasError = false;
    THashMap<TTableId, std::queue<TBufferWriteMessage>> DataQueues;

    struct TAckMessage {
        TActorId ForwardActorId;
        TWriteToken Token;
        i64 DataSize;
    };
    std::queue<TAckMessage> AckQueue;

    IShardedWriteControllerPtr ShardedWriteController = nullptr;

    TIntrusivePtr<TKqpCounters> Counters;
    TIntrusivePtr<NTxProxy::TTxProxyMon> TxProxyMon;
    THashMap<ui64, TInstant> SendTime;

    NWilson::TSpan BufferWriteActor;
    NWilson::TSpan BufferWriteActorState;
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
        , BufferActorId(ActorIdFromProto(Settings.GetBufferActorId()))
        , TxId(std::get<ui64>(args.TxId))
        , TableId(
            Settings.GetTable().GetOwnerId(),
            Settings.GetTable().GetTableId(),
            Settings.GetTable().GetVersion())
        , ForwardWriteActorSpan(TWilsonKqp::ForwardWriteActor, NWilson::TTraceId(args.TraceId), "TKqpForwardWriteActor")
    {
        EgressStats.Level = args.StatsLevel;
        Counters->ForwardActorsCount->Inc();

        TVector<NKikimrKqp::TKqpColumnMetadataProto> columnsMetadata(
            Settings.GetColumns().begin(),
            Settings.GetColumns().end());
        std::vector<ui32> writeIndex(
            Settings.GetWriteIndexes().begin(),
            Settings.GetWriteIndexes().end());
        if (Settings.GetIsOlap()) {
            Batcher = CreateColumnDataBatcher(columnsMetadata, std::move(writeIndex));
        } else {
            Batcher = CreateRowDataBatcher(columnsMetadata, std::move(writeIndex));
        }
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
        CA_LOG_D("TKqpForwardWriteActor recieve EvBufferWriteResult from " << BufferActorId);
        EgressStats.Bytes += DataSize;
        EgressStats.Chunks++;
        EgressStats.Splits++;
        EgressStats.Resume();

        Counters->ForwardActorWritesSizeHistogram->Collect(DataSize);
        Counters->ForwardActorWritesLatencyHistogram->Collect((TInstant::Now() - SendTime).MilliSeconds());

        WriteToken = result->Get()->Token;
        DataSize = 0;

        if (Closed) {
            CA_LOG_D("Finished");
            Callbacks->OnAsyncOutputFinished(GetOutputIndex());
            return;
        }
        CA_LOG_D("Resume with freeSpace=" << GetFreeSpace());
        Callbacks->ResumeExecution();
    }

    void WriteToBuffer() {
        auto ev = std::make_unique<TEvBufferWrite>();

        ev->Data = Batcher->Build();
        ev->Close = Closed;

        if (!WriteToken.IsEmpty()) {
            ev->Token = WriteToken;
        } else {
            TVector<NKikimrKqp::TKqpColumnMetadataProto> keyColumnsMetadata(
                Settings.GetKeyColumns().begin(),
                Settings.GetKeyColumns().end());
            TVector<NKikimrKqp::TKqpColumnMetadataProto> columnsMetadata(
                Settings.GetColumns().begin(),
                Settings.GetColumns().end());
            std::vector<ui32> writeIndex(
                Settings.GetWriteIndexes().begin(),
                Settings.GetWriteIndexes().end());

            ev->Settings = TWriteSettings{
                .TableId = TableId,
                .TablePath = Settings.GetTable().GetPath(),
                .OperationType = GetOperation(Settings.GetType()),
                .KeyColumns = std::move(keyColumnsMetadata),
                .Columns = std::move(columnsMetadata),
                .WriteIndex = std::move(writeIndex),
                .TransactionSettings = TTransactionSettings{
                    .TxId = TxId,
                    .LockTxId = Settings.GetLockTxId(),
                    .LockNodeId = Settings.GetLockNodeId(),
                    .InconsistentTx = Settings.GetInconsistentTx(),
                    .MvccSnapshot = Settings.GetMvccSnapshot(),
                    .LockMode = Settings.GetLockMode(),
                },
                .Priority = Settings.GetPriority(),
                .IsOlap = Settings.GetIsOlap(),
            };
        }

        SendTime = TInstant::Now();

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
        TActorBootstrapped<TKqpForwardWriteActor>::PassAway();
    }

    TString LogPrefix;
    const NKikimrKqp::TKqpTableSinkSettings Settings;
    TWriteActorSettings MessageSettings;
    const ui64 OutputIndex;
    NYql::NDq::TDqAsyncStats EgressStats;
    NYql::NDq::IDqComputeActorAsyncOutput::ICallbacks * Callbacks = nullptr;
    TIntrusivePtr<TKqpCounters> Counters;

    TActorId BufferActorId;
    IDataBatcherPtr Batcher;

    i64 DataSize = 0;
    bool Closed = false;

    const ui64 TxId;
    const TTableId TableId;

    TInstant SendTime;

    TWriteToken WriteToken;
    NWilson::TSpan ForwardWriteActorSpan;
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
