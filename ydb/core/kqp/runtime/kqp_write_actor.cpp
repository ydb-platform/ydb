#include "kqp_write_actor.h"

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/engine/minikql/minikql_engine_host.h>
#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/actorlib_impl/long_timer.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_impl.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/core/tx/data_events/events.h>
#include <ydb/core/formats/arrow/arrow_batch_builder.h>
#include <ydb/core/tx/data_events/payload_helper.h>
#include <ydb/core/tx/tx.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <util/generic/singleton.h>
#include <ydb/core/tx/data_events/shards_splitter.h>

namespace {
    struct TWriteActorBackoffSettings {
        TDuration StartRetryDelay = TDuration::MilliSeconds(100);
        TDuration MaxRetryDelay = TDuration::Seconds(5);
        double UnsertaintyRatio = 0.5;
        double Multiplier = 2.0;

        ui64 MaxWriteAttempts = 16;
    };

    const TWriteActorBackoffSettings* BackoffSettings() {
        return Singleton<TWriteActorBackoffSettings>();
    }

    TDuration CalculateNextAttemptDelay(ui64 attempt) {
        auto delay = BackoffSettings()->StartRetryDelay;
        for (ui64 index = 0; index < attempt; ++index) {
            delay *= BackoffSettings()->Multiplier;
        }

        delay *= 1 + BackoffSettings()->UnsertaintyRatio * (1 - 2 * RandomNumber<double>());
        delay = Min(delay, BackoffSettings()->MaxRetryDelay);

        return delay;
    }
}

namespace NKikimr {
namespace NKqp {

class TKqpWriteActor : public TActorBootstrapped<TKqpWriteActor>, public NYql::NDq::IDqComputeActorAsyncOutput {
    using TBase = TActorBootstrapped<TKqpWriteActor>;

    struct TEvPrivate {
        enum EEv {
            EvShardRequestTimeout = EventSpaceBegin(TKikimrEvents::ES_PRIVATE),
        };

        struct TEvShardRequestTimeout : public TEventLocal<TEvShardRequestTimeout, EvShardRequestTimeout> {
            ui64 ShardId;
            ui64 TxId;

            TEvShardRequestTimeout(ui64 shardId, ui64 txId)
                : ShardId(shardId)
                , TxId(txId) {
            }
        };
    };

public:
    TKqpWriteActor(
        NKikimrKqp::TKqpTableSinkSettings&& settings,
        NYql::NDq::TDqAsyncIoFactory::TSinkArguments&& args,
        TIntrusivePtr<TKqpCounters> counters)
        : LogPrefix(TStringBuilder() << "TxId: " << args.TxId << ", task: " << args.TaskId << ". ")
        , Settings(std::move(settings))
        , OutputIndex(args.OutputIndex)
        , Callbacks(args.Callback)
        , Counters(counters)
        , TypeEnv(args.TypeEnv)
        , Alloc(args.Alloc)
        , TxId(args.TxId)
        , TableId(Settings.GetTable().GetOwnerId(), Settings.GetTable().GetTableId())
    {
        YQL_ENSURE(std::holds_alternative<ui64>(TxId));
        Y_UNUSED(Settings, Counters, LogPrefix, TxId, TableId);
        EgressStats.Level = args.StatsLevel;

        BuildColumns();
        PrepareBatchBuilder();
    }

    void Bootstrap() {
        LogPrefix = TStringBuilder() << "SelfId: " << this->SelfId() << ", " << LogPrefix;
        ResolveTable();
        Become(&TKqpWriteActor::StateFunc);
    }

    static constexpr char ActorName[] = "KQP_WRITE_ACTOR";

private:
    virtual ~TKqpWriteActor() {
    }

    TGuard<NKikimr::NMiniKQL::TScopedAlloc> BindAllocator() {
        return TypeEnv.BindAllocator();
    }

    void CommitState(const NYql::NDqProto::TCheckpoint&) final {};
    void LoadState(const NYql::NDqProto::TSinkState&) final {};

    ui64 GetOutputIndex() const final {
        return OutputIndex;
    }

    const NYql::NDq::TDqAsyncStats& GetEgressStats() const final {
        return EgressStats;
    }

    i64 GetFreeSpace() const final {
        return MemoryLimit - MemoryUsed;
    }

    void SendData(NMiniKQL::TUnboxedValueBatch&& data, i64 dataSize, const TMaybe<NYql::NDqProto::TCheckpoint>&, bool finished) final {
        Y_UNUSED(dataSize, finished);
        YQL_ENSURE(!data.IsWide(), "Wide stream is not supported yet");

        CA_LOG_D("SendData: " << dataSize << "  " << finished);
        //EgressStats.Resume();
        
        AddToInputQueue(std::move(data));
        Finished = finished;

        ProcessRows();
    }

    void AddToInputQueue(NMiniKQL::TUnboxedValueBatch&& data) {
        TVector<TCell> cells(Columns.size());
        data.ForEachRow([&](const auto& row) {
            for (size_t index = 0; index < Columns.size(); ++index) {
                cells[index] = MakeCell(
                    Columns[index].PType,
                    row.GetElement(index),
                    TypeEnv,
                    /* copy */ true);
                // MemoryUsed += cells[index].Size();
            }
            BatchBuilder->AddRow(
                TConstArrayRef<TCell>{cells.begin(), cells.begin() + KeyColumns.size()},
                TConstArrayRef<TCell>{cells.begin() + KeyColumns.size(), cells.end()});
        });
    }

    STFUNC(StateFunc) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(NKikimr::NEvents::TDataEvents::TEvWriteResult, HandleWrite);
                hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleResolve);
                hFunc(TEvPipeCache::TEvDeliveryProblem, HandleError);
                hFunc(TEvTxUserProxy::TEvAllocateTxIdResult, HandleNewTxId);
                hFunc(TEvPrivate::TEvShardRequestTimeout, HandleTimeout);
                IgnoreFunc(TEvInterconnect::TEvNodeConnected);
                IgnoreFunc(TEvTxProxySchemeCache::TEvInvalidateTableResult);
            }
        } catch (const yexception& e) {
            RuntimeError(e.what(), NYql::NDqProto::StatusIds::INTERNAL_ERROR);
        }
    }

    void ResolveTable() {
        CA_LOG_D("Resolve TableId=" << TableId);
        TAutoPtr<NSchemeCache::TSchemeCacheNavigate> request(new NSchemeCache::TSchemeCacheNavigate());
        NSchemeCache::TSchemeCacheNavigate::TEntry entry;
        entry.TableId = TableId;
        entry.RequestType = NSchemeCache::TSchemeCacheNavigate::TEntry::ERequestType::ByTableId;
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpTable;
        entry.SyncVersion = true;
        request->ResultSet.emplace_back(entry);
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request));
    }

    void HandleResolve(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        if (ev->Get()->Request->ErrorCount > 0) {
            RuntimeError(TStringBuilder() << "Failed to get table: "
                << TableId << "'", NYql::NDqProto::StatusIds::SCHEME_ERROR);
        }

        auto& resultSet = ev->Get()->Request->ResultSet;
        YQL_ENSURE(resultSet.size() == 1);
        SchemeEntry = resultSet[0];

        YQL_ENSURE(SchemeEntry->Kind == NSchemeCache::TSchemeCacheNavigate::KindColumnTable);

        CA_LOG_D("Resolved TableId=" << TableId << " ");

        ProcessRows();
    }

    void HandleWrite(NKikimr::NEvents::TDataEvents::TEvWriteResult::TPtr& ev) {
        CA_LOG_D("GotResult: " << ev->Get()->IsError() << " " << ev->Get()->IsPrepared() << " " << ev->Get()->IsComplete() << " :: " << ev->Get()->GetError());
        CA_LOG_D("TxId > " << ev->Get()->Record.GetTxId());
        CA_LOG_D("Steps > [" << ev->Get()->Record.GetMinStep() << " , " << ev->Get()->Record.GetMaxStep() << "]");
        if (ev->Get()->GetStatus() == NKikimrDataEvents::TEvWriteResult::STATUS_PREPARED) {
            // Until LockIds are supported by columnshards we have to commit each write using separate transaction. 
            ui64 coordinator = 0;
            if (ev->Get()->Record.DomainCoordinatorsSize()) {
                auto domainCoordinators = TCoordinators(TVector<ui64>(
                    ev->Get()->Record.GetDomainCoordinators().begin(),
                    ev->Get()->Record.GetDomainCoordinators().end()));
                coordinator = domainCoordinators.Select(ev->Get()->Record.GetTxId());
            }

            YQL_ENSURE(coordinator != 0);

            auto evTnx = MakeHolder<TEvTxProxy::TEvProposeTransaction>();
            evTnx->Record.SetCoordinatorID(coordinator);

            auto& transaction = *evTnx->Record.MutableTransaction();
            auto& affectedSet = *transaction.MutableAffectedSet();

            auto& item = *affectedSet.Add();
            item.SetTabletId(ev->Get()->Record.GetOrigin());
            item.SetFlags(TEvTxProxy::TEvProposeTransaction::AffectedWrite);

            transaction.SetTxId(ev->Get()->Record.GetTxId());
            transaction.SetMinStep(ev->Get()->Record.GetMinStep());
            transaction.SetMaxStep(ev->Get()->Record.GetMaxStep());

            Send(
                PipeCacheId,
                new TEvPipeCache::TEvForward(evTnx.Release(), coordinator, /* subscribe */ true),
                IEventHandle::FlagTrackDelivery);
        } else if (ev->Get()->GetStatus() == NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED) {
            // TODO: fix it using some cookie in EvWrite.
            // Can loose data here. Duplicate completed for duplicate writes.
            InflightBatches.at(ev->Get()->Record.GetOrigin()).pop_front();
        }

        ProcessRows();
    }

    void NotifyCAResume() {
        if (GetFreeSpace() > 0) {
            Callbacks->ResumeExecution();
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
        TActorBootstrapped<TKqpWriteActor>::PassAway();
    }

    void BuildColumns() {
        KeyColumns.reserve(Settings.KeyColumnsSize());
        i32 number = 0;
        for (const auto& column : Settings.GetKeyColumns()) {
            KeyColumns.emplace_back(
                column.GetName(),
                column.GetId(),
                NScheme::TTypeInfo {
                    static_cast<NScheme::TTypeId>(column.GetTypeId()),
                    column.GetTypeId() == NScheme::NTypeIds::Pg
                        ? NPg::TypeDescFromPgTypeId(column.GetTypeInfo().GetPgTypeId())
                        : nullptr
                },
                column.GetTypeInfo().GetPgTypeMod(),
                number++
            );
        }

        ColumnIds.reserve(Settings.ColumnsSize());
        Columns.reserve(Settings.ColumnsSize());
        number = 0;
        for (const auto& column : Settings.GetColumns()) {
            ColumnIds.push_back(column.GetId());
            Columns.emplace_back(
                column.GetName(),
                column.GetId(),
                NScheme::TTypeInfo {
                    static_cast<NScheme::TTypeId>(column.GetTypeId()),
                    column.GetTypeId() == NScheme::NTypeIds::Pg
                        ? NPg::TypeDescFromPgTypeId(column.GetTypeInfo().GetPgTypeId())
                        : nullptr
                },
                column.GetTypeInfo().GetPgTypeMod(),
                number++
            );
        }
    }

    void PrepareBatchBuilder() {
        std::vector<std::pair<TString, NScheme::TTypeInfo>> columns;
        for (const auto& column : Columns) {
            columns.emplace_back(column.Name, column.PType);
        }
        std::set<std::string> notNullColumns;
        for (const auto& column : Settings.GetColumns()) {
            if (column.GetNotNull()) {
                notNullColumns.insert(column.GetName());
            }
        }

        BatchBuilder = std::make_unique<NArrow::TArrowBatchBuilder>(arrow::Compression::UNCOMPRESSED, notNullColumns);

        TString err;
        if (!BatchBuilder->Start(columns, 0, 0, err)) {
            RuntimeError("Failed to start batch builder: " + err, NYql::NDqProto::StatusIds::PRECONDITION_FAILED);
        }
    }

    void ProcessRows() {
        SplitBatchByShards();
        SendNewBatchesToShards();

        if (Finished && IsInflightBatchesEmpty()) {
            Callbacks->OnAsyncOutputFinished(GetOutputIndex());
        }
    }

    NKikimr::NEvWrite::IShardsSplitter::IEvWriteDataAccessor::TPtr GetDataAccessor(
            const std::shared_ptr<arrow::RecordBatch>& batch) const {
        struct TDataAccessor : public NKikimr::NEvWrite::IShardsSplitter::IEvWriteDataAccessor {
            std::shared_ptr<arrow::RecordBatch> Batch;

            TDataAccessor(const std::shared_ptr<arrow::RecordBatch>& batch)
                : Batch(batch) {
            }

            std::shared_ptr<arrow::RecordBatch> GetDeserializedBatch() const override {
                return Batch;
            }

            TString GetSerializedData() const override {
                return NArrow::SerializeBatchNoCompression(Batch);
            }
        };

        return std::make_shared<TDataAccessor>(batch);
    }

    void SplitBatchByShards() {
        if (!SchemeEntry || BatchBuilder->Bytes() == 0) {
            return;
        }

        auto shardsSplitter = NKikimr::NEvWrite::IShardsSplitter::BuildSplitter(*SchemeEntry);
        YQL_ENSURE(shardsSplitter);

        const auto dataAccessor = GetDataAccessor(BatchBuilder->FlushBatch(true));
        auto initStatus = shardsSplitter->SplitData(*SchemeEntry, *dataAccessor);
        if (!initStatus.Ok()) {
            RuntimeError(
                initStatus.GetErrorMessage(),
                NYql::NDqProto::StatusIds::INTERNAL_ERROR);
        }

        const auto& splittedData = shardsSplitter->GetSplitData();

        for (auto& [shard, infos] : splittedData.GetShardsInfo()) {
            for (auto&& shardInfo : infos) {
                //sumBytes += shardInfo->GetBytes();
                //rowsCount += shardInfo->GetRowsCount();
                auto& inflightBatch = InflightBatches[shard].emplace_back();
                inflightBatch.Data = shardInfo->GetData();

                RequestNewTxId();
            }
        }
    }

    void SendNewBatchesToShards() {
        for (auto& [shardId, batches] : InflightBatches) {
            if (!batches.empty() && batches.front().SendAttempts == 0) {
                if (const auto txId = AllocateTxId(); txId) {
                    batches.front().TxId = *txId;
                    SendRequestShard(shardId);
                }
            }
        }
    }

    bool IsInflightBatchesEmpty() const {
        for (const auto& [shardId, batches] : InflightBatches) {
            if (!batches.empty()) {
                return false;
            }
        }
        return true;
    }

    void SendRequestShard(const ui64 shardId) {
        auto& inflightBatch = InflightBatches.at(shardId).front();

        if (inflightBatch.SendAttempts >= BackoffSettings()->MaxWriteAttempts) {
            RuntimeError(
                TStringBuilder() << "ShardId=" << shardId << " for table '" << Settings.GetTable().GetPath() << "' retry limit exceeded",
                NYql::NDqProto::StatusIds::UNAVAILABLE);
        }

        auto evWrite = std::make_unique<NKikimr::NEvents::TDataEvents::TEvWrite>(
            inflightBatch.TxId, NKikimrDataEvents::TEvWrite::MODE_PREPARE);
        ui64 payloadIndex = NKikimr::NEvWrite::TPayloadHelper<NKikimr::NEvents::TDataEvents::TEvWrite>(*evWrite)
            .AddDataToPayload(TString(inflightBatch.Data));

        evWrite->AddOperation(
            NKikimrDataEvents::TEvWrite::TOperation::OPERATION_REPLACE,
            Settings.GetTable().GetTableId(),
            1, // TODO: Settings.GetTable().GetVersion()
            ColumnIds,
            payloadIndex,
            NKikimrDataEvents::FORMAT_ARROW);

        CA_LOG_D("Send EvWrite to ShardID=" << shardId << " TxId=" << inflightBatch.TxId << " data size = " << inflightBatch.Data.size());
        Send(
            PipeCacheId,
            new TEvPipeCache::TEvForward(evWrite.release(), shardId, true),
            IEventHandle::FlagTrackDelivery);

        TlsActivationContext->Schedule(
            CalculateNextAttemptDelay(inflightBatch.SendAttempts),
            new IEventHandle(SelfId(), SelfId(), new TEvPrivate::TEvShardRequestTimeout(shardId, inflightBatch.TxId)));

        ++inflightBatch.SendAttempts;
    }

    void HandleTimeout(TEvPrivate::TEvShardRequestTimeout::TPtr& ev) {
        if (!InflightBatches.contains(ev->Get()->ShardId)) {
            return;
        }
        if (InflightBatches.at(ev->Get()->ShardId).empty()) {
            return;
        }
        if (InflightBatches.at(ev->Get()->ShardId).front().TxId != ev->Get()->TxId) {
            return;
        }

        SendRequestShard(ev->Get()->ShardId);
    }

    void HandleError(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
        CA_LOG_D("TEvDeliveryProblem was received from tablet: " << ev->Get()->TabletId);
        if (!InflightBatches.contains(ev->Get()->TabletId)) {
            return;
        }
        SendRequestShard(ev->Get()->TabletId);
    }

    void HandleNewTxId(TEvTxUserProxy::TEvAllocateTxIdResult::TPtr& ev) {
        FreeTxIds.push_back(ev->Get()->TxId);
        ProcessRows();
    }

    void RequestNewTxId() {
        Send(MakeTxProxyID(), new TEvTxUserProxy::TEvAllocateTxId);
    }

    std::optional<ui64> AllocateTxId() {
        if (FreeTxIds.empty()) {
            return std::nullopt;
        }
        const ui64 result = FreeTxIds.back();
        FreeTxIds.pop_back();
        return result;
    }

    NActors::TActorId PipeCacheId = NKikimr::MakePipePeNodeCacheID(false);

    TString LogPrefix;
    const NKikimrKqp::TKqpTableSinkSettings Settings;
    const ui64 OutputIndex;
    NYql::NDq::TDqAsyncStats EgressStats;
    NYql::NDq::IDqComputeActorAsyncOutput::ICallbacks * Callbacks = nullptr;
    TIntrusivePtr<TKqpCounters> Counters;
    const NMiniKQL::TTypeEnvironment& TypeEnv;
    std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> Alloc;

    const NYql::NDq::TTxId TxId;
    const TTableId TableId;

    TVector<TSysTables::TTableColumnInfo> KeyColumns;
    TVector<TSysTables::TTableColumnInfo> Columns;
    std::vector<ui32> ColumnIds;

    const i64 MemoryLimit = 1024 * 1024 * 1024;
    i64 MemoryUsed = 0;

    std::optional<NSchemeCache::TSchemeCacheNavigate::TEntry> SchemeEntry;

    std::unique_ptr<NArrow::TArrowBatchBuilder> BatchBuilder;

    struct TInflightBatch {
        TString Data;
        ui32 SendAttempts = 0;
        ui64 TxId = 0;
    };
    THashMap<ui64, std::deque<TInflightBatch>> InflightBatches;
    bool Finished = false;

    std::vector<ui64> FreeTxIds;
};

void RegisterKqpWriteActor(NYql::NDq::TDqAsyncIoFactory& factory, TIntrusivePtr<TKqpCounters> counters) {
    factory.RegisterSink<NKikimrKqp::TKqpTableSinkSettings>(
        TString(NYql::KqpTableSinkName),
        [counters] (NKikimrKqp::TKqpTableSinkSettings&& settings, NYql::NDq::TDqAsyncIoFactory::TSinkArguments&& args) {
            auto* actor = new TKqpWriteActor(std::move(settings), std::move(args), counters);
            return std::make_pair<NYql::NDq::IDqComputeActorAsyncOutput*, NActors::IActor*>(actor, actor);
        });
}

}
}