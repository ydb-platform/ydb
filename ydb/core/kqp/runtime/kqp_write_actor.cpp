#include "kqp_write_actor.h"

#include "kqp_write_table.h"

#include <util/generic/singleton.h>
#include <ydb/core/actorlib_impl/long_timer.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/engine/minikql/minikql_engine_host.h>
#include <ydb/core/formats/arrow/arrow_batch_builder.h>
#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/core/tx/data_events/events.h>
#include <ydb/core/tx/data_events/payload_helper.h>
#include <ydb/core/tx/data_events/shards_splitter.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/tx.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_impl.h>
#include <ydb/core/protos/kqp_physical.pb.h>


namespace {
    struct TWriteActorBackoffSettings {
        TDuration StartRetryDelay = TDuration::MilliSeconds(150);
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

    struct TResumeNotificationManager {
    };

    constexpr i64 kInFlightMemoryLimitPerActor = 1_GB;
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
        , TxId(args.TxId)
        , TableId(
            Settings.GetTable().GetOwnerId(),
            Settings.GetTable().GetTableId(),
            Settings.GetTable().GetVersion())
    {
        YQL_ENSURE(std::holds_alternative<ui64>(TxId));
        EgressStats.Level = args.StatsLevel;
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

    void CommitState(const NYql::NDqProto::TCheckpoint&) final {};
    void LoadState(const NYql::NDqProto::TSinkState&) final {};

    ui64 GetOutputIndex() const final {
        return OutputIndex;
    }

    const NYql::NDq::TDqAsyncStats& GetEgressStats() const final {
        return EgressStats;
    }

    i64 GetFreeSpace() const final {
        return SchemeEntry
            ? MemoryLimit - (MemoryInFlight + BatchBuilder->Bytes())
            : std::numeric_limits<i64>::min(); // Can't use zero here because compute can use overcommit!
    }

    void SendData(NMiniKQL::TUnboxedValueBatch&& data, i64, const TMaybe<NYql::NDqProto::TCheckpoint>&, bool finished) final {
        YQL_ENSURE(!data.IsWide(), "Wide stream is not supported yet");

        EgressStats.Resume();
        AddToInputQueue(std::move(data));
        Finished = finished;

        ProcessRows();
    }

    void AddToInputQueue(NMiniKQL::TUnboxedValueBatch&& data) {
        YQL_ENSURE(SchemeEntry);
        YQL_ENSURE(BatchBuilder);

        TVector<TCell> cells(Columns.size());
        data.ForEachRow([&](const auto& row) {
            for (size_t index = 0; index < Columns.size(); ++index) {
                cells[SendIndexToWriteIndexMapping[index]] = MakeCell(
                    Columns[index].PType,
                    row.GetElement(index),
                    TypeEnv,
                    /* copy */ false);
            }
            BatchBuilder->AddRow(TConstArrayRef<TCell>{cells.begin(), cells.end()});
        });
    }

    STFUNC(StateFunc) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(NKikimr::NEvents::TDataEvents::TEvWriteResult, Handle);
                hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
                hFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
                hFunc(TEvTxUserProxy::TEvAllocateTxIdResult, Handle);
                hFunc(TEvPrivate::TEvShardRequestTimeout, Handle);
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

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        if (ev->Get()->Request->ErrorCount > 0) {
            RuntimeError(TStringBuilder() << "Failed to get table: "
                << TableId << "'", NYql::NDqProto::StatusIds::SCHEME_ERROR);
            return;
        }
        auto& resultSet = ev->Get()->Request->ResultSet;
        YQL_ENSURE(resultSet.size() == 1);
        SchemeEntry = resultSet[0];

        YQL_ENSURE(SchemeEntry->Kind == NSchemeCache::TSchemeCacheNavigate::KindColumnTable);

        CA_LOG_D("Resolved TableId=" << TableId << " (" << SchemeEntry->TableId.PathId.ToString() << " " << SchemeEntry->TableId.SchemaVersion << ")");

        if (SchemeEntry->TableId.SchemaVersion != TableId.SchemaVersion) {
            RuntimeError(TStringBuilder() << "Schema was updated.", NYql::NDqProto::StatusIds::SCHEME_ERROR);
            return;
        }

        Prepare();

        Callbacks->ResumeExecution();
    }

    void Handle(NKikimr::NEvents::TDataEvents::TEvWriteResult::TPtr& ev) {
        if (ev->Get()->GetStatus() == NKikimrDataEvents::TEvWriteResult::STATUS_PREPARED) {
            CA_LOG_D("Got prepared result TxId=" << ev->Get()->Record.GetTxId() << ", TabletId=" << ev->Get()->Record.GetOrigin());
            // Until LockIds are supported by columnshards we have to commit each write
            // using separate coordinated transaction. 
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
            CA_LOG_D("Got completed result TxId=" << ev->Get()->Record.GetTxId() << ", TabletId=" << ev->Get()->Record.GetOrigin());
            auto& batchesQueue = InFlightBatches.at(ev->Get()->Record.GetOrigin());

            if (!batchesQueue.empty()
                && (ev->Get()->Record.GetTxId() == batchesQueue.front().TxId
                    || std::find(
                        std::begin(batchesQueue.front().OldTxIds),
                        std::end(batchesQueue.front().OldTxIds),
                        ev->Get()->Record.GetTxId()) != std::end(batchesQueue.front().OldTxIds))) {
                const bool needToResume = (GetFreeSpace() <= 0);

                EgressStats.Bytes += batchesQueue.front().Data.size();
                EgressStats.Chunks++;
                EgressStats.Splits++;
                EgressStats.Resume();

                MemoryInFlight -= batchesQueue.front().Data.size();
                batchesQueue.pop_front();
                if (needToResume && GetFreeSpace() > 0) {
                    Callbacks->ResumeExecution();
                }
            }
        }

        ProcessRows();
    }

    void ProcessRows() {
        SplitBatchByShards();
        SendBatchesToShards();

        if (Finished && SchemeEntry && IsInFlightBatchesEmpty()) {
            Callbacks->OnAsyncOutputFinished(GetOutputIndex());
        }
    }

    void SplitBatchByShards() {
        if (!SchemeEntry || BatchBuilder->Bytes() == 0) {
            return;
        }

        const bool needToResume = (GetFreeSpace() <= 0);

        auto shardsSplitter = NKikimr::NEvWrite::IShardsSplitter::BuildSplitter(*SchemeEntry);
        YQL_ENSURE(shardsSplitter);

        const auto dataAccessor = GetDataAccessor(BatchBuilder->FlushBatch(true));
        auto initStatus = shardsSplitter->SplitData(*SchemeEntry, *dataAccessor);
        if (!initStatus.Ok()) {
            RuntimeError(
                initStatus.GetErrorMessage(),
                NYql::NDqProto::StatusIds::INTERNAL_ERROR);
            return;
        }

        const auto& splittedData = shardsSplitter->GetSplitData();

        for (auto& [shard, infos] : splittedData.GetShardsInfo()) {
            for (auto&& shardInfo : infos) {
                auto& inFlightBatch = InFlightBatches[shard].emplace_back();
                inFlightBatch.Data = shardInfo->GetData();
                YQL_ENSURE(!inFlightBatch.Data.empty());
                MemoryInFlight += inFlightBatch.Data.size();

                RequestNewTxId();
            }
        }

        if (needToResume && GetFreeSpace() > 0) {
            Callbacks->ResumeExecution();
        }
    }

    void SendBatchesToShards() {
        for (auto& [shardId, batches] : InFlightBatches) {
            if (!batches.empty() && batches.front().TxId == 0) {
                if (const auto txId = AllocateTxId(); txId) {
                    batches.front().TxId = *txId;
                    SendRequestShard(shardId);
                }
            }
        }
    }

    bool IsInFlightBatchesEmpty() const {
        for (const auto& [shardId, batches] : InFlightBatches) {
            if (!batches.empty()) {
                return false;
            }
        }
        return true;
    }

    void SendRequestShard(const ui64 shardId) {
        auto& inFlightBatch = InFlightBatches.at(shardId).front();

        if (inFlightBatch.SendAttempts >= BackoffSettings()->MaxWriteAttempts) {
            RuntimeError(
                TStringBuilder() << "ShardId=" << shardId << " for table '" << Settings.GetTable().GetPath() << "' retry limit exceeded",
                NYql::NDqProto::StatusIds::UNAVAILABLE);
            return;
        }

        auto evWrite = std::make_unique<NKikimr::NEvents::TDataEvents::TEvWrite>(
            inFlightBatch.TxId, NKikimrDataEvents::TEvWrite::MODE_PREPARE);
        ui64 payloadIndex = NKikimr::NEvWrite::TPayloadWriter<NKikimr::NEvents::TDataEvents::TEvWrite>(*evWrite)
            .AddDataToPayload(TString(inFlightBatch.Data));

        evWrite->AddOperation(
            NKikimrDataEvents::TEvWrite::TOperation::OPERATION_REPLACE,
            {
                Settings.GetTable().GetOwnerId(),
                Settings.GetTable().GetTableId(),
                Settings.GetTable().GetVersion() + 1 // TODO: SchemeShard returns wrong version.
            },
            WriteColumnIds,
            payloadIndex,
            NKikimrDataEvents::FORMAT_ARROW);

        CA_LOG_D("Send EvWrite to ShardID=" << shardId << ", TxId=" << inFlightBatch.TxId << ", Size = " << inFlightBatch.Data.size());
        Send(
            PipeCacheId,
            new TEvPipeCache::TEvForward(evWrite.release(), shardId, true),
            IEventHandle::FlagTrackDelivery);

        TlsActivationContext->Schedule(
            CalculateNextAttemptDelay(inFlightBatch.SendAttempts),
            new IEventHandle(SelfId(), SelfId(), new TEvPrivate::TEvShardRequestTimeout(shardId, inFlightBatch.TxId)));

        ++inFlightBatch.SendAttempts;
    }

    void RetryShard(const ui64 shardId) {
        if (!InFlightBatches.contains(shardId) || InFlightBatches.at(shardId).empty()) {
            return;
        }
        CA_LOG_D("Retry ShardID=" << shardId);
        auto& inFlightBatch = InFlightBatches.at(shardId).front();
        if (inFlightBatch.TxId != 0) {
            inFlightBatch.OldTxIds.push_back(inFlightBatch.TxId);
            inFlightBatch.TxId = 0;
        }
        RequestNewTxId();
    }

    void Handle(TEvPrivate::TEvShardRequestTimeout::TPtr& ev) {
        if (!InFlightBatches.contains(ev->Get()->ShardId)) {
            return;
        }
        if (InFlightBatches.at(ev->Get()->ShardId).empty()) {
            return;
        }
        if (InFlightBatches.at(ev->Get()->ShardId).front().TxId != ev->Get()->TxId) {
            return;
        }

        RetryShard(ev->Get()->ShardId);
    }

    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
        CA_LOG_D("TEvDeliveryProblem was received from tablet: " << ev->Get()->TabletId);
        if (!InFlightBatches.contains(ev->Get()->TabletId)) {
            return;
        }
        RetryShard(ev->Get()->TabletId);
    }

    void Handle(TEvTxUserProxy::TEvAllocateTxIdResult::TPtr& ev) {
        FreeTxIds.push_back(ev->Get()->TxId);
        ProcessRows();
    }

    void RequestNewTxId() {
        Send(TxProxyId, new TEvTxUserProxy::TEvAllocateTxId);
    }

    std::optional<ui64> AllocateTxId() {
        if (FreeTxIds.empty()) {
            return std::nullopt;
        }
        const ui64 result = FreeTxIds.front();
        FreeTxIds.pop_front();
        return result;
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
        Send(PipeCacheId, new TEvPipeCache::TEvUnlink(0));
        TActorBootstrapped<TKqpWriteActor>::PassAway();
    }

    void Prepare() {
        YQL_ENSURE(SchemeEntry);
        if (!SchemeEntry->ColumnTableInfo) {
            RuntimeError("Expected column table.", NYql::NDqProto::StatusIds::SCHEME_ERROR);
            return;
        }
        if (!SchemeEntry->ColumnTableInfo->Description.HasSchema()) {
            RuntimeError("Unknown schema for column table.", NYql::NDqProto::StatusIds::SCHEME_ERROR);
            return;
        }

        Serializer = CreateColumnShardPayloadSerializer(
            *SchemeEntry,
            TConstArrayRef<NKikimrKqp::TKqpColumnMetadataProto>{
                *Settings.GetColumns().data(),
                static_cast<size_t>(Settings.GetColumns().size())},
            TypeEnv);

        std::vector<std::pair<TString, NScheme::TTypeInfo>> batchBuilderColumns;
        THashMap<ui32, ui32> writeColumnIdToIndex;
        {
            batchBuilderColumns.reserve(Settings.ColumnsSize());
            WriteColumnIds.reserve(Settings.ColumnsSize());
            i32 number = 0;
            for (const auto& column : SchemeEntry->ColumnTableInfo->Description.GetSchema().GetColumns()) {
                Y_ABORT_UNLESS(column.HasTypeId());
                auto typeInfoMod = NScheme::TypeInfoModFromProtoColumnType(column.GetTypeId(),
                    column.HasTypeInfo() ? &column.GetTypeInfo() : nullptr);
                batchBuilderColumns.emplace_back(column.GetName(), typeInfoMod.TypeInfo);
                WriteColumnIds.push_back(column.GetId());
                writeColumnIdToIndex[column.GetId()] = number++;
            }
        }

        {
            SendIndexToWriteIndexMapping.resize(Settings.ColumnsSize());
            Columns.reserve(Settings.ColumnsSize());
            i32 number = 0;
            for (const auto& column : Settings.GetColumns()) {
                SendIndexToWriteIndexMapping[number] = writeColumnIdToIndex.at(column.GetId());
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

        std::set<std::string> notNullColumns;
        for (const auto& column : Settings.GetColumns()) {
            if (column.GetNotNull()) {
                notNullColumns.insert(column.GetName());
            }
        }

        BatchBuilder = std::make_unique<NArrow::TArrowBatchBuilder>(arrow::Compression::UNCOMPRESSED, notNullColumns);

        TString err;
        if (!BatchBuilder->Start(batchBuilderColumns, 0, 0, err)) {
            RuntimeError("Failed to start batch builder: " + err, NYql::NDqProto::StatusIds::PRECONDITION_FAILED);
            return;
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


    NActors::TActorId TxProxyId = MakeTxProxyID();
    NActors::TActorId PipeCacheId = NKikimr::MakePipePeNodeCacheID(false);

    TString LogPrefix;
    const NKikimrKqp::TKqpTableSinkSettings Settings;
    const ui64 OutputIndex;
    NYql::NDq::TDqAsyncStats EgressStats;
    NYql::NDq::IDqComputeActorAsyncOutput::ICallbacks * Callbacks = nullptr;
    TIntrusivePtr<TKqpCounters> Counters;
    const NMiniKQL::TTypeEnvironment& TypeEnv;

    const NYql::NDq::TTxId TxId;
    const TTableId TableId;

    TVector<TSysTables::TTableColumnInfo> Columns;
    TVector<ui32> SendIndexToWriteIndexMapping;
    std::vector<ui32> WriteColumnIds;

    std::optional<NSchemeCache::TSchemeCacheNavigate::TEntry> SchemeEntry;

    std::unique_ptr<NArrow::TArrowBatchBuilder> BatchBuilder;

    IPayloadSerializerPtr Serializer = nullptr;

    struct TInFlightBatch {
        TString Data;
        ui32 SendAttempts = 0;
        ui64 TxId = 0;
        TVector<ui64> OldTxIds;
    };
    THashMap<ui64, std::deque<TInFlightBatch>> InFlightBatches;
    bool Finished = false;

    const i64 MemoryLimit = kInFlightMemoryLimitPerActor;
    i64 MemoryInFlight = 0;

    std::deque<ui64> FreeTxIds;
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
