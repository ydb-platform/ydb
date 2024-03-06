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

    class TShardInfo {
    public:
        TShardInfo() = default;
        TShardInfo(const TShardInfo&) = delete;
        TShardInfo(TShardInfo&&) = default;
        TShardInfo& operator=(const TShardInfo&) = delete;
        TShardInfo& operator=(TShardInfo&&) = default;

        struct TInFlightBatch {
            TString Data;
            ui32 SendAttempts = 0;
            ui64 Cookie = 0;
        };

        size_t Size() const {
            return Batches.size();
        }

        bool IsEmpty() const {
            return Batches.empty();
        }

        bool IsClosed() const {
            return Closed;
        }

        bool IsFinished() const {
            return IsClosed() && IsEmpty();
        }

        void Close() {
            Closed = true;
        }

        TInFlightBatch& CurrentBatch() {
            YQL_ENSURE(!IsEmpty());
            return Batches.front();
        }

        const TInFlightBatch& CurrentBatch() const {
            YQL_ENSURE(!IsEmpty());
            return Batches.front();
        }

        const TInFlightBatch& LastBatch() const {
            YQL_ENSURE(!IsEmpty());
            return Batches.back();
        }

        std::optional<TInFlightBatch> PopBatch(const ui64 Cookie) {
            if (!IsEmpty() && Cookie == CurrentBatch().Cookie) {
                auto batch = std::move(Batches.front());
                Batches.pop_front();
                return std::move(batch);
            }
            return std::nullopt;
        }

        void PushBatch(TString&& data) {
            YQL_ENSURE(!IsClosed());
            Batches.push_back(TInFlightBatch{
                .Data = std::move(data),
                .SendAttempts = 0,
                .Cookie = ++NextCookie,
            });
        }

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
        std::deque<TInFlightBatch> Batches;
        ui64 NextCookie = 0;
        bool Closed = false;
        std::optional<NKikimrDataEvents::TLock> Lock;
    };

    class TShardsInfo {
    public:
        TShardInfo& GetShard(const ui64 shard) {
            return ShardsInfo[shard];
        }

        TVector<ui64> GetPendingShards() {
            TVector<ui64> result;
            for (const auto& [id, shard] : ShardsInfo) {
                if (!shard.IsEmpty() && shard.CurrentBatch().SendAttempts == 0) {
                    result.push_back(id);
                }
            }
            return result;
        }

        bool IsEmpty() const {
            for (const auto& [_, shard] : ShardsInfo) {
                if (!shard.IsEmpty()) {
                    return false;
                }
            }
            return true;
        }

        bool IsFinished() const {
            for (const auto& [_, shard] : ShardsInfo) {
                if (!shard.IsFinished()) {
                    return false;
                }
            }
            return true;
        }

        THashMap<ui64, TShardInfo>& GetShards() {
            return ShardsInfo;
        }

    private:
        THashMap<ui64, TShardInfo> ShardsInfo;
    };

    constexpr i64 kInFlightMemoryLimitPerActor = 1_GB;
}


namespace NKikimr {
namespace NKqp {

class TKqpWriteActor : public TActorBootstrapped<TKqpWriteActor>, public NYql::NDq::IDqComputeActorAsyncOutput {
    using TBase = TActorBootstrapped<TKqpWriteActor>;

    class TResumeNotificationManager {
    public:
    // if (needToResume && GetFreeSpace() > 0) {
    //      Callbacks->ResumeExecution();
    //  }
    //
    private:
    };

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

        // TMP
        Settings.SetLockTxId(std::get<ui64>(TxId));
        Settings.SetLockNodeId(0);
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
        return Serializer
            ? MemoryLimit - Serializer->GetMemory()
            : std::numeric_limits<i64>::min(); // Can't use zero here because compute can use overcommit!
    }

    TMaybe<google::protobuf::Any> ExtraData() override {
        NKikimrKqp::TEvKqpOutputActorResultInfo resultInfo;
        for (const auto& [_, shardInfo] : ShardsInfo.GetShards()) {
            if (const auto& lock = shardInfo.GetLock(); lock) {
                resultInfo.AddLocks()->CopyFrom(*lock);
            }
        }
        google::protobuf::Any result;
        result.PackFrom(resultInfo);
        return result;
    }

    void SendData(NMiniKQL::TUnboxedValueBatch&& data, i64, const TMaybe<NYql::NDqProto::TCheckpoint>&, bool finished) final {
        YQL_ENSURE(!data.IsWide(), "Wide stream is not supported yet");
        YQL_ENSURE(!Finished);
        Finished = finished;
        EgressStats.Resume();

        YQL_ENSURE(Serializer);
        try {
            Serializer->AddData(std::move(data), Finished);
        } catch (...) {
            RuntimeError(
                CurrentExceptionMessage(),
                NYql::NDqProto::StatusIds::INTERNAL_ERROR);
        }

        for (auto& [shardId, batches] : Serializer->FlushBatches()) {
            for (auto& batch : batches) {
                ShardsInfo.GetShard(shardId).PushBatch(std::move(batch));
            }
        }
        YQL_ENSURE(!Finished || Serializer->IsFinished());

        if (Finished) {
            for (auto& [shardId, shardInfo] : ShardsInfo.GetShards()) {
                // Add fake empty batch to each shard without unsent data for commit evwrite.
                //if (shardInfo.IsEmpty() || shardInfo.LastBatch().SendAttempts != 0) {
                shardInfo.PushBatch(TString());

                //}
                shardInfo.Close();
            }
        }

        ProcessBatches();
    }

    STFUNC(StateFunc) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(NKikimr::NEvents::TDataEvents::TEvWriteResult, Handle);
                hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
                hFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
                IgnoreFunc(TEvTxUserProxy::TEvAllocateTxIdResult);
                hFunc(TEvPrivate::TEvShardRequestTimeout, Handle);
                IgnoreFunc(TEvInterconnect::TEvNodeConnected);
                IgnoreFunc(TEvTxProxySchemeCache::TEvInvalidateTableResult);
                // TODO: locks broken
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
        if (SchemeEntry) {
            return;
        }
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
        switch (ev->Get()->GetStatus()) {
        case NKikimrDataEvents::TEvWriteResult::STATUS_PREPARED:
            ProcessWritePreparedShard(ev);
            break;
        case NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED:
            ProcessWriteCompletedShard(ev);
            break;
        // TODO: process errors
        default:
            YQL_ENSURE(false);
            break;
        }
    }

    void ProcessWriteCompletedShard(NKikimr::NEvents::TDataEvents::TEvWriteResult::TPtr& ev) {
        CA_LOG_D("Got completed result TxId=" << ev->Get()->Record.GetTxId()
            << ", TabletId=" << ev->Get()->Record.GetOrigin()
            << ", Cookie=" << ev->Cookie
            << ", LocksCount=" << ev->Get()->Record.GetTxLocks().size());

        PopShardBatch(ev->Get()->Record.GetOrigin(), ev->Cookie);
        for (const auto& lock : ev->Get()->Record.GetTxLocks()) {
            ShardsInfo.GetShard(ev->Get()->Record.GetOrigin()).AddAndCheckLock(lock);
        }
        ProcessBatches();
    }

    void ProcessWritePreparedShard(NKikimr::NEvents::TDataEvents::TEvWriteResult::TPtr& ev) {
        CA_LOG_D("Got prepared result TxId=" << ev->Get()->Record.GetTxId()
            << ", TabletId=" << ev->Get()->Record.GetOrigin()
            << ", Cookie=" << ev->Cookie);

        PopShardBatch(ev->Get()->Record.GetOrigin(), ev->Cookie);

        if (!ProposeTransaction) {
            ProposeTransaction = MakeHolder<TEvTxProxy::TEvProposeTransaction>();
        }

        auto& transaction = *ProposeTransaction->Record.MutableTransaction();
        auto& affectedSet = *transaction.MutableAffectedSet();

        auto& item = *affectedSet.Add();
        item.SetTabletId(ev->Get()->Record.GetOrigin());
        item.SetFlags(TEvTxProxy::TEvProposeTransaction::AffectedWrite);

        if (ShardsInfo.IsFinished()) {
            ui64 coordinator = 0;
            if (ev->Get()->Record.DomainCoordinatorsSize()) {
                auto domainCoordinators = TCoordinators(TVector<ui64>(
                    std::begin(ev->Get()->Record.GetDomainCoordinators()),
                    std::end(ev->Get()->Record.GetDomainCoordinators())));
                coordinator = domainCoordinators.Select(ev->Get()->Record.GetTxId());
            }

            YQL_ENSURE(coordinator != 0);
            ProposeTransaction->Record.SetCoordinatorID(coordinator);

            auto& transaction = *ProposeTransaction->Record.MutableTransaction();
            transaction.SetTxId(ev->Get()->Record.GetTxId());
            transaction.SetMinStep(ev->Get()->Record.GetMinStep());
            transaction.SetMaxStep(ev->Get()->Record.GetMaxStep());

            Send(
                PipeCacheId,
                new TEvPipeCache::TEvForward(ProposeTransaction.Release(), coordinator, true),
                0);
            Callbacks->OnAsyncOutputFinished(GetOutputIndex());
        }
    }

    void PopShardBatch(ui64 shardId, ui64 cookie) {
        auto& shardInfo = ShardsInfo.GetShard(shardId);
        if (const auto batch = shardInfo.PopBatch(cookie); batch) {
            EgressStats.Bytes += batch->Data.size();
            EgressStats.Chunks++;
            EgressStats.Splits++;
            EgressStats.Resume();
        }
    }

    void ProcessBatches() {
        SendBatchesToShards();
    }

    void SendBatchesToShards() {
        for (size_t shardId : ShardsInfo.GetPendingShards()) {
            auto& shard = ShardsInfo.GetShard(shardId);
            YQL_ENSURE(!shard.IsEmpty());

            SendDataToShard(shardId);
        }
    }

    void SendDataToShard(const ui64 shardId) {
        auto& shard = ShardsInfo.GetShard(shardId);
        YQL_ENSURE(!shard.IsEmpty());
        auto& inFlightBatch = shard.CurrentBatch();
        if (inFlightBatch.SendAttempts >= BackoffSettings()->MaxWriteAttempts) {
            RuntimeError(
                TStringBuilder() << "ShardId=" << shardId << " for table '" << Settings.GetTable().GetPath() << "': retry limit exceeded",
                NYql::NDqProto::StatusIds::UNAVAILABLE);
            return;
        }

        const bool isLastBatch = shard.Size() == 1 && Finished;
        if (isLastBatch && inFlightBatch.SendAttempts != 0) {
            RuntimeError(
                TStringBuilder() << "ShardId=" << shardId << " for table '" << Settings.GetTable().GetPath() << "': last batch can't be retried",
                NYql::NDqProto::StatusIds::INTERNAL_ERROR);
            return;
        }

        auto evWrite = std::make_unique<NKikimr::NEvents::TDataEvents::TEvWrite>(
            std::get<ui64>(TxId),
            NKikimrDataEvents::TEvWrite::MODE_PREPARE);
            //isLastBatch
            //    ? NKikimrDataEvents::TEvWrite::MODE_PREPARE
            //    : NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE

        if (!inFlightBatch.Data.empty()) {
            const ui64 payloadIndex = NKikimr::NEvWrite::TPayloadWriter<NKikimr::NEvents::TDataEvents::TEvWrite>(*evWrite)
                .AddDataToPayload(TString(inFlightBatch.Data));
            evWrite->AddOperation(
                NKikimrDataEvents::TEvWrite::TOperation::OPERATION_REPLACE,
                {
                    Settings.GetTable().GetOwnerId(),
                    Settings.GetTable().GetTableId(),
                    Settings.GetTable().GetVersion() + 1 // TODO: SchemeShard returns wrong version.
                },
                Serializer->GetWriteColumnIds(),
                payloadIndex,
                Serializer->GetDataFormat());
        } else {
            YQL_ENSURE(isLastBatch);
        }

        if (isLastBatch) {
            // TODO: Send locks structs
            evWrite->Record.MutableLocks()->AddLocks();
            evWrite->Record.MutableLocks()->SetOp(NKikimrDataEvents::TKqpLocks::Commit);
            // TODO: tmp for columnshard
            evWrite->SetLockId(Settings.GetLockTxId(), Settings.GetLockNodeId());
        } else {
            evWrite->SetLockId(Settings.GetLockTxId(), Settings.GetLockNodeId());
        }

        CA_LOG_D("Send EvWrite to ShardID=" << shardId << ", TxId=" << std::get<ui64>(TxId)
            << ", Size=" << inFlightBatch.Data.size() << ", Cookie=" << inFlightBatch.Cookie
            << (isLastBatch ? " (LastBatch)" : ""));
        Send(
            PipeCacheId,
            new TEvPipeCache::TEvForward(evWrite.release(), shardId, true),
            0, //IEventHandle::FlagTrackDelivery,
            inFlightBatch.Cookie);
        ++inFlightBatch.SendAttempts;

        if (!isLastBatch) {
            TlsActivationContext->Schedule(
                CalculateNextAttemptDelay(inFlightBatch.SendAttempts),
                new IEventHandle(SelfId(), SelfId(), new TEvPrivate::TEvShardRequestTimeout(shardId, inFlightBatch.Cookie)));
        }
    }

    void RetryShard(const ui64 shardId) {
        Y_UNUSED(shardId);
        return;
        /*if (!InFlightBatches.contains(shardId)) {
            return;
        }
        CA_LOG_D("Retry ShardID=" << shardId);
        auto& inFlightBatch = InFlightBatches.at(shardId);
        if (inFlightBatch.TxId != 0) {
            inFlightBatch.OldTxIds.push_back(inFlightBatch.TxId);
            inFlightBatch.TxId = 0;
        }
        RequestNewTxId();*/
    }

    void Handle(TEvPrivate::TEvShardRequestTimeout::TPtr& ev) {
        //if (!InFlightBatches.contains(ev->Get()->ShardId)) {
        //    return;
        //}
        //if (InFlightBatches.at(ev->Get()->ShardId).TxId != ev->Get()->TxId) {
        //    return;
        //}
//
        //RetryShard(ev->Get()->ShardId);
        Y_UNUSED(ev);
        YQL_ENSURE(false);
    }

    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
        CA_LOG_D("TEvDeliveryProblem was received from tablet: " << ev->Get()->TabletId);
        //if (!InFlightBatches.contains(ev->Get()->TabletId)) {
        //    return;
        //}
        //RetryShard(ev->Get()->TabletId);
        YQL_ENSURE(false);
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

        // TODO: do smth else
        TVector<NKikimrKqp::TKqpColumnMetadataProto> tmp;
        for (const auto & column : Settings.GetColumns()) {
            tmp.push_back(column);
        }

        try {
            Serializer = CreateColumnShardPayloadSerializer(
                *SchemeEntry,
                tmp,
                //TConstArrayRef<NKikimrKqp::TKqpColumnMetadataProto>{
                //    *Settings.GetColumns().data(),
                //    static_cast<size_t>(Settings.GetColumns().size())},
                TypeEnv);
        } catch (...) {
            RuntimeError(
                CurrentExceptionMessage(),
                NYql::NDqProto::StatusIds::INTERNAL_ERROR);
        }
    }


    NActors::TActorId TxProxyId = MakeTxProxyID();
    NActors::TActorId PipeCacheId = NKikimr::MakePipePeNodeCacheID(false);

    TString LogPrefix;
    /*const*/ NKikimrKqp::TKqpTableSinkSettings Settings;
    const ui64 OutputIndex;
    NYql::NDq::TDqAsyncStats EgressStats;
    NYql::NDq::IDqComputeActorAsyncOutput::ICallbacks * Callbacks = nullptr;
    TIntrusivePtr<TKqpCounters> Counters;
    const NMiniKQL::TTypeEnvironment& TypeEnv;

    const NYql::NDq::TTxId TxId;
    const TTableId TableId;

    std::optional<NSchemeCache::TSchemeCacheNavigate::TEntry> SchemeEntry;
    IPayloadSerializerPtr Serializer = nullptr;

    TShardsInfo ShardsInfo;
    bool Finished = false;

    const i64 MemoryLimit = kInFlightMemoryLimitPerActor;

    //TODO: delete it
    THolder<TEvTxProxy::TEvProposeTransaction> ProposeTransaction = nullptr;
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
