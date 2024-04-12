#include "kqp_write_actor.h"

#include "kqp_write_table.h"

#include <util/generic/singleton.h>
#include <ydb/core/actorlib_impl/long_timer.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/engine/minikql/minikql_engine_host.h>
#include <ydb/core/formats/arrow/arrow_batch_builder.h>
#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/protos/kqp_physical.pb.h>
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
#include <ydb/library/yql/public/issue/yql_issue_message.h>


namespace {
    struct TWriteActorBackoffSettings {
        TDuration StartRetryDelay = TDuration::MilliSeconds(250);
        TDuration MaxRetryDelay = TDuration::Seconds(10);
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

    class TShardsInfo {
    public:
        class TShardInfo {
            friend class TShardsInfo;
            TShardInfo(i64& memory)
                : Memory(memory) {
            }

        public:
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
                    Memory -= batch.Data.size();
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
                Memory += Batches.back().Data.size();
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
            i64& Memory;
        };

        TShardInfo& GetShard(const ui64 shard) {
            auto it = ShardsInfo.find(shard);
            if (it != std::end(ShardsInfo)) {
                return it->second;
            }

            auto [insertIt, _] = ShardsInfo.emplace(shard, TShardInfo(Memory));
            return insertIt->second;
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

        bool Has(ui64 shardId) const {
            return ShardsInfo.contains(shardId);
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

        i64 GetMemory() const {
            return Memory;
        }

    private:
        THashMap<ui64, TShardInfo> ShardsInfo;
        i64 Memory = 0;
    };

    constexpr i64 kInFlightMemoryLimitPerActor = 100_MB;
}


namespace NKikimr {
namespace NKqp {

class TKqpWriteActor : public TActorBootstrapped<TKqpWriteActor>, public NYql::NDq::IDqComputeActorAsyncOutput {
    using TBase = TActorBootstrapped<TKqpWriteActor>;

    class TResumeNotificationManager {
    public:
        TResumeNotificationManager(TKqpWriteActor& writer)
            : Writer(writer) {
            CheckMemory();
        }

        void CheckMemory() {
            const auto freeSpace = Writer.GetFreeSpace();
            if (freeSpace > LastFreeMemory) {
                Writer.ResumeExecution();
            }
            LastFreeMemory = freeSpace;
        }

    private:
        TKqpWriteActor& Writer;
        i64 LastFreeMemory = std::numeric_limits<i64>::max();
    };

    friend class TResumeNotificationManager;

    struct TEvPrivate {
        enum EEv {
            EvShardRequestTimeout = EventSpaceBegin(TKikimrEvents::ES_PRIVATE),
        };

        struct TEvShardRequestTimeout : public TEventLocal<TEvShardRequestTimeout, EvShardRequestTimeout> {
            ui64 ShardId;

            TEvShardRequestTimeout(ui64 shardId)
                : ShardId(shardId) {
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
        const i64 result = Serializer
            ? MemoryLimit - Serializer->GetMemory() - ShardsInfo.GetMemory()
            : std::numeric_limits<i64>::min(); // Can't use zero here because compute can use overcommit!

        if (result <= 0) {
            CA_LOG_D("No free space left. FreeSpace=" << result << " bytes.");
        }
        return result;
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

    void SendData(NMiniKQL::TUnboxedValueBatch&& data, i64 size, const TMaybe<NYql::NDqProto::TCheckpoint>&, bool finished) final {
        YQL_ENSURE(!data.IsWide(), "Wide stream is not supported yet");
        YQL_ENSURE(!Finished);
        Finished = finished;
        EgressStats.Resume();

        CA_LOG_D("New data: size=" << size << ", finished=" << finished << ".");

        YQL_ENSURE(Serializer);
        try {
            Serializer->AddData(std::move(data), Finished);
        } catch (...) {
            RuntimeError(
                CurrentExceptionMessage(),
                NYql::NDqProto::StatusIds::INTERNAL_ERROR);
        }

        TResumeNotificationManager resumeNotificator(*this);
        for (auto& [shardId, batches] : Serializer->FlushBatches()) {
            for (auto& batch : batches) {
                ShardsInfo.GetShard(shardId).PushBatch(std::move(batch));
            }
        }
        resumeNotificator.CheckMemory();
        YQL_ENSURE(!Finished || Serializer->IsFinished());

        if (Finished) {
            for (auto& [shardId, shardInfo] : ShardsInfo.GetShards()) {
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

        CA_LOG_D("Resolved TableId=" << TableId << " ("
            << SchemeEntry->TableId.PathId.ToString() << " "
            << SchemeEntry->TableId.SchemaVersion << ")");

        if (SchemeEntry->TableId.SchemaVersion != TableId.SchemaVersion) {
            RuntimeError(TStringBuilder() << "Schema was updated.", NYql::NDqProto::StatusIds::SCHEME_ERROR);
            return;
        }

        Prepare();
    }

    void Handle(NKikimr::NEvents::TDataEvents::TEvWriteResult::TPtr& ev) {
        auto getIssues = [&ev]() {
            NYql::TIssues issues;
            NYql::IssuesFromMessage(ev->Get()->Record.GetIssues(), issues);
            return issues;
        };

        switch (ev->Get()->GetStatus()) {
        case NKikimrDataEvents::TEvWriteResult::STATUS_UNSPECIFIED: {
            RuntimeError(
                TStringBuilder() << "Got UNSPECIFIED for table `"
                    << SchemeEntry->TableId.PathId.ToString() << "`.",
                NYql::NDqProto::StatusIds::UNSPECIFIED,
                getIssues());
            return;
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_PREPARED: {
            YQL_ENSURE(false);
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED: {
            ProcessWriteCompletedShard(ev);
            return;
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_ABORTED: {
            RuntimeError(
                TStringBuilder() << "Got ABORTED for table `"
                    << SchemeEntry->TableId.PathId.ToString() << "`.",
                NYql::NDqProto::StatusIds::ABORTED,
                getIssues());
            return;
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_INTERNAL_ERROR: {
            RuntimeError(
                TStringBuilder() << "Got INTERNAL ERROR for table `"
                    << SchemeEntry->TableId.PathId.ToString() << "`.",
                NYql::NDqProto::StatusIds::INTERNAL_ERROR,
                getIssues());
            return;
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_OVERLOADED: {
            CA_LOG_D("Got OVERLOADED for table `"
                << SchemeEntry->TableId.PathId.ToString() << "`. "
                << "Ignored this error.");
            return;
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_CANCELLED: {
            RuntimeError(
                TStringBuilder() << "Got CANCELLED for table `"
                    << SchemeEntry->TableId.PathId.ToString() << "`.",
                NYql::NDqProto::StatusIds::CANCELLED,
                getIssues());
            return;
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST: {
            RuntimeError(
                TStringBuilder() << "Got BAD REQUEST for table `"
                    << SchemeEntry->TableId.PathId.ToString() << "`.",
                NYql::NDqProto::StatusIds::BAD_REQUEST,
                getIssues());
            return;
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_SCHEME_CHANGED: {
            RuntimeError(
                TStringBuilder() << "Got SCHEME CHANGED for table `"
                    << SchemeEntry->TableId.PathId.ToString() << "`.",
                NYql::NDqProto::StatusIds::SCHEME_ERROR,
                getIssues());
            return;
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_LOCKS_BROKEN: {
            RuntimeError(
                TStringBuilder() << "Got LOCKS BROKEN for table `"
                    << SchemeEntry->TableId.PathId.ToString() << "`.",
                NYql::NDqProto::StatusIds::ABORTED,
                getIssues());
            return;
        }
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

    void PopShardBatch(ui64 shardId, ui64 cookie) {
        TResumeNotificationManager resumeNotificator(*this);
        auto& shardInfo = ShardsInfo.GetShard(shardId);
        if (const auto batch = shardInfo.PopBatch(cookie); batch) {
            EgressStats.Bytes += batch->Data.size();
            EgressStats.Chunks++;
            EgressStats.Splits++;
            EgressStats.Resume();
        }
        resumeNotificator.CheckMemory();
    }

    void ProcessBatches() {
        SendBatchesToShards();
        if (ShardsInfo.IsFinished()) {
            CA_LOG_D("Write actor finished");
            Callbacks->OnAsyncOutputFinished(GetOutputIndex());
        }
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

        auto evWrite = std::make_unique<NKikimr::NEvents::TDataEvents::TEvWrite>(
            std::get<ui64>(TxId),
            NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE);

        YQL_ENSURE(!inFlightBatch.Data.empty());
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

        evWrite->SetLockId(Settings.GetLockTxId(), Settings.GetLockNodeId());

        CA_LOG_D("Send EvWrite to ShardID=" << shardId << ", TxId=" << std::get<ui64>(TxId)
            << ", LockTxId=" << Settings.GetLockTxId() << ", LockNodeId=" << Settings.GetLockNodeId()
            << ", Size=" << inFlightBatch.Data.size() << ", Cookie=" << inFlightBatch.Cookie
            << "; ShardBatchesLeft=" << shard.Size() << ", ShardClosed=" << shard.IsClosed());
        Send(
            PipeCacheId,
            new TEvPipeCache::TEvForward(evWrite.release(), shardId, true),
            0,
            inFlightBatch.Cookie);
        ++inFlightBatch.SendAttempts;

        TlsActivationContext->Schedule(
            CalculateNextAttemptDelay(inFlightBatch.SendAttempts),
            new IEventHandle(SelfId(), SelfId(), new TEvPrivate::TEvShardRequestTimeout(shardId), 0, inFlightBatch.Cookie));
    }

    void RetryShard(const ui64 shardId, const std::optional<ui64> ifCookieEqual) {
        if (!ShardsInfo.Has(shardId)) {
            return;
        }
        const auto& shard = ShardsInfo.GetShard(shardId);
        if (shard.IsEmpty() || (ifCookieEqual && shard.CurrentBatch().Cookie != ifCookieEqual)) {
            return;
        }

        CA_LOG_D("Retry ShardID=" << shardId);
        SendDataToShard(shardId);
    }

    void Handle(TEvPrivate::TEvShardRequestTimeout::TPtr& ev) {
        RetryShard(ev->Get()->ShardId, ev->Cookie);
    }

    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
        CA_LOG_D("TEvDeliveryProblem was received from tablet: " << ev->Get()->TabletId);
        RetryShard(ev->Get()->TabletId, std::nullopt);
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

        TVector<NKikimrKqp::TKqpColumnMetadataProto> columnsMetadata;
        columnsMetadata.reserve(Settings.GetColumns().size());
        for (const auto & column : Settings.GetColumns()) {
            columnsMetadata.push_back(column);
        }

        try {
            Serializer = CreateColumnShardPayloadSerializer(
                *SchemeEntry,
                columnsMetadata,
                TypeEnv);
            ResumeExecution();
        } catch (...) {
            RuntimeError(
                CurrentExceptionMessage(),
                NYql::NDqProto::StatusIds::INTERNAL_ERROR);
        }
    }

    void ResumeExecution() {
        CA_LOG_D("Resuming execution.");
        Callbacks->ResumeExecution();
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

    std::optional<NSchemeCache::TSchemeCacheNavigate::TEntry> SchemeEntry;
    IPayloadSerializerPtr Serializer = nullptr;

    TShardsInfo ShardsInfo;
    bool Finished = false;

    const i64 MemoryLimit = kInFlightMemoryLimitPerActor;
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
