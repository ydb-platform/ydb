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
    constexpr i64 kInFlightMemoryLimitPerActor = 64_MB;
    constexpr i64 kMemoryLimitPerMessage = 48_MB;
    constexpr i64 kMaxBatchesPerMessage = 8;

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

            void MakeNextBatches(ui64 maxDataSize, ui64 maxCount) {
                YQL_ENSURE(BatchesInFlight == 0);
                ui64 dataSize = 0;
                while (BatchesInFlight < maxCount
                        && BatchesInFlight < Batches.size()
                        && dataSize + GetBatch(BatchesInFlight).Data.size() <= maxDataSize) {
                    dataSize += GetBatch(BatchesInFlight).Data.size();
                    ++BatchesInFlight;
                }
                YQL_ENSURE(BatchesInFlight == Batches.size() || GetBatch(BatchesInFlight).Data.size() <= maxDataSize); 
            }

            const TInFlightBatch& GetBatch(size_t index) const {
                return Batches.at(index);
            }

            std::optional<ui64> PopBatches(const ui64 cookie) {
                if (BatchesInFlight != 0 && Cookie == cookie) {
                    ui64 dataSize = 0;
                    for (size_t index = 0; index < BatchesInFlight; ++index) {
                        dataSize += Batches.front().Data.size();
                        Batches.pop_front();
                    }

                    ++Cookie;
                    SendAttempts = 0;
                    BatchesInFlight = 0;

                    Memory -= dataSize;
                    return dataSize;
                }
                return std::nullopt;
            }

            void PushBatch(TString&& data) {
                YQL_ENSURE(!IsClosed());
                Batches.push_back(TInFlightBatch{
                    .Data = std::move(data)
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

            ui64 GetCookie() const {
                return Cookie;
            }

            size_t GetBatchesInFlight() const {
                return BatchesInFlight;
            }

            ui32 GetSendAttempts() const {
                return SendAttempts;
            }

            void IncSendAttempts() {
                ++SendAttempts;
            }

        private:
            std::deque<TInFlightBatch> Batches;
            bool Closed = false;
            std::optional<NKikimrDataEvents::TLock> Lock;
            i64& Memory;

            ui64 Cookie = 1;
            ui32 SendAttempts = 0;
            size_t BatchesInFlight = 0;
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
                if (!shard.IsEmpty() && shard.GetSendAttempts() == 0) {
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
        , FinalTx(
            Settings.GetFinalTx())
        , ImmediateTx(
            Settings.GetImmediateTx())
        , InconsistentTx(
            Settings.GetInconsistentTx())
    {
        YQL_ENSURE(std::holds_alternative<ui64>(TxId));
        YQL_ENSURE(!InconsistentTx || ImmediateTx);
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
    void LoadState(const NYql::NDq::TSinkState&) final {};

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

        if (Finished || GetFreeSpace() <= 0 || SchemeEntry->Kind == NSchemeCache::TSchemeCacheNavigate::KindColumnTable) {
            TResumeNotificationManager resumeNotificator(*this);
            for (auto& [shardId, batches] : Serializer->FlushBatchesForce()) {
                for (auto& batch : batches) {
                    ShardsInfo.GetShard(shardId).PushBatch(std::move(batch));
                }
            }
            resumeNotificator.CheckMemory();
        }

        if (Finished) {
            for (auto& [shardId, shardInfo] : ShardsInfo.GetShards()) {
                shardInfo.Close();
            }

            YQL_ENSURE(Serializer->IsFinished());
        }

        ProcessBatches();
    }

    STFUNC(StateFunc) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(NKikimr::NEvents::TDataEvents::TEvWriteResult, Handle);
                hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
                hFunc(TEvTxProxySchemeCache::TEvResolveKeySetResult, Handle);
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
        entry.SyncVersion = false;
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

        CA_LOG_D("Resolved TableId=" << TableId << " ("
            << SchemeEntry->TableId.PathId.ToString() << " "
            << SchemeEntry->TableId.SchemaVersion << ")");

        if (SchemeEntry->TableId.SchemaVersion != TableId.SchemaVersion) {
            RuntimeError(TStringBuilder() << "Schema was updated.", NYql::NDqProto::StatusIds::SCHEME_ERROR);
            return;
        }

        if (SchemeEntry->Kind == NSchemeCache::TSchemeCacheNavigate::KindColumnTable) {
            YQL_ENSURE(!ImmediateTx);
            Prepare();
        } else {
            ResolveShards();
        }
    }

    void ResolveShards() {
        YQL_ENSURE(!SchemeRequest);
        YQL_ENSURE(SchemeEntry);

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
        YQL_ENSURE(!SchemeRequest);
        auto* request = ev->Get()->Request.Get();

        if (request->ErrorCount > 0) {
            RuntimeError(TStringBuilder() << "Failed to get table: "
                << TableId << "'", NYql::NDqProto::StatusIds::SCHEME_ERROR);
            return;
        }

        YQL_ENSURE(request->ResultSet.size() == 1);
        SchemeRequest = std::move(request->ResultSet[0]);

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
            CA_LOG_E("Got UNSPECIFIED for table `"
                    << SchemeEntry->TableId.PathId.ToString() << "`."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << ".");
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
            CA_LOG_E("Got ABORTED for table `"
                    << SchemeEntry->TableId.PathId.ToString() << "`."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << ".");
            RuntimeError(
                TStringBuilder() << "Got ABORTED for table `"
                    << SchemeEntry->TableId.PathId.ToString() << "`.",
                NYql::NDqProto::StatusIds::ABORTED,
                getIssues());
            return;
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_INTERNAL_ERROR: {
            CA_LOG_E("Got INTERNAL ERROR for table `"
                    << SchemeEntry->TableId.PathId.ToString() << "`."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << ".");
            RuntimeError(
                TStringBuilder() << "Got INTERNAL ERROR for table `"
                    << SchemeEntry->TableId.PathId.ToString() << "`.",
                NYql::NDqProto::StatusIds::INTERNAL_ERROR,
                getIssues());
            return;
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_OVERLOADED: {
            CA_LOG_W("Got OVERLOADED for table `"
                << SchemeEntry->TableId.PathId.ToString() << "`."
                << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                << " Sink=" << this->SelfId() << "."
                << " Ignored this error.");
            return;
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_CANCELLED: {
            CA_LOG_E("Got CANCELLED for table `"
                    << SchemeEntry->TableId.PathId.ToString() << "`."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << ".");
            RuntimeError(
                TStringBuilder() << "Got CANCELLED for table `"
                    << SchemeEntry->TableId.PathId.ToString() << "`.",
                NYql::NDqProto::StatusIds::CANCELLED,
                getIssues());
            return;
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST: {
            CA_LOG_E("Got BAD REQUEST for table `"
                    << SchemeEntry->TableId.PathId.ToString() << "`."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << ".");
            RuntimeError(
                TStringBuilder() << "Got BAD REQUEST for table `"
                    << SchemeEntry->TableId.PathId.ToString() << "`.",
                NYql::NDqProto::StatusIds::BAD_REQUEST,
                getIssues());
            return;
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_SCHEME_CHANGED: {
            CA_LOG_E("Got SCHEME CHANGED for table `"
                    << SchemeEntry->TableId.PathId.ToString() << "`."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << ".");
            RuntimeError(
                TStringBuilder() << "Got SCHEME CHANGED for table `"
                    << SchemeEntry->TableId.PathId.ToString() << "`.",
                NYql::NDqProto::StatusIds::SCHEME_ERROR,
                getIssues());
            return;
        }
        case NKikimrDataEvents::TEvWriteResult::STATUS_LOCKS_BROKEN: {
            CA_LOG_E("Got LOCKS BROKEN for table `"
                    << SchemeEntry->TableId.PathId.ToString() << "`."
                    << " ShardID=" << ev->Get()->Record.GetOrigin() << ","
                    << " Sink=" << this->SelfId() << ".");
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
        if (const auto removedDataSize = shardInfo.PopBatches(cookie); removedDataSize) {
            EgressStats.Bytes += *removedDataSize;
            EgressStats.Chunks++;
            EgressStats.Splits++;
            EgressStats.Resume();
        }
        resumeNotificator.CheckMemory();
    }

    void ProcessBatches() {
        if (!ImmediateTx || Finished || GetFreeSpace() <= 0) {
            MakeNewBatches();
            SendBatchesToShards();
            if (Finished && Serializer->IsFinished() && ShardsInfo.IsFinished()) {
                CA_LOG_D("Write actor finished");
                Callbacks->OnAsyncOutputFinished(GetOutputIndex());
            }
        }
    }

    void MakeNewBatches() {
        for (const size_t shardId : Serializer->GetShardIds()) {
            auto& shard = ShardsInfo.GetShard(shardId);
            while (true) {
                auto batch = Serializer->FlushBatch(shardId);
                if (batch.empty()) {
                    break;
                }
                shard.PushBatch(std::move(batch));
            }
            if (shard.GetBatchesInFlight() == 0) {
                shard.MakeNextBatches(
                    kMemoryLimitPerMessage,
                    SchemeEntry->Kind == NSchemeCache::TSchemeCacheNavigate::KindColumnTable
                        ? 1
                        : kMaxBatchesPerMessage);
            }
        }
    }

    void SendBatchesToShards() {
        YQL_ENSURE(!ImmediateTx || ShardsInfo.GetShards().size() == 1);

        for (const size_t shardId : ShardsInfo.GetPendingShards()) {
            const auto& shard = ShardsInfo.GetShard(shardId);
            YQL_ENSURE(!shard.IsEmpty());
            SendDataToShard(shardId);
        }
    }

    void SendDataToShard(const ui64 shardId) {
        auto& shard = ShardsInfo.GetShard(shardId);
        YQL_ENSURE(!shard.IsEmpty());
        if (shard.GetSendAttempts() >= BackoffSettings()->MaxWriteAttempts) {
            CA_LOG_E("ShardId=" << shardId
                    << " for table '" << Settings.GetTable().GetPath()
                    << "': retry limit exceeded."
                    << " Sink=" << this->SelfId() << ".");
            RuntimeError(
                TStringBuilder()
                    << "ShardId=" << shardId
                    << " for table '" << Settings.GetTable().GetPath()
                    << "': retry limit exceeded.",
                NYql::NDqProto::StatusIds::UNAVAILABLE);
            return;
        }

        auto evWrite = std::make_unique<NKikimr::NEvents::TDataEvents::TEvWrite>(
            NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE);
        
        if (ImmediateTx && FinalTx && Finished && shard.Size() == shard.GetBatchesInFlight()) {
            // Last immediate write (only for datashard)
            if (shard.GetLock()) {
                // multi immediate evwrite
                auto* locks = evWrite->Record.MutableLocks();
                locks->SetOp(NKikimrDataEvents::TKqpLocks::Commit);
                locks->AddSendingShards(shardId);
                locks->AddReceivingShards(shardId);
                *locks->AddLocks() = *shard.GetLock();
            }
        } else {
            evWrite->SetLockId(Settings.GetLockTxId(), Settings.GetLockNodeId());
        }

        ui64 totalDataSize = 0;
        for (size_t index = 0; index < shard.GetBatchesInFlight(); ++index) {
            const auto& inFlightBatch = shard.GetBatch(index);
            YQL_ENSURE(!inFlightBatch.Data.empty());
            totalDataSize += inFlightBatch.Data.size();
            const ui64 payloadIndex = NKikimr::NEvWrite::TPayloadWriter<NKikimr::NEvents::TDataEvents::TEvWrite>(*evWrite)
                    .AddDataToPayload(TString(inFlightBatch.Data));
            evWrite->AddOperation(
                NKikimrDataEvents::TEvWrite::TOperation::OPERATION_REPLACE,
                {
                    Settings.GetTable().GetOwnerId(),
                    Settings.GetTable().GetTableId(),
                    Settings.GetTable().GetVersion(),
                },
                Serializer->GetWriteColumnIds(),
                payloadIndex,
                Serializer->GetDataFormat());
        }

        CA_LOG_D("Send EvWrite to ShardID=" << shardId << ", TxId=" << std::get<ui64>(TxId)
            << ", LockTxId=" << Settings.GetLockTxId() << ", LockNodeId=" << Settings.GetLockNodeId()
            << ", Size=" << totalDataSize << ", Cookie=" << shard.GetCookie()
            << "; ShardBatchesLeft=" << shard.Size() << ", ShardClosed=" << shard.IsClosed()
            << "; Attempts=" << shard.GetSendAttempts());
        Send(
            PipeCacheId,
            new TEvPipeCache::TEvForward(evWrite.release(), shardId, true),
            0,
            shard.GetCookie());
        shard.IncSendAttempts();

        TlsActivationContext->Schedule(
            CalculateNextAttemptDelay(shard.GetSendAttempts()),
            new IEventHandle(SelfId(), SelfId(), new TEvPrivate::TEvShardRequestTimeout(shardId), 0, shard.GetCookie()));
    }

    void RetryShard(const ui64 shardId, const std::optional<ui64> ifCookieEqual) {
        if (!ShardsInfo.Has(shardId)) {
            return;
        }
        const auto& shard = ShardsInfo.GetShard(shardId);
        if (shard.IsEmpty() || (ifCookieEqual && shard.GetCookie() != ifCookieEqual)) {
            return;
        }

        CA_LOG_T("Retry ShardID=" << shardId << " with Cookie=" << ifCookieEqual.value_or(0));
        SendDataToShard(shardId);
    }

    void Handle(TEvPrivate::TEvShardRequestTimeout::TPtr& ev) {
        CA_LOG_W("Timeout shardID=" << ev->Get()->ShardId);
        RetryShard(ev->Get()->ShardId, ev->Cookie);
    }

    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
        CA_LOG_W("TEvDeliveryProblem was received from tablet: " << ev->Get()->TabletId);
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

        TVector<NKikimrKqp::TKqpColumnMetadataProto> columnsMetadata;
        columnsMetadata.reserve(Settings.GetColumns().size());
        for (const auto & column : Settings.GetColumns()) {
            columnsMetadata.push_back(column);
        }

        try {
            if (SchemeEntry->Kind == NSchemeCache::TSchemeCacheNavigate::KindColumnTable) {
                Serializer = CreateColumnShardPayloadSerializer(
                    *SchemeEntry,
                    columnsMetadata,
                    TypeEnv);
            } else {
                Serializer = CreateDataShardPayloadSerializer(
                    *SchemeEntry,
                    *SchemeRequest,
                    columnsMetadata,
                    TypeEnv);
            }
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
    const bool FinalTx;
    const bool ImmediateTx;
    const bool InconsistentTx;

    std::optional<NSchemeCache::TSchemeCacheNavigate::TEntry> SchemeEntry;
    std::optional<NSchemeCache::TSchemeCacheRequest::TEntry> SchemeRequest;
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
