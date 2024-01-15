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

namespace NKikimr {
namespace NKqp {

class TKqpWriteActor : public TActorBootstrapped<TKqpWriteActor>, public NYql::NDq::IDqComputeActorAsyncOutput {
    using TBase = TActorBootstrapped<TKqpWriteActor>;
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
        Y_UNUSED(Settings, Counters, LogPrefix, TxId, TableId);
        EgressStats.Level = args.StatsLevel;

        BuildColumns();
    }

    void Bootstrap() {
        CA_LOG_D("Created Actor !!!!!!!!!!!!!!!!!");
        LogPrefix = TStringBuilder() << "SelfId: " << this->SelfId() << ", " << LogPrefix;
        ResolveShards();
        Become(&TKqpWriteActor::StateFunc);
    }

    static constexpr char ActorName[] = "KQP_WRITE_ACTOR";

private:
    virtual ~TKqpWriteActor() {
        if (!PendingRows.empty() && Alloc) {
            TGuard<NMiniKQL::TScopedAlloc> allocGuard(*Alloc);
            PendingRows.clear();
        }
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
        ProcessRows();

        if (finished) {
            Callbacks->OnAsyncOutputFinished(GetOutputIndex());
        }
    }

    void AddToInputQueue(NMiniKQL::TUnboxedValueBatch&& data) {
        const auto guard = BindAllocator();
        data.ForEachRow([&](const auto& row) {
            PendingRows.emplace_back(Columns.size());
            for (size_t index = 0; index < Columns.size(); ++index) {
                PendingRows.back()[index] = MakeCell(
                    Columns[index].PType,
                    row.GetElement(index),
                    TypeEnv,
                    /* copy */ true);
                MemoryUsed += PendingRows.back()[index].Size();
            }
        });
    }

    STFUNC(StateFunc) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(NKikimr::NEvents::TDataEvents::TEvWriteResult, HandleWrite);
                hFunc(TEvTxProxySchemeCache::TEvResolveKeySetResult, HandleResolve);
                hFunc(TEvPipeCache::TEvDeliveryProblem, HandleError);
                //hFunc(TEvRetryShard, HandleRetry);
                //IgnoreFunc(TEvInterconnect::TEvNodeConnected);
                //IgnoreFunc(TEvTxProxySchemeCache::TEvInvalidateTableResult);
            }
        } catch (const yexception& e) {
            RuntimeError(e.what(), NYql::NDqProto::StatusIds::INTERNAL_ERROR);
        }
    }

    void ResolveShards() {
        CA_LOG_D("Resolve shards");

        Shards.reset();

        TVector<NScheme::TTypeInfo> keyColumnTypes;
        for (const auto& column : KeyColumns) {
            keyColumnTypes.push_back(column.PType);
        }
        TVector<TKeyDesc::TColumnOp> columns;
        for (const auto& column : Columns) {
            columns.push_back(TKeyDesc::TColumnOp { column.Id, TKeyDesc::EColumnOperation::Set, column.PType, 0, 0 });
        }

        auto request = MakeHolder<NSchemeCache::TSchemeCacheRequest>();
        request->ResultSet.emplace_back(MakeHolder<TKeyDesc>(
            TableId,
            TTableRange{TVector<TCell>(KeyColumns.size()), true, {}, true, false},
            TKeyDesc::ERowOperation::Update,
            keyColumnTypes,
            TVector<TKeyDesc::TColumnOp>{}));

        // TODO: invalidate if retry
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvInvalidateTable(TableId, {}));
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvResolveKeySet(request));
    }

    void HandleResolve(TEvTxProxySchemeCache::TEvResolveKeySetResult::TPtr& ev) {
        if (ev->Get()->Request->ErrorCount > 0) {
            return RuntimeError(TStringBuilder() << "Failed to get partitioning for table: "
                << TableId, NYql::NDqProto::StatusIds::SCHEME_ERROR);
        }

        auto& resultSet = ev->Get()->Request->ResultSet;
        YQL_ENSURE(resultSet.size() == 1);
        Shards = resultSet[0].KeyDescription->Partitioning;

        CA_LOG_D("Resolved shards: " << Shards->size());

        ProcessRows();
    }

    //NKikimrDataEvents::TEvWrite BuildWrites() {
    //}

    void HandleWrite(NKikimr::NEvents::TDataEvents::TEvWriteResult::TPtr&) {
    }

    void NotifyCAResume() {
        Callbacks->ResumeExecution();
    }

    void HandleError(TEvPipeCache::TEvDeliveryProblem::TPtr&) {
    }

    void RuntimeError(const TString& message, NYql::NDqProto::StatusIds::StatusCode statusCode, const NYql::TIssues& subIssues = {}) {
        NYql::TIssue issue(message);
        for (const auto& i : subIssues) {
            issue.AddSubIssue(MakeIntrusive<NYql::TIssue>(i));
        }

        NYql::TIssues issues;
        issues.AddIssue(std::move(issue));

        //if (ReadActorSpan) {
        //    ReadActorSpan.EndError(issues.ToOneLineString());
        //}

        Callbacks->OnAsyncOutputError(OutputIndex, std::move(issues), statusCode);
    }

    void PassAway() override {
        {
            const auto guard = BindAllocator();
            PendingRows.clear();
        }
        TActorBootstrapped<TKqpWriteActor>::PassAway();
    }

    void BuildColumns() {
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

        number = 0;
        for (const auto& column : Settings.GetKeyColumns()) {
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

    void ProcessRows() {
        if (!Shards) {
            return;
        }
    }

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

    const i64 MemoryLimit = 1024 * 1024 * 1024;
    i64 MemoryUsed = 0;

    std::shared_ptr<const TVector<TKeyDesc::TPartitionInfo>> Shards;

    std::deque<TVector<TCell>> PendingRows;
    //THashMap<Request, NKikimr::NMiniKQL::TUnboxedValueVector> InflightRows;

    //i64 ShardResolveAttempt;
    //const i64 MaxShardResolveAttempts = 4;

    //TVector<TKeyDesc::TPartitionInfo> Partitions;
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