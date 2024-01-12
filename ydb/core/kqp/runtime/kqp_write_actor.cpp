#include "kqp_write_actor.h"

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/engine/minikql/minikql_engine_host.h>
#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/actorlib_impl/long_timer.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_impl.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/core/actorsystem.h>
//#include <ydb/core/protos/data_events.pb.h>
#include <ydb/core/tx/data_events/events.h>

namespace NKikimr {
namespace NKqp {

class TKqpWriteActor : public TActorBootstrapped<TKqpWriteActor>, public NYql::NDq::IDqComputeActorAsyncOutput {
    using TBase = TActorBootstrapped<TKqpWriteActor>;
public:
    TKqpWriteActor()
        //const NKqpProto::TKqpTableSinkSettings* settings,
        //const NYql::NDq::TDqAsyncIoFactory::TSourceArguments& args,
        //TIntrusivePtr<TKqpCounters> counters)
        //: LogPrefix(TStringBuilder() << "TxId: " << args.TxId << ", task: " << args.TaskId << ", CA Id " << args.ComputeActorId << ". ")
    {
    }

    void Bootstrap() {
        CA_LOG_D("Created Actor !!!!!!!!!!!!!!!!!");
        LogPrefix = TStringBuilder() << "SelfId: " << this->SelfId() << ", " << LogPrefix;
        Become(&TKqpWriteActor::StateFunc);
    }

    static constexpr char ActorName[] = "KQP_WRITE_ACTOR";

private:
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
        //EgressStats.Resume();

        //DoProcess();

        if (finished) {
            Callbacks->OnAsyncOutputFinished(GetOutputIndex());
        }
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

    //void ResolveShards() {
   // //    CA_LOG_D("Resolve shards for table: ");
   // }

    void HandleResolve(TEvTxProxySchemeCache::TEvResolveKeySetResult::TPtr&) {
    }

    //void StartTableWrite() {
   // }

    //NKikimrDataEvents::TEvWrite BuildWrites() {
    //}

    void HandleWrite(NKikimr::NEvents::TDataEvents::TEvWriteResult::TPtr&) {
    }

    //void NotifyCAResume() {
    //}

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
        TActorBootstrapped<TKqpWriteActor>::PassAway();
    }

    // const NKqpProto::TKqpTableSinkSettings* Settings;
    // TIntrusivePtr<NActors::TProtoArenaHolder> Arena;
    const ui64 OutputIndex = 0;
    NYql::NDq::TDqAsyncStats EgressStats;
    NYql::NDq::IDqComputeActorAsyncOutput::ICallbacks * Callbacks = nullptr;
    // const NMiniKQL::TTypeEnvironment& TypeEnv;
    // const NMiniKQL::THolderFactory& HolderFactory;
    // std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> Alloc;
    // TIntrusivePtr<TKqpCounters> Counters;

    TString LogPrefix;
    //const TTxId TxId;
    // TTableId TableId;

    const i64 MemoryLimit = 1024 * 1024 * 1024;
    i64 MemoryUsed = 0;
    

    //i64 ShardResolveAttempt;
    //const i64 MaxShardResolveAttempts = 4;

    //TVector<TKeyDesc::TPartitionInfo> Partitions;
};

void RegisterKqpWriteActor(NYql::NDq::TDqAsyncIoFactory& factory, TIntrusivePtr<TKqpCounters> counters) {
    factory.RegisterSink<NKqpProto::TKqpTableSinkSettings>(
        TString(NYql::KqpTableSinkName),
        [counters] (NKqpProto::TKqpTableSinkSettings&& settings, NYql::NDq::TDqAsyncIoFactory::TSinkArguments&& args) {
            Y_UNUSED(settings, args, counters);
            auto* actor = new TKqpWriteActor();
            return std::make_pair<NYql::NDq::IDqComputeActorAsyncOutput*, NActors::IActor*>(actor, actor);
        });
}

}
}