#include <ydb/core/protos/tx_datashard.pb.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/accessor/accessor.h>
#include <ydb/library/yql/utils/yql_panic.h>
#include <ydb/library/yql/dq/proto/dq_tasks.pb.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>
#include <ydb/core/kqp/rm_service/kqp_rm_service.h>

#include <ydb/core/kqp/runtime/kqp_compute_scheduler.h>

#include <vector>

namespace NKikimr::NKqp {
struct TKqpFederatedQuerySetup;
}

namespace NKikimr::NKqp::NComputeActor {

class TMetaScan {
private:
    YDB_ACCESSOR_DEF(std::vector<NActors::TActorId>, ActorIds);
    YDB_ACCESSOR_DEF(NKikimrTxDataShard::TKqpTransaction::TScanTaskMeta, Meta);
public:
    explicit TMetaScan(const NKikimrTxDataShard::TKqpTransaction::TScanTaskMeta& meta)
        : Meta(meta)
    {

    }
};

class TComputeStageInfo {
private:
    YDB_ACCESSOR_DEF(std::deque<TMetaScan>, MetaInfo);
    std::map<ui32, TMetaScan*> MetaWithIds;
public:
    TComputeStageInfo() = default;

    bool GetMetaById(const ui32 metaId, NKikimrTxDataShard::TKqpTransaction::TScanTaskMeta& result) const {
        auto it = MetaWithIds.find(metaId);
        if (it == MetaWithIds.end()) {
            return false;
        }
        result = it->second->GetMeta();
        return true;
    }

    TMetaScan& MergeMetaReads(const NYql::NDqProto::TDqTask& task, const NKikimrTxDataShard::TKqpTransaction::TScanTaskMeta& meta, const bool forceOneToMany) {
        YQL_ENSURE(meta.ReadsSize(), "unexpected merge with no reads");
        if (forceOneToMany || !task.HasMetaId()) {
            MetaInfo.emplace_back(TMetaScan(meta));
            return MetaInfo.back();
        } else {
            auto it = MetaWithIds.find(task.GetMetaId());
            if (it == MetaWithIds.end()) {
                MetaInfo.emplace_back(TMetaScan(meta));
                return *MetaWithIds.emplace(task.GetMetaId(), &MetaInfo.back()).first->second;
            } else {
                return *it->second;
            }
        }
    }
};

class TComputeStagesWithScan {
private:
    std::map<ui32, TComputeStageInfo> Stages;
public:
    std::map<ui32, TComputeStageInfo>::iterator begin() {
        return Stages.begin();
    }

    std::map<ui32, TComputeStageInfo>::iterator end() {
        return Stages.end();
    }

    bool GetMetaById(const NYql::NDqProto::TDqTask& dqTask, NKikimrTxDataShard::TKqpTransaction::TScanTaskMeta& result) const {
        if (!dqTask.HasMetaId()) {
            return false;
        }
        auto it = Stages.find(dqTask.GetStageId());
        if (it == Stages.end()) {
            return false;
        } else {
            return it->second.GetMetaById(dqTask.GetMetaId(), result);
        }
    }

    TMetaScan& UpsertTaskWithScan(const NYql::NDqProto::TDqTask& dqTask, const NKikimrTxDataShard::TKqpTransaction::TScanTaskMeta& meta, const bool forceOneToMany) {
        auto it = Stages.find(dqTask.GetStageId());
        if (it == Stages.end()) {
            it = Stages.emplace(dqTask.GetStageId(), TComputeStageInfo()).first;
        }
        return it->second.MergeMetaReads(dqTask, meta, forceOneToMany);
    }
};

struct IKqpNodeState {
    virtual ~IKqpNodeState() = default;

    virtual void OnTaskTerminate(ui64 txId, ui64 taskId, bool success) = 0;
};


struct IKqpNodeComputeActorFactory {
    virtual ~IKqpNodeComputeActorFactory() = default;

public:
    struct TCreateArgs {
        const NActors::TActorId& ExecuterId;
        const ui64 TxId;
        const ui64 LockTxId;
        const ui32 LockNodeId;
        NYql::NDqProto::TDqTask* Task;
        TIntrusivePtr<NRm::TTxState> TxInfo;
        const NYql::NDq::TComputeRuntimeSettings& RuntimeSettings;
        NWilson::TTraceId TraceId;
        TIntrusivePtr<NActors::TProtoArenaHolder> Arena;
        const TString& SerializedGUCSettings;
        const ui32 NumberOfTasks;
        const ui64 OutputChunkMaxSize;
        const NKikimr::NKqp::NRm::EKqpMemoryPool MemoryPool;
        const bool WithSpilling;
        const NYql::NDqProto::EDqStatsMode StatsMode;
        const TInstant& Deadline;
        const bool ShareMailbox;
        const TMaybe<NYql::NDqProto::TRlPath>& RlPath;

        TComputeStagesWithScan* ComputesByStages = nullptr;
        std::shared_ptr<IKqpNodeState> State = nullptr;
        TComputeActorSchedulingOptions SchedulingOptions = {};
    };

    typedef std::variant<TActorId, NKikimr::NKqp::NRm::TKqpRMAllocateResult> TActorStartResult;
    virtual TActorStartResult CreateKqpComputeActor(TCreateArgs&& args) = 0;

    virtual void ApplyConfig(const NKikimrConfig::TTableServiceConfig::TResourceManager& config) = 0;
};

std::shared_ptr<IKqpNodeComputeActorFactory> MakeKqpCaFactory(const NKikimrConfig::TTableServiceConfig::TResourceManager& config,
        std::shared_ptr<NRm::IKqpResourceManager> resourceManager,
        NYql::NDq::IDqAsyncIoFactory::TPtr asyncIoFactory,
        const std::optional<TKqpFederatedQuerySetup> federatedQuerySetup);

} // namespace NKikimr::NKqp::NComputeActor
