#pragma once

#include "dqs_task_graph.h"

#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/core/yql_type_annotation.h>
#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>
#include <ydb/library/yql/providers/dq/common/yql_dq_settings.h>
#include <ydb/library/yql/minikql/mkql_function_registry.h>
#include <ydb/library/yql/providers/dq/api/protos/service.pb.h>

#include <util/generic/vector.h>

#include <tuple>

namespace NYql::NDqs {
    struct TPlan {
        TVector<NDqProto::TDqTask> Tasks;
        NActors::TActorId SourceID;
        TString ResultType;
    };

    class IDqsExecutionPlanner {
    public:
        virtual ~IDqsExecutionPlanner() = default;
        virtual TVector<NDqProto::TDqTask> GetTasks(const TVector<NActors::TActorId>& workers) = 0;
        virtual TVector<NDqProto::TDqTask>& GetTasks() = 0;
        virtual NActors::TActorId GetSourceID() const = 0;
        virtual TString GetResultType() const = 0;

        TPlan GetPlan() {
            return TPlan {
                GetTasks(),
                GetSourceID(),
                GetResultType()
            };
        }
    };

    class TDqsExecutionPlanner: public IDqsExecutionPlanner {
    public:
        explicit TDqsExecutionPlanner(const TDqSettings::TPtr& settings,
                                      TIntrusivePtr<TTypeAnnotationContext> typeContext,
                                      NYql::TExprContext& exprContext,
                                      const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
                                      NYql::TExprNode::TPtr dqExprRoot,
                                      NActors::TActorId executerID = NActors::TActorId(),
                                      NActors::TActorId resultID = NActors::TActorId(1, 0, 1, 0));

        void Clear();
        bool CanFallback();
        ui64 MaxDataSizePerJob() {
            return _MaxDataSizePerJob;
        }
        ui64 StagesCount();
        bool PlanExecution(bool canFallback = false);
        TVector<NDqProto::TDqTask> GetTasks(const TVector<NActors::TActorId>& workers) override;
        TVector<NDqProto::TDqTask>& GetTasks() override;

        NActors::TActorId GetSourceID() const override;
        TString GetResultType() const override;

        void SetPublicIds(const THashMap<ui64, ui32>& publicIds) {
            PublicIds = publicIds;
        }

    private:
        bool BuildReadStage(const NNodes::TDqPhyStage& stage, bool dqSource, bool canFallback);
        void ConfigureInputTransformStreamLookup(const NNodes::TDqCnStreamLookup& streamLookup, const NNodes::TDqPhyStage& stage , ui32 inputIndex);
        void BuildConnections(const NNodes::TDqPhyStage& stage);
        void BuildAllPrograms();
        void FillChannelDesc(NDqProto::TChannel& channelDesc, const NDq::TChannel& channel, bool enableSpilling);
        void FillInputDesc(NDqProto::TTaskInput& inputDesc, const TTaskInput& input);
        void FillOutputDesc(NDqProto::TTaskOutput& outputDesc, const TTaskOutput& output, bool enableSpilling);

        void GatherPhyMapping(THashMap<std::tuple<TString, TString>, TString>& clusters, THashMap<std::tuple<TString, TString, TString>, TString>& tables);
        void BuildCheckpointingAndWatermarksMode(bool enableCheckpoints, bool enableWatermarks);
        bool IsEgressTask(const TDqsTasksGraph::TTaskType& task) const;

    private:
        const TDqSettings::TPtr Settings;
        TIntrusivePtr<TTypeAnnotationContext> TypeContext;
        NYql::TExprContext& ExprContext;
        const NKikimr::NMiniKQL::IFunctionRegistry* FunctionRegistry;
        NYql::TExprNode::TPtr DqExprRoot;
        TVector<const TTypeAnnotationNode*> InputType;
        NActors::TActorId ExecuterID;
        NActors::TActorId ResultID;
        TMaybe<NActors::TActorId> SourceID = {};
        ui64 SourceTaskID = 0;
        ui64 _MaxDataSizePerJob = 0;

        TDqsTasksGraph TasksGraph;
        TVector<NDqProto::TDqTask> Tasks;

        THashMap<ui64, ui32> PublicIds;
        THashMap<NDq::TStageId, std::tuple<TString,ui64,ui64>> StagePrograms;
    };

    // Execution planner for TRuntimeNode
    class TDqsSingleExecutionPlanner: public IDqsExecutionPlanner {
    public:
        TDqsSingleExecutionPlanner(
            const TString& program,
            NActors::TActorId executerID,
            NActors::TActorId resultID,
            const TTypeAnnotationNode* typeAnn);

        TVector<NDqProto::TDqTask>& GetTasks() override;
        TVector<NDqProto::TDqTask> GetTasks(const TVector<NActors::TActorId>& workers) override;
        NActors::TActorId GetSourceID() const override;
        TString GetResultType() const override;

    private:
        TString Program;
        NActors::TActorId ExecuterID;
        NActors::TActorId ResultID;

        TMaybe<NActors::TActorId> SourceID = {};
        TVector<NDqProto::TDqTask> Tasks;
        const TTypeAnnotationNode* TypeAnn;
    };

    // Execution planner for Graph
    class TGraphExecutionPlanner: public IDqsExecutionPlanner {
    public:
        TGraphExecutionPlanner(
            const TVector<NDqProto::TDqTask>& tasks,
            ui64 sourceId,
            const TString& resultType,
            NActors::TActorId executerID,
            NActors::TActorId resultID);

        TVector<NDqProto::TDqTask>& GetTasks() override {
            ythrow yexception() << "unimplemented";
        }
        TVector<NDqProto::TDqTask> GetTasks(const TVector<NActors::TActorId>& workers) override;
        NActors::TActorId GetSourceID() const override;
        TString GetResultType() const override;

    private:
        TVector<NDqProto::TDqTask> Tasks;
        ui64 SourceId = 0;
        TString ResultType;

        NActors::TActorId ExecuterID;
        NActors::TActorId ResultID;

        TMaybe<NActors::TActorId> SourceID = {};
    };
}
