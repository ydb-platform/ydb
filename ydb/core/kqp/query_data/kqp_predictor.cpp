#include "kqp_predictor.h"
#include "kqp_request_predictor.h"

#include <ydb/core/base/appdata.h>
#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <util/system/info.h>
#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/core/kqp/expr_nodes/kqp_expr_nodes.h>
#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/library/services/services.pb.h>

namespace NKikimr::NKqp {

using namespace NActors;

void TStagePredictor::Prepare() {
    InputDataPrediction = 1;
    if (HasLookupFlag) {
        InputDataPrediction = 0.5;
    } else if (HasRangeScanFlag) {
        InputDataPrediction = 1;
    } else if (InputDataVolumes.size()) {
        InputDataPrediction = 0;
    }

    for (auto&& i : InputDataVolumes) {
        InputDataPrediction += i;
    }

    OutputDataPrediction = InputDataPrediction;
    if (HasTopFlag) {
        OutputDataPrediction = InputDataPrediction * 0.01;
    } else if (HasStateCombinerFlag || HasFinalCombinerFlag) {
        if (GroupByKeysCount) {
            OutputDataPrediction = InputDataPrediction;
        } else {
            OutputDataPrediction = InputDataPrediction * 0.01;
        }
    }
}

void TStagePredictor::Scan(const NYql::TExprNode::TPtr& stageNode) {
    NYql::VisitExpr(stageNode, [&](const NYql::TExprNode::TPtr& exprNode) {
        NYql::NNodes::TExprBase node(exprNode);
        ++NodesCount;
        if (node.Maybe<NYql::NNodes::TCoCondense>() || node.Ref().Content() == "WideCondense1" || node.Maybe<NYql::NNodes::TCoCondense1>()) {
            HasCondenseFlag = true;
        } else if (node.Maybe<NYql::NNodes::TKqpWideReadTable>()) {
            HasRangeScanFlag = true;
        } else if (node.Maybe<NYql::NNodes::TKqpLookupTable>()) {
            HasLookupFlag = true;
        } else if (node.Maybe<NYql::NNodes::TKqpUpsertRows>()) {
        } else if (node.Maybe<NYql::NNodes::TKqpDeleteRows>()) {

        } else if (node.Maybe<NYql::NNodes::TKqpWideReadTableRanges>() || node.Maybe<NYql::NNodes::TKqpWideReadOlapTableRanges>()) {
            HasRangeScanFlag = true;
        } else if (node.Maybe<NYql::NNodes::TCoSort>()) {
            HasSortFlag = true;
        } else if (node.Maybe<NYql::NNodes::TCoKeepTop>() || node.Maybe<NYql::NNodes::TCoTop>() || node.Maybe<NYql::NNodes::TCoWideTop>()) {
            HasTopFlag = true;
        } else if (node.Maybe<NYql::NNodes::TCoTopSort>() || node.Maybe<NYql::NNodes::TCoWideTopSort>()) {
            HasTopFlag = true;
            HasSortFlag = true;
        } else if (node.Maybe<NYql::NNodes::TCoFilterBase>()) {
            HasFilterFlag = true;
        } else if (node.Maybe<NYql::NNodes::TCoWideCombiner>()) {
            auto wCombiner = node.Cast<NYql::NNodes::TCoWideCombiner>();
            GroupByKeysCount = wCombiner.KeyExtractor().Ptr()->ChildrenSize() - 1;
            if (wCombiner.MemLimit() != "") {
                HasFinalCombinerFlag = true;
            } else {
                HasStateCombinerFlag = true;
            }
        } else if (node.Maybe<NYql::NNodes::TCoWideCombinerWithSpilling>()) {
            auto wCombiner = node.Cast<NYql::NNodes::TCoWideCombinerWithSpilling>();
            GroupByKeysCount = wCombiner.KeyExtractor().Ptr()->ChildrenSize() - 1;
            if (wCombiner.MemLimit() != "") {
                HasFinalCombinerFlag = true;
            } else {
                HasStateCombinerFlag = true;
            }
        } else if (node.Maybe<NYql::NNodes::TCoMapJoinCore>()) {
            HasMapJoinFlag = true;
        } else if (node.Maybe<NYql::NNodes::TCoUdf>()) {
            HasUdfFlag = true;
        }
        return true;
        });
}

void TStagePredictor::AcceptInputStageInfo(const TStagePredictor& info, const NYql::NNodes::TDqConnection& /*connection*/) {
    StageLevel = Max<ui32>(StageLevel, info.StageLevel + 1);
    InputDataVolumes.emplace_back(info.GetOutputDataPrediction());
}

void TStagePredictor::SerializeToKqpSettings(NYql::NDqProto::TProgram::TSettings& kqpProto) const {
    kqpProto.SetHasMapJoin(HasMapJoinFlag);
    kqpProto.SetHasSort(HasSortFlag);
    kqpProto.SetHasUdf(HasUdfFlag);
    kqpProto.SetHasFinalAggregation(HasFinalCombinerFlag);
    kqpProto.SetHasStateAggregation(HasStateCombinerFlag);
    kqpProto.SetGroupByKeysCount(GroupByKeysCount);
    kqpProto.SetHasFilter(HasFilterFlag);
    kqpProto.SetHasTop(HasTopFlag);
    kqpProto.SetHasRangeScan(HasRangeScanFlag);
    kqpProto.SetHasCondense(HasCondenseFlag);
    kqpProto.SetHasLookup(HasLookupFlag);
    kqpProto.SetNodesCount(NodesCount);
    kqpProto.SetInputDataPrediction(InputDataPrediction);
    kqpProto.SetOutputDataPrediction(OutputDataPrediction);
    kqpProto.SetStageLevel(StageLevel);
    kqpProto.SetLevelDataPrediction(LevelDataPrediction.value_or(1));
}

bool TStagePredictor::DeserializeFromKqpSettings(const NYql::NDqProto::TProgram::TSettings& kqpProto) {
    HasMapJoinFlag = kqpProto.GetHasMapJoin();
    HasSortFlag = kqpProto.GetHasSort();
    HasUdfFlag = kqpProto.GetHasUdf();
    HasFinalCombinerFlag = kqpProto.GetHasFinalAggregation();
    HasStateCombinerFlag = kqpProto.GetHasStateAggregation();
    GroupByKeysCount = kqpProto.GetGroupByKeysCount();
    HasFilterFlag = kqpProto.GetHasFilter();
    HasTopFlag = kqpProto.GetHasTop();
    HasRangeScanFlag = kqpProto.GetHasRangeScan();
    HasCondenseFlag = kqpProto.GetHasCondense();
    HasLookupFlag = kqpProto.GetHasLookup();
    NodesCount = kqpProto.GetNodesCount();
    InputDataPrediction = kqpProto.GetInputDataPrediction();
    OutputDataPrediction = kqpProto.GetOutputDataPrediction();
    StageLevel = kqpProto.GetStageLevel();
    LevelDataPrediction = kqpProto.GetLevelDataPrediction();
    return true;
}

ui32 TStagePredictor::GetUsableThreads() {
    std::optional<ui32> userPoolSize;
    if (TlsActivationContext && TlsActivationContext->ActorSystem()) {
        userPoolSize = TlsActivationContext->ActorSystem()->GetPoolThreadsCount(AppData()->UserPoolId);
    }
    if (!userPoolSize) {
        ALS_ERROR(NKikimrServices::KQP_EXECUTER) << "user pool is undefined for executer tasks construction";
        userPoolSize = NSystemInfo::NumberOfCpus();
    }
    return Max<ui32>(1, *userPoolSize);
}

ui32 TStagePredictor::CalcTasksOptimalCount(const ui32 availableThreadsCount, const std::optional<ui32> previousStageTasksCount) const {
    ui32 result = 0;
    if (!LevelDataPrediction || *LevelDataPrediction == 0) {
        ALS_ERROR(NKikimrServices::KQP_EXECUTER) << "level difficult not defined for correct calculation";
        result = availableThreadsCount;
    } else {
        result = (availableThreadsCount - previousStageTasksCount.value_or(0) * 0.25) * (InputDataPrediction / *LevelDataPrediction);
    }
    if (previousStageTasksCount) {
        result = std::min<ui32>(result, *previousStageTasksCount);
    }
    return std::max<ui32>(1, result);
}

bool TStagePredictor::NeedLLVM() const {
    return HasStateCombinerFlag || HasFinalCombinerFlag || HasCondenseFlag;
}

TStagePredictor& TRequestPredictor::BuildForStage(const NYql::NNodes::TDqPhyStage& stage, NYql::TExprContext& ctx) {
    StagePredictors.emplace_back();
    TStagePredictor& result = StagePredictors.back();
    StagesMap.emplace(stage.Ref().UniqueId(), &result);
    result.Scan(stage.Program().Ptr());

    for (ui32 inputIndex = 0; inputIndex < stage.Inputs().Size(); ++inputIndex) {
        const auto& input = stage.Inputs().Item(inputIndex);

        if (input.Maybe<NYql::NNodes::TDqSource>()) {
        } else {
            YQL_ENSURE(input.Maybe<NYql::NNodes::TDqConnection>());
            auto connection = input.Cast<NYql::NNodes::TDqConnection>();
            auto it = StagesMap.find(connection.Output().Stage().Ref().UniqueId());
            YQL_ENSURE(it != StagesMap.end(), "stage #" << connection.Output().Stage().Ref().UniqueId() << " not found in stages map for prediction: "
                << PrintKqpStageOnly(connection.Output().Stage(), ctx));
            result.AcceptInputStageInfo(*it->second, connection);
        }
    }
    result.Prepare();
    return result;
}

double TRequestPredictor::GetLevelDataVolume(const ui32 level) const {
    double result = 0;
    for (auto&& i : StagePredictors) {
        if (i.GetStageLevel() == level) {
            result += i.GetInputDataPrediction();
        }
    }
    return result;
}

}
