#include "kqp_predictor.h"
#include "kqp_request_predictor.h"

#include <ydb/core/base/appdata.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <util/system/info.h>
#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>
#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/core/kqp/expr_nodes/kqp_expr_nodes.h>
#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/library/actors/core/subsystems/stats.h>
#include <ydb/library/services/services.pb.h>

#include <util/string/cast.h>

#include <cmath>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::KQP_EXECUTER

namespace NKikimr::NKqp {

using namespace NActors;

void TStagePredictor::Prepare() {
    InputDataPrediction = 1;
    if (HasRangeScanFlag) {
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
        } else if (node.Maybe<NYql::NNodes::TCoMapJoinCore>()) {
            HasMapJoinFlag = true;
        } else if (const auto maybeWatermarkGenerator = node.Maybe<NYql::NNodes::TDqPhyWatermarkGenerator>()) {
            HasWatermarkGeneratorFlag = true;

            const auto watermarkGenerator = maybeWatermarkGenerator.Cast();
            for (const auto& nameValue : watermarkGenerator.WatermarkSettings()) {
                if (nameValue.Name().Value() != "WatermarksIdleTimeoutUs") {
                    continue;
                }

                ui64 idleTimeoutUs = 0;
                if (TryFromString<ui64>(nameValue.Value().Cast<NYql::NNodes::TCoAtom>().Value(), idleTimeoutUs)) {
                    WatermarkGeneratorIdleTimeoutUs = Max(WatermarkGeneratorIdleTimeoutUs.value_or(0), idleTimeoutUs);
                }
            }
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
    kqpProto.SetHasWatermarkGenerator(HasWatermarkGeneratorFlag);
    if (WatermarkGeneratorIdleTimeoutUs) {
        kqpProto.SetWatermarkGeneratorIdleTimeoutUs(*WatermarkGeneratorIdleTimeoutUs);
    }
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
    HasWatermarkGeneratorFlag = kqpProto.GetHasWatermarkGenerator();
    if (kqpProto.HasWatermarkGeneratorIdleTimeoutUs()) {
        WatermarkGeneratorIdleTimeoutUs = kqpProto.GetWatermarkGeneratorIdleTimeoutUs();
    } else {
        WatermarkGeneratorIdleTimeoutUs.reset();
    }
    NodesCount = kqpProto.GetNodesCount();
    InputDataPrediction = kqpProto.GetInputDataPrediction();
    OutputDataPrediction = kqpProto.GetOutputDataPrediction();
    StageLevel = kqpProto.GetStageLevel();
    LevelDataPrediction = kqpProto.GetLevelDataPrediction();
    return true;
}

ui32 TStagePredictor::GetUsableThreads() {
    std::optional<ui32> userPoolSize;
    if (HasAppData() && TlsActivationContext && TlsActivationContext->ActorSystem()) {
        userPoolSize = TlsActivationContext->ActorSystem()->GetPoolThreadsCount(AppData()->UserPoolId);
    }
    if (!userPoolSize) {
        YDB_LOG_INFO("User pool is undefined for executer tasks construction");
        userPoolSize = NSystemInfo::NumberOfCpus();
    }
    return Max<ui32>(1, *userPoolSize);
}

ui32 TStagePredictor::GetPossibleMaxLimitThreads() {
    const ui32 usableThreads = GetUsableThreads();
    if (HasAppData() && TlsActivationContext && TlsActivationContext->ActorSystem()) {
        TExecutorPoolState poolState;
        GetActorSystemStats().GetExecutorPoolState(AppData()->UserPoolId, poolState);
        if (poolState.PossibleMaxLimit > 0) {
            return Max(usableThreads, static_cast<ui32>(std::ceil(poolState.PossibleMaxLimit)));
        }
    }

    return usableThreads;
}

ui32 TStagePredictor::CalcTasksOptimalCount(const ui32 availableThreadsCount, const std::optional<ui32> previousStageTasksCount) const {
    ui32 result = 0;
    if (!LevelDataPrediction || *LevelDataPrediction == 0) {
        YDB_LOG_ERROR("Level difficulty not defined for correct calculation");
        result = availableThreadsCount;
    } else {
        result = (availableThreadsCount - previousStageTasksCount.value_or(0) * 0.25) * (InputDataPrediction / *LevelDataPrediction);
    }
    if (previousStageTasksCount && *previousStageTasksCount > 0) {
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
