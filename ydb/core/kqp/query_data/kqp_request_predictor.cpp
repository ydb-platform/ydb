#include "kqp_request_predictor.h"

#include <ydb/core/kqp/common/kqp_yql.h>

namespace NKikimr::NKqp {

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
