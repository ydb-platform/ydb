#pragma once

#include "kqp_predictor.h"

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/yql/dq/proto/dq_tasks.pb.h>
#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>

namespace NYql {
    class TExprNode;
    struct TExprContext;

    namespace NNodes {
        class TDqConnection;
    }
}

namespace NKikimr::NKqp {
class TRequestPredictor {
private:
    std::deque<TStagePredictor> StagePredictors;
    std::map<ui64, TStagePredictor*> StagesMap;
public:
    double GetLevelDataVolume(const ui32 level) const;
    TStagePredictor& BuildForStage(const NYql::NNodes::TDqPhyStage& stage, NYql::TExprContext& ctx);
};

}