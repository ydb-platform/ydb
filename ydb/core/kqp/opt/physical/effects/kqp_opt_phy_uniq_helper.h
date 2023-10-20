#pragma once

#include "kqp_opt_phy_effects_impl.h"
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>

namespace NYql {
    class TKikimrTableDescription;
}

namespace NKikimr::NKqp::NOpt {

class TUniqBuildHelper {
public:
    // table - metadata of table
    // skipPkCheck - false for insert mode, generate check on PK to issue an arror on PK conflict
    TUniqBuildHelper(const NYql::TKikimrTableDescription& table, NYql::TPositionHandle pos,
        NYql::TExprContext& ctx, bool skipPkCheck);
    size_t GetChecksNum() const;

    NYql::NNodes::TDqStage CreateComputeKeysStage(const TCondenseInputResult& condenseResult,
        NYql::TPositionHandle pos, NYql::TExprContext& ctx) const;
    NYql::NNodes::TDqPhyPrecompute CreateInputPrecompute(const NYql::NNodes::TDqStage& computeKeysStage,
        NYql::TPositionHandle pos, NYql::TExprContext& ctx) const;
    TVector<NYql::NNodes::TExprBase> CreateUniquePrecompute(const NYql::NNodes::TDqStage& computeKeysStage,
        NYql::TPositionHandle pos, NYql::TExprContext& ctx) const;
    NYql::NNodes::TDqStage CreateLookupExistStage(const NYql::NNodes::TDqStage& computeKeysStage,
        const NYql::TKikimrTableDescription& table, NYql::TExprNode::TPtr _true,
        NYql::TPositionHandle pos, NYql::TExprContext& ctx) const;

private:
    struct TUniqCheckNodes {
        using TIndexId = int;
        static constexpr TIndexId NOT_INDEX_ID = -1;
        NYql::TExprNode::TPtr DictKeys;
        NYql::TExprNode::TPtr UniqCmp;
        TIndexId IndexId = NOT_INDEX_ID;
    };

    class TChecks {
        public:
            TChecks(std::pair<TVector<TUniqCheckNodes>, NYql::TExprNode::TPtr>&& pair)
                : Checks(std::move(pair.first))
                , PkDict(pair.second)
            {}

            size_t Size() const {
                return Checks.size();
            }

            const TUniqCheckNodes& operator [](size_t i) const {
                return Checks[i];
            }

            const NYql::TExprNode::TPtr GetPkDict() const {
                return PkDict;
            }
        private:
            const TVector<TUniqCheckNodes> Checks;
            const NYql::TExprNode::TPtr PkDict;
    };

    size_t CalcComputeKeysStageOutputNum() const;

    static TUniqCheckNodes MakeUniqCheckNodes(const NYql::NNodes::TCoLambda& selector,
        const NYql::NNodes::TExprBase& rowsListArg, NYql::TPositionHandle pos, NYql::TExprContext& ctx);
    static std::pair<TVector<TUniqCheckNodes>, NYql::TExprNode::TPtr> Prepare(const NYql::NNodes::TCoArgument& rowsListArg,
        const NYql::TKikimrTableDescription& table, NYql::TPositionHandle pos, NYql::TExprContext& ctx,
        bool skipPkCheck);

    NYql::NNodes::TCoArgument RowsListArg;
    const TChecks Checks;
};

}
