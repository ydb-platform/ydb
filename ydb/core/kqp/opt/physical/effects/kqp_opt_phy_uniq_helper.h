#pragma once

#include "kqp_opt_phy_effects_impl.h"
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>

namespace NYql {
    class TKikimrTableDescription;
}

namespace NKikimr::NKqp::NOpt {

class TUniqBuildHelper {
public:
    using TPtr = std::unique_ptr<TUniqBuildHelper>;
    
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

    virtual ~TUniqBuildHelper() = default;
protected:
    // table - metadata of table
    TUniqBuildHelper(const NYql::TKikimrTableDescription& table, const TMaybe<THashSet<TStringBuf>>& inputColumns,
        const THashSet<TString>* usedIndexes, NYql::TPositionHandle pos, NYql::TExprContext& ctx, bool insertMode);
    size_t CalcComputeKeysStageOutputNum() const;

    struct TUniqCheckNodes {
        using TIndexId = int;
        static constexpr TIndexId NOT_INDEX_ID = -1;
        NYql::TExprNode::TPtr DictKeys;
        NYql::TExprNode::TPtr UniqCmp;
        TIndexId IndexId = NOT_INDEX_ID;
    };

private:
    class TChecks {
        public:
            TChecks(TVector<TUniqCheckNodes> nodes)
                : Checks(std::move(nodes))
            {}

            size_t Size() const {
                return Checks.size();
            }

            const TUniqCheckNodes& operator [](size_t i) const {
                return Checks[i];
            }

        private:
            const TVector<TUniqCheckNodes> Checks;
    };


    static TUniqCheckNodes MakeUniqCheckNodes(const NYql::NNodes::TCoLambda& selector,
        const NYql::NNodes::TExprBase& rowsListArg, NYql::TPositionHandle pos, NYql::TExprContext& ctx);
    static TVector<TUniqCheckNodes> Prepare(const NYql::NNodes::TCoArgument& rowsListArg,
        const NYql::TKikimrTableDescription& table, const TMaybe<THashSet<TStringBuf>>& inputColumns,
        const THashSet<TString>* usedIndexes, NYql::TPositionHandle pos,
        NYql::TExprContext& ctx, bool skipPkCheck);

    virtual NYql::NNodes::TDqCnUnionAll CreateLookupStageWithConnection(const NYql::NNodes::TDqStage& computeKeysStage,
        size_t stageOut, const NYql::TKikimrTableMetadata& mainTableMeta, TUniqCheckNodes::TIndexId indexId,
        NYql::TPositionHandle pos, NYql::TExprContext& ctx) const = 0;
    virtual const NYql::TExprNode::TPtr GetPkDict() const = 0;

protected:
    const NYql::NNodes::TCoArgument RowsListArg;
    const NYql::TExprNode::TPtr False;
private:
    const TChecks Checks;
    const NYql::TExprNode::TPtr RowsToPass;
};

TUniqBuildHelper::TPtr CreateInsertUniqBuildHelper(const NYql::TKikimrTableDescription& table,
    const TMaybe<THashSet<TStringBuf>>& inputColumns, NYql::TPositionHandle pos, NYql::TExprContext& ctx);

TUniqBuildHelper::TPtr CreateUpsertUniqBuildHelper(const NYql::TKikimrTableDescription& table,
    const TMaybe<THashSet<TStringBuf>>& inputColumns,
    const THashSet<TString>& usedIndexes, NYql::TPositionHandle pos, NYql::TExprContext& ctx);
}
