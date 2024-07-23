#pragma once

#include "type_ann_core.h"

#include <ydb/library/yql/core/yql_expr_type_annotation.h>

#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/ast/yql_expr_types.h>

namespace NYql {
namespace NTypeAnnImpl {

    struct TContext {
        TExprContext& Expr;

        TContext(TExprContext& expr);
    };

    struct TExtContext : public TContext {
        TTypeAnnotationContext& Types;

        TExtContext(TExprContext& expr, TTypeAnnotationContext& types);
        bool LoadUdfMetadata(const TVector<IUdfResolver::TFunction*>& functions);
        void RegisterResolvedImport(const IUdfResolver::TImport& import);
    };

    // Implemented in type_ann_join.cpp
    IGraphTransformer::TStatus JoinWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
    IGraphTransformer::TStatus JoinDictWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
    IGraphTransformer::TStatus MapJoinCoreWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
    IGraphTransformer::TStatus GraceJoinCoreWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
    IGraphTransformer::TStatus GraceSelfJoinCoreWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
    IGraphTransformer::TStatus CommonJoinCoreWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
    IGraphTransformer::TStatus EquiJoinWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
    IGraphTransformer::TStatus CombineCoreWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
    IGraphTransformer::TStatus CombineCoreWithSpillingWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
    IGraphTransformer::TStatus GroupingCoreWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);

    TMaybe<ui32> FindOrReportMissingMember(TStringBuf memberName, TPositionHandle pos, const TStructExprType& structType, TExprContext& ctx);

    TExprNode::TPtr MakeNothingData(TExprContext& ctx, TPositionHandle pos, TStringBuf data);
} // namespace NTypeAnnImpl
} // namespace NYql
