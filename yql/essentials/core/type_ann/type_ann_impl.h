#pragma once

#include "type_ann_core.h"

#include <yql/essentials/core/yql_expr_type_annotation.h>

#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/ast/yql_expr_types.h>


namespace NYql::NTypeAnnImpl {

    struct TContext {
        TExprContext& Expr;

        explicit TContext(TExprContext& expr);
    };

    struct TExtContext : public TContext {
        TTypeAnnotationContext& Types;

        TExtContext(TExprContext& expr, TTypeAnnotationContext& types);
        bool LoadUdfMetadata(const TVector<IUdfResolver::TFunction*>& functions);
        void RegisterResolvedImport(const IUdfResolver::TImport& import);
    };

    const TTypeAnnotationNode* ParseTypeCached(const TString& typeStr, TExprContext& ctx, TTypeAnnotationContext& typeCtx);
    TExprNodeBuilder& AddChildren(TExprNodeBuilder& builder, ui32 index, const TExprNode::TPtr& input);

    // Implemented in type_ann_join.cpp
    IGraphTransformer::TStatus JoinWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
    IGraphTransformer::TStatus JoinDictWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
    IGraphTransformer::TStatus MapJoinCoreWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
    IGraphTransformer::TStatus GraceJoinCoreWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
    IGraphTransformer::TStatus GraceSelfJoinCoreWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
    IGraphTransformer::TStatus CommonJoinCoreWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
    IGraphTransformer::TStatus EquiJoinWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
    IGraphTransformer::TStatus CombineCoreWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
    IGraphTransformer::TStatus GroupingCoreWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
    IGraphTransformer::TStatus DecimalBinaryWrapperBase(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx, bool blocks);
    IGraphTransformer::TStatus BlockStorageWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
    IGraphTransformer::TStatus BlockMapJoinIndexWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx);
    IGraphTransformer::TStatus BlockMapJoinCoreWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx);

    TMaybe<ui32> FindOrReportMissingMember(TStringBuf memberName, TPositionHandle pos, const TStructExprType& structType, TExprContext& ctx);

    TExprNode::TPtr MakeNothingData(TExprContext& ctx, TPositionHandle pos, TStringBuf data);
} // namespace NYql::NTypeAnnImpl
