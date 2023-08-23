#pragma once

#include "type_ann_core.h"

#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/ast/yql_expr_types.h>

namespace NYql {
namespace NTypeAnnImpl {
    template <ETypeAnnotationKind>
    IGraphTransformer::TStatus TypeWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);

    #define TYPE_ANN_TYPE_ARGUMENT_MAP(xx) \
        xx(Unknown, 0) \
        xx(OptionalItem, 1) \
        xx(ListItem, 2) \
        xx(TupleElement, 3) \
        xx(StructMember, 4) \
        xx(DictKey, 5) \
        xx(DictPayload, 6) \
        xx(CallableResult, 7) \
        xx(CallableArgument, 8) \
        xx(AddMember, 9) \
        xx(RemoveMember, 10) \
        xx(ForceRemoveMember, 11) \
        xx(FlattenMembers, 12) \
        xx(VariantUnderlying, 13) \
        xx(StreamItem, 14)

    enum class ETypeArgument {
        TYPE_ANN_TYPE_ARGUMENT_MAP(ENUM_VALUE_GEN)
    };

    template <ETypeArgument>
    IGraphTransformer::TStatus TypeArgWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
    IGraphTransformer::TStatus ParseTypeWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx);
    IGraphTransformer::TStatus FormatTypeWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
    IGraphTransformer::TStatus FormatTypeDiffWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
    IGraphTransformer::TStatus TypeHandleWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
    IGraphTransformer::TStatus SerializeTypeHandleWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
    IGraphTransformer::TStatus ParseTypeHandleWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
    IGraphTransformer::TStatus TypeKindWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
    IGraphTransformer::TStatus CallableArgumentWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);

    template <ETypeAnnotationKind>
    IGraphTransformer::TStatus SplitTypeHandleWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);

    template <ETypeAnnotationKind>
    IGraphTransformer::TStatus MakeTypeHandleWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);

    IGraphTransformer::TStatus FormatCodeWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
    IGraphTransformer::TStatus ReprCodeWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
    IGraphTransformer::TStatus RestartEvaluationWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
    IGraphTransformer::TStatus EvaluateExprIfPureWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);

    template <TExprNode::EType>
    IGraphTransformer::TStatus MakeCodeWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx);
} // namespace NTypeAnnImpl
} // namespace NYql
