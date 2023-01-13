#pragma once

#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>

#include <util/generic/set.h>

namespace NYql {

TSet<TStringBuf> GetColumnsOfStructOrSequenceOfStruct(const TTypeAnnotationNode& type);

const TTypeAnnotationNode* GetSequenceItemType(NNodes::TExprBase listNode, bool allowMultiIO);
const TTypeAnnotationNode* GetSequenceItemType(NNodes::TExprBase listNode, bool allowMultiIO, TExprContext& ctx);
const TTypeAnnotationNode* GetSequenceItemType(TPositionHandle pos, const TTypeAnnotationNode* inputType,
                                               bool allowMultiIO, TExprContext& ctx);
bool GetSequenceItemType(const TExprNode& list, const TTypeAnnotationNode*& itemType, TExprContext& ctx);
const TTypeAnnotationNode* SilentGetSequenceItemType(const TExprNode& list, bool allowMultiIO);

} // namespace NYql
