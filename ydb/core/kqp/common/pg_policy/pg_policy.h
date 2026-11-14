#pragma once

#include <yql/essentials/core/yql_type_annotation.h>

namespace NKikimr::NKqp {

NYql::TTypeAnnCallableFactory CreateKqpTypeAnnCallableFactory(NYql::TTypeAnnotationContext& types);
bool InstantAnnotateKqpTypes(NYql::TExprNode::TPtr& root, NYql::TExprContext& ctx, NYql::TTypeAnnotationContext& types);

} // namespace NKikimr::NKqp
