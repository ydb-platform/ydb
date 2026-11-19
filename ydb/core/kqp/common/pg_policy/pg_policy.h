#pragma once

#include <yql/essentials/core/yql_type_annotation.h>

namespace NKikimr::NKqp {

NYql::TTypeAnnCallableFactory CreateKqpTypeAnnCallableFactory(NYql::TTypeAnnotationContext& types, bool disablePgCompatibility);
bool InstantAnnotateKqpTypes(NYql::TExprNode::TPtr& root, NYql::TExprContext& ctx, NYql::TTypeAnnotationContext& types, bool disablePgCompatibility);

} // namespace NKikimr::NKqp
