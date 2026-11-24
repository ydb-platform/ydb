#pragma once

#include <yql/essentials/core/yql_type_annotation.h>

#include <util/generic/strbuf.h>

namespace NKikimr::NKqp {

bool IsPgCallable(TStringBuf name);

bool ValidateOptimizedKqpPgTypes(const NYql::TExprNode::TPtr& root, NYql::TExprContext& ctx);

NYql::TTypeAnnCallableFactory CreateKqpTypeAnnCallableFactory(NYql::TTypeAnnotationContext& types, bool disablePgCompatibility);
bool InstantAnnotateKqpTypes(NYql::TExprNode::TPtr& root, NYql::TExprContext& ctx, NYql::TTypeAnnotationContext& types, bool disablePgCompatibility);

} // namespace NKikimr::NKqp
