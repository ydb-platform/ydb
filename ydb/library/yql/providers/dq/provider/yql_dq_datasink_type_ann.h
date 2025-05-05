#pragma once

#include <yql/essentials/providers/common/transform/yql_visit.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>

#include <util/generic/ptr.h>

namespace NYql {

THolder<TVisitorTransformerBase> CreateDqsDataSinkTypeAnnotationTransformer(TTypeAnnotationContext* typeCtx);

} // NYql
