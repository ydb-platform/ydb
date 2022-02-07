#pragma once

#include <ydb/library/yql/core/yql_graph_transformer.h>
#include <ydb/library/yql/core/yql_type_annotation.h>

#include <util/generic/ptr.h>

namespace NYql::NDqs {

THolder<IGraphTransformer> CreateDqsPhyOptTransformer(TTypeAnnotationContext* typeCtx);

} // namespace NYql::NDqs
