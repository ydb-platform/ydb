#pragma once

#include <yql/essentials/core/yql_graph_transformer.h>
#include <yql/essentials/core/yql_type_annotation.h>
#include <ydb/library/yql/providers/dq/common/yql_dq_settings.h>

#include <util/generic/ptr.h>

namespace NYql::NDqs {

THolder<IGraphTransformer> CreateDqsLogOptTransformer(TTypeAnnotationContext* typeCtx, const TDqConfiguration::TPtr& config);

} // namespace NYql::NDqs
