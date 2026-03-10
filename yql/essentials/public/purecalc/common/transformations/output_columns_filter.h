#pragma once

#include <yql/essentials/public/purecalc/common/processor_mode.h>

#include <yql/essentials/core/yql_graph_transformer.h>
#include <yql/essentials/core/yql_type_annotation.h>

namespace NYql::NPureCalc {
/**
 * A transformer which removes unwanted columns from output.
 *
 * @param columns remove all columns that are not in this set.
 * @return a graph transformer for filtering output.
 */
TAutoPtr<IGraphTransformer> MakeOutputColumnsFilter(const TMaybe<THashSet<TString>>& columns);
} // namespace NYql::NPureCalc
