#pragma once

#include <ydb/library/yql/public/purecalc/common/processor_mode.h>

#include <ydb/library/yql/core/yql_graph_transformer.h>
#include <ydb/library/yql/core/yql_type_annotation.h>

namespace NYql {
    namespace NPureCalc {
        /**
         * A transformer which removes unwanted columns from output.
         *
         * @param columns remove all columns that are not in this set.
         * @return a graph transformer for filtering output.
         */
        TAutoPtr<IGraphTransformer> MakeOutputColumnsFilter(const TMaybe<THashSet<TString>>& columns);
    }
}
