#pragma once

#include <ydb/library/yql/public/purecalc/common/names.h>

#include <ydb/library/yql/core/yql_graph_transformer.h>
#include <ydb/library/yql/core/yql_type_annotation.h>

#include <util/generic/hash_set.h>
#include <util/generic/string.h>

namespace NYql {
    namespace NPureCalc {
        /**
         * Make transformation which builds sets of input columns from the given expression.
         *
         * @param destination a vector of string sets which will be populated with column names sets when
         *                    transformation pipeline is launched. This pointer should contain a valid
         *                    TVector<THashSet> instance. The transformation will overwrite its contents.
         * @param allColumns vector of sets with all available columns for each input.
         * @param nodeName name of the callable used to get input data, e.g. `Self`.
         * @return an extractor which scans an input structs contents and populates destination.
         */
        TAutoPtr<IGraphTransformer> MakeUsedColumnsExtractor(
            TVector<THashSet<TString>>* destination,
            const TVector<THashSet<TString>>& allColumns,
            const TString& nodeName = TString{PurecalcInputCallableName}
        );
    }
}
