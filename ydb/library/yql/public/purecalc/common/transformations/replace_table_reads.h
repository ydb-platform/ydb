#pragma once

#include <ydb/library/yql/public/purecalc/common/names.h>
#include <ydb/library/yql/public/purecalc/common/processor_mode.h>

#include <ydb/library/yql/core/yql_graph_transformer.h>

namespace NYql::NPureCalc {
    /**
     * SQL translation would generate a standard Read! call to read each input table. It will than generate
     * a Right! call to get the table data from a tuple returned by Read!. This transformation replaces any Right!
     * call with a call to special function used to get input data.
     *
     * Each table name must starts with the specified prefix and ends with an index of program input (e.g. `Input0`).
     * Name without numeric suffix is an alias for the first input.
     *
     * @param inputStructs types of each input.
     * @param useSystemColumns whether to allow special system columns in input structs.
     * @param callableName name of the special callable used to get input data (e.g. `Self`).
     * @param tablePrefix required prefix for all table names (e.g. `Input`).
     * @param return a graph transformer for replacing table reads.
     */
    TAutoPtr<IGraphTransformer> MakeTableReadsReplacer(
        const TVector<const TStructExprType*>& inputStructs,
        bool useSystemColumns,
        EProcessorMode processorMode,
        TString callableName = TString{PurecalcInputCallableName},
        TString tablePrefix = TString{PurecalcInputTablePrefix}
    );
}
