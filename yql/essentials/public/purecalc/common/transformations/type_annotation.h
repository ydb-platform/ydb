#pragma once

#include <yql/essentials/public/purecalc/common/names.h>
#include <yql/essentials/public/purecalc/common/processor_mode.h>

#include <yql/essentials/core/yql_graph_transformer.h>
#include <yql/essentials/core/yql_type_annotation.h>

namespace NYql::NPureCalc {
/**
 * Build type annotation transformer that is aware of type of the input rows.
 *
 * @param typeAnnotationContext current context.
 * @param inputStructs types of each input.
 * @param rawInputStructs container to store the resulting input item type.
 * @param processorMode current processor mode. This will affect generated input type,
 *                      e.g. list node or struct node.
 * @param nodeName name of the callable used to get input data, e.g. `Self`.
 * @return a graph transformer for type annotation.
 */
TAutoPtr<IGraphTransformer> MakeTypeAnnotationTransformer(
    TTypeAnnotationContextPtr typeAnnotationContext,
    const TVector<const TStructExprType*>& inputStructs,
    TVector<const TStructExprType*>& rawInputStructs,
    EProcessorMode processorMode,
    const TString& nodeName = TString{PurecalcInputCallableName});
} // namespace NYql::NPureCalc
