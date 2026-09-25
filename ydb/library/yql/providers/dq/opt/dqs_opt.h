#pragma once

#include <yql/essentials/core/yql_graph_transformer.h>

namespace NKikimr::NMiniKQL {
    class IFunctionRegistry;
}

namespace NYql {
    struct TTypeAnnotationContext;
}

namespace NYql::NDqs {
    class TDatabaseManager;

    std::unique_ptr<IGraphTransformer> CreateDqsFinalizingOptTransformer();
    std::unique_ptr<IGraphTransformer> CreateDqsRewritePhyCallablesTransformer(TTypeAnnotationContext& typesCtx);
    std::unique_ptr<IGraphTransformer> CreateDqsRewritePhyBlockReadOnDqIntegrationTransformer(TTypeAnnotationContext& typesCtx);
    std::unique_ptr<IGraphTransformer> CreateDqsReplacePrecomputesTransformer(TTypeAnnotationContext& typesCtx);

} // namespace NYql::NDqs
