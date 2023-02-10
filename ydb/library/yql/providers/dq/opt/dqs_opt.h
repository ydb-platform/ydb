#pragma once

#include <ydb/library/yql/core/yql_graph_transformer.h>

namespace NKikimr::NMiniKQL {
    class IFunctionRegistry;
}

namespace NYql {
    struct TTypeAnnotationContext;
}

namespace NYql::NDqs {
    class TDatabaseManager;

    THolder<IGraphTransformer> CreateDqsFinalizingOptTransformer();
    THolder<IGraphTransformer> CreateDqsRewritePhyCallablesTransformer(TTypeAnnotationContext& typesCtx);
    THolder<IGraphTransformer> CreateDqsReplacePrecomputesTransformer(TTypeAnnotationContext& typesCtx, const NKikimr::NMiniKQL::IFunctionRegistry* funcRegistry);

} // namespace NYql::NDqs
