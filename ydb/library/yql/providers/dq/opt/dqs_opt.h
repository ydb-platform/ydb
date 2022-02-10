#pragma once

#include <ydb/library/yql/core/yql_graph_transformer.h>
#include <ydb/library/yql/core/yql_type_annotation.h>
#include <ydb/library/yql/dq/proto/dq_tasks.pb.h>

namespace NYql::NDqs {
    class TDatabaseManager; 
 
    THolder<IGraphTransformer> CreateDqsWrapListsOptTransformer(); 
    THolder<IGraphTransformer> CreateDqsFinalizingOptTransformer();
    THolder<IGraphTransformer> CreateDqsBuildTransformer(); 
    THolder<IGraphTransformer> CreateDqsRewritePhyCallablesTransformer();
    THolder<IGraphTransformer> CreateDqsPeepholeTransformer(THolder<IGraphTransformer>&& typeAnnTransformer, TTypeAnnotationContext& typesCtx); 

} // namespace NYql::NDqs
