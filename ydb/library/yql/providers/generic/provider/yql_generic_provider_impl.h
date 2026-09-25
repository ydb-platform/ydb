#pragma once

#include "yql_generic_provider.h"
#include "yql_generic_describe_table.h"

#include <util/generic/ptr.h>
#include <yql/essentials/core/yql_graph_transformer.h>
#include <yql/essentials/providers/common/transform/yql_exec.h>
#include <yql/essentials/providers/common/transform/yql_visit.h>

namespace NYql {

    std::unique_ptr<IGraphTransformer> CreateGenericIODiscoveryTransformer(TGenericState::TPtr state);
    std::unique_ptr<TVisitorTransformerBase> CreateGenericDataSourceTypeAnnotationTransformer(TGenericState::TPtr state);
    std::unique_ptr<TVisitorTransformerBase> CreateGenericDataSinkTypeAnnotationTransformer(TGenericState::TPtr state);

    std::unique_ptr<TExecTransformerBase> CreateGenericDataSinkExecTransformer(TGenericState::TPtr state);

    std::unique_ptr<IGraphTransformer> CreateGenericLogicalOptProposalTransformer(TGenericState::TPtr state);
    std::unique_ptr<IGraphTransformer> CreateGenericPhysicalOptProposalTransformer(TGenericState::TPtr state);

} // namespace NYql
