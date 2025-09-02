#pragma once

#include "yql_generic_provider.h"

#include <util/generic/ptr.h>
#include <yql/essentials/core/yql_graph_transformer.h>
#include <yql/essentials/providers/common/transform/yql_exec.h>
#include <yql/essentials/providers/common/transform/yql_visit.h>

namespace NYql {

    THolder<IGraphTransformer> CreateGenericIODiscoveryTransformer(TGenericState::TPtr state);
    THolder<IGraphTransformer> CreateGenericLoadTableMetadataTransformer(TGenericState::TPtr state);

    THolder<TVisitorTransformerBase> CreateGenericDataSourceTypeAnnotationTransformer(TGenericState::TPtr state);
    THolder<TVisitorTransformerBase> CreateGenericDataSinkTypeAnnotationTransformer(TGenericState::TPtr state);

    THolder<TExecTransformerBase> CreateGenericDataSinkExecTransformer(TGenericState::TPtr state);

    THolder<IGraphTransformer> CreateGenericLogicalOptProposalTransformer(TGenericState::TPtr state);
    THolder<IGraphTransformer> CreateGenericPhysicalOptProposalTransformer(TGenericState::TPtr state);

} // namespace NYql
