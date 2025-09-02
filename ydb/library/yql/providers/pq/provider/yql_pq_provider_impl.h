#pragma once

#include "yql_pq_provider.h"

#include <yql/essentials/core/yql_graph_transformer.h>
#include <yql/essentials/providers/common/transform/yql_exec.h>
#include <yql/essentials/providers/common/transform/yql_visit.h>

#include <util/generic/ptr.h>

namespace NYql {

THolder<IGraphTransformer> CreatePqLoadTopicMetadataTransformer(TPqState::TPtr state);

THolder<IGraphTransformer> CreatePqDataSinkIODiscoveryTransformer(TPqState::TPtr state);
THolder<TVisitorTransformerBase> CreatePqDataSourceTypeAnnotationTransformer(TPqState::TPtr state);
THolder<TVisitorTransformerBase> CreatePqDataSinkTypeAnnotationTransformer(TPqState::TPtr state);

THolder<TExecTransformerBase> CreatePqDataSinkExecTransformer(TPqState::TPtr state);

THolder<IGraphTransformer> CreatePqLogicalOptProposalTransformer(TPqState::TPtr state);

THolder<IGraphTransformer> CreatePqPhysicalOptProposalTransformer(TPqState::TPtr state);

THolder<IGraphTransformer> CreatePqIODiscoveryTransformer(TPqState::TPtr state);

TString MakeTopicDisplayName(TStringBuf cluster, TStringBuf path);

} // namespace NYql
