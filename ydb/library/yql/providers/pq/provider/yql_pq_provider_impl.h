#pragma once

#include "yql_pq_provider.h"

#include <yql/essentials/core/yql_graph_transformer.h>
#include <yql/essentials/providers/common/transform/yql_exec.h>
#include <yql/essentials/providers/common/transform/yql_visit.h>

#include <util/generic/ptr.h>

namespace NYql {

std::unique_ptr<IGraphTransformer> CreatePqLoadTopicMetadataTransformer(TPqState::TPtr state);

std::unique_ptr<IGraphTransformer> CreatePqDataSinkIODiscoveryTransformer(TPqState::TPtr state);
std::unique_ptr<TVisitorTransformerBase> CreatePqDataSourceTypeAnnotationTransformer(TPqState::TPtr state);
std::unique_ptr<TVisitorTransformerBase> CreatePqDataSinkTypeAnnotationTransformer(TPqState::TPtr state);

std::unique_ptr<TExecTransformerBase> CreatePqDataSinkExecTransformer(TPqState::TPtr state);

std::unique_ptr<IGraphTransformer> CreatePqLogicalOptProposalTransformer(TPqState::TPtr state);

std::unique_ptr<IGraphTransformer> CreatePqPhysicalOptProposalTransformer(TPqState::TPtr state);

std::unique_ptr<IGraphTransformer> CreatePqIODiscoveryTransformer(TPqState::TPtr state);

TString MakeTopicDisplayName(TStringBuf cluster, TStringBuf path);

} // namespace NYql
