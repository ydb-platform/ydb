#pragma once

#include "yql_pq_provider.h"

#include <ydb/library/yql/core/yql_graph_transformer.h>
#include <ydb/library/yql/providers/common/transform/yql_exec.h>
#include <ydb/library/yql/providers/common/transform/yql_visit.h>

#include <util/generic/ptr.h>

namespace NYql {

TIntrusivePtr<IDataProvider> CreatePqDataSource(TPqState::TPtr state, IPqGateway::TPtr gateway);
TIntrusivePtr<IDataProvider> CreatePqDataSink(TPqState::TPtr state, IPqGateway::TPtr gateway);

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
