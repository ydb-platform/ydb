#pragma once

#include "yql_solomon_provider.h"

#include <ydb/library/yql/core/yql_graph_transformer.h>
#include <ydb/library/yql/providers/common/transform/yql_exec.h>
#include <ydb/library/yql/providers/common/transform/yql_visit.h>

#include <util/generic/ptr.h>

namespace NYql {

THolder<IGraphTransformer> CreateSolomonIODiscoveryTransformer(TSolomonState::TPtr state);
THolder<IGraphTransformer> CreateSolomonLoadTableMetadataTransformer(TSolomonState::TPtr state);

THolder<TVisitorTransformerBase> CreateSolomonDataSourceTypeAnnotationTransformer(TSolomonState::TPtr state);
THolder<TExecTransformerBase> CreateSolomonDataSourceExecTransformer(TSolomonState::TPtr state);

THolder<TVisitorTransformerBase> CreateSolomonDataSinkTypeAnnotationTransformer(TSolomonState::TPtr state);
THolder<TExecTransformerBase> CreateSolomonDataSinkExecTransformer(TSolomonState::TPtr state);

THolder<IGraphTransformer> CreateSoPhysicalOptProposalTransformer(TSolomonState::TPtr state);

} // namespace NYql
