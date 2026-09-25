#pragma once

#include "yql_solomon_provider.h"

#include <ydb/library/yql/providers/solomon/proto/dq_solomon_shard.pb.h>
#include <yql/essentials/core/yql_graph_transformer.h>
#include <yql/essentials/providers/common/transform/yql_exec.h>
#include <yql/essentials/providers/common/transform/yql_visit.h>

#include <util/generic/ptr.h>

namespace NYql {

std::unique_ptr<IGraphTransformer> CreateSolomonIODiscoveryTransformer(TSolomonState::TPtr state);
std::unique_ptr<IGraphTransformer> CreateSolomonLoadTableMetadataTransformer(TSolomonState::TPtr state);
std::unique_ptr<IGraphTransformer> CreateSolomonLogicalOptProposalTransformer(TSolomonState::TPtr state);

std::unique_ptr<TVisitorTransformerBase> CreateSolomonDataSourceTypeAnnotationTransformer(TSolomonState::TPtr state);
std::unique_ptr<TExecTransformerBase> CreateSolomonDataSourceExecTransformer(TSolomonState::TPtr state);

std::unique_ptr<TVisitorTransformerBase> CreateSolomonDataSinkTypeAnnotationTransformer(TSolomonState::TPtr state);
std::unique_ptr<TExecTransformerBase> CreateSolomonDataSinkExecTransformer(TSolomonState::TPtr state);

std::unique_ptr<IGraphTransformer> CreateSoPhysicalOptProposalTransformer(TSolomonState::TPtr state);

} // namespace NYql
