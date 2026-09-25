#pragma once

#include "yql_clickhouse_provider.h"

#include <yql/essentials/core/yql_graph_transformer.h>
#include <yql/essentials/providers/common/transform/yql_exec.h>
#include <yql/essentials/providers/common/transform/yql_visit.h>

#include <util/generic/ptr.h>

namespace NYql {

std::unique_ptr<IGraphTransformer> CreateClickHouseIODiscoveryTransformer(TClickHouseState::TPtr state);
std::unique_ptr<IGraphTransformer> CreateClickHouseLoadTableMetadataTransformer(TClickHouseState::TPtr state, IHTTPGateway::TPtr gateway);

std::unique_ptr<TVisitorTransformerBase> CreateClickHouseDataSourceTypeAnnotationTransformer(TClickHouseState::TPtr state);
std::unique_ptr<TVisitorTransformerBase> CreateClickHouseDataSinkTypeAnnotationTransformer(TClickHouseState::TPtr state);

std::unique_ptr<TExecTransformerBase> CreateClickHouseDataSinkExecTransformer(TClickHouseState::TPtr state);

std::unique_ptr<IGraphTransformer> CreateClickHouseLogicalOptProposalTransformer(TClickHouseState::TPtr state);
std::unique_ptr<IGraphTransformer> CreateClickHousePhysicalOptProposalTransformer(TClickHouseState::TPtr state);

} // namespace NYql
