#pragma once

#include "yql_clickhouse_provider.h"

#include <ydb/library/yql/core/yql_graph_transformer.h>
#include <ydb/library/yql/providers/common/transform/yql_exec.h>
#include <ydb/library/yql/providers/common/transform/yql_visit.h>

#include <util/generic/ptr.h>

namespace NYql {

THolder<IGraphTransformer> CreateClickHouseIODiscoveryTransformer(TClickHouseState::TPtr state);
THolder<IGraphTransformer> CreateClickHouseLoadTableMetadataTransformer(TClickHouseState::TPtr state, IHTTPGateway::TPtr gateway);

THolder<TVisitorTransformerBase> CreateClickHouseDataSourceTypeAnnotationTransformer(TClickHouseState::TPtr state);
THolder<TVisitorTransformerBase> CreateClickHouseDataSinkTypeAnnotationTransformer(TClickHouseState::TPtr state);

THolder<TExecTransformerBase> CreateClickHouseDataSinkExecTransformer(TClickHouseState::TPtr state);

THolder<IGraphTransformer> CreateClickHouseLogicalOptProposalTransformer(TClickHouseState::TPtr state);
THolder<IGraphTransformer> CreateClickHousePhysicalOptProposalTransformer(TClickHouseState::TPtr state);

} // namespace NYql
