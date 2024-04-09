#pragma once

#include "yql_s3_provider.h"
#include <ydb/library/yql/providers/common/http_gateway/yql_http_gateway.h>

#include <ydb/library/yql/core/yql_graph_transformer.h>
#include <ydb/library/yql/providers/common/transform/yql_exec.h>
#include <ydb/library/yql/providers/common/transform/yql_visit.h>

#include <util/generic/ptr.h>

namespace NYql {

THolder<TVisitorTransformerBase> CreateS3DataSourceTypeAnnotationTransformer(TS3State::TPtr state);
THolder<TVisitorTransformerBase> CreateS3DataSinkTypeAnnotationTransformer(TS3State::TPtr state);

THolder<TExecTransformerBase> CreateS3DataSinkExecTransformer(TS3State::TPtr state);

THolder<IGraphTransformer> CreateS3LogicalOptProposalTransformer(TS3State::TPtr state);
THolder<IGraphTransformer> CreateS3SourceCallableExecutionTransformer(TS3State::TPtr state);
THolder<IGraphTransformer> CreateS3IODiscoveryTransformer(TS3State::TPtr state);
THolder<IGraphTransformer> CreateS3PhysicalOptProposalTransformer(TS3State::TPtr state);

TExprNode::TPtr ExtractFormat(TExprNode::TListType& settings);

} // namespace NYql
