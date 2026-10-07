#pragma once

#include "yql_s3_provider.h"
#include <ydb/library/yql/providers/common/http_gateway/yql_http_gateway.h>

#include <yql/essentials/core/yql_graph_transformer.h>
#include <yql/essentials/providers/common/transform/yql_exec.h>
#include <yql/essentials/providers/common/transform/yql_visit.h>

#include <util/generic/ptr.h>

namespace NYql {

std::unique_ptr<TVisitorTransformerBase> CreateS3DataSourceTypeAnnotationTransformer(TS3State::TPtr state);
std::unique_ptr<TVisitorTransformerBase> CreateS3DataSinkTypeAnnotationTransformer(TS3State::TPtr state);

std::unique_ptr<TExecTransformerBase> CreateS3DataSinkExecTransformer(TS3State::TPtr state);

std::unique_ptr<IGraphTransformer> CreateS3LogicalOptProposalTransformer(TS3State::TPtr state);
std::unique_ptr<IGraphTransformer> CreateS3SourceCallableExecutionTransformer(TS3State::TPtr state);
std::unique_ptr<IGraphTransformer> CreateS3IODiscoveryTransformer(TS3State::TPtr state);
std::unique_ptr<IGraphTransformer> CreateS3PhysicalOptProposalTransformer(TS3State::TPtr state);

TExprNode::TPtr ExtractFormat(TExprNode::TListType& settings);

bool UseBlocksSink(TStringBuf format, const TExprNode::TListType& keys, const TStructExprType* outputType, TS3Configuration::TPtr configuration, TString& error);

} // namespace NYql
