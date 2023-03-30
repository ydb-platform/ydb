#pragma once

#include <ydb/library/yql/core/yql_graph_transformer.h>
#include <ydb/library/yql/dq/common/dq_common.h>

namespace NYql::NDq {

TAutoPtr<IGraphTransformer> CreateDqBuildPhyStagesTransformer(bool allowDependantConsumers, bool useWideChannels);

} // namespace NYql::NDq
