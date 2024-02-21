#pragma once

#include <ydb/library/yql/core/yql_graph_transformer.h>
#include <ydb/library/yql/core/yql_type_annotation.h>
#include <ydb/library/yql/dq/common/dq_common.h>

namespace NYql::NDq {

enum EChannelMode {
    CHANNEL_SCALAR,
    CHANNEL_WIDE_SCALAR,
    CHANNEL_WIDE_AUTO_BLOCK,
    CHANNEL_WIDE_FORCE_BLOCK,
};

TAutoPtr<IGraphTransformer> CreateDqBuildPhyStagesTransformer(bool allowDependantConsumers, TTypeAnnotationContext& typesCtx, EChannelMode mode);

// This transformer should be run in final peephole stage. It enables block channels according to "mode" argument
TAutoPtr<IGraphTransformer> CreateDqBuildWideBlockChannelsTransformer(TTypeAnnotationContext& typesCtx, EChannelMode mode);

} // namespace NYql::NDq
