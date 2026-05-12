#pragma once

#include <yql/essentials/core/yql_graph_transformer.h>
#include <yql/essentials/core/yql_type_annotation.h>
#include <ydb/library/yql/dq/common/dq_common.h>
#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>

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

NNodes::TDqPhyStage RebuildStageInputsAsWide(const NNodes::TDqPhyStage& stage, TExprContext& ctx);

} // namespace NYql::NDq
