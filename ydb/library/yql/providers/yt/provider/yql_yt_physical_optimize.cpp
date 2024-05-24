#include "yql_yt_provider_impl.h"
#include "yql_yt_transformer.h"

#include <ydb/library/yql/providers/yt/expr_nodes/yql_yt_expr_nodes.h>
#include <ydb/library/yql/providers/yt/lib/mkql_helpers/mkql_helpers.h>
#include <ydb/library/yql/providers/yt/lib/expr_traits/yql_expr_traits.h>
#include <ydb/library/yql/providers/yt/lib/key_filter/yql_key_filter.h>
#include <ydb/library/yql/providers/yt/common/yql_names.h>
#include <ydb/library/yql/providers/yt/common/yql_configuration.h>
#include <ydb/library/yql/providers/yt/opt/yql_yt_key_selector.h>
#include <ydb/library/yql/providers/stat/expr_nodes/yql_stat_expr_nodes.h>
#include <ydb/library/yql/providers/result/expr_nodes/yql_res_expr_nodes.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/common/provider/yql_provider.h>
#include <ydb/library/yql/providers/common/transform/yql_optimize.h>
#include <ydb/library/yql/providers/common/codec/yql_codec_type_flags.h>
#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <ydb/library/yql/dq/integration/yql_dq_integration.h>
#include <ydb/library/yql/providers/result/expr_nodes/yql_res_expr_nodes.gen.h>
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/core/yql_expr_type_annotation.h>
#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/core/yql_join.h>
#include <ydb/library/yql/core/yql_type_helpers.h>
#include <ydb/library/yql/core/yql_data_provider.h>
#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>
#include <ydb/library/yql/dq/opt/dq_opt_phy.h>
#include <ydb/library/yql/dq/type_ann/dq_type_ann.h>
#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/core/extract_predicate/extract_predicate.h>
#include <ydb/library/yql/utils/yql_panic.h>

#include <util/string/cast.h>
#include <util/string/type.h>
#include <util/generic/xrange.h>
#include <util/generic/maybe.h>
#include <util/generic/map.h>
#include <util/generic/set.h>
#include <util/generic/size_literals.h>

#include <algorithm>
#include <type_traits>

namespace NYql {

namespace {

using namespace NNodes;
using namespace NDq;


class TAsyncSyncCompositeTransformer : public TGraphTransformerBase {
public:
    TAsyncSyncCompositeTransformer(THolder<IGraphTransformer>&& async, THolder<IGraphTransformer>&& sync)
        : Async(std::move(async))
        , Sync(std::move(sync))
    {
    }
private:
    TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) override {
        auto status = Async->Transform(input, output, ctx);
        if (status.Level != TStatus::Ok) {
            return status;
        }
        return InstantTransform(*Sync, output, ctx, true);
    }

    NThreading::TFuture<void> DoGetAsyncFuture(const TExprNode& input) override {
        return Async->GetAsyncFuture(input);
    }

    TStatus DoApplyAsyncChanges(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) override {
        return Async->ApplyAsyncChanges(input, output, ctx);
    }

    void Rewind() final {
        Async->Rewind();
        Sync->Rewind();
    }

    const THolder<IGraphTransformer> Async;
    const THolder<IGraphTransformer> Sync;

};

} // namespce

THolder<IGraphTransformer> CreateYtPhysicalOptProposalTransformer(TYtState::TPtr state) {
    return THolder(new TAsyncSyncCompositeTransformer(CreateYtLoadColumnarStatsTransformer(state),
                                              THolder<IGraphTransformer>(new TYtPhysicalOptProposalTransformer(state))));
}

} // namespace NYql
