#pragma once

#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/opt/kqp_column_statistics_utils.h>
#include <yql/essentials/core/yql_graph_transformer.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/core/yql_opt_utils.h>
#include <ydb/core/kqp/provider/yql_kikimr_settings.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider.h>
#include <ydb/core/kqp/opt/kqp_opt.h>

namespace NKikimr::NKqp::NOpt {

    using namespace NYql;
    using namespace NYql::NNodes;

    TAutoPtr<IGraphTransformer> CreateKqpTrueCardinalities(
        TIntrusivePtr<NOpt::TKqpOptimizeContext>& kqpCtx,
        TTypeAnnotationContext& typesCtx,
        const TKikimrConfiguration::TPtr& config,
        const TString& cluster,
        const TString& database,
        TActorSystem* actorSystem);

} // namespace NKikimr::NKqp::NOpt
