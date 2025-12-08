#pragma once

#include <ydb/core/kqp/expr_nodes/kqp_expr_nodes.h>
#include <ydb/core/kqp/opt/kqp_opt.h>
#include <ydb/core/kqp/provider/yql_kikimr_expr_nodes.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider.h>
#include <ydb/core/kqp/provider/yql_kikimr_settings.h>
#include <ydb/core/protos/kqp_physical.pb.h>

namespace NKikimr {
namespace NKqp {

class IKqpQueryCompiler : public TThrRefBase {
public:
    virtual bool CompilePhysicalQuery(const NYql::NNodes::TKqpPhysicalQuery& query,
        const NYql::NNodes::TKiDataQueryBlocks& dataQueryBlocks, NKqpProto::TKqpPhyQuery& queryProto,
        NYql::TExprContext& ctx) = 0;
};

TIntrusivePtr<IKqpQueryCompiler> CreateKqpQueryCompiler(const TString& cluster,
    const NMiniKQL::IFunctionRegistry& funcRegistry, NYql::TTypeAnnotationContext& typesCtx,
    NOpt::TKqpOptimizeContext& optimizeCtx, NYql::TKikimrConfiguration::TPtr config);

} // namespace NKqp
} // namespace NKikimr
