#include "kqp_transform.h"

#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>
#include <ydb/library/yql/providers/common/provider/yql_provider.h>
#include <ydb/library/yql/minikql/mkql_node.h>

namespace NKikimr::NKqp {

using namespace NYql;
using namespace NKikimr::NMiniKQL;
using namespace NUdf;

IGraphTransformer::TStatus TLogExprTransformer::operator()(const TExprNode::TPtr& input, TExprNode::TPtr& output,
    TExprContext& ctx)
{
    Y_UNUSED(ctx);

    output = input;
    LogExpr(*input, ctx, Description, Component, Level);
    return IGraphTransformer::TStatus::Ok;
}

TAutoPtr<IGraphTransformer> TLogExprTransformer::Sync(const TString& description, NYql::NLog::EComponent component,
    NYql::NLog::ELevel level)
{
    return CreateFunctorTransformer(TLogExprTransformer(description, component, level));
}

void TLogExprTransformer::LogExpr(const TExprNode& input, TExprContext& ctx, const TString& description, NYql::NLog::EComponent component,
    NYql::NLog::ELevel level)
{
    YQL_CVLOG(level, component) << description << ":\n" << KqpExprToPrettyString(input, ctx);
}

} // namespace NKikimr::NKqp
