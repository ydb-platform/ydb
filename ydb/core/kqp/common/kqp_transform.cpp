#include "kqp_transform.h" 
#include "kqp_yql.h" 
 
#include <ydb/library/yql/providers/common/provider/yql_provider.h>
 
namespace NKikimr { 
namespace NKqp { 
 
using namespace NYql; 
 
IGraphTransformer::TStatus TLogExprTransformer::operator()(const TExprNode::TPtr& input, TExprNode::TPtr& output, 
    TExprContext& ctx) 
{ 
    Y_UNUSED(ctx); 
 
    output = input; 
    LogExpr(*input, ctx, Description, Component, Level);
    return IGraphTransformer::TStatus::Ok; 
} 
 
TAutoPtr<IGraphTransformer> TLogExprTransformer::Sync(const TString& description, NLog::EComponent component, 
    NLog::ELevel level) 
{ 
    return CreateFunctorTransformer(TLogExprTransformer(description, component, level)); 
} 
 
void TLogExprTransformer::LogExpr(const TExprNode& input, TExprContext& ctx, const TString& description, NLog::EComponent component,
    NLog::ELevel level) 
{ 
    YQL_CVLOG(level, component) << description << ":\n" << KqpExprToPrettyString(input, ctx); 
} 
 
} // namespace NKqp
} // namespace NKikimr 
