#include "yql_out_transformers.h"

#include <ydb/library/yql/ast/yql_expr.h>

#include <library/cpp/yson/writer.h>

#include <util/stream/null.h>
#include <util/stream/str.h>

namespace NYql {

IGraphTransformer::TStatus TExprOutputTransformer::operator()(
    const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx)
{
    Y_UNUSED(ctx);
    output = input;
    if (!ExprRoot_) {
        return IGraphTransformer::TStatus::Ok;
    }

    auto ast = ConvertToAst(*ExprRoot_, ctx, WithTypes_ ? TExprAnnotationFlags::Types : TExprAnnotationFlags::None, true);
    ui32 prettyFlags = TAstPrintFlags::ShortQuote;
    if (!WithTypes_) {
        prettyFlags |= TAstPrintFlags::PerLine;
    }

    if (DirectOut_) {
        ast.Root->PrettyPrintTo(*DirectOut_, prettyFlags);
    }
    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus TPlanOutputTransformer::operator()(
    const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx)
{
    Y_UNUSED(ctx);
    output = input;
    if (DirectOut_) {
        NYson::TYsonWriter writer(DirectOut_, OutputFormat_);
        Builder_.WritePlan(writer, input, PlanSettings_);
    } else {
        TNullOutput null;
        NYson::TYsonWriter writer(&null, OutputFormat_);
        Builder_.WritePlan(writer, input, PlanSettings_);
    }

    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus TExprLogTransformer::operator()(
    const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx)
{
    Y_UNUSED(ctx);
    output = input;
    if (YQL_CVLOG_ACTIVE(Level, Component)) {
        TConvertToAstSettings settings;
        settings.AllowFreeArgs = true;
        settings.RefAtoms = true;
        auto ast = ConvertToAst(*input, ctx, settings);
        TStringStream out;
        ast.Root->PrettyPrintTo(out, TAstPrintFlags::ShortQuote | TAstPrintFlags::PerLine);
        YQL_CVLOG(Level, Component) << Description << ":\n" << out.Str();
    }
    return IGraphTransformer::TStatus::Ok;
}

}
