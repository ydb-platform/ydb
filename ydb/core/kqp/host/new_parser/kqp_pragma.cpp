#include "kqp_pragma.h"
#include "kqp_new_parser.h"

#include <yql/essentials/core/yql_type_annotation.h>

namespace NKikimr::NKqp::NNewYql {

bool ApplyProviderPragmas(const TScript& script, const TProviderPragmas& pragmas,
    NYql::TExprContext& ctx, NYql::TTypeAnnotationContext& types)
{
    using namespace NYql;
    for (const auto& statement : script.Statements) {
        const auto* pragma = std::get_if<TPragmaStatement>(&statement.Value);
        const auto* setting = pragma ? pragmas.FindPtr(pragma) : nullptr;
        if (!setting) { continue; }
        const auto* provider = types.DataSourceMap.FindPtr(setting->Provider);
        if (!provider) {
            ythrow TNewYqlParserFallback() << "Provider is unavailable for native pragma: " << setting->Provider;
        }
        const auto pos = ctx.AppendPosition(TPosition(pragma->Name.Range.Column + 1,
            pragma->Name.Range.Line, script.File));
        TExprNode::TListType source = {ctx.NewAtom(pos, setting->Provider)};
        if (setting->Provider != "config") {
            source.push_back(ctx.NewAtom(pos, "$all"));
        }
        TExprNode::TListType args = {ctx.NewWorld(pos), ctx.NewCallable(pos, "DataSource", std::move(source))};
        if (setting->Provider != "config") {
            args.push_back(ctx.NewAtom(pos, "Attr"));
            args.push_back(ctx.NewAtom(pos, setting->Name));
        }
        if (!setting->Default) {
            for (const auto& value : setting->Values) {
                args.push_back(ctx.NewAtom(pos, value));
            }
        }
        auto input = ctx.NewCallable(pos, "Configure!", std::move(args));
        TExprNode::TPtr output;
        const auto status = (*provider)->GetConfigurationTransformer().Transform(input, output, ctx);
        if (status == IGraphTransformer::TStatus::Error) { return false; }
        YQL_ENSURE(status == IGraphTransformer::TStatus::Ok);
    }
    return true;
}

} // namespace NKikimr::NKqp::NNewYql
