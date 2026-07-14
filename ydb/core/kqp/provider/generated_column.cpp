#include "generated_column.h"

#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/sql/sql.h>
#include <yql/essentials/sql/v1/lexer/antlr4/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4_ansi/lexer.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4/proto_parser.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4_ansi/proto_parser.h>
#include <yql/essentials/sql/v1/sql.h>

namespace NYql {

TString AssembleGeneratedQuery(const TString& context, const TString& exprBody) {
    return TStringBuilder() << context << "SELECT " << exprBody << " FROM `__yql_generated_column_source`;";
}

namespace {

// SqlToYql + CompileExpr of the stored text
TExprNode::TPtr CompileText(const TString& sqlText, TExprContext& ctx, NKikimr::NKqp::TKqpTranslationSettingsBuilder& settingsBuilder,
    const IModuleResolver::TPtr& moduleResolver)
{
    auto translationSettings = settingsBuilder.Build(ctx);
    translationSettings.Mode = NSQLTranslation::ESqlMode::LIMITED_VIEW;

    NSQLTranslationV1::TLexers lexers;
    lexers.Antlr4 = NSQLTranslationV1::MakeAntlr4LexerFactory();
    lexers.Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiLexerFactory();

    NSQLTranslationV1::TParsers parsers;
    parsers.Antlr4 = NSQLTranslationV1::MakeAntlr4ParserFactory(settingsBuilder.GetIsAmbiguityError());
    parsers.Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiParserFactory();

    NSQLTranslation::TTranslators translators(nullptr, NSQLTranslationV1::MakeTranslator(lexers, parsers), nullptr);

    auto queryAst = NSQLTranslation::SqlToYql(translators, sqlText, translationSettings);
    ctx.IssueManager.AddIssues(queryAst.Issues);
    if (!queryAst.IsOk()) {
        return nullptr;
    }

    TExprNode::TPtr queryGraph;
    if (!CompileExpr(*queryAst.Root, queryGraph, ctx, moduleResolver.get(), nullptr)) {
        return nullptr;
    }

    return queryGraph;
}

}   // namespace

TExprNode::TPtr CompileGeneratedExpr(const TString& sqlText, const TString& columnName, TExprContext& ctx,
    NKikimr::NKqp::TKqpTranslationSettingsBuilder& settingsBuilder, const IModuleResolver::TPtr& moduleResolver)
{
    auto queryGraph = CompileText(sqlText, ctx, settingsBuilder, moduleResolver);
    if (!queryGraph) {
        ctx.AddError(TIssue(TStringBuilder() << "Failed to compile the expression of generated column " << columnName));
        return nullptr;
    }

    TExprNode::TPtr projectionLambda;
    ui32 projectionItems = 0;
    bool hasStar = false;

    VisitExpr(queryGraph, [&](const TExprNode::TPtr& node) {
        if (node->IsCallable("SqlProjectStarItem")) {
            hasStar = true;
        } else if (node->IsCallable("SqlProjectItem")) {
            ++projectionItems;
            projectionLambda = node->ChildPtr(2);
        }
        return true;
    });

    if (hasStar || projectionItems != 1 || !projectionLambda || projectionLambda->Type() != TExprNode::Lambda) {
        ctx.AddError(TIssue(ctx.GetPosition(queryGraph->Pos()), TStringBuilder()
            << "Generated column " << columnName << " must be defined by a single scalar expression"));
        return nullptr;
    }

    return projectionLambda;
}

}   // namespace NYql
