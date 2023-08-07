#include "sql.h"
#include "sql_query.h"
#include <ydb/library/yql/parser/proto_ast/collect_issues/collect_issues.h>
#include <ydb/library/yql/sql/v1/lexer/lexer.h>
#include <ydb/library/yql/sql/v1/proto_parser/proto_parser.h>

namespace NSQLTranslationV1 {

using namespace NSQLv1Generated;

TAstNode* SqlASTToYql(const google::protobuf::Message& protoAst, TContext& ctx) {
    const google::protobuf::Descriptor* d = protoAst.GetDescriptor();
    if (d && d->name() != "TSQLv1ParserAST") {
        ctx.Error() << "Invalid AST structure: " << d->name() << ", expected TSQLv1ParserAST";
        return nullptr;
    }
    TSqlQuery query(ctx, ctx.Settings.Mode, true);
    TNodePtr node(query.Build(static_cast<const TSQLv1ParserAST&>(protoAst)));
    try {
        if (node && node->Init(ctx, nullptr)) {
            return node->Translate(ctx);
        }
    } catch (const NProtoAST::TTooManyErrors&) {
        // do not add error issue, no room for it
    }

    return nullptr;
}

void SqlASTToYqlImpl(NYql::TAstParseResult& res, const google::protobuf::Message& protoAst,
        TContext& ctx) {
    YQL_ENSURE(!ctx.Issues.Size());
    res.Root = SqlASTToYql(protoAst, ctx);
    res.Pool = std::move(ctx.Pool);
    if (!res.Root) {
        if (ctx.Issues.Size()) {
            ctx.IncrementMonCounter("sql_errors", "AstToYqlError");
        } else {
            ctx.IncrementMonCounter("sql_errors", "AstToYqlSilentError");
            ctx.Error() << "Error occurred on parse SQL query, but no error is collected" <<
                ", please send this request over bug report into YQL interface or write on yql@ maillist";
        }
    } else {
        ctx.WarnUnusedHints();
    }
}

NYql::TAstParseResult SqlASTToYql(const google::protobuf::Message& protoAst,
    const NSQLTranslation::TSQLHints& hints,
    const NSQLTranslation::TTranslationSettings& settings)
{
    YQL_ENSURE(IsQueryMode(settings.Mode));
    TAstParseResult res;
    TContext ctx(settings, hints, res.Issues);
    SqlASTToYqlImpl(res, protoAst, ctx);
    return res;
}

NYql::TAstParseResult SqlToYql(const TString& query, const NSQLTranslation::TTranslationSettings& settings, NYql::TWarningRules* warningRules)
{
    TAstParseResult res;
    const TString queryName = "query";

    NSQLTranslation::TSQLHints hints;
    auto lexer = MakeLexer(settings.AnsiLexer);
    YQL_ENSURE(lexer);
    if (!CollectSqlHints(*lexer, query, queryName, settings.File, hints, res.Issues, settings.MaxErrors)) {
        return res;
    }

    TContext ctx(settings, hints, res.Issues);
    NSQLTranslation::TErrorCollectorOverIssues collector(res.Issues, settings.MaxErrors, settings.File);

    google::protobuf::Message* ast(SqlAST(query, queryName, collector, settings.AnsiLexer, settings.Arena));
    if (ast) {
        SqlASTToYqlImpl(res, *ast, ctx);
    } else {
        ctx.IncrementMonCounter("sql_errors", "AstError");
    }
    if (warningRules) {
        *warningRules = ctx.WarningPolicy.GetRules();
        ctx.WarningPolicy.Clear();
    }
    return res;
}

} // namespace NSQLTranslationV1
