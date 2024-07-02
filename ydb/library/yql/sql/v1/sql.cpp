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

TAstNode* SqlASTsToYqls(const std::vector<::NSQLv1Generated::TRule_sql_stmt_core>& ast, TContext& ctx) {
    TSqlQuery query(ctx, ctx.Settings.Mode, true);
    TNodePtr node(query.Build(ast));
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

void SqlASTsToYqlsImpl(NYql::TAstParseResult& res, const std::vector<::NSQLv1Generated::TRule_sql_stmt_core>& ast, TContext& ctx) {
    res.Root = SqlASTsToYqls(ast, ctx);
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
    res.ActualSyntaxType = NYql::ESyntaxType::YQLv1;
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
    res.ActualSyntaxType = NYql::ESyntaxType::YQLv1;
    return res;
}

bool NeedUseForAllStatements(const TRule_sql_stmt_core::AltCase& subquery) {
    switch (subquery) {
        case TRule_sql_stmt_core::kAltSqlStmtCore1:  // pragma
        case TRule_sql_stmt_core::kAltSqlStmtCore3:  // named nodes
        case TRule_sql_stmt_core::kAltSqlStmtCore6:  // use
        case TRule_sql_stmt_core::kAltSqlStmtCore12: // declare
        case TRule_sql_stmt_core::kAltSqlStmtCore13: // import
        case TRule_sql_stmt_core::kAltSqlStmtCore14: // export
        case TRule_sql_stmt_core::kAltSqlStmtCore18: // define action or subquery
            return true;
        case TRule_sql_stmt_core::ALT_NOT_SET:
        case TRule_sql_stmt_core::kAltSqlStmtCore2:  // select
        case TRule_sql_stmt_core::kAltSqlStmtCore4:  // create table
        case TRule_sql_stmt_core::kAltSqlStmtCore5:  // drop table
        case TRule_sql_stmt_core::kAltSqlStmtCore7:  // into table
        case TRule_sql_stmt_core::kAltSqlStmtCore8:  // commit
        case TRule_sql_stmt_core::kAltSqlStmtCore9:  // update
        case TRule_sql_stmt_core::kAltSqlStmtCore10: // delete
        case TRule_sql_stmt_core::kAltSqlStmtCore11: // rollback
        case TRule_sql_stmt_core::kAltSqlStmtCore15: // alter table
        case TRule_sql_stmt_core::kAltSqlStmtCore16: // alter external table
        case TRule_sql_stmt_core::kAltSqlStmtCore17: // do
        case TRule_sql_stmt_core::kAltSqlStmtCore19: // if
        case TRule_sql_stmt_core::kAltSqlStmtCore20: // for
        case TRule_sql_stmt_core::kAltSqlStmtCore21: // values 
        case TRule_sql_stmt_core::kAltSqlStmtCore22: // create user
        case TRule_sql_stmt_core::kAltSqlStmtCore23: // alter user
        case TRule_sql_stmt_core::kAltSqlStmtCore24: // create group
        case TRule_sql_stmt_core::kAltSqlStmtCore25: // alter group 
        case TRule_sql_stmt_core::kAltSqlStmtCore26: // drop role 
        case TRule_sql_stmt_core::kAltSqlStmtCore27: // create object 
        case TRule_sql_stmt_core::kAltSqlStmtCore28: // alter object 
        case TRule_sql_stmt_core::kAltSqlStmtCore29: // drop object 
        case TRule_sql_stmt_core::kAltSqlStmtCore30: // create external data source 
        case TRule_sql_stmt_core::kAltSqlStmtCore31: // alter external data source 
        case TRule_sql_stmt_core::kAltSqlStmtCore32: // drop external data source 
        case TRule_sql_stmt_core::kAltSqlStmtCore33: // create replication 
        case TRule_sql_stmt_core::kAltSqlStmtCore34: // drop replication 
        case TRule_sql_stmt_core::kAltSqlStmtCore35: // create topic 
        case TRule_sql_stmt_core::kAltSqlStmtCore36: // alter topic 
        case TRule_sql_stmt_core::kAltSqlStmtCore37: // drop topic
        case TRule_sql_stmt_core::kAltSqlStmtCore38: // grant permissions
        case TRule_sql_stmt_core::kAltSqlStmtCore39: // revoke permissions
        case TRule_sql_stmt_core::kAltSqlStmtCore40: // alter table store
        case TRule_sql_stmt_core::kAltSqlStmtCore41: // upsert object
        case TRule_sql_stmt_core::kAltSqlStmtCore42: // create view
        case TRule_sql_stmt_core::kAltSqlStmtCore43: // drop view
        case TRule_sql_stmt_core::kAltSqlStmtCore44: // alter replication
        case TRule_sql_stmt_core::kAltSqlStmtCore45: // create resource pool
        case TRule_sql_stmt_core::kAltSqlStmtCore46: // alter resource pool
        case TRule_sql_stmt_core::kAltSqlStmtCore47: // drop resource pool
            return false;
    }
}

TVector<NYql::TAstParseResult> SqlToAstStatements(const TString& query, const NSQLTranslation::TTranslationSettings& settings, TIssues& issues,
    NYql::TWarningRules* warningRules, TVector<NYql::TStmtParseInfo>* stmtParseInfo)
{
    TVector<TAstParseResult> result;
    const TString queryName = "query";

    NSQLTranslation::TSQLHints hints;
    auto lexer = MakeLexer(settings.AnsiLexer);
    YQL_ENSURE(lexer);
    if (!CollectSqlHints(*lexer, query, queryName, settings.File, hints, issues, settings.MaxErrors)) {
        return result;
    }

    TContext ctx(settings, hints, issues);
    NSQLTranslation::TErrorCollectorOverIssues collector(issues, settings.MaxErrors, settings.File);

    google::protobuf::Message* astProto(SqlAST(query, queryName, collector, settings.AnsiLexer, settings.Arena));
    if (astProto) {
        auto ast = static_cast<const TSQLv1ParserAST&>(*astProto);
        const auto& query = ast.GetRule_sql_query();
        if (query.Alt_case() == NSQLv1Generated::TRule_sql_query::kAltSqlQuery1) {
            std::vector<::NSQLv1Generated::TRule_sql_stmt_core> commonStates;
            std::vector<::NSQLv1Generated::TRule_sql_stmt_core> statementResult;
            const auto& statements = query.GetAlt_sql_query1().GetRule_sql_stmt_list1();
            if (NeedUseForAllStatements(statements.GetRule_sql_stmt2().GetRule_sql_stmt_core2().Alt_case())) {
                commonStates.push_back(statements.GetRule_sql_stmt2().GetRule_sql_stmt_core2());
            } else {
                TContext ctx(settings, hints, issues);
                result.emplace_back();
                if (stmtParseInfo) {
                    stmtParseInfo->push_back({});
                }
                SqlASTsToYqlsImpl(result.back(), {statements.GetRule_sql_stmt2().GetRule_sql_stmt_core2()}, ctx);
                result.back().Issues = std::move(issues);
                issues.Clear();
            }
            for (auto block: statements.GetBlock3()) {
                if (NeedUseForAllStatements(block.GetRule_sql_stmt2().GetRule_sql_stmt_core2().Alt_case())) {
                    commonStates.push_back(block.GetRule_sql_stmt2().GetRule_sql_stmt_core2());
                    continue;
                }
                TContext ctx(settings, hints, issues);
                result.emplace_back();
                if (stmtParseInfo) {
                    stmtParseInfo->push_back({});
                }
                statementResult = commonStates;
                statementResult.push_back(block.GetRule_sql_stmt2().GetRule_sql_stmt_core2());
                SqlASTsToYqlsImpl(result.back(), statementResult, ctx);
                result.back().Issues = std::move(issues);
                issues.Clear();
            }
        }
    } else {
        ctx.IncrementMonCounter("sql_errors", "AstError");
    }
    if (warningRules) {
        *warningRules = ctx.WarningPolicy.GetRules();
        ctx.WarningPolicy.Clear();
    }
    return result;
}

} // namespace NSQLTranslationV1
