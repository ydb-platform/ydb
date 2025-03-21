#include "sql.h"
#include "sql_query.h"
#include <yql/essentials/parser/proto_ast/collect_issues/collect_issues.h>
#include <yql/essentials/sql/v1/lexer/lexer.h>
#include <yql/essentials/sql/v1/proto_parser/proto_parser.h>

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
    } catch (const NAST::TTooManyErrors&) {
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
    } catch (const NAST::TTooManyErrors&) {
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

NYql::TAstParseResult SqlASTToYql(const TLexers& lexers, const TParsers& parsers,
    const TString& query,
    const google::protobuf::Message& protoAst,
    const NSQLTranslation::TSQLHints& hints,
    const NSQLTranslation::TTranslationSettings& settings)
{
    YQL_ENSURE(IsQueryMode(settings.Mode));
    TAstParseResult res;
    TContext ctx(lexers, parsers, settings, hints, res.Issues, query);
    SqlASTToYqlImpl(res, protoAst, ctx);
    res.ActualSyntaxType = NYql::ESyntaxType::YQLv1;
    return res;
}

NYql::TAstParseResult SqlToYql(const TLexers& lexers, const TParsers& parsers, const TString& query, const NSQLTranslation::TTranslationSettings& settings, NYql::TWarningRules* warningRules)
{
    TAstParseResult res;
    const TString queryName = settings.File;

    NSQLTranslation::TSQLHints hints;
    auto lexer = MakeLexer(lexers, settings.AnsiLexer, settings.Antlr4Parser);
    YQL_ENSURE(lexer);
    if (!CollectSqlHints(*lexer, query, queryName, settings.File, hints, res.Issues, settings.MaxErrors, settings.Antlr4Parser)) {
        return res;
    }

    TContext ctx(lexers, parsers, settings, hints, res.Issues, query);
    NSQLTranslation::TErrorCollectorOverIssues collector(res.Issues, settings.MaxErrors, settings.File);

    google::protobuf::Message* ast(SqlAST(parsers, query, queryName, collector, settings.AnsiLexer,  settings.Antlr4Parser, settings.Arena));
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
        case TRule_sql_stmt_core::kAltSqlStmtCore48: // create backup collection
        case TRule_sql_stmt_core::kAltSqlStmtCore49: // alter backup collection
        case TRule_sql_stmt_core::kAltSqlStmtCore50: // drop backup collection
        case TRule_sql_stmt_core::kAltSqlStmtCore51: // analyze
        case TRule_sql_stmt_core::kAltSqlStmtCore52: // create resource pool classifier
        case TRule_sql_stmt_core::kAltSqlStmtCore53: // alter resource pool classifier
        case TRule_sql_stmt_core::kAltSqlStmtCore54: // drop resource pool classifier
        case TRule_sql_stmt_core::kAltSqlStmtCore55: // backup
        case TRule_sql_stmt_core::kAltSqlStmtCore56: // restore
        case TRule_sql_stmt_core::kAltSqlStmtCore57: // alter sequence
        case TRule_sql_stmt_core::kAltSqlStmtCore58: // create transfer
        case TRule_sql_stmt_core::kAltSqlStmtCore59: // alter transfer
        case TRule_sql_stmt_core::kAltSqlStmtCore60: // drop transfer
        case TRule_sql_stmt_core::kAltSqlStmtCore61: // alter database
        case TRule_sql_stmt_core::kAltSqlStmtCore62: // show create table
            return false;
    }
}

TVector<NYql::TAstParseResult> SqlToAstStatements(const TLexers& lexers, const TParsers& parsers, const TString& queryText, const NSQLTranslation::TTranslationSettings& settings, NYql::TWarningRules* warningRules,
    TVector<NYql::TStmtParseInfo>* stmtParseInfo)
{
    TVector<TAstParseResult> result;
    const TString queryName = settings.File;
    TIssues issues;

    NSQLTranslation::TSQLHints hints;
    auto lexer = MakeLexer(lexers, settings.AnsiLexer, settings.Antlr4Parser);
    YQL_ENSURE(lexer);
    if (!CollectSqlHints(*lexer, queryText, queryName, settings.File, hints, issues, settings.MaxErrors, settings.Antlr4Parser)) {
        return result;
    }

    TContext ctx(lexers, parsers, settings, hints, issues, queryText);
    NSQLTranslation::TErrorCollectorOverIssues collector(issues, settings.MaxErrors, settings.File);

    google::protobuf::Message* astProto(SqlAST(parsers, queryText, queryName, collector, settings.AnsiLexer, settings.Antlr4Parser, settings.Arena));
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
                TContext ctx(lexers, parsers, settings, hints, issues, queryText);
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
                TContext ctx(lexers, parsers, settings, hints, issues, queryText);
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

bool SplitQueryToStatements(const TLexers& lexers, const TParsers& parsers, const TString& query, TVector<TString>& statements, NYql::TIssues& issues,
    const NSQLTranslation::TTranslationSettings& settings) {
    auto lexer = NSQLTranslationV1::MakeLexer(lexers, settings.AnsiLexer, settings.Antlr4Parser);

    TVector<TString> parts;
    if (!SplitQueryToStatements(query, lexer, parts, issues)) {
        return false;
    }

    for (auto& currentQuery : parts) {
        NYql::TIssues parserIssues;
        auto message = NSQLTranslationV1::SqlAST(parsers, currentQuery, settings.File, parserIssues, NSQLTranslation::SQL_MAX_PARSER_ERRORS,
            settings.AnsiLexer, settings.Antlr4Parser, settings.Arena);
        if (!message) {
            // Skip empty statements
            continue;
        }

        statements.push_back(std::move(currentQuery));
    }

    return true;
}

class TTranslator : public NSQLTranslation::ITranslator {
public:
    TTranslator(const TLexers& lexers, const TParsers& parsers)
        : Lexers_(lexers)
        , Parsers_(parsers)
    {
    }

    NSQLTranslation::ILexer::TPtr MakeLexer(const NSQLTranslation::TTranslationSettings& settings) final {
        return NSQLTranslationV1::MakeLexer(Lexers_, settings.AnsiLexer, settings.Antlr4Parser);
    }

    NYql::TAstParseResult TextToAst(const TString& query, const NSQLTranslation::TTranslationSettings& settings,
        NYql::TWarningRules* warningRules, NYql::TStmtParseInfo* stmtParseInfo) final {
        Y_UNUSED(stmtParseInfo);
        return SqlToYql(Lexers_, Parsers_, query, settings, warningRules);
    }

    google::protobuf::Message* TextToMessage(const TString& query, const TString& queryName,
        NYql::TIssues& issues, size_t maxErrors, const NSQLTranslation::TTranslationSettings& settings) final {
        return SqlAST(Parsers_, query, queryName, issues, maxErrors, settings.AnsiLexer, settings.Antlr4Parser, settings.Arena);
    }

    NYql::TAstParseResult TextAndMessageToAst(const TString& query, const google::protobuf::Message& protoAst,
        const NSQLTranslation::TSQLHints& hints, const NSQLTranslation::TTranslationSettings& settings) final {
        return SqlASTToYql(Lexers_, Parsers_, query, protoAst, hints, settings);
    }

    TVector<NYql::TAstParseResult> TextToManyAst(const TString& query, const NSQLTranslation::TTranslationSettings& settings,
        NYql::TWarningRules* warningRules, TVector<NYql::TStmtParseInfo>* stmtParseInfo) final {
        return SqlToAstStatements(Lexers_, Parsers_, query, settings, warningRules, stmtParseInfo);
    }

private:
    const TLexers Lexers_;
    const TParsers Parsers_;
};

NSQLTranslation::TTranslatorPtr MakeTranslator(const TLexers& lexers, const TParsers& parsers) {
    return MakeIntrusive<TTranslator>(lexers, parsers);
}

} // namespace NSQLTranslationV1
