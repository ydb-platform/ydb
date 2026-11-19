#include "syntax.h"
#include "stage_error.h"
#include "engine.h"

#include <ydb/core/kqp/host/kqp_translate.h>
#include <ydb/core/kqp/provider/yql_kikimr_settings.h>
#include <ydb/tests/tools/parsed_query_bench/grammar_fingerprint.h>

#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/sql/v1/lexer/antlr4/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4_ansi/lexer.h>
#include <yql/essentials/sql/v1/proto_parser/parse_tree.h>
#include <yql/essentials/sql/v1/proto_parser/reflection.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4/proto_parser.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4_ansi/proto_parser.h>
#include <yql/essentials/sql/v1/translation/sql.h>

#include <google/protobuf/arena.h>

#include <util/generic/yexception.h>

namespace NKikimr::NKqp::NParsedQueryBench {
namespace {

using namespace Ydb::Query;

TKqpTranslationSettingsBuilder SettingsBuilder(const TString& sql, const TRequest& envelope, const TEngine& engine) {
    const auto& config = engine.TranslationConfig();
    TKqpTranslationSettingsBuilder builder(NYql::EKikimrQueryType::Query, engine.Cluster, sql,
        config.GetYqlBindingsMode(), std::make_shared<TGUCSettings>());
    builder.SetFromConfig(config).SetKqpTablePathPrefix(engine.PathPrefix)
        .SetIsEnableExternalDataSources(config.FeatureFlags.GetEnableExternalDataSources())
        .SetSqlAutoCommit(false).SetYqlSelect(NSQLTranslation::EYqlSelect::Force);
    if (!envelope.parameters().empty()) {
        auto types = std::make_shared<std::map<TString, Ydb::Type>>();
        for (const auto& [name, value] : envelope.parameters()) {
            Y_ENSURE(value.has_type(), "Missing parameter type for " << name);
            types->emplace(name, value.type());
        }
        builder.SetQueryParameters(types);
    }
    return builder;
}

struct TGrammarContext {
    NSQLTranslationV1::TLexers Lexers;
    NSQLTranslationV1::TParsers Parsers;
    NSQLTranslation::TTranslationSettings Settings;
    google::protobuf::Arena Arena;
    NYql::TIssues Issues;

    TGrammarContext(const TString& sql, const TRequest& envelope, const TEngine& engine) {
        Lexers.Antlr4 = NSQLTranslationV1::MakeAntlr4LexerFactory();
        Lexers.Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiLexerFactory();
        auto builder = SettingsBuilder(sql, envelope, engine);
        Parsers.Antlr4 = NSQLTranslationV1::MakeAntlr4ParserFactory(builder.GetIsAmbiguityError());
        Parsers.Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiParserFactory();
        NYql::TExprContext ctx;
        Settings = builder.Build(ctx);
        Settings.Arena = &Arena;
        Y_ENSURE(NSQLTranslation::ParseTranslationSettings(sql, Settings, Issues), Issues.ToString());
        Y_ENSURE(Settings.SyntaxVersion == 1 && !Settings.PgParser && !Settings.Syntax,
            "Expected YQL v1 translation settings");
    }
};

TString RecoverSource(const MainSqlV1ParserAst& input) {
    Y_ENSURE_EX(!input.tokens().empty(), TStageError() << "Missing grammar token stream");
    TString sql;
    for (int i = 0; i + 1 < input.tokens_size(); ++i) {
        sql += input.tokens(i).content();
    }
    return sql;
}

void ExportNode(const NYql::TAstNode& node, YqlAst::Node& output) {
    const auto& pos = node.GetPosition();
    output.set_row(pos.Row);
    output.set_column(pos.Column);
    output.set_file(pos.File.data(), pos.File.size());
    if (node.IsAtom()) {
        const auto content = node.GetContent();
        auto* atom = output.mutable_atom();
        atom->set_content(content.data(), content.size());
        atom->set_flags(node.GetFlags());
    } else {
        auto* list = output.mutable_list();
        for (const auto* child : node.GetChildren()) {
            ExportNode(*child, *list->add_children());
        }
    }
}

NYql::TAstNode* ImportNode(const YqlAst::Node& input, TMemoryPool& pool) {
    NYql::TPosition position(input.column(), input.row(), input.file());
    if (input.has_atom()) {
        Y_ENSURE_EX(!(input.atom().flags() & ~NYql::TAstNodeFlags::FlagsMask), TStageError() << "Unknown YQL atom flags");
        return NYql::TAstNode::NewAtom(position, input.atom().content(), pool, input.atom().flags());
    }
    Y_ENSURE_EX(input.has_list(), TStageError() << "Missing YQL node kind");
    TVector<NYql::TAstNode*> children;
    children.reserve(input.list().children_size());
    for (const auto& child : input.list().children()) {
        children.push_back(ImportNode(child, pool));
    }
    return NYql::TAstNode::NewList(position, children.data(), children.size(), pool);
}

} // namespace

NYql::TAstParseResult TranslateText(const TString& sql, const TRequest& envelope, const TEngine& engine) {
    NYql::TExprContext ctx;
    auto builder = SettingsBuilder(sql, envelope, engine);
    TMaybe<ui16> version;
    TMaybe<TString> command;
    bool deprecated = false;
    bool cache = false;
    auto result = ParseQuery(sql, true, version, deprecated, ctx, builder, cache, command);
    Y_ENSURE(result.IsOk() && result.ActualSyntaxType == NYql::ESyntaxType::YQLv1, result.Issues.ToString());
    return result;
}

void ExportGrammar(const TString& sql, const TRequest& envelope, const TEngine& engine, MainSqlV1ParserAst& output) {
    TGrammarContext grammar(sql, envelope, engine);
    const auto& settings = grammar.Settings;
    const auto* ast = NSQLTranslationV1::SqlAST(grammar.Parsers, sql, settings.File, grammar.Issues,
        settings.MaxErrors, settings.AnsiLexer, &grammar.Arena);
    Y_ENSURE(ast, grammar.Issues.ToString());
    output.mutable_ast()->CopyFrom(*ast);
    output.set_grammar_fingerprint(GrammarFingerprint, sizeof(GrammarFingerprint) - 1);

    auto lexer = NSQLTranslationV1::MakeLexer(grammar.Lexers, settings.AnsiLexer);
    Y_ENSURE(lexer->Tokenize(sql, settings.File, [&](NSQLTranslation::TParsedToken&& token) {
        auto* item = output.add_tokens();
        item->set_name(std::move(token.Name));
        item->set_content(std::move(token.Content));
        item->set_line(token.Line);
        item->set_column(token.LinePos);
    }, grammar.Issues, settings.MaxErrors), grammar.Issues.ToString());
    Y_ENSURE(RecoverSource(output) == sql, "Token stream cannot reproduce the original SQL");
}

NYql::TAstParseResult TranslateGrammar(const MainSqlV1ParserAst& input, const TRequest& envelope, const TEngine& engine) {
    Y_ENSURE_EX(input.has_ast() && input.grammar_fingerprint() == TStringBuf(GrammarFingerprint, sizeof(GrammarFingerprint) - 1), TStageError() << "Grammar schema fingerprint mismatch or missing AST");
    using namespace NSQLv1Generated;
    const auto& query = input.ast().GetRule_sql_query();
    Y_ENSURE_EX(query.GetAltCase() != TRule_sql_query::ALT_NOT_SET, TStageError() << "Missing SQL query alternative");
    for (const auto* statement : NSQLTranslationV1::Statements(query)) {
        Y_ENSURE_EX(statement->GetAltCase() != TRule_sql_stmt_core::ALT_NOT_SET, TStageError() << "Missing SQL statement alternative");
        const auto description = NSQLTranslationV1::AltDescription(*statement);
        Y_ENSURE_EX(description.Contains('_') &&
            (statement->GetAltCase() != TRule_sql_stmt_core::kAltSqlStmtCore2 || description == "select_stmt"),
            TStageError() << "Invalid SQL statement description");
    }
    const auto sql = RecoverSource(input);
    TGrammarContext grammar(sql, envelope, engine);
    NSQLTranslation::TParsedTokenList tokens;
    tokens.reserve(input.tokens_size());
    for (const auto& token : input.tokens()) {
        tokens.push_back({token.name(), token.content(), token.line(), token.column()});
    }
    auto result = NSQLTranslationV1::SqlASTToYql(grammar.Lexers, grammar.Parsers, sql, input.ast(), tokens, grammar.Settings);
    Y_ENSURE(result.IsOk(), result.Issues.ToString());
    return result;
}

void ExportAst(const NYql::TAstNode& root, YqlAst& output) {
    output.set_format_version(1);
    ExportNode(root, *output.mutable_root());
}

NYql::TAstParseResult ImportAst(const YqlAst& input) {
    Y_ENSURE_EX(input.format_version() == 1 && input.has_root(), TStageError() << "Unsupported or empty YQL AST");
    NYql::TAstParseResult result;
    result.Pool = std::make_unique<TMemoryPool>(4096);
    result.Root = ImportNode(input.root(), *result.Pool);
    return result;
}

} // namespace NKikimr::NKqp::NParsedQueryBench
