#include "syntax.h"
#include "engine.h"

#include <ydb/core/kqp/host/kqp_translate.h>
#include <ydb/core/kqp/provider/yql_kikimr_settings.h>
#include <ydb/tests/tools/parsed_query_bench/grammar_fingerprint.h>

#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/parser/common/antlr4/error_listener.h>
#include <yql/essentials/parser/common/issue.h>
#include <yql/essentials/parser/proto_ast/gen/v1_antlr4/SQLv1Antlr4Lexer.h>
#include <yql/essentials/parser/proto_ast/gen/v1_ansi_antlr4/SQLv1Antlr4Lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4_ansi/lexer.h>
#include <yql/essentials/sql/v1/proto_parser/parse_tree.h>
#include <yql/essentials/sql/v1/proto_parser/reflection.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4/proto_parser.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4_ansi/proto_parser.h>
#include <yql/essentials/sql/v1/translation/sql.h>

#include <google/protobuf/arena.h>

#include <util/charset/utf8.h>
#include <util/generic/yexception.h>

#include <algorithm>

namespace NKikimr::NKqp::NParsedQueryBench {
namespace {

using namespace Ydb::Query;

using TDefaultLexer = NALPDefaultAntlr4::SQLv1Antlr4Lexer;
using TAnsiLexer = NALPAnsiAntlr4::SQLv1Antlr4Lexer;
constexpr size_t MaxGrammarSourceBytes = 1 << 20;
constexpr int MaxGrammarTokens = 100'000;

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
    NSQLTranslation::TParsedSettings Directives;
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
        Y_ENSURE(NSQLTranslation::ParseTranslationSettingsFromComments(sql, Directives, Issues)
            && Directives.ApplyTo(Settings, Issues), Issues.ToString());
        Y_ENSURE(Settings.SyntaxVersion == 1 && !Settings.PgParser && !Settings.Syntax,
            "Expected YQL v1 translation settings");
    }
};

bool IsHint(TStringBuf text) {
    return text.StartsWith("/*+") || text.StartsWith("--+");
}

void CheckSource(TStringBuf sql) {
    Y_ENSURE(sql.size() <= MaxGrammarSourceBytes && !sql.StartsWith("\xef\xbb\xbf")
        && sql.find('\0') == TStringBuf::npos, "Unsupported ANTLR AST source");
}

template <class TLexer>
void ExportTokens(const TString& sql, TGrammarContext& grammar, SqlV1ParserAst& output) {
    const auto& settings = grammar.Settings;
    NSQLTranslation::TErrorCollectorOverIssues collector(grammar.Issues, settings.MaxErrors, settings.File);
    bool lexerError = false;
    antlr4::YqlErrorListener listener(&collector, &lexerError);
    antlr4::ANTLRInputStream source(sql);
    TLexer lexer(&source);
    lexer.removeErrorListeners();
    lexer.addErrorListener(&listener);
    TString recovered;
    for (;;) {
        const auto token = lexer.nextToken();
        const bool eof = token->getType() == TLexer::EOF;
        Y_ENSURE(output.tokens_size() < MaxGrammarTokens, "ANTLR AST token limit exceeded");
        auto& item = *output.add_tokens();
        item.set_type(eof ? -1 : static_cast<int>(token->getType()));
        item.set_text(token->getText());
        item.set_channel(token->getChannel());
        item.set_line(token->getLine());
        item.set_column(token->getCharPositionInLine());
        item.set_begin(token->getStartIndex());
        item.set_end(eof ? token->getStartIndex() : token->getStopIndex() + 1);
        Y_ENSURE(item.channel() == antlr4::Token::DEFAULT_CHANNEL || !IsHint(item.text()),
            "ANTLR AST query hints are unsupported");
        if (eof) {
            break;
        }
        recovered += item.text();
    }
    Y_ENSURE(!lexerError, grammar.Issues.ToString());
    Y_ENSURE(recovered == sql, "Token stream cannot reproduce the original SQL");
}

struct TGrammarInput {
    TString Sql;
    NSQLTranslation::TParsedTokenList Tokens;
};

template <class TLexer>
TGrammarInput ImportTokens(const SqlV1ParserAst& input) {
    static const TLexer lexer(nullptr);
    TGrammarInput result;
    result.Tokens.reserve(input.tokens_size());
    size_t offset = 0;
    size_t line = 1;
    size_t column = 0;
    for (int i = 0; i != input.tokens_size(); ++i) {
        const auto& token = input.tokens(i);
        const bool eof = i + 1 == input.tokens_size();
        const auto type = eof ? TLexer::EOF : static_cast<size_t>(token.type());
        const TStringBuf text(token.text());
        Y_ENSURE(eof ? token.type() == -1 && text == "<EOF>"
                : token.type() > 0 && type <= lexer.getATN().maxTokenType && !text.empty(),
            "Invalid ANTLR AST token type or EOF");
        const bool hidden = type == TLexer::TOKEN_WS || type == TLexer::TOKEN_COMMENT;
        Y_ENSURE(token.channel() == (hidden ? antlr4::Token::HIDDEN_CHANNEL : antlr4::Token::DEFAULT_CHANNEL),
            "Invalid ANTLR AST token channel");
        Y_ENSURE(!hidden || !IsHint(text), "ANTLR AST query hints are unsupported");
        Y_ENSURE(token.begin() == offset && token.line() == line && token.column() == column,
            "Invalid ANTLR AST token position");
        if (!eof) {
            Y_ENSURE(text.size() <= MaxGrammarSourceBytes - result.Sql.size(), "ANTLR AST source byte limit exceeded");
            result.Sql += text;
            const size_t length = GetNumberOfUTF8Chars(text);
            offset += length;
            const size_t lastNewline = text.rfind('\n');
            if (lastNewline == TStringBuf::npos) {
                column += length;
            } else {
                line += std::count(text.begin(), text.end(), '\n');
                column = GetNumberOfUTF8Chars(text.substr(lastNewline + 1));
            }
        }
        Y_ENSURE(token.end() == offset, "Invalid ANTLR AST token end");
        const auto name = lexer.getVocabulary().getSymbolicName(type);
        result.Tokens.push_back({name.empty() ? TString(NAST::INVALID_TOKEN_NAME) : TString(name),
            TString(text), token.line(), token.column()});
    }
    CheckSource(result.Sql);
    return result;
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
        Y_ENSURE(!(input.atom().flags() & ~NYql::TAstNodeFlags::FlagsMask), "Unknown YQL atom flags");
        return NYql::TAstNode::NewAtom(position, input.atom().content(), pool, input.atom().flags());
    }
    Y_ENSURE(input.has_list(), "Missing YQL node kind");
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

void ExportGrammar(const TString& sql, const TRequest& envelope, const TEngine& engine, SqlV1ParserAst& output) {
    CheckSource(sql);
    TGrammarContext grammar(sql, envelope, engine);
    const auto& settings = grammar.Settings;
    const auto* ast = NSQLTranslationV1::SqlAST(grammar.Parsers, sql, settings.File, grammar.Issues,
        settings.MaxErrors, settings.AnsiLexer, &grammar.Arena);
    Y_ENSURE(ast, grammar.Issues.ToString());
    output.set_format_version(1);
    output.mutable_ast()->CopyFrom(*ast);
    auto& metadata = *output.mutable_metadata();
    metadata.set_grammar_fingerprint(GrammarFingerprint, sizeof(GrammarFingerprint) - 1);
    metadata.set_source_name(settings.File);
    metadata.set_lexer_mode(settings.AnsiLexer ? YqlParserMetadata::ANSI : YqlParserMetadata::DEFAULT);
    metadata.mutable_directives()->set_explicit_syntax_v1(grammar.Directives.HasSyntaxV1);
    metadata.mutable_directives()->set_explicit_ansi_lexer(grammar.Directives.HasAnsiLexer);

    if (settings.AnsiLexer) {
        ExportTokens<TAnsiLexer>(sql, grammar, output);
    } else {
        ExportTokens<TDefaultLexer>(sql, grammar, output);
    }
}

NYql::TAstParseResult TranslateGrammar(const SqlV1ParserAst& input, const TRequest& envelope, const TEngine& engine) {
    Y_ENSURE(input.format_version() == 1 && input.has_ast() && input.has_metadata(), "Invalid ANTLR AST envelope");
    const auto& metadata = input.metadata();
    Y_ENSURE(metadata.grammar_fingerprint() == TStringBuf(GrammarFingerprint, sizeof(GrammarFingerprint) - 1),
        "Grammar schema fingerprint mismatch");
    Y_ENSURE(metadata.lexer_mode() == YqlParserMetadata::DEFAULT || metadata.lexer_mode() == YqlParserMetadata::ANSI,
        "Unsupported ANTLR AST lexer mode");
    const bool ansi = metadata.lexer_mode() == YqlParserMetadata::ANSI;
    Y_ENSURE(metadata.hints().empty() && metadata.source_name().size() <= 4096, "Unsupported ANTLR AST metadata");
    Y_ENSURE(input.tokens_size() > 0 && input.tokens_size() <= MaxGrammarTokens, "Invalid ANTLR AST token count");

    using namespace NSQLv1Generated;
    const auto& query = input.ast().GetRule_sql_query();
    Y_ENSURE(query.GetAltCase() != TRule_sql_query::ALT_NOT_SET, "Missing SQL query alternative");
    for (const auto* statement : NSQLTranslationV1::Statements(query)) {
        Y_ENSURE(statement->GetAltCase() != TRule_sql_stmt_core::ALT_NOT_SET, "Missing SQL statement alternative");
        const auto description = NSQLTranslationV1::AltDescription(*statement);
        Y_ENSURE(description.Contains('_') &&
            (statement->GetAltCase() != TRule_sql_stmt_core::kAltSqlStmtCore2 || description == "select_stmt"),
            "Invalid SQL statement description");
    }

    auto [sql, tokens] = ansi ? ImportTokens<TAnsiLexer>(input) : ImportTokens<TDefaultLexer>(input);
    TGrammarContext grammar(sql, envelope, engine);
    Y_ENSURE(grammar.Directives.HasSyntaxV1 == metadata.directives().explicit_syntax_v1()
        && grammar.Directives.HasAnsiLexer == metadata.directives().explicit_ansi_lexer()
        && (!grammar.Directives.HasAnsiLexer || ansi), "Inconsistent ANTLR AST directives");
    grammar.Settings.AnsiLexer = ansi;
    grammar.Settings.File = metadata.source_name();
    auto result = NSQLTranslationV1::SqlASTToYql(grammar.Lexers, grammar.Parsers, sql, input.ast(), tokens, grammar.Settings);
    Y_ENSURE(result.IsOk(), result.Issues.ToString());
    return result;
}

void ExportAst(const NYql::TAstNode& root, YqlAst& output) {
    output.set_format_version(1);
    ExportNode(root, *output.mutable_root());
}

NYql::TAstParseResult ImportAst(const YqlAst& input) {
    Y_ENSURE(input.format_version() == 1 && input.has_root(), "Unsupported or empty YQL AST");
    NYql::TAstParseResult result;
    result.Pool = std::make_unique<TMemoryPool>(4096);
    result.Root = ImportNode(input.root(), *result.Pool);
    return result;
}

} // namespace NKikimr::NKqp::NParsedQueryBench
