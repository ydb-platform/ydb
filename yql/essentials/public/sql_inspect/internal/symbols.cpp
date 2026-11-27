#include "symbols.h"

#include <yql/essentials/ast/yql_ast_escaping.h>
#include <yql/essentials/parser/proto_ast/gen/v1_proto_split_antlr4/SQLv1Antlr4Parser.pb.main.h>
#include <yql/essentials/sql/settings/translation_settings.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4/proto_parser.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4_ansi/proto_parser.h>
#include <yql/essentials/sql/v1/proto_parser/proto_parser.h>
#include <yql/essentials/utils/yql_panic.h>

#include <util/generic/algorithm.h>
#include <util/stream/str.h>
#include <util/string/subst.h>

#include <tuple>

namespace NYql::NSqlInspect {
namespace {

template <class TVisitor>
void Walk(const google::protobuf::Message& root, TVisitor&& visitor) {
    TVector<const google::protobuf::Message*> pending{&root};
    while (!pending.empty()) {
        const auto* message = pending.back();
        pending.pop_back();
        if (!visitor(*message)) {
            continue;
        }
        const auto* reflection = message->GetReflection();
        TVector<const google::protobuf::FieldDescriptor*> fields;
        reflection->ListFields(*message, &fields);
        for (auto field = fields.rbegin(); field != fields.rend(); ++field) {
            if ((*field)->cpp_type() != google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE) {
                continue;
            }
            if ((*field)->is_repeated()) {
                for (int index = reflection->FieldSize(*message, *field); index > 0; --index) {
                    pending.push_back(&reflection->GetRepeatedMessage(*message, *field, index - 1));
                }
            } else {
                pending.push_back(&reflection->GetMessage(*message, *field));
            }
        }
    }
}

TVector<NSQLv1Generated::TToken> Tokens(const google::protobuf::Message& message) {
    TVector<NSQLv1Generated::TToken> tokens;
    Walk(message, [&](const auto& node) {
        if (node.GetDescriptor() == NSQLv1Generated::TToken::descriptor()) {
            tokens.push_back(static_cast<const NSQLv1Generated::TToken&>(node));
            return false;
        }
        return true;
    });
    return tokens;
}

TString BacktickIdentifier(const NSQLv1Generated::TToken& token, TIssues& issues) {
    TString name;
    TStringOutput output(name);
    size_t readBytes = 0;
    const TStringBuf atom = TStringBuf(token.GetValue()).SubStr(1);
    const auto status = UnescapeArbitraryAtom(atom, '`', &output, &readBytes);
    if (status != EUnescapeResult::OK || readBytes != atom.size()) {
        issues.AddIssue(TIssue(TPosition(token.GetColumn(), token.GetLine()), "Invalid quoted identifier"));
    }
    return name;
}

TString Identifier(const NSQLv1Generated::TToken& token, bool ansi, TIssues& issues) {
    const TString& value = token.GetValue();
    YQL_ENSURE(!value.empty(), "Empty identifier token");
    if (value.front() == '\'' || value.StartsWith("@@") || (value.front() == '"' && !ansi)) {
        issues.AddIssue(TIssue(TPosition(token.GetColumn(), token.GetLine()), "String literal cannot be used as an identifier"));
        return {};
    }
    if (value.front() == '"') {
        if (value.size() < 2 || value.back() != '"') {
            issues.AddIssue(TIssue(TPosition(token.GetColumn(), token.GetLine()), "Expected double quoted identifier without a string literal suffix"));
            return {};
        }
        TString name = value.substr(1, value.size() - 2);
        SubstGlobal(name, "\"\"", "\"");
        return name;
    }
    return value.front() == '`' ? BacktickIdentifier(token, issues) : value;
}

TString SymbolName(const NSQLv1Generated::TRule_bind_parameter& symbol, bool ansi, TIssues& issues) {
    const auto tokens = Tokens(symbol);
    YQL_ENSURE(tokens.size() == 2 && tokens.front().GetValue() == "$", "Invalid bind parameter AST");
    return "$" + Identifier(tokens.back(), ansi, issues);
}

TVector<TString> ModulePath(const NSQLv1Generated::TRule_module_path& path, bool ansi, TIssues& issues) {
    TVector<TString> components;
    const auto appendComponent = [&](const auto& component) {
        const auto tokens = Tokens(component);
        YQL_ENSURE(tokens.size() == 1, "Expected one token in a module path component");
        components.push_back(Identifier(tokens.front(), ansi, issues));
    };
    appendComponent(path.GetRule_an_id2());
    for (const auto& block : path.GetBlock3()) {
        appendComponent(block.GetRule_an_id2());
    }
    return components;
}

void CollectImport(const NSQLv1Generated::TRule_import_stmt& statement, bool selected, bool ansi, TSymbols& symbols, TIssues& issues) {
    const auto module = ModulePath(statement.GetRule_module_path2(), ansi, issues);
    Walk(statement.GetRule_named_bind_parameter_list4(), [&](const auto& node) {
        if (node.GetDescriptor() != NSQLv1Generated::TRule_named_bind_parameter::descriptor()) {
            return true;
        }
        const auto& binding = static_cast<const NSQLv1Generated::TRule_named_bind_parameter&>(node);
        const TString name = SymbolName(binding.GetRule_bind_parameter1(), ansi, issues);
        if (selected) {
            symbols.Imports.push_back({.Name = name, .Module = module, .IsAbsolute = statement.GetRule_module_path2().HasBlock1()});
        }
        return false;
    });
}

void CollectExport(const NSQLv1Generated::TRule_export_stmt& statement, bool selected, bool ansi, TSymbols& symbols, TIssues& issues) {
    Walk(statement.GetRule_bind_parameter_list2(), [&](const auto& node) {
        if (node.GetDescriptor() != NSQLv1Generated::TRule_bind_parameter::descriptor()) {
            return true;
        }
        const TString name = SymbolName(static_cast<const NSQLv1Generated::TRule_bind_parameter&>(node), ansi, issues);
        if (selected) {
            symbols.Exports.push_back({.Name = name});
        }
        return false;
    });
}

bool CollectStatement(const google::protobuf::Message& node, const TSymbolSettings& settings, TSymbols& symbols, TIssues& issues) {
    const auto enabled = [&](ESymbolType type) { return !settings.Filter || settings.Filter->contains(type); };
    if (node.GetDescriptor() == NSQLv1Generated::TRule_import_stmt::descriptor()) {
        CollectImport(static_cast<const NSQLv1Generated::TRule_import_stmt&>(node), enabled(ESymbolType::Import), settings.Ansi, symbols, issues);
        return false;
    }
    if (node.GetDescriptor() == NSQLv1Generated::TRule_export_stmt::descriptor()) {
        CollectExport(static_cast<const NSQLv1Generated::TRule_export_stmt&>(node), enabled(ESymbolType::Export), settings.Ansi, symbols, issues);
        return false;
    }
    if (node.GetDescriptor() == NSQLv1Generated::TRule_declare_stmt::descriptor()) {
        const auto& statement = static_cast<const NSQLv1Generated::TRule_declare_stmt&>(node);
        const TString name = SymbolName(statement.GetRule_bind_parameter2(), settings.Ansi, issues);
        if (enabled(ESymbolType::Parameter)) {
            symbols.Parameters.push_back({.Name = name});
        }
        return false;
    }
    return true;
}

TSymbolsResult CollectSymbols(const google::protobuf::Message& ast, const TSymbolSettings& settings) {
    TSymbols symbols;
    TIssues issues;
    Walk(ast, [&](const auto& node) {
        return CollectStatement(node, settings, symbols, issues);
    });
    if (!issues.Empty()) {
        return issues;
    }
    SortUniqueBy(symbols.Imports, [](const auto& symbol) { return std::tie(symbol.IsAbsolute, symbol.Module, symbol.Name); });
    SortUniqueBy(symbols.Exports, [](const auto& symbol) { return symbol.Name; });
    SortUniqueBy(symbols.Parameters, [](const auto& symbol) { return symbol.Name; });
    return symbols;
}

} // namespace

TSymbolsResult GetSymbolsImpl(const TString& sql, const TSymbolSettings& settings) {
    NSQLTranslation::TTranslationSettings translationSettings;
    translationSettings.AnsiLexer = settings.Ansi;
    translationSettings.LangVer = settings.LangVer;
    translationSettings.InferSyntaxVersion = true;
    TIssues issues;
    if (!NSQLTranslation::ParseTranslationSettings(sql, translationSettings, issues)) {
        return issues;
    }
    if (translationSettings.Syntax || translationSettings.PgParser || translationSettings.SyntaxVersion != 1) {
        issues.AddIssue(TIssue("SQL inspection supports only v1 syntax"));
        return issues;
    }
    google::protobuf::Arena arena;
    const NSQLTranslationV1::TParsers parsers{
        .Antlr4 = NSQLTranslationV1::MakeAntlr4ParserFactory(),
        .Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiParserFactory(),
    };
    const auto* ast = NSQLTranslationV1::SqlAST(parsers, sql, "", issues,
                                                translationSettings.MaxErrors, translationSettings.AnsiLexer, &arena);
    if (!ast) {
        return issues;
    }
    TSymbolSettings effectiveSettings = settings;
    effectiveSettings.Ansi = translationSettings.AnsiLexer;
    return CollectSymbols(*ast, effectiveSettings);
}

} // namespace NYql::NSqlInspect
