#include "translator.h"

#include <yql/essentials/ast/yql_ast_escaping.h>
#include <yql/essentials/core/sql_types/normalize_name.h>
#include <yql/essentials/sql/v1/lexer/antlr4/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4_ansi/lexer.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4/proto_parser.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4_ansi/proto_parser.h>
#include <yql/essentials/sql/v1/translation/antlr_token.h>
#include <yql/essentials/sql/v1/translation/sql.h>

#include <util/stream/str.h>
#include <util/string/ascii.h>

#include <vector>

namespace NKikimr::NKqp {
namespace {

using namespace NSQLv1Generated;

constexpr TStringBuf AliasPrefix = "ApplyRemoteEmbedder";
constexpr TStringBuf StreamAlias = "ApplyRemoteEmbedderStream";

TStringBuf GetAlias(TStringBuf name, bool normalize) {
    TString decoded;
    if (name.size() >= 2 && name.StartsWith('`') && name.EndsWith('`')) {
        name.Skip(1);
        TStringOutput out(decoded);
        size_t consumed = 0;
        if (NYql::UnescapeArbitraryAtom(name, '`', &out, &consumed) != NYql::EUnescapeResult::OK || consumed != name.size()) {
            return {};
        }
        name = decoded;
    } else if (name.size() >= 2 && name.StartsWith('"') && name.EndsWith('"')) {
        // A doubled quote cannot be part of either alias.
        name = name.SubStr(1, name.size() - 2);
    }
    TString normalized;
    if (normalize && name.Contains('_')) {
        normalized = name;
        if (NYql::NormalizeName({}, normalized)) {
            return {};
        }
        name = normalized;
    }
    if (AsciiEqualsIgnoreCase(name, AliasPrefix)) {
        return AliasPrefix;
    }
    if (AsciiEqualsIgnoreCase(name, StreamAlias)) {
        return StreamAlias;
    }
    return {};
}

bool MayContainAlias(TStringBuf query) {
    constexpr TStringBuf prefix = "applyremoteembedder";
    size_t matched = 0;
    for (const char c : query) {
        if (c == '_') {
            continue;
        }
        const auto lower = AsciiToLower(c);
        if (lower == prefix[matched]) {
            if (++matched == prefix.size()) {
                return true;
            }
        } else {
            matched = lower == prefix.front() ? 1 : 0;
        }
    }
    return query.Contains('`') && query.Contains('\\');
}

void SetToken(TToken& token, const TToken& source, ui32 id, ui32 element, TStringBuf value) {
    token = source;
    token.SetId(id);
    token.SetElement(element);
    token.SetValue(TString(value));
}

// The synthetic qualified name represents the single alias token in the SQL text.
void SetAliasTokenRange(google::protobuf::Message& node, ui32 index) {
    using google::protobuf::FieldDescriptor;
    const auto* descriptor = node.GetDescriptor();
    const auto* start = descriptor->FindFieldByName("StartTokenIndex");
    const auto* end = descriptor->FindFieldByName("EndTokenIndex");
    if (!start || !end) {
        return;
    }

    const auto* reflection = node.GetReflection();
    reflection->SetUInt32(&node, start, index);
    reflection->SetUInt32(&node, end, index);
    std::vector<const FieldDescriptor*> fields;
    reflection->ListFields(node, &fields);
    for (const auto* field : fields) {
        if (field->cpp_type() == FieldDescriptor::CPPTYPE_MESSAGE) {
            SetAliasTokenRange(*reflection->MutableMessage(&node, field), index);
        }
    }
}

TToken& IdentifierToken(TRule_id_or_type& id) {
    return *id.MutableAlt_id_or_type1()->MutableRule_id1()->MutableAlt_id1()->MutableRule_identifier1()->MutableToken1();
}

TToken& IdentifierToken(TRule_an_id_or_type& id) {
    return IdentifierToken(*id.MutableAlt_an_id_or_type1()->MutableRule_id_or_type1());
}

template <class TQualifiedName>
void SetQualifiedName(TQualifiedName& name, TToken& function, const TToken& source, TStringBuf alias) {
    using TLexer = NALPDefaultAntlr4::SQLv1Antlr4Lexer;
    SetToken(IdentifierToken(*name.MutableRule_an_id_or_type1()), source, TLexer::TOKEN_ID_PLAIN, 1, "Knn");
    SetToken(*name.MutableToken2(), source, TLexer::TOKEN_NAMESPACE, 2, "::");
    SetToken(function, source, TLexer::TOKEN_ID_PLAIN, 1, alias);
}

const TToken& NameToken(const TRule_unary_casual_subexpr& expr, bool ansi) {
    const auto& block = expr.GetBlock1();
    if (ansi && block.HasAlt2()) {
        return block.GetAlt2().GetRule_atom_expr1().GetAlt_atom_expr1().GetRule_literal_value1()
            .GetAlt_literal_value3().GetToken1();
    }
    return block.GetAlt1().GetRule_id_expr1().GetAlt_id_expr1().GetRule_identifier1().GetToken1();
}

const TToken& NameToken(const TRule_in_unary_casual_subexpr& expr, bool ansi) {
    const auto& block = expr.GetBlock1();
    if (ansi && block.HasAlt2()) {
        return block.GetAlt2().GetRule_in_atom_expr1().GetAlt_in_atom_expr1().GetRule_literal_value1()
            .GetAlt_literal_value3().GetToken1();
    }
    return block.GetAlt1().GetRule_id_expr_in1().GetAlt_id_expr_in1().GetRule_identifier1().GetToken1();
}

auto& QualifiedName(TRule_unary_casual_subexpr& expr) {
    return *expr.MutableBlock1()->MutableAlt2()->MutableRule_atom_expr1()->MutableAlt_atom_expr7();
}

auto& QualifiedName(TRule_in_unary_casual_subexpr& expr) {
    return *expr.MutableBlock1()->MutableAlt2()->MutableRule_in_atom_expr1()->MutableAlt_in_atom_expr6();
}

template <class TExpression>
void QualifyCall(TExpression& expr, bool ansi) {
    const auto& suffix = expr.GetRule_unary_subexpr_suffix2().GetBlock1();
    if (suffix.empty() || !suffix[0].GetBlock1().HasAlt2()) {
        return;
    }
    const auto& token = NameToken(expr, ansi);
    if (!expr.GetBlock1().HasAlt1() && !TStringBuf(token.GetValue()).StartsWith('"')) {
        return;
    }
    if (const auto alias = GetAlias(token.GetValue(), /*normalize=*/true); !alias.empty()) {
        const auto index = expr.GetBlock1().GetStartTokenIndex();
        const TToken source = token; // Switching alternatives invalidates the original token.
        auto& name = QualifiedName(expr);
        auto& function = IdentifierToken(*name.MutableBlock3()->MutableAlt1()->MutableRule_id_or_type1());
        SetQualifiedName(name, function, source, alias);
        SetAliasTokenRange(*expr.MutableBlock1(), index);
    }
}

void QualifyCall(TRule_using_call_expr& expr, bool ansi) {
    const auto& block = expr.GetBlock1();
    if (!block.HasAlt2()) {
        return;
    }
    const auto& identifier = block.GetAlt2().GetRule_an_id_expr1();
    const auto& token = ansi && identifier.HasAlt_an_id_expr2()
        ? identifier.GetAlt_an_id_expr2().GetToken1()
        : identifier.GetAlt_an_id_expr1().GetRule_id_expr1().GetAlt_id_expr1().GetRule_identifier1().GetToken1();
    if (identifier.HasAlt_an_id_expr2() && !TStringBuf(token.GetValue()).StartsWith('"')) {
        return;
    }
    if (const auto alias = GetAlias(token.GetValue(), /*normalize=*/false); !alias.empty()) {
        const auto index = block.GetStartTokenIndex();
        const TToken source = token;
        auto& name = *expr.MutableBlock1()->MutableAlt1();
        auto& function = IdentifierToken(*name.MutableRule_an_id_or_type3());
        SetQualifiedName(name, function, source, alias);
        SetAliasTokenRange(*expr.MutableBlock1(), index);
    }
}

void QualifyCalls(google::protobuf::Message& message, bool ansi) {
    TVector<google::protobuf::Message*> pending{&message};
    std::vector<const google::protobuf::FieldDescriptor*> fields;
    while (!pending.empty()) {
        auto* node = pending.back();
        pending.pop_back();
        const auto* type = node->GetDescriptor();
        if (type == TRule_unary_casual_subexpr::descriptor()) {
            QualifyCall(static_cast<TRule_unary_casual_subexpr&>(*node), ansi);
        } else if (type == TRule_in_unary_casual_subexpr::descriptor()) {
            QualifyCall(static_cast<TRule_in_unary_casual_subexpr&>(*node), ansi);
        } else if (type == TRule_using_call_expr::descriptor()) {
            QualifyCall(static_cast<TRule_using_call_expr&>(*node), ansi);
        }

        const auto* reflection = node->GetReflection();
        reflection->ListFields(*node, &fields);
        for (const auto* field : fields) {
            if (field->cpp_type() != google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE) {
                continue;
            }
            if (field->is_repeated()) {
                for (int i = 0; i < reflection->FieldSize(*node, field); ++i) {
                    pending.push_back(reflection->MutableRepeatedMessage(node, field, i));
                }
            } else {
                pending.push_back(reflection->MutableMessage(node, field));
            }
        }
    }
}

class TKqpSqlParser final : public NSQLTranslation::IParser {
public:
    TKqpSqlParser(std::unique_ptr<NSQLTranslation::IParser> parser, bool ansi)
        : Parser_(std::move(parser))
        , Ansi_(ansi)
    {}

    google::protobuf::Message* Parse(const TString& query, const TString& queryName,
        NAST::IErrorCollector& err, google::protobuf::Arena* arena) override
    {
        auto* ast = Parser_->Parse(query, queryName, err, arena);
        if (ast && MayContainAlias(query)) {
            // Keep the original SQL and token positions for hints and diagnostics.
            QualifyCalls(*ast, Ansi_);
        }
        return ast;
    }

private:
    const std::unique_ptr<NSQLTranslation::IParser> Parser_;
    const bool Ansi_;
};

class TKqpSqlParserFactory final : public NSQLTranslation::IParserFactory {
public:
    TKqpSqlParserFactory(NSQLTranslation::TParserFactoryPtr factory, bool ansi)
        : Factory_(std::move(factory))
        , Ansi_(ansi)
    {}

    std::unique_ptr<NSQLTranslation::IParser> MakeParser() const override {
        return std::make_unique<TKqpSqlParser>(Factory_->MakeParser(), Ansi_);
    }

private:
    const NSQLTranslation::TParserFactoryPtr Factory_;
    const bool Ansi_;
};

} // namespace

NSQLTranslation::TTranslatorPtr MakeKqpSqlV1Translator(bool isAmbiguityError) {
    NSQLTranslationV1::TLexers lexers;
    lexers.Antlr4 = NSQLTranslationV1::MakeAntlr4LexerFactory();
    lexers.Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiLexerFactory();

    NSQLTranslationV1::TParsers parsers;
    parsers.Antlr4 = MakeIntrusive<TKqpSqlParserFactory>(NSQLTranslationV1::MakeAntlr4ParserFactory(isAmbiguityError), /*ansi=*/false);
    parsers.Antlr4Ansi = MakeIntrusive<TKqpSqlParserFactory>(NSQLTranslationV1::MakeAntlr4AnsiParserFactory(), /*ansi=*/true);
    return NSQLTranslationV1::MakeTranslator(lexers, parsers);
}

} // namespace NKikimr::NKqp
