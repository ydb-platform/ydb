#include "parser.h"
#include "quoted.h"

#include <algorithm>
#include <array>
#include <cctype>
#include <deque>
#include <utility>

namespace NKikimr::NKqp::NFrontend {
namespace {

enum class EToken { End, Identifier, QuotedIdentifier, Number, String, Symbol };

enum EIdentifier { ExprIdentifier = 1, TableIdentifier = 2, AliasIdentifier = 4, GeneralIdentifier = 8 };
struct TKeyword { std::string_view Word; unsigned Identifiers; };
#include <ydb/core/kqp/opt/rbo/frontend/grammar_keywords.inc>

// Matching identities are lexical, not semantic. AS_TABLE is an ordinary
// function name; reserved words inherit their permissions from the grammar.
static constexpr auto MatchKeywords = [] {
    std::array<TKeyword, std::size(Keywords) + 1> result{};
    std::copy(std::begin(Keywords), std::end(Keywords), result.begin());
    result[std::size(Keywords)] = {"AS_TABLE", 15};
    std::sort(result.begin(), result.end(), [](const auto& a, const auto& b) { return a.Word < b.Word; });
    return result;
}();
static constexpr std::string_view SingleSymbols = "()[]{},.;:+-*/%=<>?!$&|^~";
static constexpr std::array<std::string_view, 11> SymbolPairs{"<=", ">=", "!=", "<>", "==", "||", "??", "::", "->", "<<", ">>"};
static constexpr std::uint16_t PairCode = 256, KeywordCode = 512;

struct TTokenMatch {
    std::uint16_t Code;
    consteval TTokenMatch(const char* spelling) : Code(0) {
        const std::string_view text(spelling);
        if (text.size() == 1 && SingleSymbols.find(text.front()) != SingleSymbols.npos) {
            Code = static_cast<unsigned char>(text.front());
            return;
        }
        for (std::size_t i = 0; i < SymbolPairs.size(); ++i) if (SymbolPairs[i] == text) {
            Code = PairCode + i;
            return;
        }
        for (std::size_t i = 0; i < MatchKeywords.size(); ++i) if (MatchKeywords[i].Word == text) {
            Code = KeywordCode + i;
            return;
        }
        throw "Parser match must have a registered lexical identity";
    }
    std::string_view Text() const {
        if (Code >= KeywordCode) return MatchKeywords[Code - KeywordCode].Word;
        if (Code >= PairCode) return SymbolPairs[Code - PairCode];
        return SingleSymbols.substr(SingleSymbols.find(static_cast<char>(Code)), 1);
    }
};

struct TToken {
    EToken Kind;
    std::string_view Text;
    TSourcePosition Position;
    std::size_t EndOffset = 0;
    // Canonical keyword spelling points into the generated static table.
    // Ordinary names need only their original text, not an uppercase copy.
    std::string_view Word = {};
    unsigned Identifiers = 15;
    char Quote = 0;
    std::uint16_t MatchCode = 0;
};

std::string Upper(std::string_view text) {
    std::string result(text);
    for (auto& ch : result) {
        if (ch >= 'a' && ch <= 'z') ch -= 'a' - 'A';
    }
    return result;
}

int CompareKeyword(std::string_view keyword, std::string_view text) {
    const auto size = std::min(keyword.size(), text.size());
    for (std::size_t i = 0; i < size; ++i) {
        const char ch = text[i] >= 'a' && text[i] <= 'z' ? text[i] - ('a' - 'A') : text[i];
        if (keyword[i] != ch) return keyword[i] < ch ? -1 : 1;
    }
    return (keyword.size() > text.size()) - (keyword.size() < text.size());
}

bool Alpha(char ch) {
    return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_';
}

bool Digit(char ch) { return ch >= '0' && ch <= '9'; }

bool SupportedNumber(std::string_view text) {
    const auto upper = Upper(text);
    std::size_t end = 0;
    if (upper.starts_with("0X") || upper.starts_with("0B") || upper.starts_with("0O")) {
        const auto digits = upper[1] == 'X' ? "0123456789ABCDEF" : upper[1] == 'B' ? "01" : "012345678";
        end = 2;
        while (end < upper.size() && std::string_view(digits).find(upper[end]) != std::string_view::npos) ++end;
        if (end == 2) return false;
    } else {
        while (end < upper.size() && Digit(upper[end])) ++end;
        bool real = false;
        if (end < upper.size() && upper[end] == '.') {
            real = true; ++end;
            while (end < upper.size() && Digit(upper[end])) ++end;
        }
        if (end < upper.size() && upper[end] == 'E') {
            real = true; ++end;
            if (end < upper.size() && (upper[end] == '+' || upper[end] == '-')) ++end;
            const auto start = end;
            while (end < upper.size() && Digit(upper[end])) ++end;
            if (end == start) return false;
        }
        if (real) return end == upper.size() || upper.substr(end) == "F";
    }
    if (end < upper.size() && (upper[end] == 'U' || upper[end] == 'P')) ++end;
    if (end < upper.size() && std::string_view("LSTIBN").find(upper[end]) != std::string_view::npos) ++end;
    return end == upper.size();
}

bool IsRealNumber(std::string_view text) {
    const bool radix = text.size() > 1 && text[0] == '0' &&
        (text[1] == 'x' || text[1] == 'X' || text[1] == 'b' || text[1] == 'B' || text[1] == 'o' || text[1] == 'O');
    return !radix && text.find_first_of(".eE") != text.npos;
}

class TLexer {
public:
    TLexer(std::string_view text, bool ansi, std::deque<std::string>& decoded)
        : Text(text), Ansi(ansi), Decoded(decoded) {}

    TToken Next() {
        SkipSpace();
        const auto pos = Position;
        const auto start = Position.Offset;
        if (start == Text.size()) return {EToken::End, {}, pos};
        const char ch = Peek();
        if (Alpha(ch)) {
            do { Advance(); } while (Alpha(Peek()) || Digit(Peek()));
            TToken token{EToken::Identifier, Text.substr(start, Position.Offset - start), pos};
            const auto it = std::lower_bound(MatchKeywords.begin(), MatchKeywords.end(), token.Text,
                [](const TKeyword& key, std::string_view word) { return CompareKeyword(key.Word, word) < 0; });
            if (it != MatchKeywords.end() && CompareKeyword(it->Word, token.Text) == 0) {
                token.Word = it->Word;
                token.Identifiers = it->Identifiers;
                token.MatchCode = KeywordCode + (it - MatchKeywords.begin());
            }
            return token;
        }
        if (Digit(ch)) {
            Advance();
            if (ch == '0' && (Peek() == 'x' || Peek() == 'X' || Peek() == 'o' || Peek() == 'O' || Peek() == 'b' || Peek() == 'B')) {
                Advance();
                while (Alpha(Peek()) || Digit(Peek())) Advance();
            } else {
                while (Digit(Peek())) Advance();
                if (Peek() == '.') { Advance(); while (Digit(Peek())) Advance(); }
                if (Peek() == 'e' || Peek() == 'E') {
                    Advance();
                    if (Peek() == '+' || Peek() == '-') Advance();
                    if (!Digit(Peek())) throw TFrontendError(Position, "expected exponent digits");
                    while (Digit(Peek())) Advance();
                }
                while (Alpha(Peek()) || Digit(Peek())) Advance();
            }
            const auto number = Text.substr(start, Position.Offset - start);
            if (!SupportedNumber(number)) throw TUnsupportedFeature(EFeature::Syntax, pos, "numeric token requires grammar parser");
            return {EToken::Number, number, pos};
        }
        if (ch == '\'' || ch == '"' || ch == '`') {
            const bool identifier = ch == '`' || (Ansi && ch == '"');
            Advance();
            std::string value;
            auto chunk = Position.Offset;
            bool closed = false;
            bool decoded = false;
            while (Position.Offset < Text.size()) {
                char current = Peek();
                Advance();
                if (current == ch) {
                    if ((Ansi || identifier) && Peek() == ch) {
                        decoded = true;
                        value.append(Text.substr(chunk, Position.Offset - chunk - 1));
                        Advance(); value += ch; chunk = Position.Offset; continue;
                    }
                    if (decoded) value.append(Text.substr(chunk, Position.Offset - chunk - 1));
                    closed = true;
                    break;
                }
                if (current == '\\' && (!Ansi || ch == '`')) {
                    decoded = true;
                    value.append(Text.substr(chunk, Position.Offset - chunk - 1));
                    if (Position.Offset == Text.size()) break;
                    const auto consumed = AppendQuotedEscape(Text.substr(Position.Offset), value, pos);
                    for (std::size_t i = 0; i < consumed; ++i) Advance();
                    chunk = Position.Offset;
                }
            }
            if (!closed) throw TFrontendError(pos, "unterminated quoted value");
            std::string_view content = Text.substr(start + 1, Position.Offset - start - 2);
            std::string_view suffix;
            if (!identifier && (Peek() == 'u' || Peek() == 'U' || Peek() == 's' || Peek() == 'S' ||
                    Peek() == 'j' || Peek() == 'J' || Peek() == 'y' || Peek() == 'Y') &&
                !Alpha(Peek(1)) && !Digit(Peek(1))) {
                suffix = Peek() == 'u' || Peek() == 'U' ? "U" : Peek() == 's' || Peek() == 'S' ? "S" :
                    Peek() == 'j' || Peek() == 'J' ? "J" : "Y";
                Advance();
            }
            if (!identifier && (Alpha(Peek()) || Peek() == '@'))
                throw TUnsupportedFeature(EFeature::Syntax, pos, "typed or adjacent string literal requires grammar parser");
            if (decoded) {
                Decoded.push_back(std::move(value));
                content = Decoded.back();
            }
            return {identifier ? EToken::QuotedIdentifier : EToken::String, content, pos, Position.Offset, suffix, 15, ch};
        }
        if (ch == '@' && Peek(1) == '@') {
            throw TUnsupportedFeature(EFeature::Syntax, pos, "multiline literal requires grammar parser");
        }
        for (std::size_t i = 0; i < SymbolPairs.size(); ++i) {
            if (Text.substr(start, 2) == SymbolPairs[i]) {
                Advance(); Advance();
                TToken token{EToken::Symbol, SymbolPairs[i], pos};
                token.MatchCode = PairCode + i;
                return token;
            }
        }
        if (SingleSymbols.find(ch) == SingleSymbols.npos) throw TFrontendError(pos, "unexpected character");
        Advance();
        TToken token{EToken::Symbol, Text.substr(start, 1), pos};
        token.MatchCode = static_cast<unsigned char>(ch);
        return token;
    }

private:
    char Peek(std::size_t ahead = 0) const {
        return Position.Offset + ahead < Text.size() ? Text[Position.Offset + ahead] : '\0';
    }
    void Advance() {
        if (Position.Offset == Text.size()) return;
        if (Text[Position.Offset++] == '\n') { ++Position.Line; Position.Column = 1; }
        else ++Position.Column;
    }
    void SkipSpace() {
        while (Position.Offset < Text.size()) {
            if (std::string_view(" \r\t\f\n").find(Peek()) != std::string_view::npos) { Advance(); continue; }
            if (Peek() == '-' && Peek(1) == '-') {
                while (Position.Offset < Text.size() && Peek() != '\n' && Peek() != '\r') Advance();
                continue;
            }
            if (Peek() == '/' && Peek(1) == '*') {
                const auto pos = Position;
                Advance(); Advance();
                std::size_t depth = 1;
                while (Position.Offset < Text.size() && depth) {
                    if (Ansi && Peek() == '/' && Peek(1) == '*') { Advance(); Advance(); ++depth; }
                    else if (Peek() == '*' && Peek(1) == '/') { Advance(); Advance(); --depth; }
                    else Advance();
                }
                if (depth) throw TFrontendError(pos, "unterminated comment");
                continue;
            }
            break;
        }
    }
    std::string_view Text;
    bool Ansi;
    std::deque<std::string>& Decoded;
    TSourcePosition Position;
};

class TParser {
public:
    TParser(std::string_view text, const TParseSettings& settings) : Settings(settings) {
        if (text.size() > settings.MaxQueryBytes) throw TResourceLimitError({}, "query size limit exceeded");
        // The grammar runtime validates UTF-8 and reports character positions.
        // Delegate non-ASCII input until the fast lexer has the same contract.
        for (unsigned char c : text) if (c >= 128)
            throw TUnsupportedFeature(EFeature::Syntax, {}, "non-ASCII input requires grammar parser");
        TLexer lexer(text, settings.AnsiQuotes, DecodedTokens);
        do {
            Tokens.push_back(lexer.Next());
            if ((Tokens.size() - 1) / 8 >= settings.MaxNodes) throw TResourceLimitError(Tokens.back().Position, "token limit exceeded");
        } while (Tokens.back().Kind != EToken::End);
    }

    TParsedQuery Run() {
        while (!End()) {
            if (Take(";")) continue;
            const auto position = Token().Position;
            auto explain = EExplainMode::None;
            if (Take("EXPLAIN")) {
                explain = EExplainMode::Explain;
                if (Take("QUERY")) { Expect("PLAN"); explain = EExplainMode::QueryPlan; }
            }
            auto statement = Statement();
            statement.Explain = explain;
            if (explain != EExplainMode::None) statement.Position = position;
            Result.Statements.push_back(std::move(statement));
            if (!End()) Expect(";");
        }
        return std::move(Result);
    }

    TParsedExpression RunExpression() {
        const auto root = Expression();
        if (!End()) Fail("expected end of expression");
        return {std::move(Result.Expressions), root, std::move(Result.NamePaths)};
    }

private:
    struct TDepth {
        TParser& Parser;
        explicit TDepth(TParser& parser) : Parser(parser) {
            if (++Parser.Depth > Parser.Settings.MaxDepth) throw TResourceLimitError(Parser.Token().Position, "nesting limit exceeded");
        }
        ~TDepth() { --Parser.Depth; }
    };

    const TToken& Token(std::size_t ahead = 0) const { return Tokens[std::min(Index + ahead, Tokens.size() - 1)]; }
    bool End() const { return Token().Kind == EToken::End; }
    [[gnu::always_inline]] inline bool Is(TTokenMatch text, std::size_t ahead = 0) const {
        return Token(ahead).MatchCode == text.Code;
    }
    [[gnu::always_inline]] inline bool Take(TTokenMatch text) { if (!Is(text)) return false; ++Index; return true; }
    void Expect(TTokenMatch text) { if (!Take(text)) Fail("expected " + std::string(text.Text())); }
    bool TakeTypeEnd() {
        if (Take(">")) return true;
        if (!Is(">>")) return false;
        Tokens[Index].Text = ">";
        Tokens[Index].MatchCode = TTokenMatch(">").Code;
        ++Tokens[Index].Position.Offset; ++Tokens[Index].Position.Column;
        return true;
    }
    void TypeEnd() { if (!TakeTypeEnd()) Fail("expected >"); }
    [[noreturn]] void Fail(std::string message) const { throw TFrontendError(Token().Position, std::move(message)); }
    std::string Identifier(unsigned category = GeneralIdentifier) {
        if (Token().Kind == EToken::String)
            throw TUnsupportedFeature(EFeature::Syntax, Token().Position, "string literals as identifiers are outside native-read-v1; use backticks");
        if (Token().Kind != EToken::Identifier && Token().Kind != EToken::QuotedIdentifier &&
            Token().Kind != EToken::String) Fail("expected identifier");
        if (Token().Kind == EToken::Identifier && !(Token().Identifiers & category)) Fail("keyword is not an identifier here");
        return std::string(Tokens[Index++].Text);
    }
    std::string Variable() { Expect("$"); return Identifier(); }
    TSyntaxId Add(ESyntaxKind kind, TSourcePosition pos, std::string text = {}, std::vector<TSyntaxId> args = {}) {
        if (Result.Expressions.size() >= Settings.MaxNodes || Result.Expressions.size() >= NoSyntax)
            throw TResourceLimitError(pos, "expression limit exceeded");
        const auto id = static_cast<TSyntaxId>(Result.Expressions.size());
        Result.Expressions.push_back({kind, 0, pos, std::move(text), std::move(args), {}, false});
        return id;
    }
    bool CanAlias() const {
        if (Token().Kind == EToken::QuotedIdentifier || Token().Kind == EToken::String) return true;
        if (Token().Kind != EToken::Identifier) return false;
        // These words can be aliases in isolation, but the grammar's adaptive
        // prediction gives a following clause precedence. An isolated alias
        // using one of them is handled by the generated parser.
        for (auto word : {"GROUP", "ORDER", "INNER", "LEFT", "RIGHT", "CROSS", "JOIN", "ON", "USING",
                          "NATURAL", "EXCLUSION", "OUTER", "ANY", "WINDOW", "WITH", "WITHOUT", "ASSUME",
                          "INTO", "SAMPLE", "TABLESAMPLE", "FLATTEN", "RETURNING"})
            if (Token().Word == word) return false;
        return Token().Identifiers & AliasIdentifier;
    }

    std::string TypeName() {
        TDepth depth(*this);
        std::string name = Identifier();
        if (Upper(name) == "OPTIONAL") {
            Expect("<"); name = TypeName(); TypeEnd(); name += '?';
        } else if (Upper(name) == "LIST") {
            Expect("<"); name = "List<" + TypeName(); TypeEnd(); name += '>';
        } else if (Upper(name) == "TUPLE") {
            Expect("<"); name = "Tuple<";
            if (!TakeTypeEnd()) {
                do {
                    name += TypeName();
                    if (TakeTypeEnd()) break;
                    Expect(",");
                    if (TakeTypeEnd()) break;
                    name += ',';
                } while (true);
            }
            name += '>';
        } else if (Upper(name) == "DECIMAL") {
            Expect("(");
            if (Token().Kind != EToken::Number) Fail("expected Decimal precision");
            name += "("; name += Tokens[Index++].Text;
            Expect(",");
            if (Token().Kind != EToken::Number) Fail("expected Decimal scale");
            name += ","; name += Tokens[Index++].Text; name += ")";
            Expect(")");
        }
        while (Is("?") || Is("??")) name += Tokens[Index++].Text;
        return name;
    }

    TPragmaValue PragmaArgument() {
        using K = EPragmaValueKind;
        const auto pos = Token().Position;
        if (Is("$")) return {K::Binding, "$" + Variable(), pos};
        if (Is("TRUE") || Is("FALSE")) return {K::Boolean, Upper(Tokens[Index++].Text), pos};
        if (Is("+") || Is("-") || Token().Kind == EToken::Number) {
            std::string sign;
            if (Is("+") || Is("-")) sign = Tokens[Index++].Text;
            if (Token().Kind != EToken::Number) Fail("expected numeric pragma value");
            const auto token = Tokens[Index++];
            return {IsRealNumber(token.Text) ? K::Real : K::Integer, sign + std::string(token.Text), pos};
        }
        if (Token().Kind == EToken::String || (Settings.AnsiQuotes && Token().Quote == '"')) {
            if (!Token().Word.empty())
                throw TUnsupportedFeature(EFeature::Syntax, pos, "typed pragma strings are outside the native profile");
            return {K::String, std::string(Tokens[Index++].Text), pos};
        }
        return {K::Identifier, Identifier(), pos};
    }

    TStatement Statement() {
        const auto pos = Token().Position;
        if (Take("DECLARE")) {
            TStatement result{EStatementKind::Declaration, pos};
            result.Name = Variable(); Expect("AS");
            if (Take("LIST")) {
                Expect("<");
                if (!Take("STRUCT")) {
                    result.ScalarList = true; result.Value = TypeName(); TypeEnd(); return result;
                }
                result.RowFields.emplace();
                if (!Take("<>")) {
                    Expect("<");
                    if (!TakeTypeEnd()) while (true) {
                        if (result.RowFields->size() >= Settings.MaxNodes) throw TResourceLimitError(pos, "parameter field limit exceeded");
                        // Struct tags are strings in the grammar as well as identifiers.
                        auto name = Token().Kind == EToken::String && Token().Word.empty() ? std::string(Tokens[Index++].Text) : Identifier();
                        Expect(":");
                        result.RowFields->emplace_back(std::move(name), TypeName());
                        if (TakeTypeEnd()) break;
                        Expect(","); if (TakeTypeEnd()) break;
                    }
                }
                TypeEnd();
            } else result.Value = TypeName();
            return result;
        }
        if (Take("USE")) {
            TStatement result{EStatementKind::Use, pos}; result.Name = Identifier(); return result;
        }
        if (Take("PRAGMA")) {
            const auto name = [&] {
                return Settings.AnsiQuotes && Token().Kind == EToken::String && Token().Word.empty() ? std::string(Tokens[Index++].Text) : Identifier();
            };
            TStatement result{EStatementKind::Pragma, pos}; result.Name = name();
            if (Take(".")) result.Name += "." + name();
            const bool parenthesized = Take("(");
            if (parenthesized || Take("=")) {
                result.PragmaForm = parenthesized ? EPragmaValueForm::List : EPragmaValueForm::Assignment;
                do {
                    result.PragmaValues.push_back(PragmaArgument());
                } while (parenthesized && Take(","));
                if (parenthesized) Expect(")");
            }
            return result;
        }
        if (Is("$")) {
            TStatement result{EStatementKind::Binding, pos}; result.Name = Variable(); Expect("=");
            if (Is("SELECT") || Is("WITH") || Is("FROM")) result.Query = Select();
            else if (Is("(") && (Is("SELECT", 1) || Is("WITH", 1) || Is("FROM", 1))) {
                Expect("("); result.Query = Select(); Expect(")");
            } else result.Expression = Expression();
            return result;
        }
        if (Is("INSERT") || Is("UPSERT") || Is("REPLACE") || Is("UPDATE") || Is("DELETE") || Is("BATCH")) {
            TStatement result{EStatementKind::Write, pos};
            result.Write = std::make_shared<TWriteStatement>(Write()); return result;
        }
        TStatement result{EStatementKind::Query, pos};
        result.Query = Select(); return result;
    }

    TWriteStatement Write() {
        TWriteStatement result{};
        result.Batch = Take("BATCH");
        const bool into = Is("INSERT") || Is("UPSERT") || Is("REPLACE");
        if (Take("INSERT")) {
            result.Kind = EWriteKind::Insert;
            if (Take("OR")) {
                if (Take("REVERT")) result.OnConflict = "revert";
                else if (!Take("ABORT"))
                    throw TUnsupportedFeature(EFeature::Statement, Token().Position, "INSERT OR IGNORE is not supported");
            }
        } else if (Take("UPSERT")) result.Kind = EWriteKind::Upsert;
        else if (Take("REPLACE")) result.Kind = EWriteKind::Replace;
        else if (Take("UPDATE")) result.Kind = EWriteKind::Update;
        else { Expect("DELETE"); Expect("FROM"); result.Kind = EWriteKind::Delete; }
        if (into) { if (result.Batch) Fail("BATCH requires UPDATE or DELETE"); Expect("INTO"); }
        result.Target.Position = Token().Position;
        result.Target.Name = Identifier();
        if (Take(".")) {
            result.Target.Cluster = std::move(result.Target.Name);
            if (result.Target.Cluster.empty()) throw TFrontendError(result.Target.Position, "empty cluster name");
            result.Target.Name = Identifier();
        }
        if (into || Take("ON")) {
            if (Take("(")) {
                do { result.Columns.push_back(Identifier()); } while (Take(","));
                Expect(")");
            }
            if (Take("VALUES")) {
                do {
                    Expect("(");
                    auto& row = result.Rows.emplace_back();
                    do { row.push_back(Expression()); } while (Take(","));
                    Expect(")");
                } while (Take(",") && Is("("));
            } else result.Input = Select();
        } else {
            if (result.Kind == EWriteKind::Update) {
                Expect("SET");
                do {
                    const auto name = Identifier(); Expect("=");
                    result.Assignments.push_back({Expression(), name});
                } while (Take(","));
            }
            if (Take("WHERE")) result.Where = Expression();
        }
        if (Take("RETURNING")) {
            result.ReturningAll = Take("*");
            if (!result.ReturningAll) do { result.Returning.push_back(Identifier()); } while (Take(","));
        }
        return result;
    }

    bool IsLambda() const {
        if (!Is("(")) return false;
        std::size_t ahead = 1;
        if (Is(")", ahead)) return Is("->", ahead + 1);
        while (Is("$", ahead)) {
            if (Token(ahead + 1).Kind != EToken::Identifier && Token(ahead + 1).Kind != EToken::QuotedIdentifier) return false;
            ahead += 2;
            if (Is(")", ahead)) return Is("->", ahead + 1);
            if (!Is(",", ahead)) return false;
            ++ahead;
        }
        return false;
    }

    std::vector<std::string> ColumnAliases() {
        std::vector<std::string> names;
        Expect("(");
        do { names.push_back(Identifier()); } while (Take(",") && !Is(")"));
        Expect(")");
        return names;
    }

    TSource Source() {
        TSource result; result.Position = Token().Position;
        result.Any = Take("ANY");
        if (Take("(")) { result.Query = Select(); Expect(")"); }
        else if (Is("AS_TABLE") && Is("(", 1)) {
            ++Index; Expect("("); result.Name = "$" + Variable(); Expect(")"); result.ParameterTable = true; result.NameIsBinding = true;
        }
        else {
            if (Take("$")) { result.Name = "$" + Identifier(); result.NameIsBinding = true; }
            else {
                result.Name = Identifier(TableIdentifier);
                if (Take(".")) {
                    result.Cluster = std::move(result.Name);
                    if (result.Cluster.empty()) throw TFrontendError(result.Position, "empty cluster name");
                    result.Name = Identifier(TableIdentifier);
                }
            }
            if (Take("VIEW")) {
                if (Is("PRIMARY") && Is("KEY", 1))
                    throw TUnsupportedFeature(EFeature::Backend, Token().Position, "VIEW PRIMARY KEY");
                result.Index = Identifier();
            }
        }
        if (Take("AS") || CanAlias()) {
            result.Alias = Identifier();
            if (Is("(")) result.ColumnAliases = ColumnAliases();
        }
        return result;
    }

    void From(TSelect& select) {
        select.Source = Source();
        while (true) {
            EJoinKind kind;
            if (Take(",")) kind = EJoinKind::Cross;
            else if (Take("CROSS")) { Expect("JOIN"); kind = EJoinKind::Cross; }
            else if (Take("JOIN")) kind = EJoinKind::Inner;
            else if (Take("INNER")) { Expect("JOIN"); kind = EJoinKind::Inner; }
            else if (Take("LEFT")) {
                kind = Take("SEMI") ? EJoinKind::LeftSemi : Take("ONLY") ? EJoinKind::LeftOnly : EJoinKind::Left;
                Take("OUTER"); Expect("JOIN");
            } else if (Take("RIGHT")) {
                kind = Take("SEMI") ? EJoinKind::RightSemi : Take("ONLY") ? EJoinKind::RightOnly : EJoinKind::Right;
                Take("OUTER"); Expect("JOIN");
            } else if (Take("FULL")) { Take("OUTER"); Expect("JOIN"); kind = EJoinKind::Full; }
            else if (Take("EXCLUSION")) { Expect("JOIN"); kind = EJoinKind::Exclusion; }
            else break;
            TJoin join{kind, Source()};
            if (Take("ON")) join.Predicate = Expression();
            else if (Take("USING")) {
                Expect("("); do { join.Using.push_back(Identifier()); } while (Take(",")); Expect(")");
            } else if (kind != EJoinKind::Cross) Fail("expected ON or USING for join");
            select.Joins.push_back(std::move(join));
        }
        if (select.Source->Any && select.Joins.empty())
            throw TUnsupportedFeature(EFeature::Backend, select.Source->Position, "ANY requires a JOIN");
    }

    void AppendSet(std::shared_ptr<TSelect>& left, ESetOperation op, std::shared_ptr<TSelect> right) {
        if (left->SetInputs.empty() || !left->OrderBy.empty() || left->Limit != NoSyntax) {
            auto set = std::make_shared<TSelect>(); set->Position = left->Position;
            set->Ctes = std::move(left->Ctes);
            set->AppendSetInput(std::move(left)); left = std::move(set);
        }
        left->AppendSetInput(std::move(right));
        left->SetOperations.push_back(op);
    }

    std::shared_ptr<TSelect> Intersect() {
        auto result = SelectCore();
        while (Take("INTERSECT")) {
            const bool all = Take("ALL"); if (!all) Take("DISTINCT");
            if (Is("WITH")) Fail("set-operation branch cannot start with WITH");
            AppendSet(result, all ? ESetOperation::IntersectAll : ESetOperation::Intersect, SelectCore());
        }
        return result;
    }

    std::shared_ptr<TSelect> Select() {
        TDepth depth(*this);
        auto result = Intersect();
        while (Is("UNION") || Is("EXCEPT")) {
            const bool unite = Take("UNION"); if (!unite) Expect("EXCEPT");
            const bool all = Take("ALL"); if (!all) Take("DISTINCT");
            if (Is("WITH")) Fail("set-operation branch cannot start with WITH");
            const auto op = unite ? (all ? ESetOperation::UnionAll : ESetOperation::Union) :
                (all ? ESetOperation::ExceptAll : ESetOperation::Except);
            AppendSet(result, op, Intersect());
        }
        if (Take("ORDER")) {
            Expect("BY");
            do {
                TSortKey key{Expression()};
                if (Take("DESC")) key.Descending = true; else Take("ASC");
                result->OrderBy.push_back(key);
            } while (Take(","));
        }
        if (Take("LIMIT")) {
            result->Limit = Expression();
            if (Take(",")) { result->Offset = result->Limit; result->Limit = Expression(); }
            else if (Take("OFFSET")) result->Offset = Expression();
        }
        return result;
    }

    std::shared_ptr<TSelect> SelectCore() {
        if (Take("(")) {
            // SQL v1's parenthesized set operand is one SELECT kind. Nested
            // compounds and WITH belong in a derived table, not this form.
            if (!Is("SELECT") && !Is("FROM")) Fail("expected SELECT in parenthesized set operand");
            auto result = Select();
            if (!result->SetInputs.empty()) Fail("compound query is not a parenthesized set operand");
            Expect(")");
            return result;
        }
        auto result = std::make_shared<TSelect>(); result->Position = Token().Position;
        if (Take("WITH")) {
            do {
                auto name = Identifier(TableIdentifier);
                auto columns = Is("(") ? ColumnAliases() : std::vector<std::string>{};
                Expect("AS"); Expect("(");
                auto query = Select(); Expect(")");
                result->Ctes.push_back({std::move(name), std::move(columns), std::move(query)});
            } while (Take(",") && !Is("SELECT") && !Is("FROM"));
        }
        if (Take("FROM")) From(*result);
        Expect("SELECT");
        if (Take("DISTINCT")) result->Distinct = true; else Take("ALL");
        do {
            TNamedExpr item;
            if (Take("*")) item.Expression = Add(ESyntaxKind::Star, Tokens[Index - 1].Position);
            else if (Is(".", 1) && Is("*", 2)) {
                const auto position = Token().Position;
                auto qualifier = Identifier(); Expect("."); Expect("*");
                item.Expression = Add(ESyntaxKind::Star, position, std::move(qualifier));
            } else item.Expression = Expression();
            if (Result.Expressions[item.Expression].Kind == ESyntaxKind::Star) {
                if (Is("AS") || CanAlias()) Fail("star cannot have an alias");
            } else if (Take("AS") || CanAlias()) item.Alias = Identifier();
            result->Projection.push_back(std::move(item));
        } while (Take(",") && !Is("FROM") && !Is("WHERE") && !Is("GROUP") && !Is("HAVING") &&
            !Is("ORDER") && !Is("LIMIT") && !Is("UNION") && !End() && !Is(";") && !Is(")"));
        if (Take("FROM")) {
            if (result->Source) Fail("duplicate FROM clause");
            From(*result);
        }
        if (Take("WHERE")) result->Where = Expression();
        if (Take("GROUP")) {
            Expect("BY");
            bool composite = false;
            std::vector<TGroupingSets> elements;
            do {
                elements.push_back(GroupingElement(*result, composite));
            } while (Take(","));
            if (composite) result->GroupingSets = ExpandGroupingSets(elements, EGroupingOperation::Product, Settings.MaxNodes, result->Position);
        }
        if (Take("HAVING")) result->Having = Expression();
        return result;
    }

    TGroupingSets GroupingElement(TSelect& query, bool& composite, bool ordinary = false) {
        TDepth depth(*this);
        const auto pos = Token().Position;
        if (!ordinary && (Is("ROLLUP") || Is("CUBE") || (Is("GROUPING") && Is("SETS", 1)))) {
            composite = true;
            const auto op = Take("ROLLUP") ? EGroupingOperation::Rollup : Take("CUBE") ? EGroupingOperation::Cube : EGroupingOperation::Sets;
            if (op == EGroupingOperation::Sets) { Expect("GROUPING"); Expect("SETS"); }
            Expect("(");
            std::vector<TGroupingSets> elements;
            do {
                elements.push_back(GroupingElement(query, composite, op != EGroupingOperation::Sets));
                if (!Take(",")) break;
                if (op == EGroupingOperation::Sets && Is(")")) break;
            } while (true);
            Expect(")");
            return ExpandGroupingSets(elements, op, Settings.MaxNodes, pos);
        }
        // Parenthesized lists are grouping bundles, while (a + b) remains an
        // ordinary scalar expression and may carry an alias.
        if (Is("(")) {
            unsigned nesting = 0;
            bool tuple = Is(")", 1);
            for (std::size_t i = Index; i < Tokens.size(); ++i) {
                if (Tokens[i].Kind != EToken::Symbol) continue;
                if (Tokens[i].Text == "(") ++nesting;
                else if (Tokens[i].Text == ")" && !--nesting) break;
                else if (nesting == 1 && Tokens[i].Text == ",") tuple = true;
            }
            if (tuple) {
                composite = true;
                Expect("(");
                std::vector<TGroupingSets> elements;
                if (!Take(")")) {
                    do { elements.push_back(GroupingElement(query, composite, true)); } while (Take(",") && !Is(")"));
                    Expect(")");
                }
                return ExpandGroupingSets(elements, EGroupingOperation::Product, Settings.MaxNodes, pos);
            }
        }
        TNamedExpr key{Expression()}; if (Take("AS")) key.Alias = Identifier();
        const auto index = static_cast<std::uint32_t>(query.GroupBy.size());
        query.GroupBy.push_back(std::move(key));
        return {{index}};
    }

    // YQL precedence, from SQLv1Antlr4.g.in: OR, AND, XOR, equality /
    // predicates, ordering comparisons, bit ops / ??, +/-, */%, ||, unary.
    int Precedence() const {
        const auto& token = Token();
        if (token.Kind == EToken::Symbol) {
            // Delimiters end expressions immediately. In particular, a wide
            // projection should not compare every comma against every keyword.
            const auto& op = token.Text;
            switch (op[0]) {
                case '=': return 4;
                case '!': return op == "!=" ? 4 : 0;
                case '<': return op == "<>" ? 4 : op == "<<" ? 6 : 5;
                case '>': return op == ">>" ? 6 : 5;
                case '?': return op == "??" ? 6 : 0;
                case '&': case '^': return 6;
                case '|': return op == "||" ? 9 : 6;
                case '+': case '-': return 7;
                case '*': case '/': case '%': return 8;
                default: return 0;
            }
        }
        if (token.Kind != EToken::Identifier) return 0;
        const auto& word = token.Word;
        if (word == "OR") return 1;
        if (word == "AND") return 2;
        if (word == "XOR") return 3;
        if (word == "IS" || word == "ISNULL" || word == "NOTNULL" || word == "IN" ||
            word == "BETWEEN" || word == "LIKE" || word == "ILIKE" ||
            (word == "NOT" && (Is("IN", 1) || Is("BETWEEN", 1) || Is("LIKE", 1) || Is("ILIKE", 1)))) return 4;
        return 0;
    }

    TSyntaxId Expression(int minimum = 1) {
        TDepth depth(*this);
        auto left = Primary();
        while (Is("[") || Is("(") || Is(".")) {
            const auto position = Token().Position;
            if (Take(".")) {
                auto name = Identifier();
                auto& value = Result.Expressions[left];
                if (value.Kind == ESyntaxKind::Column)
                    Result.AppendNamePart(value.Text, value.NamePath, name, position);
                else left = Add(ESyntaxKind::Member, position, std::move(name), {left});
            } else if (Take("[")) {
                const auto index = Expression(); Expect("]");
                left = Add(ESyntaxKind::Index, position, {}, {left, index});
            } else {
                Expect("(");
                std::vector<TSyntaxId> args{left};
                if (!Take(")")) {
                    do { args.push_back(CallArgument()); } while (Take(",") && !Is(")"));
                    Expect(")");
                }
                const auto& callee = Result.Expressions[left];
                if (callee.Kind == ESyntaxKind::Column || callee.Kind == ESyntaxKind::Variable) {
                    auto name = callee.Kind == ESyntaxKind::Variable ? "$" + callee.Text : callee.Text;
                    args.erase(args.begin());
                    left = Add(ESyntaxKind::Call, position, std::move(name), std::move(args));
                } else left = Add(ESyntaxKind::Invoke, position, {}, std::move(args));
            }
        }
        bool predicate = false, ordering = false;
        while (true) {
            const int precedence = Precedence();
            if (precedence < minimum) break;
            const auto pos = Token().Position;
            if ((precedence == 4 && std::exchange(predicate, true)) ||
                (precedence == 5 && std::exchange(ordering, true)))
                throw TUnsupportedFeature(EFeature::Expression, pos, "chained comparison requires grammar parser");
            auto op = Upper(Tokens[Index++].Text);
            bool negate = false;
            if (op == "NOT") { negate = true; op = Upper(Tokens[Index++].Text); }
            if (op == "IS" || op == "ISNULL" || op == "NOTNULL") {
                const bool notNull = op == "NOTNULL" || (op == "IS" && Take("NOT"));
                if (op == "IS" && Take("DISTINCT")) {
                    Expect("FROM");
                    left = Add(ESyntaxKind::Binary, pos, notNull ? "IS NOT DISTINCT FROM" : "IS DISTINCT FROM",
                        {left, Expression(precedence + 1)});
                } else {
                    if (op == "IS") Expect("NULL");
                    left = Add(ESyntaxKind::Unary, pos, notNull ? "IS NOT NULL" : "IS NULL", {left});
                }
            } else if (op == "BETWEEN") {
                const bool symmetric = Take("SYMMETRIC");
                if (!symmetric) Take("ASYMMETRIC");
                auto low = Expression(precedence + 1); Expect("AND"); auto high = Expression(precedence + 1);
                left = Add(ESyntaxKind::Between, pos, symmetric ? (negate ? "NOT BETWEEN SYMMETRIC" : "BETWEEN SYMMETRIC")
                    : (negate ? "NOT BETWEEN" : "BETWEEN"), {left, low, high});
                negate = false;
            } else if (op == "IN") {
                // The native scalar backend owns membership evaluation; this
                // legacy execution hint does not change its value semantics.
                Take("COMPACT");
                if (!Is("[") && !Is("(")) {
                    if (!Is("$")) throw TUnsupportedFeature(EFeature::Syntax, pos, "IN expression requires the grammar parser");
                    const auto collection = Expression(precedence + 1);
                    left = Add(ESyntaxKind::In, pos, "collection", {left, collection});
                    if (negate) left = Add(ESyntaxKind::Unary, pos, "NOT", {left});
                    continue;
                }
                const bool list = Take("[");
                if (!list) Expect("(");
                const auto end = list ? TTokenMatch("]") : TTokenMatch(")");
                if (!list && (Is("SELECT") || Is("WITH") || Is("FROM"))) {
                    auto query = Select(); Expect(")");
                    auto id = Add(ESyntaxKind::In, pos, {}, {left}); Result.Expressions[id].Query = std::move(query); left = id;
                } else {
                    std::vector<TSyntaxId> args{left};
                    bool comma = false;
                    if (list && Take(",")) Expect(end);
                    else if (Take(end)) {}
                    else {
                        do {
                            args.push_back(Expression());
                            if (!Take(",")) break;
                            comma = true;
                        } while (!Is(end));
                        Expect(end);
                    }
                    if (list && Precedence() > precedence) Fail("unexpected operator after IN list");
                    // Parentheses around a query do not turn its rows into a
                    // scalar singleton. A comma explicitly constructs a tuple.
                    std::shared_ptr<TSelect> query;
                    if (!list && !comma && args.size() == 2 && Result.Expressions[args[1]].Kind == ESyntaxKind::ScalarSubquery) {
                        query = Result.Expressions[args[1]].Query;
                        args.resize(1);
                    }
                    left = Add(ESyntaxKind::In, pos, list ? "list" : "", std::move(args));
                    Result.Expressions[left].Query = std::move(query);
                }
            } else {
                auto right = Expression(op == "??" ? precedence : precedence + 1);
                std::vector<TSyntaxId> args{left, right};
                if ((op == "LIKE" || op == "ILIKE") && Take("ESCAPE")) args.push_back(Expression(precedence + 1));
                left = Add(ESyntaxKind::Binary, pos, std::move(op), std::move(args));
            }
            if (negate) left = Add(ESyntaxKind::Unary, pos, "NOT", {left});
        }
        return left;
    }

    std::shared_ptr<TSyntaxWindow> Window() {
        TDepth depth(*this);
        if (!Is("(")) throw TUnsupportedFeature(EFeature::Window, Token().Position, "named window requires the grammar parser");
        const auto position = Token().Position;
        Expect("(");
        auto result = std::make_shared<TSyntaxWindow>();
        result->Position = position;
        if (Take("PARTITION")) {
            Expect("BY");
            do { result->Partition.push_back({Expression()}); } while (Take(","));
        }
        if (Take("ORDER")) {
            Expect("BY");
            do {
                TSortKey key{Expression()};
                if (Take("DESC")) key.Descending = true; else Take("ASC");
                result->Order.push_back(key);
            } while (Take(","));
        }
        if (Is("ROWS") || Is("RANGE") || Is("GROUPS")) {
            result->Frame.Type = Take("ROWS") ? EWindowFrame::Rows : Take("RANGE") ? EWindowFrame::Range : EWindowFrame::Groups;
            if (result->Frame.Type == EWindowFrame::Groups) Expect("GROUPS");
            const auto bound = [&](EWindowBound& kind, TSyntaxId& offset) {
                if (Take("CURRENT")) { Expect("ROW"); kind = EWindowBound::CurrentRow; return; }
                const bool unbounded = Take("UNBOUNDED");
                if (!unbounded) offset = Expression();
                const bool preceding = Take("PRECEDING");
                if (!preceding) Expect("FOLLOWING");
                kind = unbounded ? (preceding ? EWindowBound::UnboundedPreceding : EWindowBound::UnboundedFollowing) :
                    (preceding ? EWindowBound::Preceding : EWindowBound::Following);
            };
            if (Take("BETWEEN")) {
                result->ExplicitFrameEnd = true;
                bound(result->Frame.Begin, result->BeginOffset);
                Expect("AND");
                bound(result->Frame.End, result->EndOffset);
            } else bound(result->Frame.Begin, result->BeginOffset);
        }
        Expect(")");
        return result;
    }

    TSyntaxId CallArgument() {
        const auto value = Expression();
        if (!Take("AS")) return value;
        const auto position = Token().Position;
        return Add(ESyntaxKind::Named, position, Identifier(), {value});
    }

    TSyntaxId Primary() {
        const auto& token = Token();
        const auto pos = token.Position;
        if (IsLambda()) {
            auto lambda = std::make_shared<TSyntaxLambda>();
            Expect("(");
            if (!Take(")")) { do { lambda->Parameters.push_back(Variable()); } while (Take(",")); Expect(")"); }
            Expect("->");
            std::vector<TSyntaxId> operands;
            if (Take("{")) {
                while (Take(";")) {}
                while (Is("$")) {
                    auto local = Statement();
                    lambda->Locals.push_back(std::move(local.Name));
                    if (local.Query) {
                        local.Expression = Add(ESyntaxKind::ScalarSubquery, local.Position);
                        Result.Expressions[local.Expression].Query = std::move(local.Query);
                    }
                    operands.push_back(local.Expression);
                    Expect(";"); while (Take(";")) {}
                }
                Expect("RETURN"); operands.push_back(Expression());
                while (Take(";")) {}
                Expect("}");
            } else {
                Expect("("); operands.push_back(Expression()); Expect(")");
            }
            const auto id = Add(ESyntaxKind::Lambda, pos, {}, std::move(operands));
            Result.Expressions[id].Lambda = std::move(lambda);
            return id;
        }
        if (Take("+") || Take("-") || Take("NOT") || Take("~")) {
            if (Is("+") || Is("-") || Is("NOT") || Is("~")) Fail("multiple unary operators require parentheses");
            return Add(ESyntaxKind::Unary, pos, Upper(token.Text), {Expression(10)});
        }
        if (Take("NULL")) return Add(ESyntaxKind::Null, pos);
        if (Take("TRUE")) return Add(ESyntaxKind::Boolean, pos, "true");
        if (Take("FALSE")) return Add(ESyntaxKind::Boolean, pos, "false");
        if (token.Kind == EToken::Number) {
            ++Index;
            return Add(IsRealNumber(token.Text) ? ESyntaxKind::Real : ESyntaxKind::Integer, pos, std::string(token.Text));
        }
        if (token.Kind == EToken::String) {
            ++Index;
            return Add(token.Word == "U" ? ESyntaxKind::Utf8String : token.Word == "S" ? ESyntaxKind::ByteString :
                token.Word == "J" ? ESyntaxKind::JsonString : token.Word == "Y" ? ESyntaxKind::YsonString : ESyntaxKind::String,
                pos, std::string(token.Text));
        }
        if (Is("CAST") || Is("BITCAST")) {
            const auto kind = Take("CAST") ? ESyntaxKind::Cast : ESyntaxKind::BitCast;
            if (kind == ESyntaxKind::BitCast) ++Index;
            Expect("("); auto arg = Expression(); Expect("AS"); auto type = TypeName(); Expect(")");
            return Add(kind, pos, std::move(type), {arg});
        }
        if (Is("JSON_VALUE") && Is("(", 1)) {
            ++Index; Expect("("); auto input = Expression(); Expect(",");
            if (Token().Kind != EToken::String) Fail("JSON_VALUE requires a literal path");
            if (!Token().Word.empty())
                throw TUnsupportedFeature(EFeature::Syntax, Token().Position, "typed JSON paths are outside the native profile");
            auto path = std::string(Tokens[Index++].Text);
            // Extended clauses go through the generated grammar and receive
            // an explicit Unsupported result; they are not ordinary call args.
            Expect(")");
            return Add(ESyntaxKind::JsonValue, pos, std::move(path), {input});
        }
        if (Take("CASE")) {
            TSyntaxId value = NoSyntax;
            if (!Is("WHEN")) value = Expression();
            std::vector<TSyntaxId> args;
            if (!Is("WHEN")) Fail("expected WHEN");
            while (Take("WHEN")) {
                auto condition = Expression();
                if (value != NoSyntax) condition = Add(ESyntaxKind::Binary, pos, "==", {value, condition});
                Expect("THEN"); args.push_back(condition); args.push_back(Expression());
            }
            const bool hasElse = Take("ELSE");
            args.push_back(hasElse ? Expression() : Add(ESyntaxKind::Null, pos)); Expect("END");
            return Add(ESyntaxKind::Case, pos, hasElse ? "" : "implicit_else", std::move(args));
        }
        if (Take("EXISTS")) {
            Expect("("); auto query = Select(); Expect(")"); auto id = Add(ESyntaxKind::Exists, pos);
            Result.Expressions[id].Query = std::move(query); return id;
        }
        if (Take("[")) {
            std::vector<TSyntaxId> items;
            if (Take(",")) Expect("]");
            else if (!Take("]")) {
                do { items.push_back(Expression()); } while (Take(",") && !Is("]"));
                Expect("]");
            }
            return Add(ESyntaxKind::Call, pos, "AsList", std::move(items));
        }
        if (Take("(")) {
            if (Is("SELECT") || Is("WITH") || Is("FROM")) {
                auto query = Select(); Expect(")"); auto id = Add(ESyntaxKind::ScalarSubquery, pos);
                Result.Expressions[id].Query = std::move(query); return id;
            }
            auto result = Expression(); Expect(")"); return result;
        }
        const bool variable = Take("$");
        std::string name = Identifier(variable || Is("::", 1) ? GeneralIdentifier : ExprIdentifier);
        std::uint32_t path = 0;
        if (!variable) {
            if (Take("::")) name += "::" + Identifier();
            while (Take(".")) {
                Result.AppendNamePart(name, path, Identifier(), pos);
            }
        }
        if (!Take("(")) {
            const auto id = Add(variable ? ESyntaxKind::Variable : ESyntaxKind::Column, pos, std::move(name));
            Result.Expressions[id].NamePath = path;
            return id;
        }
        const bool distinct = Take("DISTINCT");
        const bool quantified = distinct || Take("ALL");
        std::vector<TSyntaxId> args;
        if (Take("*")) {
            if (quantified) Fail("invalid star argument");
            args.push_back(Add(ESyntaxKind::Star, Tokens[Index - 1].Position)); Expect(")");
        } else if (!Take(")")) { do { args.push_back(CallArgument()); } while (Take(",") && !Is(")")); Expect(")"); }
        if (quantified && args.empty()) Fail("set quantifier requires an argument");
        for (auto arg : args) if (Result.Expressions[arg].Kind == ESyntaxKind::Star &&
            (quantified || args.size() != 1 || !Result.Expressions[arg].Text.empty())) Fail("invalid star argument");
        TSyntaxId filter = NoSyntax;
        if (Take("FILTER")) {
            Expect("("); Expect("WHERE"); filter = Expression(); Expect(")");
        }
        // Parse modifier children before their parent: syntax IDs are topological.
        auto window = Take("OVER") ? Window() : nullptr;
        auto id = Add(ESyntaxKind::Call, pos, variable ? "$" + name : name, std::move(args));
        Result.Expressions[id].Distinct = distinct;
        Result.Expressions[id].Filter = filter;
        Result.Expressions[id].Window = std::move(window);
        return id;
    }

    const TParseSettings& Settings;
    // Tokens borrow ordinary lexemes from the SQL input. Decoded quoted text
    // stays stable here; the returned syntax owns every retained string.
    std::deque<std::string> DecodedTokens;
    std::vector<TToken> Tokens;
    std::size_t Index = 0;
    std::size_t Depth = 0;
    TParsedQuery Result;
};

} // namespace

TGroupingSets ExpandGroupingSets(const std::vector<TGroupingSets>& elements,
    EGroupingOperation operation, std::size_t limit, TSourcePosition position) {
    auto check = [&](const TGroupingSets& sets) {
        std::size_t size = sets.size();
        if (size > limit) throw TResourceLimitError(position, "grouping set expansion limit exceeded");
        for (const auto& keys : sets) {
            if (keys.size() > limit - size) throw TResourceLimitError(position, "grouping set expansion limit exceeded");
            size += keys.size();
        }
    };
    auto product = [&](const TGroupingSets& left, const TGroupingSets& right) {
        TGroupingSets result;
        if (!right.empty() && left.size() > limit / right.size())
            throw TResourceLimitError(position, "grouping set expansion limit exceeded");
        std::size_t size = left.size() * right.size();
        for (const auto& a : left) for (const auto& b : right) {
            if (a.size() > limit - size || b.size() > limit - size - a.size())
                throw TResourceLimitError(position, "grouping set expansion limit exceeded");
            size += a.size() + b.size();
        }
        result.reserve(left.size() * right.size());
        for (const auto& a : left) for (const auto& b : right) {
            auto& keys = result.emplace_back(a); keys.insert(keys.end(), b.begin(), b.end());
        }
        return result;
    };
    TGroupingSets result = operation == EGroupingOperation::Sets ? TGroupingSets{} : TGroupingSets{{}};
    TGroupingSets prefix{{}};
    for (const auto& element : elements) {
        check(element);
        if (operation == EGroupingOperation::Product) result = product(result, element);
        else if (operation == EGroupingOperation::Sets) result.insert(result.end(), element.begin(), element.end());
        else if (operation == EGroupingOperation::Rollup) {
            prefix = product(prefix, element);
            result.insert(result.end(), prefix.begin(), prefix.end());
        } else {
            auto extra = product(result, element);
            result.insert(result.end(), std::make_move_iterator(extra.begin()), std::make_move_iterator(extra.end()));
        }
        check(result);
    }
    // Preserve the existing SQL-to-RBO branch order: ROLLUP starts with
    // the grand total, and CUBE appends each additional subset in key order.
    // Explicit GROUPING SETS and products keep their written order.
    return result;
}

TParsedQuery Parse(std::string_view sql, const TParseSettings& settings) {
    return TParser(sql, settings).Run();
}

void TParsedQuery::AppendNamePart(std::string& name, std::uint32_t& path,
    std::string_view part, TSourcePosition position) {
    if (name.size() >= NoSyntax || NamePaths.size() >= NoSyntax - 2)
        throw TResourceLimitError(position, "qualified name limit exceeded");
    if (!path) {
        path = static_cast<std::uint32_t>(NamePaths.size() + 1);
        NamePaths.push_back(0);
    } else if (path + NamePaths[path - 1] != NamePaths.size()) {
        // Parenthesized names can resume a path after another expression.
        const auto previous = path;
        const auto count = NamePaths[previous - 1];
        if (count >= NoSyntax - NamePaths.size() - 2)
            throw TResourceLimitError(position, "qualified name limit exceeded");
        path = static_cast<std::uint32_t>(NamePaths.size() + 1);
        NamePaths.push_back(count);
        for (std::uint32_t i = 0; i < count; ++i) NamePaths.push_back(NamePaths[previous + i]);
    }
    ++NamePaths[path - 1];
    NamePaths.push_back(static_cast<std::uint32_t>(name.size()));
    name += '.';
    name += part;
}

std::span<const std::uint32_t> TParsedQuery::NameSeparators(const TSyntaxExpr& expression) const {
    if (!expression.NamePath) return {};
    const auto offset = expression.NamePath;
    return std::span(NamePaths).subspan(offset, NamePaths.at(offset - 1));
}

TParsedExpression ParseExpression(std::string_view sql, const TParseSettings& settings) {
    return TParser(sql, settings).RunExpression();
}

} // namespace NKikimr::NKqp::NFrontend
