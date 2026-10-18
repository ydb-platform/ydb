#include "parser.h"

#include "lexer.h"

#include <util/string/cast.h>

namespace NKikimr::NSql {

namespace {

TParseResult Unsupported(TStringBuf node) {
    TParseResult result;
    result.SqlState = "0A000";
    result.Message = TString(node.empty() ? TStringBuf("end of statement") : node);
    return result;
}

class TParser {
public:
    explicit TParser(TStringBuf sql)
        : Lexer(sql)
    {
        Advance();
    }

    TParseResult ParseSelect() {
        if (Current.Kind == ETokenKind::Error) {
            return Unsupported(Current.Text);
        }
        if (!IsKeyword("select")) {
            return Unsupported(Current.Text);
        }
        Advance();

        TSelectStmt select;
        if (Current.Kind == ETokenKind::Star) {
            select.Star = true;
            Advance();
        } else if (Current.Kind == ETokenKind::Identifier) {
            select.Columns.push_back(Identifier());
            Advance();
            while (Current.Kind == ETokenKind::Comma) {
                Advance();
                if (Current.Kind != ETokenKind::Identifier) {
                    return Unsupported(Current.Text);
                }
                select.Columns.push_back(Identifier());
                Advance();
            }
        } else {
            return Unsupported(Current.Text);
        }

        if (!IsKeyword("from")) {
            return Unsupported(Current.Text);
        }
        Advance();
        if (Current.Kind != ETokenKind::Identifier) {
            return Unsupported(Current.Text);
        }
        const TIdentifier first = Identifier();
        Advance();
        if (Current.Kind == ETokenKind::Dot) {
            Advance();
            if (Current.Kind != ETokenKind::Identifier) {
                return Unsupported(Current.Text);
            }
            select.Schema = first;
            select.From = Identifier();
            Advance();
        } else {
            select.From = first;
        }

        if (IsKeyword("where")) {
            Advance();
            if (auto error = ParseWhere(select)) {
                return *error;
            }
        }
        if (IsKeyword("limit")) {
            Advance();
            if (Current.Kind != ETokenKind::Number) {
                return Unsupported(Current.Text);
            }
            ui64 limit = 0;
            if (!TryFromString(Current.Text, limit)) {
                return Unsupported(Current.Text);
            }
            select.Limit = limit;
            Advance();
        }

        if (Current.Kind == ETokenKind::Semicolon) {
            Advance();
        }
        if (Current.Kind != ETokenKind::End) {
            return Unsupported(Current.Text);
        }

        TParseResult result;
        result.Select = std::move(select);
        return result;
    }

private:
    void Advance() {
        Current = Lexer.Next();
    }

    bool IsKeyword(TStringBuf keyword) const {
        return Current.Kind == ETokenKind::Identifier && !Current.Quoted && Current.Text == keyword;
    }

    TIdentifier Identifier() const {
        return {Current.Text, Current.Quoted};
    }

    std::optional<TParseResult> ParseWhere(TSelectStmt& select) {
        for (;;) {
            if (IsKeyword("or")) {
                return Unsupported(Current.Text);
            }
            if (Current.Kind != ETokenKind::Identifier) {
                return Unsupported(Current.Text);
            }
            TComparison comparison;
            comparison.Column = Identifier();
            Advance();
            if (Current.Kind != ETokenKind::Operator) {
                return Unsupported(Current.Text);
            }
            if (Current.Text == "=") {
                comparison.Op = ECompareOp::Eq;
            } else if (Current.Text == "<>" || Current.Text == "!=") {
                comparison.Op = ECompareOp::NotEq;
            } else if (Current.Text == "<") {
                comparison.Op = ECompareOp::Lt;
            } else if (Current.Text == ">") {
                comparison.Op = ECompareOp::Gt;
            } else if (Current.Text == "<=") {
                comparison.Op = ECompareOp::Le;
            } else if (Current.Text == ">=") {
                comparison.Op = ECompareOp::Ge;
            } else {
                return Unsupported(Current.Text);
            }
            Advance();
            if (Current.Kind == ETokenKind::Number) {
                comparison.Value = {TLiteral::EKind::Integer, Current.Text};
            } else if (Current.Kind == ETokenKind::String) {
                comparison.Value = {TLiteral::EKind::String, Current.Text};
            } else {
                return Unsupported(Current.Text);
            }
            select.Where.push_back(std::move(comparison));
            Advance();
            if (!IsKeyword("and")) {
                return std::nullopt;
            }
            Advance();
        }
    }

    TLexer Lexer;
    TToken Current;
};

} // namespace

TParseResult Parse(TStringBuf sql) {
    return TParser(sql).ParseSelect();
}

} // namespace NKikimr::NSql
