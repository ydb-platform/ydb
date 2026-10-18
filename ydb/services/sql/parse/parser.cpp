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

    TParseResult ParseStatement() {
        if (Current.Kind == ETokenKind::Error) {
            return Unsupported(Current.Text);
        }
        if (IsKeyword("select")) {
            return ParseSelect();
        }
        if (IsKeyword("create")) {
            return ParseCreate();
        }
        return Unsupported(Current.Text);
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

    TParseResult ParseCreate() {
        Advance();
        if (!IsKeyword("table")) {
            return Unsupported(Current.Text);
        }
        Advance();

        TCreateTableStmt create;
        if (IsKeyword("if")) {
            Advance();
            if (!IsKeyword("not") ) {
                return Unsupported(Current.Text);
            }
            Advance();
            if (!IsKeyword("exists")) {
                return Unsupported(Current.Text);
            }
            Advance();
            create.IfNotExists = true;
        }
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
            create.Schema = first;
            create.Name = Identifier();
            Advance();
        } else {
            create.Name = first;
        }
        if (Current.Kind != ETokenKind::LeftParen) {
            return Unsupported(Current.Text);
        }
        Advance();
        if (Current.Kind == ETokenKind::RightParen) {
            return Unsupported(Current.Text);
        }

        TVector<TIdentifier> inlineKey;
        while (Current.Kind != ETokenKind::RightParen) {
            if (IsKeyword("unique")) {
                TVector<TIdentifier> columns;
                if (auto error = ParseColumnList(columns)) {
                    return *error;
                }
                create.Unique.push_back(std::move(columns));
            } else if (IsKeyword("primary")) {
                if (!create.PrimaryKey.empty() || !inlineKey.empty()) {
                    return Unsupported("primary key");
                }
                if (auto error = ParsePrimaryKey(create.PrimaryKey)) {
                    return *error;
                }
            } else if (IsTableConstraint()) {
                return Unsupported(Current.Text);
            } else if (Current.Kind == ETokenKind::Identifier) {
                TColumnDef column;
                bool inlinePrimary = false;
                if (auto error = ParseColumn(column, inlinePrimary)) {
                    return *error;
                }
                if (inlinePrimary) {
                    if (!create.PrimaryKey.empty() || !inlineKey.empty()) {
                        return Unsupported("primary key");
                    }
                    inlineKey.push_back(column.Name);
                }
                create.Columns.push_back(std::move(column));
            } else {
                return Unsupported(Current.Text);
            }
            if (Current.Kind == ETokenKind::Comma) {
                Advance();
                continue;
            }
            if (Current.Kind != ETokenKind::RightParen) {
                return Unsupported(Current.Text);
            }
        }
        Advance();
        if (!inlineKey.empty()) {
            create.PrimaryKey = std::move(inlineKey);
        }
        if (create.PrimaryKey.empty()) {
            return Unsupported("primary key");
        }
        if (auto error = Finish()) {
            return *error;
        }
        for (const TIdentifier& key : create.PrimaryKey) {
            for (TColumnDef& column : create.Columns) {
                if (column.Name.Name == key.Name) {
                    column.NotNull = true;
                }
            }
        }

        TParseResult result;
        result.CreateTable = std::move(create);
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

    bool IsTableConstraint() const {
        return IsKeyword("primary") || IsKeyword("unique") || IsKeyword("check")
            || IsKeyword("constraint") || IsKeyword("foreign");
    }

    bool IsColumnConstraint() const {
        return IsKeyword("null") || IsKeyword("not") || IsKeyword("default") || IsKeyword("primary")
            || IsKeyword("unique") || IsKeyword("check") || IsKeyword("references")
            || IsKeyword("collate") || IsKeyword("constraint");
    }

    std::optional<TParseResult> Finish() {
        if (Current.Kind == ETokenKind::Semicolon) {
            Advance();
        }
        if (Current.Kind != ETokenKind::End) {
            return Unsupported(Current.Text);
        }
        return std::nullopt;
    }

    std::optional<TParseResult> ParseColumnList(TVector<TIdentifier>& columns) {
        Advance();
        if (Current.Kind != ETokenKind::LeftParen) {
            return Unsupported(Current.Text);
        }
        Advance();
        if (Current.Kind != ETokenKind::Identifier) {
            return Unsupported(Current.Text);
        }
        columns.push_back(Identifier());
        Advance();
        while (Current.Kind == ETokenKind::Comma) {
            Advance();
            if (Current.Kind != ETokenKind::Identifier) {
                return Unsupported(Current.Text);
            }
            columns.push_back(Identifier());
            Advance();
        }
        if (Current.Kind != ETokenKind::RightParen) {
            return Unsupported(Current.Text);
        }
        Advance();
        return std::nullopt;
    }

    std::optional<TParseResult> ParsePrimaryKey(TVector<TIdentifier>& key) {
        Advance();
        if (!IsKeyword("key")) {
            return Unsupported(Current.Text);
        }
        if (auto error = ParseColumnList(key)) {
            return error;
        }
        return std::nullopt;
    }

    std::optional<TParseResult> ParseColumn(TColumnDef& column, bool& inlinePrimary) {
        column.Name = Identifier();
        Advance();
        if (auto error = ParseType(column.Type)) {
            return error;
        }
        while (IsColumnConstraint()) {
            if (IsKeyword("null")) {
                column.NotNull = false;
                Advance();
                continue;
            }
            if (IsKeyword("not")) {
                Advance();
                if (!IsKeyword("null")) {
                    return Unsupported(Current.Text);
                }
                column.NotNull = true;
                Advance();
                continue;
            }
            if (IsKeyword("default")) {
                Advance();
                TLiteral literal;
                if (Current.Kind == ETokenKind::Number) {
                    literal = {TLiteral::EKind::Integer, Current.Text};
                } else if (Current.Kind == ETokenKind::String) {
                    literal = {TLiteral::EKind::String, Current.Text};
                } else {
                    return Unsupported(Current.Text);
                }
                column.Default = std::move(literal);
                Advance();
                continue;
            }
            if (IsKeyword("primary")) {
                Advance();
                if (!IsKeyword("key")) {
                    return Unsupported(Current.Text);
                }
                Advance();
                inlinePrimary = true;
                continue;
            }
            return Unsupported(Current.Text);
        }
        return std::nullopt;
    }

    std::optional<TParseResult> ParseType(TPgType& type) {
        if (Current.Kind != ETokenKind::Identifier) {
            return Unsupported(Current.Text);
        }
        if (Current.Quoted) {
            type.Name = Current.Text;
            type.Quoted = true;
            Advance();
        } else {
            type.Name = Current.Text;
            Advance();
            while (Current.Kind == ETokenKind::Identifier && !Current.Quoted && !IsColumnConstraint() && !IsTableConstraint()) {
                type.Name += " ";
                type.Name += Current.Text;
                Advance();
            }
        }
        if (Current.Kind != ETokenKind::LeftParen) {
            return std::nullopt;
        }
        Advance();
        ui32 precision = 0;
        if (Current.Kind != ETokenKind::Number || !TryFromString(Current.Text, precision)) {
            return Unsupported(Current.Text);
        }
        type.Precision = precision;
        Advance();
        if (Current.Kind == ETokenKind::Comma) {
            Advance();
            ui32 scale = 0;
            if (Current.Kind != ETokenKind::Number || !TryFromString(Current.Text, scale)) {
                return Unsupported(Current.Text);
            }
            type.Scale = scale;
            Advance();
        }
        if (Current.Kind != ETokenKind::RightParen) {
            return Unsupported(Current.Text);
        }
        Advance();
        return std::nullopt;
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
    return TParser(sql).ParseStatement();
}

} // namespace NKikimr::NSql
