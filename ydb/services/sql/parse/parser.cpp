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

// PostgreSQL 18 gram.y, rewritten as C++. A rule that is not implemented yet
// returns 0A000 at the token where that rule starts.
class TGrammar {
public:
    explicit TGrammar(TStringBuf sql)
        : Lexer(sql)
    {
        Advance();
    }

    // stmt
    TParseResult Stmt() {
        if (Current.Kind == ETokenKind::Error) {
            return Unsupported(Current.Text);
        }
        if (IsKeyword("select") || IsKeyword("with") || IsKeyword("values") || IsKeyword("table")
            || Current.Kind == ETokenKind::LeftParen) {
            return SelectStmt();
        }
        if (IsKeyword("create")) {
            return CreateIntroducer();
        }
        if (IsKeyword("drop")) {
            return DropObject();
        }
        if (IsKeyword("alter")) {
            return AlterIntroducer();
        }
        return Unsupported(Current.Text);
    }

    // SelectStmt / select_no_parens / simple_select
    TParseResult SelectStmt() {
        if (IsKeyword("with") || IsKeyword("values") || IsKeyword("table") || Current.Kind == ETokenKind::LeftParen) {
            return Unsupported(Current.Text);
        }
        if (Current.Kind == ETokenKind::Error || !IsKeyword("select")) {
            return Unsupported(Current.Text);
        }
        Advance();

        TSelectStmt select;
        if (IsKeyword("all")) {
            Advance();
        } else if (IsKeyword("distinct")) {
            return Unsupported(Current.Text);
        }

        if (auto error = TargetList(select)) {
            return *error;
        }
        if (IsKeyword("into")) {
            return Unsupported(Current.Text);
        }
        if (auto error = FromClause(select)) {
            return *error;
        }
        if (IsKeyword("where")) {
            Advance();
            if (auto error = WhereClause(select)) {
                return *error;
            }
        }
        if (IsKeyword("group") || IsKeyword("having") || IsKeyword("window")) {
            return Unsupported(Current.Text);
        }
        if (IsKeyword("union") || IsKeyword("intersect") || IsKeyword("except") || IsKeyword("order") || IsKeyword("for")) {
            return Unsupported(Current.Text);
        }
        if (IsKeyword("limit")) {
            Advance();
            if (auto error = LimitClause(select)) {
                return *error;
            }
        } else if (IsKeyword("offset") || IsKeyword("fetch")) {
            return Unsupported(Current.Text);
        }
        if (auto error = Finish()) {
            return *error;
        }

        TParseResult result;
        result.Select = std::move(select);
        return result;
    }

    TParseResult CreateIntroducer() {
        Advance();
        if (IsKeyword("or") || IsKeyword("temp") || IsKeyword("temporary") || IsKeyword("unlogged")
            || IsKeyword("global") || IsKeyword("local")) {
            return Unsupported(Current.Text);
        }
        if (IsKeyword("table")) {
            return CreateStmt();
        }
        if (IsKeyword("sequence")) {
            return CreateSequence();
        }
        return Unsupported(Current.Text);
    }

    // CreateStmt: CREATE TABLE [IF NOT EXISTS] qualified_name '(' TableElementList ')'
    TParseResult CreateStmt() {
        Advance();
        TCreateTableStmt create;
        if (IsKeyword("if")) {
            Advance();
            if (!IsKeyword("not")) {
                return Unsupported(Current.Text);
            }
            Advance();
            if (!IsKeyword("exists")) {
                return Unsupported(Current.Text);
            }
            Advance();
            create.IfNotExists = true;
        }
        if (auto error = QualifiedName(create.Schema, create.Name)) {
            return *error;
        }
        if (IsKeyword("of") || IsKeyword("partition")) {
            return Unsupported(Current.Text);
        }
        if (Current.Kind != ETokenKind::LeftParen) {
            return Unsupported(Current.Text);
        }
        Advance();
        if (auto error = TableElementList(create)) {
            return *error;
        }
        if (IsKeyword("inherits") || IsKeyword("partition") || IsKeyword("using") || IsKeyword("with")
            || IsKeyword("on") || IsKeyword("tablespace")) {
            return Unsupported(Current.Text);
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

    TParseResult AlterIntroducer() {
        Advance();
        if (IsKeyword("table")) {
            return AlterTableStmt();
        }
        if (IsKeyword("sequence")) {
            return AlterSequence();
        }
        return Unsupported(Current.Text);
    }

    // AlterTableStmt + alter_table_cmd ADD [COLUMN] columnDef
    TParseResult AlterTableStmt() {
        Advance();
        if (IsKeyword("if")) {
            return Unsupported(Current.Text);
        }
        TAlterTableStmt alter;
        if (auto error = QualifiedName(alter.Schema, alter.Name)) {
            return *error;
        }
        if (IsKeyword("drop")) {
            return DropColumn(alter);
        }
        if (IsKeyword("alter")) {
            return AlterColumn(alter);
        }
        if (!IsKeyword("add")) {
            return Unsupported(Current.Text);
        }
        Advance();
        if (IsKeyword("column")) {
            Advance();
        }
        if (IsKeyword("if")) {
            return Unsupported(Current.Text);
        }
        bool inlinePrimary = false;
        if (auto error = ColumnDef(alter.Column, inlinePrimary)) {
            return *error;
        }
        if (inlinePrimary) {
            return Unsupported("primary key");
        }
        if (auto error = Finish()) {
            return *error;
        }
        TParseResult result;
        result.AlterTable = std::move(alter);
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

    bool IsTableConstraintStart() const {
        return IsKeyword("primary") || IsKeyword("unique") || IsKeyword("check") || IsKeyword("constraint")
            || IsKeyword("foreign") || IsKeyword("like") || IsKeyword("not");
    }

    bool IsColumnConstraintStart() const {
        return IsKeyword("null") || IsKeyword("not") || IsKeyword("default") || IsKeyword("primary")
            || IsKeyword("unique") || IsKeyword("check") || IsKeyword("references") || IsKeyword("collate")
            || IsKeyword("constraint") || IsKeyword("storage") || IsKeyword("compression") || IsKeyword("generated");
    }

    std::optional<TParseResult> TakeInteger(i64& value) {
        if (Current.Kind != ETokenKind::Number || !TryFromString(Current.Text, value)) {
            return Unsupported(Current.Text);
        }
        Advance();
        return std::nullopt;
    }

    TParseResult DropObject() {
        Advance();
        TDropStmt drop;
        if (!IsKeyword("table")) {
            return Unsupported(Current.Text);
        }
        Advance();
        drop.Kind = EDropKind::Table;
        if (IsKeyword("if")) {
            Advance();
            if (!IsKeyword("exists")) {
                return Unsupported(Current.Text);
            }
            Advance();
            drop.IfExists = true;
        }
        if (auto error = QualifiedName(drop.Schema, drop.Name)) {
            return *error;
        }
        if (IsKeyword("cascade")) {
            return Unsupported(Current.Text);
        }
        if (auto error = Finish()) {
            return *error;
        }
        TParseResult result;
        result.Drop = std::move(drop);
        return result;
    }

    TParseResult AlterSequence() {
        Advance();
        TAlterSequenceStmt sequence;
        if (auto error = QualifiedName(sequence.Schema, sequence.Name)) {
            return *error;
        }
        while (Current.Kind == ETokenKind::Identifier && !Current.Quoted) {
            if (IsKeyword("as")) {
                Advance();
                if (IsKeyword("smallint")) {
                    sequence.DataType = "Int16";
                } else if (IsKeyword("integer") || IsKeyword("int") || IsKeyword("int4")) {
                    sequence.DataType = "Int32";
                } else if (IsKeyword("bigint") || IsKeyword("int8")) {
                    sequence.DataType = "Int64";
                } else {
                    return Unsupported(Current.Text);
                }
                Advance();
                continue;
            }
            if (IsKeyword("restart")) {
                Advance();
                sequence.Restart = true;
                if (IsKeyword("with")) {
                    Advance();
                    i64 value = 0;
                    if (auto error = TakeInteger(value)) {
                        return *error;
                    }
                    sequence.RestartWith = value;
                }
                continue;
            }
            if (IsKeyword("owned")) {
                return Unsupported(Current.Text);
            }
            break;
        }
        if (auto error = Finish()) {
            return *error;
        }
        TParseResult result;
        result.AlterSequence = std::move(sequence);
        return result;
    }

    TParseResult CreateSequence() {
        Advance();
        TCreateSequenceStmt sequence;
        if (IsKeyword("if")) {
            Advance();
            if (!IsKeyword("not")) {
                return Unsupported(Current.Text);
            }
            Advance();
            if (!IsKeyword("exists")) {
                return Unsupported(Current.Text);
            }
            Advance();
            sequence.IfNotExists = true;
        }
        if (auto error = QualifiedName(sequence.Schema, sequence.Name)) {
            return *error;
        }
        while (Current.Kind == ETokenKind::Identifier && !Current.Quoted) {
            if (IsKeyword("as")) {
                Advance();
                if (IsKeyword("smallint")) {
                    sequence.DataType = "Int16";
                } else if (IsKeyword("integer") || IsKeyword("int") || IsKeyword("int4")) {
                    sequence.DataType = "Int32";
                } else if (IsKeyword("bigint") || IsKeyword("int8")) {
                    sequence.DataType = "Int64";
                } else {
                    return Unsupported(Current.Text);
                }
                Advance();
                continue;
            }
            if (IsKeyword("increment")) {
                Advance();
                if (IsKeyword("by")) {
                    Advance();
                }
                i64 value = 0;
                if (auto error = TakeInteger(value)) {
                    return *error;
                }
                sequence.Increment = value;
                continue;
            }
            if (IsKeyword("minvalue")) {
                Advance();
                i64 value = 0;
                if (auto error = TakeInteger(value)) {
                    return *error;
                }
                sequence.MinValue = value;
                continue;
            }
            if (IsKeyword("maxvalue")) {
                Advance();
                i64 value = 0;
                if (auto error = TakeInteger(value)) {
                    return *error;
                }
                sequence.MaxValue = value;
                continue;
            }
            if (IsKeyword("start")) {
                Advance();
                if (IsKeyword("with")) {
                    Advance();
                }
                i64 value = 0;
                if (auto error = TakeInteger(value)) {
                    return *error;
                }
                sequence.StartValue = value;
                continue;
            }
            if (IsKeyword("cache")) {
                Advance();
                ui64 value = 0;
                if (Current.Kind != ETokenKind::Number || !TryFromString(Current.Text, value)) {
                    return Unsupported(Current.Text);
                }
                Advance();
                sequence.Cache = value;
                continue;
            }
            if (IsKeyword("cycle")) {
                Advance();
                sequence.Cycle = true;
                continue;
            }
            if (IsKeyword("no")) {
                Advance();
                if (IsKeyword("cycle")) {
                    Advance();
                    sequence.Cycle = false;
                    continue;
                }
                return Unsupported(Current.Text);
            }
            if (IsKeyword("owned") || IsKeyword("temporary") || IsKeyword("temp") || IsKeyword("unlogged")) {
                return Unsupported(Current.Text);
            }
            break;
        }
        if (auto error = Finish()) {
            return *error;
        }
        TParseResult result;
        result.CreateSequence = std::move(sequence);
        return result;
    }

    TParseResult AlterColumn(TAlterTableStmt alter) {
        Advance();
        if (IsKeyword("column")) {
            Advance();
        }
        if (Current.Kind != ETokenKind::Identifier) {
            return Unsupported(Current.Text);
        }
        alter.Column.Name = Identifier();
        Advance();
        if (IsKeyword("type")) {
            return Unsupported("type");
        }
        if (IsKeyword("set")) {
            Advance();
            if (IsKeyword("data") || IsKeyword("type")) {
                return Unsupported("type");
            }
            if (IsKeyword("not")) {
                Advance();
                if (!IsKeyword("null")) {
                    return Unsupported(Current.Text);
                }
                Advance();
                alter.Kind = EAlterTableKind::SetNotNull;
            } else if (IsKeyword("default")) {
                Advance();
                TLiteral literal;
                if (Current.Kind == ETokenKind::Number) {
                    literal = {TLiteral::EKind::Integer, Current.Text};
                } else if (Current.Kind == ETokenKind::String) {
                    literal = {TLiteral::EKind::String, Current.Text};
                } else {
                    return Unsupported(Current.Text);
                }
                Advance();
                alter.Kind = EAlterTableKind::SetDefault;
                alter.Column.Default = std::move(literal);
            } else {
                return Unsupported(Current.Text);
            }
        } else if (IsKeyword("drop")) {
            Advance();
            if (IsKeyword("not")) {
                Advance();
                if (!IsKeyword("null")) {
                    return Unsupported(Current.Text);
                }
                Advance();
                alter.Kind = EAlterTableKind::DropNotNull;
            } else if (IsKeyword("default")) {
                Advance();
                alter.Kind = EAlterTableKind::DropDefault;
            } else {
                return Unsupported(Current.Text);
            }
        } else {
            return Unsupported(Current.Text);
        }
        if (auto error = Finish()) {
            return *error;
        }
        TParseResult result;
        result.AlterTable = std::move(alter);
        return result;
    }

    TParseResult DropColumn(TAlterTableStmt alter) {
        Advance();
        if (IsKeyword("column")) {
            Advance();
        }
        alter.Kind = EAlterTableKind::DropColumn;
        if (IsKeyword("if")) {
            Advance();
            if (!IsKeyword("exists")) {
                return Unsupported(Current.Text);
            }
            Advance();
            alter.IfExists = true;
        }
        if (Current.Kind != ETokenKind::Identifier) {
            return Unsupported(Current.Text);
        }
        alter.Column.Name = Identifier();
        Advance();
        if (auto error = Finish()) {
            return *error;
        }
        TParseResult result;
        result.AlterTable = std::move(alter);
        return result;
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

    std::optional<TParseResult> QualifiedName(std::optional<TIdentifier>& schema, TIdentifier& name) {
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
            schema = first;
            name = Identifier();
            Advance();
        } else {
            name = first;
        }
        return std::nullopt;
    }

    std::optional<TParseResult> TargetList(TSelectStmt& select) {
        if (Current.Kind == ETokenKind::Star) {
            select.Star = true;
            Advance();
            return std::nullopt;
        }
        if (Current.Kind != ETokenKind::Identifier) {
            return Unsupported(Current.Text);
        }
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
        return std::nullopt;
    }

    std::optional<TParseResult> FromClause(TSelectStmt& select) {
        if (!IsKeyword("from")) {
            return Unsupported(Current.Text);
        }
        Advance();
        if (auto error = QualifiedName(select.Schema, select.From)) {
            return error;
        }
        if (Current.Kind == ETokenKind::Comma || IsKeyword("join") || IsKeyword("inner") || IsKeyword("left")
            || IsKeyword("right") || IsKeyword("full") || IsKeyword("cross") || IsKeyword("natural")
            || IsKeyword("as")) {
            return Unsupported(Current.Text);
        }
        return std::nullopt;
    }

    std::optional<TParseResult> WhereClause(TSelectStmt& select) {
        for (;;) {
            if (IsKeyword("or") || IsKeyword("not") || Current.Kind == ETokenKind::LeftParen) {
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

    std::optional<TParseResult> LimitClause(TSelectStmt& select) {
        if (Current.Kind != ETokenKind::Number) {
            return Unsupported(Current.Text);
        }
        ui64 limit = 0;
        if (!TryFromString(Current.Text, limit)) {
            return Unsupported(Current.Text);
        }
        select.Limit = limit;
        Advance();
        if (IsKeyword("offset") || IsKeyword("fetch")) {
            return Unsupported(Current.Text);
        }
        return std::nullopt;
    }

    std::optional<TParseResult> NameList(TVector<TIdentifier>& names) {
        Advance();
        if (Current.Kind != ETokenKind::LeftParen) {
            return Unsupported(Current.Text);
        }
        Advance();
        if (Current.Kind != ETokenKind::Identifier) {
            return Unsupported(Current.Text);
        }
        names.push_back(Identifier());
        Advance();
        while (Current.Kind == ETokenKind::Comma) {
            Advance();
            if (Current.Kind != ETokenKind::Identifier) {
                return Unsupported(Current.Text);
            }
            names.push_back(Identifier());
            Advance();
        }
        if (Current.Kind != ETokenKind::RightParen) {
            return Unsupported(Current.Text);
        }
        Advance();
        return std::nullopt;
    }

    std::optional<TParseResult> TableElementList(TCreateTableStmt& create) {
        if (Current.Kind == ETokenKind::RightParen) {
            return Unsupported(Current.Text);
        }
        TVector<TIdentifier> inlineKey;
        while (Current.Kind != ETokenKind::RightParen) {
            if (IsKeyword("unique")) {
                TVector<TIdentifier> columns;
                if (auto error = NameList(columns)) {
                    return error;
                }
                create.Unique.push_back(std::move(columns));
            } else if (IsKeyword("primary")) {
                if (!create.PrimaryKey.empty() || !inlineKey.empty()) {
                    return Unsupported("primary key");
                }
                if (auto error = PrimaryKey(create.PrimaryKey)) {
                    return error;
                }
            } else if (IsTableConstraintStart()) {
                return Unsupported(Current.Text);
            } else if (Current.Kind == ETokenKind::Identifier) {
                TColumnDef column;
                bool inlinePrimary = false;
                if (auto error = ColumnDef(column, inlinePrimary)) {
                    return error;
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
        return std::nullopt;
    }

    std::optional<TParseResult> PrimaryKey(TVector<TIdentifier>& key) {
        Advance();
        if (!IsKeyword("key")) {
            return Unsupported(Current.Text);
        }
        return NameList(key);
    }

    std::optional<TParseResult> ColumnDef(TColumnDef& column, bool& inlinePrimary) {
        if (Current.Kind != ETokenKind::Identifier) {
            return Unsupported(Current.Text);
        }
        column.Name = Identifier();
        Advance();
        if (auto error = Typename(column.Type)) {
            return error;
        }
        return ColQualList(column, inlinePrimary);
    }

    std::optional<TParseResult> Typename(TPgType& type) {
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
            while (Current.Kind == ETokenKind::Identifier && !Current.Quoted && !IsColumnConstraintStart()
                && !IsTableConstraintStart()) {
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

    std::optional<TParseResult> ColQualList(TColumnDef& column, bool& inlinePrimary) {
        while (IsColumnConstraintStart()) {
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

    TLexer Lexer;
    TToken Current;
};

} // namespace

TParseResult Parse(TStringBuf sql) {
    return TGrammar(sql).Stmt();
}

} // namespace NKikimr::NSql
