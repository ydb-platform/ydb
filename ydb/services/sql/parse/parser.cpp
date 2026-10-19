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
        if (IsKeyword("insert")) {
            return InsertStmt();
        }
        if (IsKeyword("update")) {
            return UpdateStmt();
        }
        if (IsKeyword("delete")) {
            return DeleteStmt();
        }
        if (IsKeyword("begin") || IsKeyword("start") || IsKeyword("commit") || IsKeyword("end")
            || IsKeyword("rollback") || IsKeyword("abort")) {
            return TransactionStmt();
        }
        if (IsKeyword("set")) {
            return SetStmt();
        }
        if (IsKeyword("show")) {
            return ShowStmt();
        }
        return Unsupported(Current.Text);
    }

    TParseResult SetStmt() {
        Advance();
        if (IsKeyword("transaction") || IsKeyword("session") || IsKeyword("local")) {
            return Unsupported(Current.Text);
        }
        if (Current.Kind != ETokenKind::Identifier) {
            return Unsupported(Current.Text);
        }
        TSetStmt set;
        set.Name = Identifier().Name;
        Advance();
        if (IsKeyword("to")) {
            Advance();
        } else if (Current.Kind == ETokenKind::Operator && Current.Text == "=") {
            Advance();
        } else {
            return Unsupported(Current.Text);
        }
        if (Current.Kind == ETokenKind::String || Current.Kind == ETokenKind::Number) {
            set.Value = Current.Text;
            Advance();
        } else if (Current.Kind == ETokenKind::Identifier) {
            set.Value = Identifier().Name;
            Advance();
        } else {
            return Unsupported(Current.Text);
        }
        if (auto error = Finish()) {
            return *error;
        }
        TParseResult result;
        result.Set = std::move(set);
        return result;
    }

    TParseResult ShowStmt() {
        Advance();
        if (Current.Kind != ETokenKind::Identifier) {
            return Unsupported(Current.Text);
        }
        TShowStmt show;
        show.Name = Identifier().Name;
        Advance();
        if (auto error = Finish()) {
            return *error;
        }
        TParseResult result;
        result.Show = std::move(show);
        return result;
    }

    TParseResult TransactionStmt() {
        TTxStmt tx;
        if (IsKeyword("begin")) {
            tx.Kind = ETxKind::Begin;
            Advance();
        } else if (IsKeyword("start")) {
            Advance();
            if (!IsKeyword("transaction")) {
                return Unsupported(Current.Text);
            }
            Advance();
            tx.Kind = ETxKind::Begin;
        } else if (IsKeyword("commit") || IsKeyword("end")) {
            tx.Kind = ETxKind::Commit;
            Advance();
        } else {
            tx.Kind = ETxKind::Rollback;
            Advance();
        }
        if (auto error = Finish()) {
            return *error;
        }
        TParseResult result;
        result.Tx = tx;
        return result;
    }

    TParseResult DeleteStmt() {
        Advance();
        if (!IsKeyword("from")) {
            return Unsupported(Current.Text);
        }
        Advance();
        TDeleteStmt statement;
        if (auto error = QualifiedName(statement.Schema, statement.Name)) {
            return *error;
        }
        if (IsKeyword("using") || Current.Kind == ETokenKind::Star || Current.Kind == ETokenKind::Comma) {
            return Unsupported(Current.Text);
        }
        if (IsKeyword("where")) {
            Advance();
            if (auto error = WhereClause(statement.Where)) {
                return *error;
            }
        }
        if (IsKeyword("returning")) {
            if (auto error = ReturningClause(statement.Returning)) {
                return *error;
            }
        }
        if (auto error = Finish()) {
            return *error;
        }
        TParseResult result;
        result.Delete = std::move(statement);
        return result;
    }

    TParseResult UpdateStmt() {
        Advance();
        TUpdateStmt update;
        if (auto error = QualifiedName(update.Schema, update.Name)) {
            return *error;
        }
        if (IsKeyword("from") || IsKeyword("using") || Current.Kind == ETokenKind::Star) {
            return Unsupported(Current.Text);
        }
        if (!IsKeyword("set")) {
            return Unsupported(Current.Text);
        }
        Advance();
        for (;;) {
            if (Current.Kind != ETokenKind::Identifier) {
                return Unsupported(Current.Text);
            }
            TSetAssign assign;
            assign.Column = Identifier();
            Advance();
            if (Current.Kind != ETokenKind::Operator || Current.Text != "=") {
                return Unsupported(Current.Text);
            }
            Advance();
            if (Current.Kind == ETokenKind::Identifier) {
                assign.FromColumn = true;
                assign.Source = Identifier();
                Advance();
                if (Current.Kind == ETokenKind::Operator || Current.Kind == ETokenKind::LeftParen || Current.Kind == ETokenKind::Dot) {
                    return Unsupported(Current.Text);
                }
            } else if (auto error = ValueLiteral(assign.Value)) {
                return *error;
            }
            update.Set.push_back(std::move(assign));
            if (Current.Kind != ETokenKind::Comma) {
                break;
            }
            Advance();
        }
        if (update.Set.empty()) {
            return Unsupported("set");
        }
        if (IsKeyword("from") || IsKeyword("using")) {
            return Unsupported(Current.Text);
        }
        if (IsKeyword("where")) {
            Advance();
            if (auto error = WhereClause(update.Where)) {
                return *error;
            }
        }
        if (IsKeyword("returning")) {
            if (auto error = ReturningClause(update.Returning)) {
                return *error;
            }
        }
        if (auto error = Finish()) {
            return *error;
        }
        TParseResult result;
        result.Update = std::move(update);
        return result;
    }

    TParseResult InsertStmt() {
        Advance();
        if (!IsKeyword("into")) {
            return Unsupported(Current.Text);
        }
        Advance();
        TInsertStmt insert;
        if (auto error = QualifiedName(insert.Schema, insert.Name)) {
            return *error;
        }
        if (Current.Kind == ETokenKind::LeftParen) {
            if (auto error = ParenthesizedNames(insert.Columns)) {
                return *error;
            }
        }
        if (IsKeyword("select")) {
            TParseResult selected = SelectStmt();
            if (!selected.Select) {
                return selected;
            }
            insert.Source = std::move(*selected.Select);
            TParseResult result;
            result.Insert = std::move(insert);
            return result;
        }
        if (!IsKeyword("values")) {
            return Unsupported(Current.Text);
        }
        Advance();
        if (Current.Kind != ETokenKind::LeftParen) {
            return Unsupported(Current.Text);
        }
        Advance();
        for (;;) {
            TLiteral literal;
            if (IsKeyword("null")) {
                literal = {TLiteral::EKind::Null, {}};
            } else if (Current.Kind == ETokenKind::Number) {
                literal = {TLiteral::EKind::Integer, Current.Text};
            } else if (Current.Kind == ETokenKind::Parameter) {
                literal = {TLiteral::EKind::Parameter, Current.Text};
            } else if (Current.Kind == ETokenKind::String) {
                literal = {TLiteral::EKind::String, Current.Text};
            } else {
                return Unsupported(Current.Text);
            }
            insert.Values.push_back(std::move(literal));
            Advance();
            if (Current.Kind == ETokenKind::Comma) {
                Advance();
                continue;
            }
            if (Current.Kind == ETokenKind::RightParen) {
                Advance();
                break;
            }
            return Unsupported(Current.Text);
        }
        if (Current.Kind == ETokenKind::Comma) {
            return Unsupported(Current.Text);
        }
        if (IsKeyword("on")) {
            if (auto error = ConflictClause(insert)) {
                return *error;
            }
        }
        if (IsKeyword("returning")) {
            if (auto error = ReturningClause(insert.Returning)) {
                return *error;
            }
        }
        if (auto error = Finish()) {
            return *error;
        }
        TParseResult result;
        result.Insert = std::move(insert);
        return result;
    }

    std::optional<TParseResult> ValueLiteral(TLiteral& literal) {
        if (IsKeyword("null")) {
            literal = {TLiteral::EKind::Null, {}};
        } else if (Current.Kind == ETokenKind::Number) {
            literal = {TLiteral::EKind::Integer, Current.Text};
        } else if (Current.Kind == ETokenKind::Parameter) {
            literal = {TLiteral::EKind::Parameter, Current.Text};
        } else if (Current.Kind == ETokenKind::String) {
            literal = {TLiteral::EKind::String, Current.Text};
        } else {
            return Unsupported(Current.Text);
        }
        Advance();
        return std::nullopt;
    }

    std::optional<TParseResult> ConflictClause(TInsertStmt& insert) {
        Advance();
        if (!IsKeyword("conflict")) {
            return Unsupported(Current.Text);
        }
        Advance();
        if (auto error = ParenthesizedNames(insert.ConflictTarget)) {
            return error;
        }
        if (!IsKeyword("do")) {
            return Unsupported(Current.Text);
        }
        Advance();
        if (IsKeyword("nothing")) {
            insert.Conflict = EConflictAction::Nothing;
            Advance();
            return std::nullopt;
        }
        if (!IsKeyword("update")) {
            return Unsupported(Current.Text);
        }
        Advance();
        if (!IsKeyword("set")) {
            return Unsupported(Current.Text);
        }
        Advance();
        insert.Conflict = EConflictAction::Update;
        for (;;) {
            if (Current.Kind != ETokenKind::Identifier) {
                return Unsupported(Current.Text);
            }
            TUpdateAssign assign;
            assign.Column = Identifier();
            Advance();
            if (Current.Kind != ETokenKind::Operator || Current.Text != "=") {
                return Unsupported(Current.Text);
            }
            Advance();
            if (IsKeyword("excluded")) {
                Advance();
                if (Current.Kind != ETokenKind::Dot) {
                    return Unsupported(Current.Text);
                }
                Advance();
                if (Current.Kind != ETokenKind::Identifier) {
                    return Unsupported(Current.Text);
                }
                assign.Excluded = true;
                assign.ExcludedColumn = Identifier();
                Advance();
            } else if (auto error = ValueLiteral(assign.Value)) {
                return error;
            }
            insert.Update.push_back(std::move(assign));
            if (Current.Kind != ETokenKind::Comma) {
                break;
            }
            Advance();
        }
        if (IsKeyword("where")) {
            Advance();
            TConflictWhere where;
            if (IsKeyword("excluded")) {
                where.Excluded = true;
                Advance();
                if (Current.Kind != ETokenKind::Dot) {
                    return Unsupported(Current.Text);
                }
                Advance();
            } else if (Current.Kind == ETokenKind::Identifier) {
                const TIdentifier first = Identifier();
                Advance();
                if (Current.Kind == ETokenKind::Dot) {
                    Advance();
                    if (Current.Kind != ETokenKind::Identifier) {
                        return Unsupported(Current.Text);
                    }
                    where.Column = Identifier();
                    Advance();
                } else {
                    where.Column = first;
                }
            } else {
                return Unsupported(Current.Text);
            }
            if (where.Excluded) {
                if (Current.Kind != ETokenKind::Identifier) {
                    return Unsupported(Current.Text);
                }
                where.Column = Identifier();
                Advance();
            }
            if (Current.Kind != ETokenKind::Operator) {
                return Unsupported(Current.Text);
            }
            if (Current.Text == "=") {
                where.Op = ECompareOp::Eq;
            } else if (Current.Text == "<>" || Current.Text == "!=") {
                where.Op = ECompareOp::NotEq;
            } else if (Current.Text == "<") {
                where.Op = ECompareOp::Lt;
            } else if (Current.Text == ">") {
                where.Op = ECompareOp::Gt;
            } else if (Current.Text == "<=") {
                where.Op = ECompareOp::Le;
            } else if (Current.Text == ">=") {
                where.Op = ECompareOp::Ge;
            } else {
                return Unsupported(Current.Text);
            }
            Advance();
            if (auto error = ValueLiteral(where.Value)) {
                return error;
            }
            insert.ConflictWhere = std::move(where);
        }
        return std::nullopt;
    }

    std::optional<TParseResult> ReturningClause(TVector<TReturningItem>& returning) {
        Advance();
        for (;;) {
            TReturningItem item;
            if (Current.Kind == ETokenKind::Star) {
                item.Star = true;
                Advance();
            } else if (Current.Kind == ETokenKind::Identifier) {
                item.Column = Identifier();
                Advance();
                if (Current.Kind == ETokenKind::LeftParen || Current.Kind == ETokenKind::Operator) {
                    return Unsupported(Current.Text);
                }
                if (IsKeyword("as")) {
                    Advance();
                    if (Current.Kind != ETokenKind::Identifier) {
                        return Unsupported(Current.Text);
                    }
                    item.Alias = Identifier();
                    Advance();
                }
            } else {
                return Unsupported(Current.Text);
            }
            returning.push_back(std::move(item));
            if (Current.Kind != ETokenKind::Comma) {
                break;
            }
            Advance();
        }
        return std::nullopt;
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
            if (auto error = WhereClause(select.Where)) {
                return *error;
            }
        }
        if (IsKeyword("group")) {
            Advance();
            if (!IsKeyword("by")) {
                return Unsupported(Current.Text);
            }
            Advance();
            for (;;) {
                TIdentifier column;
                if (auto error = ColumnRef(column)) {
                    return *error;
                }
                select.Group.push_back(std::move(column));
                if (Current.Kind != ETokenKind::Comma) {
                    break;
                }
                Advance();
            }
        }
        if (IsKeyword("having") || IsKeyword("window")) {
            return Unsupported(Current.Text);
        }
        if (IsKeyword("order")) {
            if (auto error = OrderByClause(select)) {
                return *error;
            }
        }
        if (IsKeyword("union") || IsKeyword("intersect") || IsKeyword("except") || IsKeyword("for")) {
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
        if (IsKeyword("schema")) {
            return CreateSchema();
        }
        if (IsKeyword("unique")) {
            Advance();
            if (!IsKeyword("index")) {
                return Unsupported(Current.Text);
            }
            return CreateIndex(true);
        }
        if (IsKeyword("index")) {
            return CreateIndex(false);
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
        if (IsKeyword("schema")) {
            return AlterSchema();
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
        return {Current.Text, Current.Quoted, {}};
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

    TParseResult CreateIndex(bool unique) {
        Advance();
        TCreateIndexStmt index;
        index.Unique = unique;
        if (IsKeyword("concurrently") || IsKeyword("using") || IsKeyword("tablespace")) {
            return Unsupported(Current.Text);
        }
        if (Current.Kind != ETokenKind::Identifier) {
            return Unsupported(Current.Text);
        }
        index.Name = Identifier();
        Advance();
        if (!IsKeyword("on")) {
            return Unsupported(Current.Text);
        }
        Advance();
        if (auto error = QualifiedName(index.Schema, index.Table)) {
            return *error;
        }
        if (IsKeyword("using") || IsKeyword("tablespace")) {
            return Unsupported(Current.Text);
        }
        if (auto error = ParenthesizedNames(index.Keys)) {
            return *error;
        }
        if (IsKeyword("include")) {
            Advance();
            if (auto error = ParenthesizedNames(index.Include)) {
                return *error;
            }
        }
        if (IsKeyword("where") || IsKeyword("tablespace") || IsKeyword("using")) {
            return Unsupported(Current.Text);
        }
        if (auto error = Finish()) {
            return *error;
        }
        TParseResult result;
        result.CreateIndex = std::move(index);
        return result;
    }

    TParseResult CreateSchema() {
        Advance();
        TSchemaStmt schema;
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
            schema.IfNotExists = true;
        }
        if (Current.Kind != ETokenKind::Identifier) {
            return Unsupported(Current.Text);
        }
        schema.Name = Identifier();
        Advance();
        if (schema.Name.Name == "information_schema" || schema.Name.Name == "pg_catalog") {
            return Unsupported(schema.Name.Name);
        }
        if (IsKeyword("authorization") || IsKeyword("create")) {
            return Unsupported(Current.Text);
        }
        if (auto error = Finish()) {
            return *error;
        }
        TParseResult result;
        result.CreateSchema = std::move(schema);
        return result;
    }

    TParseResult DropObject() {
        Advance();
        TDropStmt drop;
        if (IsKeyword("index")) {
            return DropIndex();
        }
        if (IsKeyword("table")) {
            drop.Kind = EDropKind::Table;
        } else if (IsKeyword("sequence")) {
            drop.Kind = EDropKind::Sequence;
        } else if (IsKeyword("schema")) {
            drop.Kind = EDropKind::Schema;
        } else {
            return Unsupported(Current.Text);
        }
        Advance();
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

    // DROP INDEX [CONCURRENTLY] [IF EXISTS] qualified_name [CASCADE | RESTRICT]
    TParseResult DropIndex() {
        Advance();
        TDropStmt drop;
        drop.Kind = EDropKind::Index;
        if (IsKeyword("concurrently")) {
            return Unsupported(Current.Text);
        }
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
        if (IsKeyword("restrict")) {
            Advance();
        }
        if (auto error = Finish()) {
            return *error;
        }
        TParseResult result;
        result.Drop = std::move(drop);
        return result;
    }

    TParseResult AlterSchema() {
        Advance();
        if (Current.Kind != ETokenKind::Identifier) {
            return Unsupported(Current.Text);
        }
        Advance();
        if (IsKeyword("rename") || IsKeyword("owner")) {
            return Unsupported(Current.Text);
        }
        return Unsupported(Current.Text);
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
        for (;;) {
            if (auto error = SelectItem(select)) {
                return error;
            }
            if (Current.Kind != ETokenKind::Comma) {
                break;
            }
            Advance();
        }
        return std::nullopt;
    }

    std::optional<TParseResult> SelectItem(TSelectStmt& select) {
        if (Current.Kind != ETokenKind::Identifier) {
            return Unsupported(Current.Text);
        }
        TIdentifier name = Identifier();
        Advance();
        if (Current.Kind == ETokenKind::LeftParen) {
            EAggFunc func;
            if (name.Name == "count") {
                func = EAggFunc::Count;
            } else if (name.Name == "sum") {
                func = EAggFunc::Sum;
            } else if (name.Name == "min") {
                func = EAggFunc::Min;
            } else if (name.Name == "max") {
                func = EAggFunc::Max;
            } else {
                return Unsupported(name.Name);
            }
            Advance();
            TAggCall call;
            call.Func = func;
            call.Target = select.Columns.size();
            if (Current.Kind == ETokenKind::Star) {
                if (func != EAggFunc::Count) {
                    return Unsupported(Current.Text);
                }
                call.Star = true;
                Advance();
            } else if (auto error = ColumnRef(call.Argument)) {
                return error;
            }
            if (Current.Kind != ETokenKind::RightParen) {
                return Unsupported(Current.Text);
            }
            Advance();
            select.Aggs.push_back(std::move(call));
            select.Columns.push_back({name.Name, name.Quoted, {}});
            return std::nullopt;
        }
        if (Current.Kind == ETokenKind::Dot) {
            Advance();
            if (Current.Kind != ETokenKind::Identifier) {
                return Unsupported(Current.Text);
            }
            const TIdentifier second = Identifier();
            name.Qualifier = name.Name;
            name.Name = second.Name;
            name.Quoted = second.Quoted;
            Advance();
        }
        select.Columns.push_back(std::move(name));
        return std::nullopt;
    }

    std::optional<TParseResult> ColumnRef(TIdentifier& column) {
        if (Current.Kind != ETokenKind::Identifier) {
            return Unsupported(Current.Text);
        }
        column = Identifier();
        Advance();
        if (Current.Kind != ETokenKind::Dot) {
            return std::nullopt;
        }
        Advance();
        if (Current.Kind != ETokenKind::Identifier) {
            return Unsupported(Current.Text);
        }
        const TIdentifier second = Identifier();
        column.Qualifier = column.Name;
        column.Name = second.Name;
        column.Quoted = second.Quoted;
        Advance();
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
        if (IsKeyword("as")) {
            Advance();
            if (Current.Kind != ETokenKind::Identifier) {
                return Unsupported(Current.Text);
            }
            select.Alias = Identifier().Name;
            Advance();
        }
        if (IsKeyword("cross")) {
            Advance();
            if (!IsKeyword("join")) {
                return Unsupported(Current.Text);
            }
            Advance();
            return JoinTail(select, EJoinKind::Cross, false);
        }
        if (Current.Kind == ETokenKind::Comma) {
            Advance();
            return JoinTail(select, EJoinKind::Cross, false);
        }
        EJoinKind kind = EJoinKind::Inner;
        if (IsKeyword("left")) {
            Advance();
            if (IsKeyword("outer")) {
                Advance();
            }
            if (!IsKeyword("join")) {
                return Unsupported(Current.Text);
            }
            kind = EJoinKind::Left;
        } else if (IsKeyword("right")) {
            Advance();
            if (IsKeyword("outer")) {
                Advance();
            }
            if (!IsKeyword("join")) {
                return Unsupported(Current.Text);
            }
            kind = EJoinKind::Right;
        } else if (IsKeyword("full")) {
            Advance();
            if (IsKeyword("outer")) {
                Advance();
            }
            if (!IsKeyword("join")) {
                return Unsupported(Current.Text);
            }
            kind = EJoinKind::Full;
        } else if (IsKeyword("inner")) {
            Advance();
            if (!IsKeyword("join")) {
                return Unsupported(Current.Text);
            }
        }
        if (IsKeyword("join")) {
            Advance();
            return JoinTail(select, kind, true);
        }
        if (IsKeyword("full") || IsKeyword("natural")) {
            return Unsupported(Current.Text);
        }
        return std::nullopt;
    }

    std::optional<TParseResult> JoinTail(TSelectStmt& select, EJoinKind kind, bool needOn) {
        TJoin join;
        join.Kind = kind;
        if (auto error = QualifiedName(join.Schema, join.Name)) {
            return error;
        }
        if (IsKeyword("as")) {
            Advance();
            if (Current.Kind != ETokenKind::Identifier) {
                return Unsupported(Current.Text);
            }
            join.Alias = Identifier().Name;
            Advance();
        }
        if (needOn) {
            if (!IsKeyword("on")) {
                return Unsupported(Current.Text);
            }
            Advance();
            if (auto error = ColumnRef(join.LeftColumn)) {
                return error;
            }
            if (Current.Kind != ETokenKind::Operator || Current.Text != "=") {
                return Unsupported(Current.Text);
            }
            Advance();
            if (auto error = ColumnRef(join.RightColumn)) {
                return error;
            }
        }
        join.LeftAlias = select.Alias;
        select.Join = std::move(join);
        return std::nullopt;
    }

    std::optional<TParseResult> WhereClause(TVector<TComparison>& where) {
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
            } else if (Current.Kind == ETokenKind::Parameter) {
                comparison.Value = {TLiteral::EKind::Parameter, Current.Text};
            } else if (Current.Kind == ETokenKind::String) {
                comparison.Value = {TLiteral::EKind::String, Current.Text};
            } else {
                return Unsupported(Current.Text);
            }
            where.push_back(std::move(comparison));
            Advance();
            if (!IsKeyword("and")) {
                return std::nullopt;
            }
            Advance();
        }
    }

    std::optional<TParseResult> OrderByClause(TSelectStmt& select) {
        Advance();
        if (!IsKeyword("by")) {
            return Unsupported(Current.Text);
        }
        Advance();
        for (;;) {
            TOrderKey key;
            if (auto error = ColumnRef(key.Column)) {
                return error;
            }
            if (IsKeyword("asc")) {
                Advance();
            } else if (IsKeyword("desc")) {
                key.Desc = true;
                Advance();
            }
            if (IsKeyword("nulls")) {
                Advance();
                if (IsKeyword("first")) {
                    key.NullsFirst = true;
                } else if (IsKeyword("last")) {
                    key.NullsFirst = false;
                } else {
                    return Unsupported(Current.Text);
                }
                Advance();
            } else {
                key.NullsFirst = key.Desc;
            }
            select.Order.push_back(std::move(key));
            if (Current.Kind != ETokenKind::Comma) {
                break;
            }
            Advance();
        }
        return std::nullopt;
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

    std::optional<TParseResult> ParenthesizedNames(TVector<TIdentifier>& names) {
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
