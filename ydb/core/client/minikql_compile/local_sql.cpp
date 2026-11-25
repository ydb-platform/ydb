#include "local_sql.h"

#include <yql/essentials/ast/yql_ast.h>
#include <yql/essentials/public/issue/yql_issue.h>

#include <util/generic/algorithm.h>
#include <util/generic/hash_set.h>
#include <util/generic/maybe.h>
#include <util/generic/yexception.h>
#include <util/memory/pool.h>
#include <util/string/ascii.h>
#include <util/string/builder.h>
#include <util/string/cast.h>

namespace NYql {
namespace {

bool IsIdentChar(char c) {
    return IsAsciiAlnum(c) || c == '_';
}

void SkipTrivia(TStringBuf text, size_t& pos) {
    while (pos < text.size()) {
        if (IsAsciiSpace(text[pos])) {
            ++pos;
            continue;
        }
        if (text[pos] == '-' && pos + 1 < text.size() && text[pos + 1] == '-') {
            pos += 2;
            while (pos < text.size() && text[pos] != '\n') {
                ++pos;
            }
            continue;
        }
        if (text[pos] == '/' && pos + 1 < text.size() && text[pos + 1] == '*') {
            pos += 2;
            bool closed = false;
            while (pos + 1 < text.size()) {
                if (text[pos] == '*' && text[pos + 1] == '/') {
                    pos += 2;
                    closed = true;
                    break;
                }
                ++pos;
            }
            if (!closed) {
                ythrow yexception() << "unclosed comment";
            }
            continue;
        }
        break;
    }
}

enum class EValueKind {
    Null,
    Bool,
    Int,
    Float,
    String,
    Parameter,
};

struct TValue {
    EValueKind Kind = EValueKind::Null;
    TString Text;
};

enum class ECmp {
    Eq,
    Lt,
    Le,
    Gt,
    Ge,
};

struct TPred {
    TString Column;
    ECmp Cmp = ECmp::Eq;
    TValue Value;
};

enum class EStatementKind {
    Select,
    Update,
    Erase,
    Upsert,
};

struct TStatement {
    EStatementKind Kind = EStatementKind::Select;
    TString Table;
    bool Star = false;
    TVector<TString> Columns;
    TVector<TPred> Predicates;
    TVector<TString> AssignColumns;
    TVector<TValue> AssignValues;
    TMaybe<TString> OrderBy;
    bool Descending = false;
    TMaybe<ui64> Limit;
};

class TParser {
public:
    explicit TParser(TStringBuf sql)
        : Sql_(sql)
    {
    }

    TVector<TStatement> Parse() {
        Skip();
        if (Eof()) {
            Fail("empty SQL");
        }

        TVector<TStatement> statements;
        while (!Eof()) {
            statements.push_back(ParseStatement());
            Skip();
            if (!TryChar(';')) {
                break;
            }
            Skip();
        }
        if (!Eof()) {
            Fail("unexpected trailing input");
        }
        return statements;
    }

private:
    void Skip() {
        SkipTrivia(Sql_, Pos_);
    }

    bool Eof() const {
        return Pos_ >= Sql_.size();
    }

    [[noreturn]] void Fail(TStringBuf message) const {
        TStringBuf rest = Sql_.SubStr(Pos_, 40);
        ythrow yexception() << message << " near '" << rest << "'";
    }

    bool TryChar(char c) {
        Skip();
        if (!Eof() && Sql_[Pos_] == c) {
            ++Pos_;
            return true;
        }
        return false;
    }

    void ExpectChar(char c) {
        if (!TryChar(c)) {
            Fail(TStringBuilder() << "expected '" << c << "'");
        }
    }

    bool TryKeyword(TStringBuf keyword) {
        Skip();
        if (Pos_ + keyword.size() > Sql_.size()) {
            return false;
        }
        if (!AsciiEqualsIgnoreCase(Sql_.SubStr(Pos_, keyword.size()), keyword)) {
            return false;
        }
        const size_t after = Pos_ + keyword.size();
        if (after < Sql_.size() && IsIdentChar(Sql_[after])) {
            return false;
        }
        Pos_ = after;
        return true;
    }

    void ExpectKeyword(TStringBuf keyword) {
        if (!TryKeyword(keyword)) {
            Fail(TStringBuilder() << "expected " << keyword);
        }
    }

    TString ParseIdent() {
        Skip();
        if (Eof()) {
            Fail("expected identifier");
        }
        if (Sql_[Pos_] == '"') {
            ++Pos_;
            TString name;
            while (!Eof() && Sql_[Pos_] != '"') {
                if (Sql_[Pos_] == '\\' && Pos_ + 1 < Sql_.size()) {
                    name.push_back(Sql_[Pos_ + 1]);
                    Pos_ += 2;
                    continue;
                }
                name.push_back(Sql_[Pos_]);
                ++Pos_;
            }
            if (Eof()) {
                Fail("unclosed quoted identifier");
            }
            ++Pos_;
            if (name.empty()) {
                Fail("empty identifier");
            }
            return name;
        }
        if (!IsAsciiAlpha(Sql_[Pos_]) && Sql_[Pos_] != '_') {
            Fail("expected identifier");
        }
        const size_t begin = Pos_;
        ++Pos_;
        while (!Eof() && IsIdentChar(Sql_[Pos_])) {
            ++Pos_;
        }
        return TString(Sql_.SubStr(begin, Pos_ - begin));
    }

    TValue ParseValue() {
        Skip();
        if (Eof()) {
            Fail("expected value");
        }
        if (TryKeyword("NULL")) {
            return TValue{EValueKind::Null, {}};
        }
        if (TryKeyword("TRUE")) {
            return TValue{EValueKind::Bool, "true"};
        }
        if (TryKeyword("FALSE")) {
            return TValue{EValueKind::Bool, "false"};
        }
        if (TryChar('$')) {
            TValue value;
            value.Kind = EValueKind::Parameter;
            value.Text = ParseIdent();
            return value;
        }
        if (TryChar('\'')) {
            TString text;
            while (!Eof() && Sql_[Pos_] != '\'') {
                if (Sql_[Pos_] == '\\' && Pos_ + 1 < Sql_.size()) {
                    text.push_back(Sql_[Pos_ + 1]);
                    Pos_ += 2;
                    continue;
                }
                text.push_back(Sql_[Pos_]);
                ++Pos_;
            }
            if (Eof()) {
                Fail("unclosed string");
            }
            ++Pos_;
            // A doubled quote is the other SQL escape.
            if (!Eof() && Sql_[Pos_] == '\'') {
                Fail("doubled quotes in a string are not supported; use a backslash");
            }
            return TValue{EValueKind::String, std::move(text)};
        }

        Skip();
        const size_t begin = Pos_;
        if (!Eof() && (Sql_[Pos_] == '+' || Sql_[Pos_] == '-')) {
            ++Pos_;
        }
        if (Eof() || !IsAsciiDigit(Sql_[Pos_])) {
            Pos_ = begin;
            Fail("expected literal or parameter");
        }
        while (!Eof() && IsAsciiDigit(Sql_[Pos_])) {
            ++Pos_;
        }
        bool fraction = false;
        if (!Eof() && Sql_[Pos_] == '.') {
            fraction = true;
            ++Pos_;
            if (Eof() || !IsAsciiDigit(Sql_[Pos_])) {
                Fail("expected digits after the decimal point");
            }
            while (!Eof() && IsAsciiDigit(Sql_[Pos_])) {
                ++Pos_;
            }
        }
        TValue value;
        value.Kind = fraction ? EValueKind::Float : EValueKind::Int;
        value.Text = TString(Sql_.SubStr(begin, Pos_ - begin));
        return value;
    }

    ECmp ParseCmp() {
        Skip();
        if (TryConsume(">=")) {
            return ECmp::Ge;
        }
        if (TryConsume("<=")) {
            return ECmp::Le;
        }
        if (TryConsume("<>") || TryConsume("!=")) {
            Fail("inequality filter is not supported");
        }
        if (TryConsume(">")) {
            return ECmp::Gt;
        }
        if (TryConsume("<")) {
            return ECmp::Lt;
        }
        if (TryConsume("=")) {
            return ECmp::Eq;
        }
        Fail("expected a comparison");
        return ECmp::Eq;
    }

    bool TryConsume(TStringBuf token) {
        Skip();
        if (Sql_.SubStr(Pos_).StartsWith(token)) {
            Pos_ += token.size();
            return true;
        }
        return false;
    }

    TPred ParsePredicate() {
        TPred pred;
        pred.Column = ParseIdent();
        if (TryKeyword("IS")) {
            const bool negated = TryKeyword("NOT");
            ExpectKeyword("NULL");
            if (negated) {
                Fail("IS NOT NULL is not supported");
            }
            pred.Cmp = ECmp::Eq;
            pred.Value = TValue{EValueKind::Null, {}};
            return pred;
        }
        pred.Cmp = ParseCmp();
        pred.Value = ParseValue();
        return pred;
    }

    void ParseWhere(TStatement& statement) {
        if (!TryKeyword("WHERE")) {
            return;
        }
        do {
            statement.Predicates.push_back(ParsePredicate());
        } while (TryKeyword("AND"));
    }

    TStatement ParseSelect() {
        TStatement statement;
        statement.Kind = EStatementKind::Select;
        if (TryChar('*')) {
            statement.Star = true;
        } else {
            do {
                statement.Columns.push_back(ParseIdent());
            } while (TryChar(','));
        }
        ExpectKeyword("FROM");
        statement.Table = ParseIdent();
        ParseWhere(statement);
        if (TryKeyword("ORDER")) {
            ExpectKeyword("BY");
            statement.OrderBy = ParseIdent();
            if (TryKeyword("DESC")) {
                statement.Descending = true;
            } else {
                TryKeyword("ASC");
            }
        }
        if (TryKeyword("LIMIT")) {
            Skip();
            const size_t begin = Pos_;
            if (Eof() || !IsAsciiDigit(Sql_[Pos_])) {
                Fail("expected LIMIT");
            }
            while (!Eof() && IsAsciiDigit(Sql_[Pos_])) {
                ++Pos_;
            }
            ui64 limit = 0;
            if (!TryFromString(Sql_.SubStr(begin, Pos_ - begin), limit) || limit == 0) {
                Fail("LIMIT must be a positive integer");
            }
            statement.Limit = limit;
        }
        return statement;
    }

    TStatement ParseUpdate() {
        TStatement statement;
        statement.Kind = EStatementKind::Update;
        statement.Table = ParseIdent();
        ExpectKeyword("SET");
        do {
            statement.AssignColumns.push_back(ParseIdent());
            ExpectChar('=');
            statement.AssignValues.push_back(ParseValue());
        } while (TryChar(','));
        if (!TryKeyword("WHERE")) {
            Fail("UPDATE needs an equality for every key column");
        }
        do {
            statement.Predicates.push_back(ParsePredicate());
        } while (TryKeyword("AND"));
        return statement;
    }

    TStatement ParseUpsert() {
        TStatement statement;
        statement.Kind = EStatementKind::Upsert;
        ExpectKeyword("INTO");
        statement.Table = ParseIdent();
        ExpectChar('(');
        do {
            statement.Columns.push_back(ParseIdent());
        } while (TryChar(','));
        ExpectChar(')');
        ExpectKeyword("VALUES");
        ExpectChar('(');
        do {
            statement.AssignValues.push_back(ParseValue());
        } while (TryChar(','));
        ExpectChar(')');
        if (statement.Columns.size() != statement.AssignValues.size()) {
            Fail("UPSERT column list and VALUES list have different lengths");
        }
        return statement;
    }

    TStatement ParseStatement() {
        if (TryKeyword("SELECT")) {
            return ParseSelect();
        }
        if (TryKeyword("DELETE")) {
            // WHERE is parsed below without consuming it twice.
            TStatement statement;
            statement.Kind = EStatementKind::Erase;
            ExpectKeyword("FROM");
            statement.Table = ParseIdent();
            ExpectKeyword("WHERE");
            do {
                statement.Predicates.push_back(ParsePredicate());
            } while (TryKeyword("AND"));
            return statement;
        }
        if (TryKeyword("UPDATE")) {
            return ParseUpdate();
        }
        if (TryKeyword("UPSERT")) {
            return ParseUpsert();
        }
        Fail("expected SELECT, UPDATE, DELETE or UPSERT");
        return {};
    }

    TStringBuf Sql_;
    size_t Pos_ = 0;
};

const TLocalSqlTable& FindTable(const TLocalSqlSchema& schema, const TString& name) {
    const auto* table = schema.Tables.FindPtr(name);
    if (!table) {
        ythrow yexception() << "unknown table " << name.Quote();
    }
    return *table;
}

const TLocalSqlColumn* FindColumn(const TLocalSqlTable& table, const TString& name) {
    for (const auto& column : table.Columns) {
        if (column.Name == name) {
            return &column;
        }
    }
    return nullptr;
}

TVector<const TLocalSqlColumn*> KeyColumns(const TLocalSqlTable& table) {
    TVector<const TLocalSqlColumn*> keys;
    for (const auto& column : table.Columns) {
        if (column.KeyOrder < 0) {
            continue;
        }
        const size_t index = static_cast<size_t>(column.KeyOrder);
        if (keys.size() <= index) {
            keys.resize(index + 1);
        }
        if (keys[index]) {
            ythrow yexception() << "duplicate key position in table " << table.Name.Quote();
        }
        keys[index] = &column;
    }
    for (const auto* key : keys) {
        if (!key) {
            ythrow yexception() << "gap in the key of table " << table.Name.Quote();
        }
    }
    return keys;
}

TString TypeNameOf(const TLocalSqlColumn& column) {
    const TString name = NKikimr::NScheme::TypeName(column.Type);
    const char* supported[] = {
        "Bool",
        "Int8", "Uint8", "Int16", "Uint16", "Int32", "Uint32", "Int64", "Uint64",
        "Float", "Double",
        "String", "Utf8", "Json", "Yson", "JsonDocument", "DyNumber", "Uuid",
        "Date", "Datetime", "Timestamp", "Interval",
        "Date32", "Datetime64", "Timestamp64", "Interval64",
    };
    for (const char* item : supported) {
        if (name == item) {
            return name;
        }
    }
    ythrow yexception() << "column " << column.Name.Quote() << " has unsupported type " << name.Quote();
}

bool IsBoolType(const TString& typeName) {
    return typeName == "Bool";
}

bool IsFloatType(const TString& typeName) {
    return typeName == "Float" || typeName == "Double";
}

bool IsStringType(const TString& typeName) {
    return typeName == "String" || typeName == "Utf8" || typeName == "Json" || typeName == "Yson" || typeName == "JsonDocument" || typeName == "DyNumber" || typeName == "Uuid";
}

bool IsIntegerType(const TString& typeName) {
    return !IsBoolType(typeName) && !IsFloatType(typeName) && !IsStringType(typeName);
}

class TEmitter {
public:
    explicit TEmitter(const TLocalSqlSchema& schema)
        : Schema_(schema)
        , Pool_(64 << 10)
    {
    }

    TString Emit(const TVector<TStatement>& statements) {
        if (statements.empty()) {
            ythrow yexception() << "empty SQL";
        }

        ui32 selects = 0;
        TVector<TAstNode*> effects;
        for (const auto& statement : statements) {
            if (statement.Kind == EStatementKind::Select) {
                ++selects;
                if (selects > 1) {
                    ythrow yexception() << "only one SELECT is supported";
                }
            }
            effects.push_back(EmitStatement(statement));
        }

        auto* program = Call("AsList", effects);
        auto* body = List({
            Call("let", {Atom("pgmReturn"), program}),
            Call("return", {Atom("pgmReturn")}),
        });
        return body->ToString();
    }

private:
    TPosition Pos() const {
        return {};
    }

    TAstNode* Atom(TStringBuf content, ui32 flags = TNodeFlags::Default) {
        return TAstNode::NewAtom(Pos(), content, Pool_, flags);
    }

    TAstNode* Quoted(TStringBuf content, ui32 flags = TNodeFlags::Default) {
        return TAstNode::Quote(Pos(), Pool_, Atom(content, flags));
    }

    TAstNode* List(TVector<TAstNode*> children) {
        return TAstNode::NewList(Pos(), children.data(), children.size(), Pool_);
    }

    TAstNode* Call(TStringBuf name, TVector<TAstNode*> args) {
        TVector<TAstNode*> children;
        children.reserve(args.size() + 1);
        children.push_back(Atom(name));
        children.insert(children.end(), args.begin(), args.end());
        return List(std::move(children));
    }

    TAstNode* DataType(const TString& typeName) {
        return Call("DataType", {Quoted(typeName)});
    }

    TAstNode* Nothing(const TString& typeName) {
        return Call("Nothing", {Call("OptionalType", {DataType(typeName)})});
    }

    TAstNode* Literal(const TLocalSqlColumn& column, const TValue& value) {
        const TString typeName = TypeNameOf(column);
        switch (value.Kind) {
            case EValueKind::Null:
                // A null border is also the -inf sentinel used to pad a short key.
                // Callers reject a user-written NULL on a NOT NULL column.
                return Nothing(typeName);
            case EValueKind::Parameter:
                return Call("Parameter", {Quoted(value.Text, TNodeFlags::ArbitraryContent), DataType(typeName)});
            case EValueKind::Bool:
                if (!IsBoolType(typeName)) {
                    ythrow yexception() << "boolean literal is not valid for " << column.Name.Quote() << " of type " << typeName.Quote();
                }
                return Call(typeName, {Quoted(value.Text)});
            case EValueKind::String:
                if (!IsStringType(typeName)) {
                    ythrow yexception() << "string literal is not valid for " << column.Name.Quote() << " of type " << typeName.Quote();
                }
                return Call(typeName, {Quoted(value.Text, TNodeFlags::ArbitraryContent)});
            case EValueKind::Float:
                if (!IsFloatType(typeName)) {
                    ythrow yexception() << "fractional literal is not valid for " << column.Name.Quote() << " of type " << typeName.Quote();
                }
                return Call(typeName, {Quoted(value.Text, TNodeFlags::ArbitraryContent)});
            case EValueKind::Int:
                if (!IsIntegerType(typeName) && !IsFloatType(typeName)) {
                    ythrow yexception() << "integer literal is not valid for " << column.Name.Quote() << " of type " << typeName.Quote();
                }
                return Call(typeName, {Quoted(value.Text, value.Text.StartsWith('-') || value.Text.StartsWith('+') ? TNodeFlags::ArbitraryContent : TNodeFlags::Default)});
        }
        ythrow yexception() << "unsupported value";
    }

    TAstNode* KeyTuple(const TVector<std::pair<const TLocalSqlColumn*, TValue>>& keys) {
        TVector<TAstNode*> pairs;
        for (const auto& key : keys) {
            pairs.push_back(TAstNode::Quote(Pos(), Pool_, List({
                Quoted(key.first->Name),
                Literal(*key.first, key.second),
            })));
        }
        return TAstNode::Quote(Pos(), Pool_, List(std::move(pairs)));
    }

    TAstNode* ColumnList(const TVector<TString>& columns) {
        TVector<TAstNode*> names;
        for (const auto& column : columns) {
            names.push_back(Quoted(column));
        }
        return TAstNode::Quote(Pos(), Pool_, List(std::move(names)));
    }

    TVector<TString> ReadColumns(const TStatement& statement, const TLocalSqlTable& table) {
        if (statement.Star) {
            TVector<const TLocalSqlColumn*> ordered = KeyColumns(table);
            TVector<const TLocalSqlColumn*> rest;
            for (const auto& column : table.Columns) {
                if (column.KeyOrder < 0) {
                    rest.push_back(&column);
                }
            }
            Sort(rest, [](const TLocalSqlColumn* left, const TLocalSqlColumn* right) {
                return left->Name < right->Name;
            });
            ordered.insert(ordered.end(), rest.begin(), rest.end());
            TVector<TString> names;
            for (const auto* column : ordered) {
                names.push_back(column->Name);
            }
            if (names.empty()) {
                ythrow yexception() << "table " << table.Name.Quote() << " has no columns";
            }
            return names;
        }
        if (statement.Columns.empty()) {
            ythrow yexception() << "SELECT needs a column list";
        }
        THashSet<TString> seen;
        for (const auto& name : statement.Columns) {
            if (!FindColumn(table, name)) {
                ythrow yexception() << "unknown column " << name.Quote() << " in table " << table.Name.Quote();
            }
            if (!seen.insert(name).second) {
                ythrow yexception() << "duplicate column " << name.Quote();
            }
        }
        return statement.Columns;
    }

    struct TBorder {
        bool Inclusive = true;
        TVector<TValue> Values;
        TVector<const TLocalSqlColumn*> Columns;
    };

    struct TReadPlan {
        bool Point = false;
        TVector<std::pair<const TLocalSqlColumn*, TValue>> PointKey;
        TBorder From;
        TBorder To;
    };

    struct TKeySlot {
        TMaybe<TPred> Eq;
        TMaybe<TPred> Lower;
        TMaybe<TPred> Upper;

        bool Any() const {
            return Eq || Lower || Upper;
        }
    };

    static void RejectUserNull(const TLocalSqlColumn& column, const TValue& value) {
        if (value.Kind == EValueKind::Null && column.NotNull) {
            ythrow yexception() << "NULL is not allowed for " << column.Name.Quote();
        }
    }

    TReadPlan PlanRead(const TLocalSqlTable& table, const TVector<TPred>& predicates) {
        const auto keys = KeyColumns(table);
        if (keys.empty()) {
            ythrow yexception() << "table " << table.Name.Quote() << " has no primary key";
        }
        TVector<TKeySlot> byKey(keys.size());
        for (const auto& pred : predicates) {
            const auto* column = FindColumn(table, pred.Column);
            if (!column) {
                ythrow yexception() << "unknown column " << pred.Column.Quote() << " in table " << table.Name.Quote();
            }
            if (column->KeyOrder < 0) {
                ythrow yexception() << "column " << column->Name.Quote() << " is not a key; local SQL cannot filter it";
            }
            RejectUserNull(*column, pred.Value);
            auto& slot = byKey[column->KeyOrder];
            if (pred.Cmp == ECmp::Eq) {
                if (slot.Any()) {
                    ythrow yexception() << "column " << column->Name.Quote() << " is constrained more than once";
                }
                slot.Eq = pred;
            } else if (pred.Cmp == ECmp::Gt || pred.Cmp == ECmp::Ge) {
                if (slot.Eq || slot.Lower) {
                    ythrow yexception() << "column " << column->Name.Quote() << " is constrained more than once";
                }
                slot.Lower = pred;
            } else {
                if (slot.Eq || slot.Upper) {
                    ythrow yexception() << "column " << column->Name.Quote() << " is constrained more than once";
                }
                slot.Upper = pred;
            }
        }

        size_t prefix = 0;
        while (prefix < byKey.size() && byKey[prefix].Any()) {
            ++prefix;
        }
        for (size_t index = prefix; index < byKey.size(); ++index) {
            if (byKey[index].Any()) {
                ythrow yexception() << "key predicates must be a prefix of the primary key, missing " << keys[prefix]->Name.Quote();
            }
        }

        size_t eqCount = 0;
        while (eqCount < prefix && byKey[eqCount].Eq && !byKey[eqCount].Lower && !byKey[eqCount].Upper) {
            ++eqCount;
        }
        if (eqCount < prefix) {
            const auto& slot = byKey[eqCount];
            if (slot.Eq || (!slot.Lower && !slot.Upper)) {
                ythrow yexception() << "only the last constrained key column may be a range, see " << keys[eqCount]->Name.Quote();
            }
            for (size_t index = eqCount + 1; index < prefix; ++index) {
                ythrow yexception() << "only the last constrained key column may be a range, see " << keys[index]->Name.Quote();
            }
        }

        TReadPlan plan;
        if (eqCount == keys.size() && eqCount == prefix) {
            plan.Point = true;
            for (size_t index = 0; index < keys.size(); ++index) {
                plan.PointKey.emplace_back(keys[index], byKey[index].Eq->Value);
            }
            return plan;
        }

        const bool hasRange = eqCount < prefix;
        auto addEqual = [&](TBorder& border) {
            for (size_t index = 0; index < eqCount; ++index) {
                border.Values.push_back(byKey[index].Eq->Value);
                border.Columns.push_back(keys[index]);
            }
        };
        // Null cells sort below every value, so they extend an inclusive border
        // across the unconstrained tail of a composite key. The s-expression
        // compiler also rejects a range whose From side has no component, so an
        // absent lower bound is the same complete null key: -inf.
        auto padNulls = [&](TBorder& border) {
            for (size_t index = border.Columns.size(); index < keys.size(); ++index) {
                border.Values.push_back(TValue{EValueKind::Null, {}});
                border.Columns.push_back(keys[index]);
            }
        };

        if (!hasRange) {
            if (eqCount == 0) {
                plan.From.Inclusive = true;
                plan.To.Inclusive = true;
                padNulls(plan.From);
                return plan;
            }
            addEqual(plan.From);
            plan.From.Inclusive = true;
            padNulls(plan.From);
            addEqual(plan.To);
            plan.To.Inclusive = true;
            return plan;
        }

        const auto& rangeSlot = byKey[eqCount];
        if (rangeSlot.Lower) {
            addEqual(plan.From);
            plan.From.Values.push_back(rangeSlot.Lower->Value);
            plan.From.Columns.push_back(keys[eqCount]);
            plan.From.Inclusive = rangeSlot.Lower->Cmp == ECmp::Ge;
            if (plan.From.Inclusive && plan.From.Columns.size() < keys.size()) {
                padNulls(plan.From);
            }
        } else if (eqCount == 0) {
            plan.From.Inclusive = true;
            padNulls(plan.From);
        } else {
            addEqual(plan.From);
            plan.From.Inclusive = true;
            if (plan.From.Columns.size() < keys.size()) {
                padNulls(plan.From);
            }
        }

        if (rangeSlot.Upper) {
            addEqual(plan.To);
            plan.To.Values.push_back(rangeSlot.Upper->Value);
            plan.To.Columns.push_back(keys[eqCount]);
            plan.To.Inclusive = rangeSlot.Upper->Cmp == ECmp::Le;
            if (!plan.To.Inclusive && plan.To.Columns.size() < keys.size()) {
                ythrow yexception() << "strict upper bound on " << keys[eqCount]->Name.Quote()
                                    << " is ambiguous because key columns follow it";
            }
        } else if (eqCount == 0) {
            plan.To.Inclusive = true;
        } else {
            addEqual(plan.To);
            plan.To.Inclusive = true;
        }
        return plan;
    }

    TVector<std::pair<const TLocalSqlColumn*, TValue>> PointKey(const TLocalSqlTable& table, const TVector<TPred>& predicates, TStringBuf what) {
        auto plan = PlanRead(table, predicates);
        if (!plan.Point) {
            ythrow yexception() << what << " needs an equality for every key column of " << table.Name.Quote();
        }
        return plan.PointKey;
    }

    TAstNode* RangeTuple(const TBorder& from, const TBorder& to) {
        const size_t width = Max(from.Columns.size(), to.Columns.size());
        TVector<TAstNode*> items;
        items.push_back(Quoted(from.Inclusive ? "IncFrom" : "ExcFrom"));
        items.push_back(Quoted(to.Inclusive ? "IncTo" : "ExcTo"));
        for (size_t index = 0; index < width; ++index) {
            const TLocalSqlColumn* column = index < from.Columns.size() ? from.Columns[index] : to.Columns[index];
            TAstNode* fromExpr = index < from.Columns.size() ? Literal(*column, from.Values[index]) : Call("Void", {});
            TAstNode* toExpr = index < to.Columns.size() ? Literal(*column, to.Values[index]) : Call("Void", {});
            items.push_back(TAstNode::Quote(Pos(), Pool_, List({Quoted(column->Name), fromExpr, toExpr})));
        }
        return TAstNode::Quote(Pos(), Pool_, List(std::move(items)));
    }

    TAstNode* Options(const TStatement& statement) {
        TVector<TAstNode*> items;
        if (statement.Limit) {
            items.push_back(TAstNode::Quote(Pos(), Pool_, List({
                Quoted("ItemsLimit"),
                Call("Uint64", {Quoted(ToString(*statement.Limit))}),
            })));
        }
        if (statement.Descending) {
            items.push_back(TAstNode::Quote(Pos(), Pool_, List({
                Quoted("Reverse"),
                Call("Bool", {Quoted("true")}),
            })));
        }
        return TAstNode::Quote(Pos(), Pool_, List(std::move(items)));
    }

    TAstNode* EmitStatement(const TStatement& statement) {
        const auto& table = FindTable(Schema_, statement.Table);
        switch (statement.Kind) {
            case EStatementKind::Select:
                return EmitSelect(statement, table);
            case EStatementKind::Update:
                return EmitUpdate(statement, table);
            case EStatementKind::Erase:
                return EmitErase(statement, table);
            case EStatementKind::Upsert:
                return EmitUpsert(statement, table);
        }
        ythrow yexception() << "unsupported statement";
    }

    TAstNode* EmitSelect(const TStatement& statement, const TLocalSqlTable& table) {
        if (statement.OrderBy) {
            const auto keys = KeyColumns(table);
            if (keys.empty() || *statement.OrderBy != keys[0]->Name) {
                ythrow yexception() << "ORDER BY must be the first key column";
            }
        }
        auto columns = ReadColumns(statement, table);
        auto plan = PlanRead(table, statement.Predicates);
        TAstNode* rows = nullptr;
        if (plan.Point) {
            rows = Call("SelectRow", {Quoted(table.Name), KeyTuple(plan.PointKey), ColumnList(columns)});
        } else {
            rows = Call("Member", {
                Call("SelectRange", {
                    Quoted(table.Name),
                    RangeTuple(plan.From, plan.To),
                    ColumnList(columns),
                    Options(statement),
                }),
                Quoted("List"),
            });
        }
        return Call("SetResult", {Quoted("result"), rows});
    }

    TAstNode* UpdateTuple(const TLocalSqlTable& table, const TVector<TString>& columns, const TVector<TValue>& values) {
        if (columns.empty()) {
            ythrow yexception() << "UPDATE needs at least one column";
        }
        TVector<TAstNode*> pairs;
        THashSet<TString> seen;
        for (size_t index = 0; index < columns.size(); ++index) {
            const auto* column = FindColumn(table, columns[index]);
            if (!column) {
                ythrow yexception() << "unknown column " << columns[index].Quote() << " in table " << table.Name.Quote();
            }
            if (column->KeyOrder >= 0) {
                ythrow yexception() << "key column " << column->Name.Quote() << " cannot be updated";
            }
            if (!seen.insert(column->Name).second) {
                ythrow yexception() << "duplicate column " << column->Name.Quote();
            }
            RejectUserNull(*column, values[index]);
            pairs.push_back(TAstNode::Quote(Pos(), Pool_, List({
                Quoted(column->Name),
                Literal(*column, values[index]),
            })));
        }
        return TAstNode::Quote(Pos(), Pool_, List(std::move(pairs)));
    }

    TAstNode* EmitUpdate(const TStatement& statement, const TLocalSqlTable& table) {
        if (statement.OrderBy || statement.Limit) {
            ythrow yexception() << "UPDATE does not take ORDER BY or LIMIT";
        }
        auto key = PointKey(table, statement.Predicates, "UPDATE");
        return Call("UpdateRow", {Quoted(table.Name), KeyTuple(key), UpdateTuple(table, statement.AssignColumns, statement.AssignValues)});
    }

    TAstNode* EmitErase(const TStatement& statement, const TLocalSqlTable& table) {
        if (statement.OrderBy || statement.Limit) {
            ythrow yexception() << "DELETE does not take ORDER BY or LIMIT";
        }
        auto key = PointKey(table, statement.Predicates, "DELETE");
        return Call("EraseRow", {Quoted(table.Name), KeyTuple(key)});
    }

    TAstNode* EmitUpsert(const TStatement& statement, const TLocalSqlTable& table) {
        const auto keys = KeyColumns(table);
        THashMap<TString, TValue> values;
        for (size_t index = 0; index < statement.Columns.size(); ++index) {
            if (!values.emplace(statement.Columns[index], statement.AssignValues[index]).second) {
                ythrow yexception() << "duplicate column " << statement.Columns[index].Quote();
            }
        }
        TVector<std::pair<const TLocalSqlColumn*, TValue>> key;
        for (const auto* column : keys) {
            const auto* value = values.FindPtr(column->Name);
            if (!value) {
                ythrow yexception() << "UPSERT is missing key column " << column->Name.Quote();
            }
            key.emplace_back(column, *value);
            values.erase(column->Name);
        }
        TVector<TString> columns;
        TVector<TValue> assigns;
        for (const auto& column : table.Columns) {
            const auto* value = values.FindPtr(column.Name);
            if (!value) {
                continue;
            }
            columns.push_back(column.Name);
            assigns.push_back(*value);
            values.erase(column.Name);
        }
        if (!values.empty()) {
            ythrow yexception() << "unknown column " << values.begin()->first.Quote() << " in table " << table.Name.Quote();
        }
        if (columns.empty()) {
            ythrow yexception() << "UPSERT needs a non-key column";
        }
        return Call("UpdateRow", {Quoted(table.Name), KeyTuple(key), UpdateTuple(table, columns, assigns)});
    }

    const TLocalSqlSchema& Schema_;
    TMemoryPool Pool_;
};

} // namespace

bool IsLocalMiniKqlSExpr(TStringBuf text) {
    try {
        size_t pos = 0;
        SkipTrivia(text, pos);
        return pos >= text.size() || text[pos] == '(';
    } catch (const yexception&) {
        return false;
    }
}

TLocalSqlTranslation TranslateLocalSql(TStringBuf sql, const TLocalSqlSchema& schema) {
    TLocalSqlTranslation translation;
    try {
        translation.Program = TEmitter(schema).Emit(TParser(sql).Parse());
    } catch (const yexception& error) {
        translation.Error = error.AsStrBuf();
        translation.Program.clear();
    }
    return translation;
}

} // namespace NYql
