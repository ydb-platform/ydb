#include "db_schema.h"

#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <util/string/printf.h>

namespace NFq {

using namespace NYdb;

TSqlQueryBuilder::TSqlQueryBuilder(const TString& tablePrefix, const TString& queryName)
    : TablePrefix(tablePrefix)
    , QueryName(queryName)
{ }

TSqlQueryBuilder& TSqlQueryBuilder::PushPk() {
    PkStack.emplace_back(Pk);
    Pk.clear();
    return *this;
}

TSqlQueryBuilder& TSqlQueryBuilder::PopPk() {
    Pk = PkStack.back();
    PkStack.pop_back();
    return *this;
}

TSqlQueryBuilder& TSqlQueryBuilder::AddPk(const TString& name, const TValue& value)
{
    Pk.push_back(name);
    return AddValue(name, value, "==");
}

TSqlQueryBuilder& TSqlQueryBuilder::AddValue(const TString& name, const NYdb::TValue& value)
{
    const auto typeStr = value.GetType().ToString();
    auto it = Parameters.find(name);
    if (it == Parameters.end()) {
        Parameters.emplace(name, typeStr);
        ParametersOrdered.emplace_back(name, typeStr);
        ParamsBuilder.AddParam("$" + name, value);
    } else if (it->second != typeStr) {
        ythrow yexception() << "Parameter `" << name << "' type " << typeStr << " != " << it->second;
    }
    return *this;
}

TSqlQueryBuilder& TSqlQueryBuilder::AddValue(const TString& name, const TValue& value, const TString& pred)
{
    TStringBuilder var; var << name;
    if (Op) {
        var << Counter;
    }
    auto typeStr = value.GetType().ToString();
    auto it = Parameters.find(var);
    if (it == Parameters.end()) {
        Parameters.emplace(var, typeStr);
        ParametersOrdered.emplace_back(var, typeStr);
        ParamsBuilder.AddParam("$" + var, value);
        if (Op) {
            Fields.emplace_back(name, var, pred);
        }
    } else if (it->second != typeStr) {
        ythrow yexception() << "Parameter `" << name << "' type " << typeStr << " != " << it->second;
    }
    return *this;
}

TSqlQueryBuilder& TSqlQueryBuilder::AddColNames(TVector<TString>&& colNames)
{
    ColNames = colNames;
    return *this;
}

TSqlQueryBuilder& TSqlQueryBuilder::MaybeAddValue(const TString& name, const TMaybe<TValue>& value) {
    if (value) {
        return AddValue(name, *value, "==");
    } else {
        return *this;
    }
}

TSqlQueryBuilder& TSqlQueryBuilder::AddText(const TString& sql, const TString& tableName)
{
    if (tableName) {
        Text << SubstGlobalCopy(sql, TString("{TABLENAME}"), tableName);
    } else {
        Text << sql;
    }
    return *this;
}

TSqlQueryBuilder& TSqlQueryBuilder::AddBeforeQuery(const TString& sql, const TString& tableName ) {
    if (tableName) {
        BeforeQuery << SubstGlobalCopy(sql, TString("{TABLENAME}"), tableName);
    } else {
        BeforeQuery << sql;
    }
    return *this;

}

TSqlQueryBuilder& TSqlQueryBuilder::Upsert(const TString& tableName) {
    Op = "UPSERT";
    Table = tableName;
    return *this;
}

TSqlQueryBuilder& TSqlQueryBuilder::Update(const TString& tableName) {
    Op = "UPDATE";
    Table = tableName;
    return *this;
}

TSqlQueryBuilder& TSqlQueryBuilder::Insert(const TString& tableName) {
    Op = "INSERT";
    Table = tableName;
    return *this;
}

TSqlQueryBuilder& TSqlQueryBuilder::Delete(const TString& tableName, int limit) {
    Op = "DELETE";
    Table = tableName;
    Limit = limit;
    return *this;
}

TSqlQueryBuilder& TSqlQueryBuilder::Get(const TString& tableName, int limit) {
    Op = "SELECT";
    Table = tableName;
    Limit = limit;
    return *this;
}

TSqlQueryBuilder& TSqlQueryBuilder::GetCount(const TString& tableName) {
    Op = "SELECT COUNT(*)";
    Table = tableName;
    return *this;
}

TSqlQueryBuilder& TSqlQueryBuilder::GetLastDeleted() {
    Op = "SELECT LAST DELETED";
    return *this;
}

void TSqlQueryBuilder::ConstructWhereFilter(TStringBuilder& text) {
    text << "WHERE ";
    size_t i = 0;
    for (const auto& pk : Pk) {
        text << "`" << pk << "` == " << "$" << pk;
        ++i;
        if (Fields || i != Pk.size()) {
            text << " and ";
        }
    }
    i = 0;
    for (const auto& t : Fields) {
        auto k = std::get<0>(t); // structured capture don't work on win32
        auto v = std::get<1>(t); // structured capture don't work on win32
        auto p = std::get<2>(t); // structured capture don't work on win32

        text << "`" << k << "` " << p << " " << "$" << v;
        ++i;
        if (i != Fields.size()) {
            text << " and ";
        }
    }
}

TSqlQueryBuilder& TSqlQueryBuilder::End()
{
    if (!Op) {
        ythrow yexception() << "Empty operation";
    }
    TStringBuilder text;

    if (Op == "DELETE") {
        text << "$todelete" << Counter << " = (SELECT * FROM `" << Table << "`\n";
        if (Pk.empty() && Fields.empty() && !Limit) {
            ythrow yexception() << "dangerous sql query";
        }
        if (!Pk.empty() || !Fields.empty()) {
            ConstructWhereFilter(text);
        }
        if (Limit > 0) {
            text << " LIMIT " << Limit;
        }
        text << ");\n";
        BeforeQuery << text;

        text.clear();

        text << "DELETE FROM `" << Table << "`\n";
        text << "ON (SELECT * from $todelete" << Counter << ");\n";
    } else if (Op == "SELECT") {
        text << Op << ' ';
        size_t i = 0;
        for (const auto& name : ColNames) {
            text << '`' << name << '`';
            ++i;
            if (i != ColNames.size()) {
                text << ", ";
            }
        }
        text << "\nFROM `" << Table << "`\n";
        ConstructWhereFilter(text);
        if (Limit > 0) {
            text << " LIMIT " << Limit;
        }
        text << ";\n";
    } else if (Op == "SELECT LAST DELETED") {
        text << "SELECT ";
        size_t i = 0;
        for (const auto& name : ColNames) {
            text << '`' << name << '`';
            ++i;
            if (i != ColNames.size()) {
                text << ", ";
            }
        }
        text << "\n FROM $todelete" << (Counter-1) << ";\n";
    } else if (Op == "SELECT COUNT(*)") {
        text << Op ;
        text << "\nFROM `" << Table << "`\n";
        ConstructWhereFilter(text);
        text << ";\n";
    } else if (Op == "UPDATE") {
        text << Op << " `" << Table << "`\n";
        if (Pk.empty() && Fields.empty()) {
            ythrow yexception() << "dangerous sql query";
        }
        text << "SET\n";
        size_t i = 0;
        for (const auto& t : Fields) {
            auto k = std::get<0>(t); // structured capture don't work on win32
            auto v = std::get<1>(t); // structured capture don't work on win32
            auto p = std::get<2>(t); // structured capture don't work on win32

            text << "`" << k << "` " << " = " << "$" << v;
            ++i;
            if (i != Fields.size()) {
                text << ", ";
            }
        }
        text << "\nWHERE\n";
        for (const auto& pk : Pk) {
            text << "$" << pk << " == " << "`" << pk << "` and ";
        }
        text << " $count != 0;\n";
        BeforeQuery << text;
        text.clear();
    } else {
        text << Op << " INTO `" << Table << "`\n";
        text << "(\n" << "  ";
        for (const auto& pk : Pk) {
            text << "`" << pk << "`" << ",";
        }
        size_t i = 0;
        for (const auto& t : Fields) {
            auto field = std::get<0>(t); // structured capture don't work on win32
            text << "`" << field << "`";
            i++;
            if (i != Fields.size()) {
                text << ",";
            }
        }
        text << "\n)\n";
        text << "VALUES\n";
        text << "(\n" << "  ";
        for (const auto& pk : Pk) {
            text << "$" << pk << ", ";
        }
        i = 0;
        for (const auto& t : Fields) {
            auto field = std::get<1>(t); // structured capture don't work on win32
            text << "$" << field;
            i++;
            if (i != Fields.size()) {
                text << ", ";
            }
        }
        text << "\n);\n";
    }
    Fields.clear();
    AddText(text);
    Counter ++;
    return *this;
}

TSqlQueryBuilder::TResult TSqlQueryBuilder::Build()
{
    TStringBuilder declr;
    declr << "--!syntax_v1\n";
    declr << "-- Query name: " << QueryName << "\n";
    declr << "PRAGMA TablePathPrefix(\"" << TablePrefix << "\");\n";
    for (const auto& [k, v] : ParametersOrdered) {
        declr << "DECLARE $" << k << " as " << v << ";\n";
    }
    declr << "\n\n";
    declr << BeforeQuery;
    declr << Text;
    return {declr, ParamsBuilder.Build()};
}

} // namespace NFq
