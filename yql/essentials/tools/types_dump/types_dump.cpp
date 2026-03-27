#include <yql/essentials/core/sql_types/simple_types.h>
#include <yql/essentials/parser/pg_catalog/catalog.h>
#include <yql/essentials/utils/backtrace/backtrace.h>
#include <library/cpp/json/writer/json.h>
#include <util/generic/yexception.h>

using namespace NYql;

int Main(int argc, const char** argv)
{
    Y_UNUSED(argc);
    Y_UNUSED(argv);
    NJsonWriter::TBuf json;
    json.BeginList();
    EnumerateSimpleTypes([&](auto name, auto kind) {
        json.BeginObject();
        json.WriteKey("name");
        json.WriteString(name);
        json.WriteKey("kind");
        json.WriteString(kind);
        json.EndObject();
    }, true);
    TVector<TString> pgNames;
    NPg::EnumTypes([&](ui32, const NPg::TTypeDesc& desc) {
        pgNames.push_back(desc.Name);
    });
    Sort(pgNames);
    for (const auto& name : pgNames) {
        json.BeginObject();
        json.WriteKey("name");
        if (name.StartsWith('_')) {
            json.WriteString("_pg" + name.substr(1));
        } else {
            json.WriteString("pg" + name);
        }

        json.WriteKey("kind");
        json.WriteString("Pg");
        json.EndObject();
    }

    json.EndList();
    Cout << json.Str() << Endl;

    return 0;
}

int main(int argc, const char** argv) {
    NYql::NBacktrace::RegisterKikimrFatalActions();
    NYql::NBacktrace::EnableKikimrSymbolize();

    try {
        return Main(argc, argv);
    } catch (...) {
        Cerr << CurrentExceptionMessage() << Endl;
        return 1;
    }
}
