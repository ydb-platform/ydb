#include <yql/essentials/core/sql_types/simple_types.h>
#include <yql/essentials/parser/pg_catalog/catalog.h>
#include <yql/essentials/utils/backtrace/backtrace.h>
#include <library/cpp/json/writer/json.h>
#include <util/generic/yexception.h>

using namespace NYql;

int Main(int argc, const char *argv[])
{
    Y_UNUSED(argc);
    Y_UNUSED(argv);
    NJsonWriter::TBuf json;
    json.BeginObject();
    EnumerateSimpleTypes([&](auto name, auto kind) {
        json.WriteKey(name);
        json.BeginObject();
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
        if (name.StartsWith('_')) {
            json.WriteKey("_pg" + name.substr(1));
        } else {
            json.WriteKey("pg" + name);
        }

        json.BeginObject();
        json.WriteKey("kind");
        json.WriteString("Pg");
        json.EndObject();
    }

    json.EndObject();
    Cout << json.Str() << Endl;

    return 0;
}

int main(int argc, const char *argv[]) {
    NYql::NBacktrace::RegisterKikimrFatalActions();
    NYql::NBacktrace::EnableKikimrSymbolize();

    try {
        return Main(argc, argv);
    }
    catch (...) {
        Cerr << CurrentExceptionMessage() << Endl;
        return 1;
    }
}
