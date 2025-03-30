#include <yql/essentials/sql/v1/node.h>
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
    NSQLTranslationV1::EnumerateBuiltins([&](auto name, auto kind) {
        json.WriteKey(name);
        json.BeginObject();
        json.WriteKey("kind");
        json.WriteString(kind);
        json.EndObject();
    });

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
