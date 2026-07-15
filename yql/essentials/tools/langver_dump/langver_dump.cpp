#include <yql/essentials/public/langver/yql_langver.h>
#include <yql/essentials/utils/backtrace/backtrace.h>
#include <library/cpp/json/writer/json.h>
#include <util/generic/yexception.h>

using namespace NYql;

void WriteVersion(NJsonWriter::TBuf& json, TLangVersion ver) {
    TLangVersionBuffer buffer;
    TStringBuf result;
    Y_ENSURE(FormatLangVersion(ver, buffer, result));
    json.WriteString(result);
}

int Main(int argc, const char** argv)
{
    Y_UNUSED(argc);
    Y_UNUSED(argv);
    NJsonWriter::TBuf json;
    json.BeginObject();
    json.WriteKey("min");
    WriteVersion(json, MinLangVersion);
    json.WriteKey("max");
    WriteVersion(json, GetMaxLangVersion());
    json.WriteKey("max_released");
    WriteVersion(json, GetMaxReleasedLangVersion());
    json.WriteKey("valid");
    json.BeginList();
    EnumerateLangVersions([&](TLangVersion ver) {
        WriteVersion(json, ver);
    });
    json.EndList();
    json.EndObject();
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
