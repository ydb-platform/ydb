#include <yql/essentials/sql/v1/translation/node.h>
#include <yql/essentials/public/langver/yql_langver.h>
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
    NSQLTranslationV1::EnumerateBuiltins([&](auto name, const NSQLTranslationV1::TFuncInfo& meta) {
        json.BeginObject();
        json.WriteKey("name");
        json.WriteString(name);
        json.WriteKey("kind");
        json.WriteString(meta.Kind);
        if (meta.ArgCount) {
            json.WriteKey("argCount");
            json.WriteULongLong(*meta.ArgCount);
        }
        if (meta.OptionalArgCount.GetOrElse(0) > 0) {
            json.WriteKey("optionalArgCount");
            json.WriteULongLong(*meta.OptionalArgCount);
        }
        if (meta.MinLangVer != NYql::UnknownLangVersion) {
            json.WriteKey("minLangVer");
            json.WriteString(NYql::FormatLangVersion(meta.MinLangVer).GetRef());
        }
        if (meta.MaxLangVer != NYql::UnknownLangVersion) {
            json.WriteKey("maxLangVer");
            json.WriteString(NYql::FormatLangVersion(meta.MaxLangVer).GetRef());
        }
        json.EndObject();
    });

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
