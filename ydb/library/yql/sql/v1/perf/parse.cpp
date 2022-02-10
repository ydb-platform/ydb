#include <ydb/library/yql/sql/v1/sql.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <util/datetime/cputimer.h>
#include <util/string/builder.h>

using namespace NSQLTranslationV1;

enum class EDebugOutput {
    None,
    ToCerr,
};

TString Err2Str(NYql::TAstParseResult& res, EDebugOutput debug = EDebugOutput::None) {
    TStringStream s;
    res.Issues.PrintTo(s);

    if (debug == EDebugOutput::ToCerr) {
        Cerr << s.Str() << Endl;
    }
    return s.Str();
}

NYql::TAstParseResult SqlToYqlWithMode(const TString& query, NSQLTranslation::ESqlMode mode = NSQLTranslation::ESqlMode::QUERY, size_t maxErrors = 10, const TString& provider = {}, EDebugOutput debug = EDebugOutput::None) {
    google::protobuf::Arena arena;
    const auto service = provider ? provider : TString(NYql::YtProviderName);
    const TString cluster = "plato";
    NSQLTranslation::TTranslationSettings settings;
    settings.ClusterMapping[cluster] = service;
    settings.MaxErrors = maxErrors;
    settings.Mode = mode;
    settings.Arena = &arena;
    auto res = SqlToYql(query, settings);
    if (debug == EDebugOutput::ToCerr) {
        Err2Str(res, debug);
    }
    return res;
}

NYql::TAstParseResult SqlToYql(const TString& query, size_t maxErrors = 10, const TString& provider = {}, EDebugOutput debug = EDebugOutput::None) {
    return SqlToYqlWithMode(query, NSQLTranslation::ESqlMode::QUERY, maxErrors, provider, debug);
}

int main(int, char**) {
    TStringBuilder builder;
    builder << "USE plato;\n";
    for (ui32 i = 0; i < 10; ++i) {
        builder << "$query = SELECT ";
        for (ui32 j = 0; j < 500; ++j) {
            if (j > 0) {
                builder << ",";
            }

            builder << "fld" << j;
        };

        builder << " FROM " << (i == 0? "Input" : "$query") << ";\n";
    }

    builder << "SELECT * FROM $query;\n";
    TString sql = builder;
    //Cerr << sql;
    TSimpleTimer timer;
    for (ui32 i = 0; i < 100; ++i) {
        NYql::TAstParseResult res = SqlToYql(sql);
        Y_ENSURE(res.Root);
    }

    Cerr << "Elapsed: " << timer.Get() << "\n";
    return 0;
}
