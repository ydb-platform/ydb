#include <yql/essentials/sql/v1/highlight/sql_highlight_json.h>
#include <yql/essentials/sql/v1/highlight/sql_highlight.h>
#include <yql/essentials/sql/v1/highlight/sql_highlighter.h>

#include <library/cpp/getopt/last_getopt.h>
#include <library/cpp/colorizer/colors.h>
#include <library/cpp/json/json_writer.h>

#include <util/stream/input.h>

using namespace NSQLHighlight;

int RunGenerateJSON() {
    THighlighting highlighting = MakeHighlighting();
    NJson::TJsonValue json = ToJson(highlighting);
    NJson::WriteJson(&Cout, &json, /* formatOutput = */ true);
    return 0;
}

int RunHighlighter() {
    THashMap<EUnitKind, NColorizer::EAnsiCode> ColorByKind = {
        {EUnitKind::Keyword, NColorizer::BLUE},
        {EUnitKind::Punctuation, NColorizer::DARK_WHITE},
        {EUnitKind::QuotedIdentifier, NColorizer::DARK_CYAN},
        {EUnitKind::BindParamterIdentifier, NColorizer::YELLOW},
        {EUnitKind::TypeIdentifier, NColorizer::GREEN},
        {EUnitKind::FunctionIdentifier, NColorizer::MAGENTA},
        {EUnitKind::Identifier, NColorizer::DEFAULT},
        {EUnitKind::Literal, NColorizer::LIGHT_GREEN},
        {EUnitKind::StringLiteral, NColorizer::DARK_RED},
        {EUnitKind::Comment, NColorizer::DARK_GREEN},
        {EUnitKind::Whitespace, NColorizer::DEFAULT},
        {EUnitKind::Error, NColorizer::RED},
    };

    TString query = Cin.ReadAll();

    THighlighting highlighting = MakeHighlighting();
    IHighlighter::TPtr highlighter = MakeHighlighter(highlighting);
    TVector<TToken> tokens = Tokenize(*highlighter, query);

    for (auto& token : tokens) {
        TStringBuf content = TStringBuf(query).SubString(token.Begin, token.Length);
        Cout << ColorByKind[token.Kind] << content << NColorizer::RESET;
    }

    return 0;
}

int Run(int argc, char* argv[]) {
    TString target;

    NLastGetopt::TOpts opts = NLastGetopt::TOpts::Default();
    opts.AddLongOption('g', "generate", "generate a highlighting configuration")
        .RequiredArgument("target")
        .Choices({"json"})
        .StoreResult(&target);
    opts.SetFreeArgsNum(0);
    opts.AddHelpOption();

    NLastGetopt::TOptsParseResult res(&opts, argc, argv);
    if (res.Has("generate")) {
        if (target == "json") {
            return RunGenerateJSON();
        }
        Y_ABORT();
    }
    return RunHighlighter();
}

int main(int argc, char* argv[]) try {
    return Run(argc, argv);
} catch (const yexception& e) {
    Cerr << "Caught exception:" << e.what() << Endl;
    return 1;
} catch (...) {
    Cerr << CurrentExceptionMessage() << Endl;
    return 1;
}
