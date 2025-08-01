#include "generator_json.h"
#include "generator_textmate.h"
#include "generator_vim.h"

#include <yql/essentials/sql/v1/highlight/sql_highlight.h>
#include <yql/essentials/sql/v1/highlight/sql_highlighter.h>

#include <library/cpp/getopt/last_getopt.h>
#include <library/cpp/colorizer/colors.h>
#include <library/cpp/json/json_writer.h>

#include <util/stream/input.h>

using namespace NSQLHighlight;

using TGeneratorFactory = std::function<IGenerator::TPtr()>;

using TGeneratorMap = THashMap<TString, TGeneratorFactory>;

const TGeneratorMap generators = {
    {"json", MakeJsonGenerator},
    {"tmlanguage", MakeTextMateJsonGenerator},
    {"tmbundle", MakeTextMateBundleGenerator},
    {"vim", MakeVimGenerator},
};

const TVector<TString> targets = []() {
    TVector<TString> result;
    for (const auto& [name, _] : generators) {
        result.push_back(name);
    }
    return result;
}();

int RunHighlighter() {
    THashMap<EUnitKind, NColorizer::EAnsiCode> ColorByKind = {
        {EUnitKind::Keyword, NColorizer::BLUE},
        {EUnitKind::Punctuation, NColorizer::DARK_WHITE},
        {EUnitKind::QuotedIdentifier, NColorizer::DARK_CYAN},
        {EUnitKind::BindParameterIdentifier, NColorizer::YELLOW},
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
    TString path;

    NLastGetopt::TOpts opts = NLastGetopt::TOpts::Default();
    opts.AddLongOption('g', "generate", "generate a highlighting configuration")
        .RequiredArgument("target")
        .Choices(targets)
        .StoreResult(&target);
    opts.AddLongOption('o', "output", "path to output file")
        .OptionalArgument("path")
        .StoreResult(&path);
    opts.SetFreeArgsNum(0);
    opts.AddHelpOption();

    NLastGetopt::TOptsParseResult res(&opts, argc, argv);
    if (res.Has("generate")) {
        const TGeneratorFactory* generator = generators.FindPtr(target);
        Y_ENSURE(generator, "No generator for target '" << target << "'");

        if (res.Has("output")) {
            TFsPath stdpath(path.c_str());
            (*generator)()->Write(stdpath, MakeHighlighting());
        } else {
            (*generator)()->Write(Cout, MakeHighlighting());
        }

        return 0;
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
