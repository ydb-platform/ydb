#include "generator_highlight_js.h"
#include "generator_json.h"
#include "generator_monarch.h"
#include "generator_textmate.h"
#include "generator_vim.h"
#include "yqls_highlight.h"

#include <yql/essentials/sql/v1/highlight/sql_highlight.h>
#include <yql/essentials/sql/v1/highlight/sql_highlighter.h>

#include <library/cpp/getopt/last_getopt.h>
#include <library/cpp/colorizer/colors.h>
#include <library/cpp/json/json_writer.h>

#include <util/stream/input.h>

using namespace NSQLHighlight;

using THighlightingFactory = std::function<THighlighting()>;
using THighlightingMap = THashMap<TString, THighlightingFactory>;

using TGeneratorFactory = std::function<IGenerator::TPtr()>;
using TGeneratorMap = THashMap<TString, TGeneratorFactory>;

const THighlightingMap highlightings = {
    {"yql", [] { return MakeHighlighting(); }},
    {"yqls", [] { return MakeYQLsHighlighting(); }},
};

const TGeneratorMap generators = {
    {"json", MakeJsonGenerator},
    {"monarch", MakeMonarchGenerator},
    {"tmlanguage", MakeTextMateJsonGenerator},
    {"tmbundle", MakeTextMateBundleGenerator},
    {"vim", MakeVimGenerator},
    {"highlightjs", MakeHighlightJSGenerator},
};

const TVector<TString> modes = {
    "default",
    "ansi",
};

template <class TMap>
TVector<TString> Keys(const TMap& map) {
    TVector<TString> result;
    for (const auto& [name, _] : map) {
        result.push_back(name);
    }
    return result;
}

int RunHighlighter(const THighlighting& highlighting) {
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

    IHighlighter::TPtr highlighter = MakeHighlighter(highlighting);
    TVector<TToken> tokens = Tokenize(*highlighter, query);

    for (auto& token : tokens) {
        TStringBuf content = TStringBuf(query).SubString(token.Begin, token.Length);
        Cout << ColorByKind[token.Kind] << content << NColorizer::RESET;
    }

    return 0;
}

int Run(int argc, char** argv) {
    TString syntax;
    TString target;
    TString path;
    TString mode;

    NLastGetopt::TOpts opts = NLastGetopt::TOpts::Default();
    opts.AddLongOption('l', "language", "choice a syntax")
        .RequiredArgument("syntax")
        .Choices(Keys(highlightings))
        .DefaultValue("yql")
        .StoreResult(&syntax);
    opts.AddLongOption('g', "generate", "generate a highlighting configuration")
        .RequiredArgument("target")
        .Choices(Keys(generators))
        .StoreResult(&target);
    opts.AddLongOption('m', "mode", "set a lexer mode")
        .RequiredArgument("mode")
        .Choices(modes)
        .DefaultValue("default")
        .StoreResult(&mode);
    opts.AddLongOption('o', "output", "path to output file")
        .OptionalArgument("path")
        .StoreResult(&path);
    opts.SetFreeArgsNum(0);
    opts.AddHelpOption();

    NLastGetopt::TOptsParseResult res(&opts, argc, argv);

    const THighlightingFactory* factory = highlightings.FindPtr(syntax);
    Y_ENSURE(factory, "No highlighting for syntax '" << syntax << "'");

    const THighlighting highlighting = (*factory)();

    if (res.Has("generate")) {
        const TGeneratorFactory* generator = generators.FindPtr(target);
        Y_ENSURE(generator, "No generator for target '" << target << "'");

        const bool ansi = (mode == "ansi");

        if (res.Has("output")) {
            TFsPath stdpath(path.c_str());
            (*generator)()->Write(stdpath, highlighting, ansi);
        } else {
            (*generator)()->Write(Cout, highlighting, ansi);
        }

        return 0;
    }

    return RunHighlighter(highlighting);
}

int main(int argc, char** argv) try {
    return Run(argc, argv);
} catch (const yexception& e) {
    Cerr << "Caught exception:" << e.what() << Endl;
    return 1;
} catch (...) {
    Cerr << CurrentExceptionMessage() << Endl;
    return 1;
}
