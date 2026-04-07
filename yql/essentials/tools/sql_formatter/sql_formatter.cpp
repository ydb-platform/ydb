#include <yql/essentials/sql/v1/format/sql_format.h>
#include <yql/essentials/sql/v1/lexer/antlr4/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4_ansi/lexer.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4/proto_parser.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4_ansi/proto_parser.h>
#include <yql/essentials/public/langver/yql_langver.h>
#include <yql/essentials/core/langver/yql_core_langver.h>

#include <library/cpp/getopt/last_getopt.h>
#include <google/protobuf/arena.h>

#include <util/stream/file.h>

int RunFormat(int argc, char** argv) {
    NLastGetopt::TOpts opts = NLastGetopt::TOpts::Default();

    TString outFileName;
    TString inFileName;
    TString queryString;
    NYql::TLangVersion langver = NYql::GetMaxReleasedLangVersion();

    opts.AddLongOption('o', "output", "save output to file").RequiredArgument("file").StoreResult(&outFileName);
    opts.AddLongOption('i', "input", "input file").RequiredArgument("input").StoreResult(&inFileName);
    opts.AddLongOption('p', "print-query", "print given query before parsing").NoArgument();
    opts.AddLongOption('f', "obfuscate", "obfuscate query").NoArgument();
    opts.AddLongOption("ansi-lexer", "use ansi lexer").NoArgument();
    opts.AddLongOption("langver", "Set current language version").Optional().RequiredArgument("VER").Handler1T<TString>([&](const TString& str) {
        if (!NYql::ParseLangVersion(str, langver)) {
            throw yexception() << "Failed to parse language version: " << str;
        }
    });

    opts.SetFreeArgsNum(0);
    opts.AddHelpOption();

    NLastGetopt::TOptsParseResult res(&opts, argc, argv);
    TMaybe<NYql::TIssue> verIssue;
    if (!NYql::CheckLangVersion(langver, NYql::GetMaxReleasedLangVersion(), verIssue)) {
        throw yexception() << verIssue->ToString();
    }

    THolder<TFixedBufferFileOutput> outFile;
    if (!outFileName.empty()) {
        outFile.Reset(new TFixedBufferFileOutput(outFileName));
    }
    IOutputStream& out = outFile ? *outFile.Get() : Cout;

    THolder<TUnbufferedFileInput> inFile;
    if (!inFileName.empty()) {
        inFile.Reset(new TUnbufferedFileInput(inFileName));
    }
    IInputStream& in = inFile ? *inFile.Get() : Cin;

    queryString = in.ReadAll();
    int errors = 0;
    TString queryFile("query");
    if (res.Has("print-query")) {
        out << queryString << Endl;
    }
    google::protobuf::Arena arena;
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = langver;
    settings.Arena = &arena;
    settings.AnsiLexer = res.Has("ansi-lexer");
    NSQLTranslationV1::TLexers lexers;
    lexers.Antlr4 = NSQLTranslationV1::MakeAntlr4LexerFactory();
    lexers.Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiLexerFactory();
    NSQLTranslationV1::TParsers parsers;
    parsers.Antlr4 = NSQLTranslationV1::MakeAntlr4ParserFactory();
    parsers.Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiParserFactory();
    auto formatter = NSQLFormat::MakeSqlFormatter(lexers, parsers, settings);
    TString frm_query;
    TString error;
    NYql::TIssues issues;
    if (!formatter->Format(queryString, frm_query, issues, res.Has("obfuscate") ? NSQLFormat::EFormatMode::Obfuscate : NSQLFormat::EFormatMode::Pretty)) {
        ++errors;
        Cerr << "Error formatting query: " << issues.ToString() << Endl;
    } else {
        out << frm_query;
    }

    return errors;
}

int main(int argc, char** argv) {
    try {
        return RunFormat(argc, argv);
    } catch (const yexception& e) {
        Cerr << "Caught exception: " << e.what() << Endl;
        return 1;
    } catch (...) {
        Cerr << CurrentExceptionMessage() << Endl;
        return 1;
    }
    return 0;
}
