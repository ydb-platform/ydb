#include <yql/essentials/ast/yql_ast.h>
#include <yql/essentials/ast/yql_ast_annotation.h>
#include <yql/essentials/ast/yql_expr.h>

#include <yql/essentials/parser/lexer_common/hints.h>

#include <yql/essentials/sql/sql.h>
#include <yql/essentials/sql/v1/sql.h>
#include <yql/essentials/sql/v1/lexer/antlr4/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4_ansi/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4_pure/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4_pure_ansi/lexer.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4/proto_parser.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4_ansi/proto_parser.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>
#include <yql/essentials/parser/pg_wrapper/interface/parser.h>

#include <library/cpp/getopt/last_getopt.h>
#include <yql/essentials/sql/v1/format/sql_format.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/file.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/string.h>
#include <util/string/escape.h>

#include <google/protobuf/message.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/repeated_field.h>

struct TPosOutput {
    IOutputStream& Out;
    ui32 Line;
    ui32 Column;

    TPosOutput(IOutputStream& out)
        : Out(out)
        , Line(1)
        , Column(0)
    {
    }

    void Output(ui32 line, ui32 column, const TString& value) {
        while (Line < line) {
            Out << Endl;
            ++Line;
            Column = 0;
        }
        while (Column < column) {
            Out << " ";
            ++Column;
        }
        if (value != "<EOF>") {
            Out << value;
            Column += value.size();
        }
    }
};

static void ExtractQuery(TPosOutput& out, const google::protobuf::Message& node);

static void VisitField(TPosOutput& out, const google::protobuf::FieldDescriptor& descr, const google::protobuf::Message& field) {
    using namespace google::protobuf;
    const Descriptor* d = descr.message_type();
    if (!d) {
        ythrow yexception() << "Invalid AST: non-message node encountered";
    }
    if (d->name() == "TToken") {
        const Reflection* r = field.GetReflection();
        out.Output(r->GetUInt32(field, d->field(0)), r->GetUInt32(field, d->field(1)), r->GetString(field, d->field(2)));
    } else {
        ExtractQuery(out, field);
    }
}

static void ExtractQuery(TPosOutput& out, const google::protobuf::Message& node) {
    using namespace google::protobuf;
    TVector<const FieldDescriptor*> fields;
    const Reflection* ref = node.GetReflection();
    ref->ListFields(node, &fields);

    for (auto it = fields.begin(); it != fields.end(); ++it) {
        if ((*it)->is_repeated()) {
            const ui32 fieldSize = ref->FieldSize(node, *it);
            for (ui32 i = 0; i < fieldSize; ++i) {
                VisitField(out, **it, ref->GetRepeatedMessage(node, *it, i));
            }
        } else {
            VisitField(out, **it, ref->GetMessage(node, *it));
        }
    }
}

bool TestFormat(
    const NSQLTranslation::TTranslators& translators,
    const TString& query,
    const NSQLTranslation::TTranslationSettings& settings,
    const TString& queryFile,
    const NYql::TAstParseResult& parseRes,
    const TString& outFileName,
    const bool checkDoubleFormatting
) {
    TStringStream yqlProgram;
    parseRes.Root->PrettyPrintTo(yqlProgram, NYql::TAstPrintFlags::PerLine | NYql::TAstPrintFlags::ShortQuote);

    TString frmQuery;
    NYql::TIssues issues;
    NSQLTranslationV1::TLexers lexers;
    lexers.Antlr4 = NSQLTranslationV1::MakeAntlr4LexerFactory();
    lexers.Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiLexerFactory();
    NSQLTranslationV1::TParsers parsers;
    parsers.Antlr4 = NSQLTranslationV1::MakeAntlr4ParserFactory();
    parsers.Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiParserFactory();
    auto formatter = NSQLFormat::MakeSqlFormatter(lexers, parsers, settings);
    if (!formatter->Format(query, frmQuery, issues)) {
        Cerr << "Failed to format query: " << issues.ToString() << Endl;
        return false;
    }
    NYql::TAstParseResult frmParseRes = NSQLTranslation::SqlToYql(translators, frmQuery, settings);
    if (!frmParseRes.Issues.Empty()) {
        frmParseRes.Issues.PrintWithProgramTo(Cerr, queryFile, frmQuery);
        if (AnyOf(frmParseRes.Issues, [](const auto& issue) { return issue.GetSeverity() == NYql::TSeverityIds::S_ERROR;})) {
            return false;
        }
    }
    if (!frmParseRes.IsOk()) {
        Cerr << "No error reported, but no yql compiled result!" << Endl << Endl;
        return false;
    }
    TStringStream frmYqlProgram;
    frmParseRes.Root->PrettyPrintTo(frmYqlProgram, NYql::TAstPrintFlags::PerLine | NYql::TAstPrintFlags::ShortQuote);
    if (yqlProgram.Str() != frmYqlProgram.Str()) {
        Cerr << "source query's AST and formatted query's AST are not same\n";
        return false;
    }

    TString frmQuery2;
    if (!formatter->Format(frmQuery, frmQuery2, issues)) {
        Cerr << "Failed to format already formatted query: " << issues.ToString() << Endl;
        return false;
    }

    if (checkDoubleFormatting && frmQuery != frmQuery2) {
        Cerr << "Formatting an already formatted query yielded a different resut" << Endl
             << "Add /* skip double format */ to suppress" << Endl;
        return false;
    }

    if (!outFileName.empty()) {
        TFixedBufferFileOutput out{outFileName};
        out << frmQuery;
    }
    return true;
}

bool TestLexers(
    const TString& query
) {
    NSQLTranslationV1::TLexers lexers;
    NSQLTranslation::TTranslationSettings settings;
    NYql::TIssues issues;
    if (!NSQLTranslation::ParseTranslationSettings(query, settings, issues)) {
        Cerr << issues.ToString();
        return false;
    }

    lexers.Antlr4 = NSQLTranslationV1::MakeAntlr4LexerFactory();
    lexers.Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiLexerFactory();
    lexers.Antlr4Pure = NSQLTranslationV1::MakeAntlr4PureLexerFactory();
    lexers.Antlr4PureAnsi = NSQLTranslationV1::MakeAntlr4PureAnsiLexerFactory();
    auto lexerMain = NSQLTranslationV1::MakeLexer(lexers, settings.AnsiLexer, true, false);
    auto lexerPure = NSQLTranslationV1::MakeLexer(lexers, settings.AnsiLexer, true, true);
    TVector<NSQLTranslation::TParsedToken> mainTokens;
    if (!lexerMain->Tokenize(query, "", [&](auto token) { mainTokens.push_back(token);}, issues, NSQLTranslation::SQL_MAX_PARSER_ERRORS)) {
        Cerr << issues.ToString();
        return false;
    }

    TVector<NSQLTranslation::TParsedToken> pureTokens;
    if (!lexerPure->Tokenize(query, "", [&](auto token) { pureTokens.push_back(token);}, issues, NSQLTranslation::SQL_MAX_PARSER_ERRORS)) {
        Cerr << issues.ToString();
        return false;
    }

    bool hasErrors = false;
    if (mainTokens.size() != pureTokens.size()) {
        hasErrors = true;
        Cerr << "Mismatch token count, main: " << mainTokens.size() << ", pure: " << pureTokens.size() << "\n";
    }

    for (size_t i = 0; i < Min(mainTokens.size(), pureTokens.size()); ++i) {
        if (mainTokens[i].Name != pureTokens[i].Name || mainTokens[i].Content != pureTokens[i].Content) {
            hasErrors = true;
            Cerr << "Mismatch token #" << i << ", main: " << mainTokens[i].Name << ":" << mainTokens[i].Content
                << ", pure: " << pureTokens[i].Name << ":" << pureTokens[i].Content << "\n";
            break;
        }
    }

    return !hasErrors;
}

class TStoreMappingFunctor: public NLastGetopt::IOptHandler {
public:
    TStoreMappingFunctor(THashMap<TString, TString>* target, char delim = '@')
        : Target(target)
        , Delim(delim)
    {
    }

    void HandleOpt(const NLastGetopt::TOptsParser* parser) final {
        const TStringBuf val(parser->CurValOrDef());
        const auto service = TString(val.After(Delim));
        auto res = Target->emplace(TString(val.Before(Delim)), service);
        if (!res.second) {
            /// force replace already exist parametr
            res.first->second = service;
        }
    }

private:
    THashMap<TString, TString>* Target;
    char Delim;
};

int BuildAST(int argc, char* argv[]) {
    NLastGetopt::TOpts opts = NLastGetopt::TOpts::Default();

    TString outFileName;
    TString queryString;
    ui16 syntaxVersion;
    TString outFileNameFormat;
    THashMap<TString, TString> clusterMapping;
    clusterMapping["plato"] = NYql::YtProviderName;
    clusterMapping["pg_catalog"] = NYql::PgProviderName;
    clusterMapping["information_schema"] = NYql::PgProviderName;

    THashMap<TString, TString> tables;
    THashSet<TString> flags;

    opts.AddLongOption('o', "output", "save output to file").RequiredArgument("file").StoreResult(&outFileName);
    opts.AddLongOption('q', "query", "query string").RequiredArgument("query").StoreResult(&queryString);
    opts.AddLongOption('t', "tree", "print AST proto text").NoArgument();
    opts.AddLongOption('d', "diff", "print inlined diff for original query and query build from AST if they differ").NoArgument();
    opts.AddLongOption('D', "dump", "dump inlined diff for original query and query build from AST").NoArgument();
    opts.AddLongOption('p', "print-query", "print given query before parsing").NoArgument();
    opts.AddLongOption('y', "yql", "translate result to Yql and print it").NoArgument();
    opts.AddLongOption('l', "lexer", "print query token stream").NoArgument();
    opts.AddLongOption("ansi-lexer", "use ansi lexer").NoArgument();
    opts.AddLongOption("pg", "use pg_query parser").NoArgument();
    opts.AddLongOption('a', "ann", "print Yql annotations").NoArgument();
    opts.AddLongOption('C', "cluster", "set cluster to service mapping").RequiredArgument("name@service").Handler(new TStoreMappingFunctor(&clusterMapping));
    opts.AddLongOption('T', "table", "set table to filename mapping").RequiredArgument("table@path").Handler(new TStoreMappingFunctor(&tables));
    opts.AddLongOption('R', "replace", "replace Output table with each statement result").NoArgument();
    opts.AddLongOption("sqllogictest", "input files are in sqllogictest format").NoArgument();
    opts.AddLongOption("syntax-version", "SQL syntax version").StoreResult(&syntaxVersion).DefaultValue(1);
    opts.AddLongOption('F', "flags", "SQL pragma flags").SplitHandler(&flags, ',');
    opts.AddLongOption("assume-ydb-on-slash", "Assume YDB provider if cluster name starts with '/'").NoArgument();
    opts.AddLongOption("test-format", "compare formatted query's AST with the original query's AST (only syntaxVersion=1 is supported).").NoArgument();
    opts.AddLongOption("test-double-format", "check if formatting already formatted query produces the same result").NoArgument();
    opts.AddLongOption("test-antlr4", "check antlr4 parser").NoArgument();
    opts.AddLongOption("test-lexers", "check other lexers").NoArgument();
    opts.AddLongOption("format-output", "Saves formatted query to it").RequiredArgument("format-output").StoreResult(&outFileNameFormat);
    opts.SetFreeArgDefaultTitle("query file");
    opts.AddHelpOption();

    NLastGetopt::TOptsParseResult res(&opts, argc, argv);
    TVector<TString> queryFiles(res.GetFreeArgs());

    THolder<TFixedBufferFileOutput> outFile;
    if (!outFileName.empty()) {
        outFile.Reset(new TFixedBufferFileOutput(outFileName));
    }
    IOutputStream& out = outFile ? *outFile.Get() : Cout;

    if (!res.Has("query") && queryFiles.empty()) {
        Cerr << "No --query nor query file was specified" << Endl << Endl;
        opts.PrintUsage(argv[0], Cerr);
    }

    NSQLTranslationV1::TLexers lexers;
    lexers.Antlr4 = NSQLTranslationV1::MakeAntlr4LexerFactory();
    lexers.Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiLexerFactory();
    NSQLTranslationV1::TParsers parsers;
    parsers.Antlr4 = NSQLTranslationV1::MakeAntlr4ParserFactory();
    parsers.Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiParserFactory();

    NSQLTranslation::TTranslators translators(
        nullptr,
        NSQLTranslationV1::MakeTranslator(lexers, parsers),
        NSQLTranslationPG::MakeTranslator()
    );

    TVector<TString> queries;
    int errors = 0;
    for (ui32 i = 0; i <= queryFiles.size(); ++i) {
        queries.clear();
        TString queryFile("query");
        if (i < queryFiles.size()) {
            queryFile = queryFiles[i];
            TAutoPtr<TFileInput> filePtr;
            if (queryFile != "-") {
                filePtr.Reset(new TFileInput(queryFile));
            }
            IInputStream& in = filePtr.Get() ? *filePtr : Cin;
            if (res.Has("sqllogictest")) {
                ui32 lineNum = 1;
                TString line;
                bool take = false;
                while (in.ReadLine(line)) {
                    if (line.StartsWith("statement") || line.StartsWith("query")) {
                        take = true;
                        queries.emplace_back();
                        queryFile = queryFiles[i] + " line " + ToString(lineNum + 1);
                    } else if (line.StartsWith("----") || line.empty()) {
                        take = false;
                    } else if (take) {
                        queries.back().append(line).append("\n");
                    }
                    ++lineNum;

                }
            } else {
                queries.push_back(in.ReadAll());
            }
        } else {
            queries.push_back(queryString);
        }

        for (const auto& query: queries) {
            if (query.empty()) {
                continue;
            }
            if (res.Has("print-query")) {
                out << query << Endl;
            }

            google::protobuf::Arena arena;
            NSQLTranslation::TTranslationSettings settings;
            settings.Arena = &arena;
            settings.ClusterMapping = clusterMapping;
            settings.Flags = flags;
            settings.SyntaxVersion = syntaxVersion;
            settings.AnsiLexer = res.Has("ansi-lexer");
            settings.WarnOnV0 = false;
            settings.V0ForceDisable = false;
            settings.AssumeYdbOnClusterWithSlash = res.Has("assume-ydb-on-slash");
            settings.TestAntlr4 = res.Has("test-antlr4");
            settings.EmitReadsForExists = true;

            if (res.Has("lexer")) {
                NYql::TIssues issues;
                auto lexer = NSQLTranslation::SqlLexer(translators, query, issues, settings);
                NSQLTranslation::TParsedTokenList tokens;
                if (lexer && NSQLTranslation::Tokenize(*lexer, query, queryFile, tokens, issues, NSQLTranslation::SQL_MAX_PARSER_ERRORS)) {
                    for (auto& token : tokens) {
                        out << token.Line << ":" << token.LinePos << "\t\t" << token.Name << "(" << EscapeC(token.Content) << ")\n";
                    }
                }
                if (!issues.Empty()) {
                    issues.PrintTo(Cerr);
                }

                bool hasError = AnyOf(issues, [](const auto& issue) { return issue.GetSeverity() == NYql::TSeverityIds::S_ERROR;});
                if (hasError) {
                    ++errors;
                }
                continue;
            }

            NYql::TAstParseResult parseRes;
            if (res.Has("pg")) {
                parseRes = NSQLTranslationPG::PGToYql(query, settings);
            } else {
                if (res.Has("tree") || res.Has("diff") || res.Has("dump")) {
                    google::protobuf::Message* ast(NSQLTranslation::SqlAST(translators, query, queryFile, parseRes.Issues,
                        NSQLTranslation::SQL_MAX_PARSER_ERRORS, settings));
                    if (ast) {
                        if (res.Has("tree")) {
                            out << ast->DebugString() << Endl;
                        }
                        if (res.Has("diff") || res.Has("dump")) {
                            TStringStream result;
                            TPosOutput posOut(result);
                            ExtractQuery(posOut, *ast);
                            if (res.Has("dump") || query != result.Str()) {
                              out << NUnitTest::ColoredDiff(query, result.Str()) << Endl;
                           }
                        }

                        NSQLTranslation::TSQLHints hints;
                        auto lexer = SqlLexer(translators, query, parseRes.Issues, settings);
                        if (lexer && CollectSqlHints(*lexer, query, queryFile, settings.File, hints, parseRes.Issues,
                            settings.MaxErrors, settings.Antlr4Parser)) {
                            parseRes = NSQLTranslation::SqlASTToYql(translators, query, *ast, hints, settings);
                        }
                   }
                } else {
                   parseRes = NSQLTranslation::SqlToYql(translators, query, settings);
                }
            }

            if (parseRes.Root) {
                TStringStream yqlProgram;
                parseRes.Root->PrettyPrintTo(yqlProgram, NYql::TAstPrintFlags::PerLine | NYql::TAstPrintFlags::ShortQuote);
                if (res.Has("yql")) {
                    out << yqlProgram.Str();
                }
                if (res.Has("ann")) {
                    TMemoryPool pool(1024);
                    NYql::AnnotatePositions(*parseRes.Root, pool)->PrettyPrintTo(out, NYql::TAstPrintFlags::PerLine);
                }
            }

            bool hasError = false;
            if (!parseRes.Issues.Empty()) {
                parseRes.Issues.PrintWithProgramTo(Cerr, queryFile, query);
                hasError = AnyOf(parseRes.Issues, [](const auto& issue) { return issue.GetSeverity() == NYql::TSeverityIds::S_ERROR;});
            }

            if (!parseRes.IsOk() && !hasError) {
                hasError = true;
                Cerr << "No error reported, but no yql compiled result!" << Endl << Endl;
            }

            if (res.Has("test-format") && syntaxVersion == 1 && !hasError && parseRes.Root) {
                hasError = !TestFormat(translators, query, settings, queryFile, parseRes, outFileNameFormat, res.Has("test-double-format"));
            }

            if (res.Has("test-lexers") && syntaxVersion == 1 && !hasError && parseRes.Root) {
                hasError = !TestLexers(query);
            }

            if (hasError) {
                ++errors;
            }
        }
    }

    return errors;
}

int main(int argc, char* argv[]) {
    try {
        return BuildAST(argc, argv);
    } catch (const yexception& e) {
        Cerr << "Caught exception:" << e.what() << Endl;
        return 1;
    } catch (...) {
        Cerr << "Caught exception" << Endl;
        return 1;
    }
    return 0;
}
