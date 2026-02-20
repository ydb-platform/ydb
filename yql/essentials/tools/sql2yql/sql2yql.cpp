#include <yql/essentials/ast/yql_ast.h>
#include <yql/essentials/ast/yql_ast_annotation.h>
#include <yql/essentials/ast/yql_expr.h>

#include <yql/essentials/parser/lexer_common/hints.h>

#include <yql/essentials/sql/sql.h>
#include <yql/essentials/sql/v1/sql.h>
#include <yql/essentials/sql/v1/complete/check/check_complete.h>
#include <yql/essentials/sql/v1/format/sql_format.h>
#include <yql/essentials/sql/v1/format/check/check_format.h>
#include <yql/essentials/sql/v1/lexer/check/check_lexers.h>
#include <yql/essentials/sql/v1/lexer/antlr4/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4_ansi/lexer.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4/proto_parser.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4_ansi/proto_parser.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>
#include <yql/essentials/providers/common/proto/gateways_config.pb.h>
#include <yql/essentials/parser/pg_wrapper/interface/parser.h>
#include <yql/essentials/parser/pg_catalog/catalog.h>
#include <yql/essentials/core/pg_ext/yql_pg_ext.h>
#include <yql/essentials/utils/mem_limit.h>
#include <yql/essentials/parser/pg_wrapper/interface/context.h>
#include <yql/essentials/providers/common/gateways_utils/gateways_utils.h>

#include <library/cpp/getopt/last_getopt.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/file.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/string.h>
#include <util/string/escape.h>
#include <util/string/split.h>

#include <google/protobuf/message.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/repeated_field.h>

struct TPosOutput {
    IOutputStream& Out;
    ui32 Line;
    ui32 Column;

    explicit TPosOutput(IOutputStream& out)
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

namespace {

void ExtractQuery(TPosOutput& out, const google::protobuf::Message& node);

void VisitField(TPosOutput& out, const google::protobuf::FieldDescriptor& descr, const google::protobuf::Message& field) {
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

void ExtractQuery(TPosOutput& out, const google::protobuf::Message& node) {
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

} // namespace

bool TestFormat(
    const TString& query,
    const NSQLTranslation::TTranslationSettings& settings,
    const TString& outFileName,
    const bool checkDoubleFormatting)
{
    NYql::TIssues issues;
    TMaybe<TString> formatted = NSQLFormat::CheckedFormat(query, settings, issues, checkDoubleFormatting);
    if (!formatted) {
        Cerr << issues.ToString() << Endl;
        return false;
    }

    if (!outFileName.empty()) {
        TFixedBufferFileOutput out{outFileName};
        out << *formatted;
    }

    return true;
}

bool TestLexers(const TString& query) {
    NYql::TIssues issues;
    if (!NSQLTranslationV1::CheckLexers({}, query, issues)) {
        Cerr << issues.ToString();
        return false;
    }

    return true;
}

bool TestComplete(const TString& query, NYql::TAstNode& root) {
    NYql::TIssues issues;
    if (!NSQLComplete::CheckComplete(query, root, issues)) {
        Cerr << issues.ToString() << Endl;
        return false;
    }
    return true;
}

class TStoreMappingFunctor: public NLastGetopt::IOptHandler {
public:
    explicit TStoreMappingFunctor(THashMap<TString, TString>* target, char delim = '@')
        : Target_(target)
        , Delim_(delim)
    {
    }

    void HandleOpt(const NLastGetopt::TOptsParser* parser) final {
        const TStringBuf val(parser->CurValOrDef());
        const auto service = TString(val.After(Delim_));
        auto res = Target_->emplace(TString(val.Before(Delim_)), service);
        if (!res.second) {
            /// force replace already exist parametr
            res.first->second = service;
        }
    }

private:
    THashMap<TString, TString>* Target_;
    char Delim_;
};

void ParseProtoConfig(const TString& cfgFile, google::protobuf::Message* config) {
    TString configData = TFileInput(cfgFile).ReadAll();

    using ::google::protobuf::TextFormat;
    if (!TextFormat::ParseFromString(configData, config)) {
        throw yexception() << "Bad format of config file " << cfgFile;
    }
}

namespace {

template <typename TMessage>
THolder<TMessage> ParseProtoConfig(const TString& cfgFile) {
    auto config = MakeHolder<TMessage>();
    ParseProtoConfig(cfgFile, config.Get());
    return config;
}

} // namespace

int BuildAST(int argc, char** argv) {
    NLastGetopt::TOpts opts = NLastGetopt::TOpts::Default();

    TString outFileName;
    TString queryString;
    ui16 syntaxVersion;
    TString outFileNameFormat;
    NYql::TLangVersion langVer = NYql::MinLangVersion;
    THashMap<TString, TString> clusterMapping;
    clusterMapping["plato"] = NYql::YtProviderName;
    clusterMapping["pg_catalog"] = NYql::PgProviderName;
    clusterMapping["information_schema"] = NYql::PgProviderName;

    auto langVerHandler = [&langVer](const TString& str) {
        if (str == "unknown") {
            langVer = NYql::UnknownLangVersion;
        } else if (!NYql::ParseLangVersion(str, langVer)) {
            throw yexception() << "Failed to parse language version: " << str;
        }
    };

    NYql::NPg::SetSqlLanguageParser(NSQLTranslationPG::CreateSqlLanguageParser());
    NYql::NPg::LoadSystemFunctions(*NSQLTranslationPG::CreateSystemFunctionsParser());

    THashSet<TString> flags;
    bool noDebug = false;
    THolder<NYql::TGatewaysConfig> gatewaysConfig;

    opts.AddLongOption('o', "output", "save output to file").RequiredArgument("file").StoreResult(&outFileName);
    opts.AddLongOption('q', "query", "query string").RequiredArgument("query").StoreResult(&queryString);
    opts.AddLongOption("ndebug", "do not print anything when no errors present").Optional().StoreTrue(&noDebug);
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
    opts.AddLongOption('R', "replace", "replace Output table with each statement result").NoArgument();
    opts.AddLongOption("sqllogictest", "input files are in sqllogictest format").NoArgument();
    opts.AddLongOption("syntax-version", "SQL syntax version").StoreResult(&syntaxVersion).DefaultValue(1);
    opts.AddLongOption('F', "flags", "SQL pragma flags").SplitHandler(&flags, ',');
    opts.AddLongOption("assume-ydb-on-slash", "Assume YDB provider if cluster name starts with '/'").NoArgument();
    opts.AddLongOption("test-format", "compare formatted query's AST with the original query's AST (only syntaxVersion=1 is supported).").NoArgument();
    opts.AddLongOption("test-double-format", "check if formatting already formatted query produces the same result").NoArgument();
    opts.AddLongOption("test-antlr4", "check antlr4 parser").NoArgument();
    opts.AddLongOption("test-lexers", "check other lexers").NoArgument();
    opts.AddLongOption("test-complete", "check completion engine").NoArgument();
    opts.AddLongOption("test-syntax-ambiguity", "test syntax ambiguity").NoArgument();
    opts.AddLongOption("debug-syntax-ambiguity", "debug syntax ambiguity").NoArgument();
    opts.AddLongOption("format-output", "Saves formatted query to it").RequiredArgument("format-output").StoreResult(&outFileNameFormat);
    opts.AddLongOption("langver", "Set current language version").Optional().RequiredArgument("VER").Handler1T<TString>(langVerHandler);
    opts.AddLongOption("mem-limit", "Set memory limit in megabytes").Handler1T<ui32>(0, NYql::SetAddressSpaceLimit);
    opts.AddLongOption("gateways-cfg", "Gateways configuration file").Optional().RequiredArgument("FILE").Handler1T<TString>([&gatewaysConfig, &clusterMapping](const TString& file) {
        gatewaysConfig = ParseProtoConfig<NYql::TGatewaysConfig>(file);
        GetClusterMappingFromGateways(*gatewaysConfig, clusterMapping);
    });
    opts.AddLongOption("pg-ext", "Pg extensions config file").Optional().RequiredArgument("FILE").Handler1T<TString>([](const TString& file) {
        auto pgExtConfig = ParseProtoConfig<NYql::NProto::TPgExtensions>(file);
        if (!pgExtConfig) {
            throw yexception() << "Bad format of config file " << file;
        }
        TVector<NYql::NPg::TExtensionDesc> extensions;
        NYql::PgExtensionsFromProto(*pgExtConfig, extensions);
        NYql::NPg::RegisterExtensions(extensions, true,
                                      *NSQLTranslationPG::CreateExtensionSqlParser(),
                                      NKikimr::NMiniKQL::CreateExtensionLoader().get());
    });
    opts.SetFreeArgDefaultTitle("query file");
    opts.AddHelpOption();

    NLastGetopt::TOptsParseResult res(&opts, argc, argv);
    TVector<TString> queryFiles(res.GetFreeArgs());
    NYql::NPg::GetSqlLanguageParser()->Freeze();

    THolder<TFixedBufferFileOutput> outFile;
    if (!outFileName.empty()) {
        outFile.Reset(new TFixedBufferFileOutput(outFileName));
    }

    IOutputStream& out = outFile ? *outFile.Get() : Cout;

    if (gatewaysConfig) {
        NYql::TGatewaySQLFlags::FromTesting(*gatewaysConfig).CollectAllTo(flags);
    }

    if (!res.Has("query") && queryFiles.empty()) {
        Cerr << "No --query nor query file was specified" << Endl << Endl;
        opts.PrintUsage(argv[0], Cerr);
    }

    NSQLTranslationV1::TLexers lexers;
    lexers.Antlr4 = NSQLTranslationV1::MakeAntlr4LexerFactory();
    lexers.Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiLexerFactory();
    NSQLTranslationV1::TParsers parsers;
    parsers.Antlr4 = NSQLTranslationV1::MakeAntlr4ParserFactory(
        res.Has("test-syntax-ambiguity"),
        res.Has("debug-syntax-ambiguity"));
    parsers.Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiParserFactory(
        res.Has("test-syntax-ambiguity"),
        res.Has("debug-syntax-ambiguity"));

    NSQLTranslation::TTranslators translators(
        nullptr,
        NSQLTranslationV1::MakeTranslator(lexers, parsers),
        NSQLTranslationPG::MakeTranslator());

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

        for (const auto& query : queries) {
            if (query.empty()) {
                continue;
            }
            if (res.Has("print-query")) {
                out << query << Endl;
            }

            google::protobuf::Arena arena;
            NSQLTranslation::TTranslationSettings settings;
            settings.Arena = &arena;
            settings.LangVer = langVer;
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

                bool hasError = AnyOf(issues, [](const auto& issue) { return issue.GetSeverity() == NYql::TSeverityIds::S_ERROR; });
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
            if (noDebug && parseRes.IsOk()) {
                continue;
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
                hasError = AnyOf(parseRes.Issues, [](const auto& issue) { return issue.GetSeverity() <= NYql::TSeverityIds::S_ERROR; });

                if (hasError || !noDebug) {
                    parseRes.Issues.PrintWithProgramTo(Cerr, queryFile, query);
                }
            }

            if (!parseRes.IsOk() && !hasError) {
                hasError = true;
                Cerr << "No error reported, but no yql compiled result!" << Endl << Endl;
            }

            if (parseRes.IsOk() && hasError && flags.contains("StrictWarningAsError")) {
                hasError = true;
                Cerr << "Errors reported, but yql compiled result!" << Endl << Endl;
            }

            if (res.Has("test-format") && syntaxVersion == 1 && !hasError && parseRes.Root) {
                hasError = !TestFormat(query, settings, outFileNameFormat, res.Has("test-double-format"));
            }

            if (res.Has("test-lexers") && syntaxVersion == 1 && !res.Has("pg") && !hasError && parseRes.Root) {
                hasError = !TestLexers(query);
            }

            if (res.Has("test-complete") && syntaxVersion == 1 && !hasError && parseRes.Root) {
                hasError = !TestComplete(query, *parseRes.Root);
            }

            if (hasError) {
                ++errors;
            }
        }
    }

    return errors;
}

int main(int argc, char** argv) {
    try {
        return BuildAST(argc, argv);
    } catch (...) {
        Cerr << "Caught exception: " << FormatCurrentException() << Endl;
        return 1;
    }
    return 0;
}
