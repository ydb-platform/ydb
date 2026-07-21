#include <yql/essentials/ast/yql_ast.h>
#include <yql/essentials/parser/lexer_common/lexer.h>
#include <yql/essentials/sql/sql.h>
#include <yql/essentials/sql/v1/sql.h>
#include <yql/essentials/sql/v1/format/sql_format.h>
#include <yql/essentials/sql/v1/lexer/antlr4/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4_ansi/lexer.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4/proto_parser.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4_ansi/proto_parser.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <google/protobuf/arena.h>
#include <google/protobuf/message.h>

#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/stream/str.h>
#include <util/string/builder.h>
#include <util/system/types.h>
#include <util/system/yassert.h>

#include <array>

namespace {

constexpr size_t MaxInputSize = 4 * 1024;
constexpr size_t MaxGeneratedQuerySize = 16 * 1024;
constexpr size_t MaxLiteralBytes = 512;
constexpr size_t MaxIdentifierBytes = 48;
constexpr ui32 AstPrintFlags = NYql::TAstPrintFlags::PerLine |
                               NYql::TAstPrintFlags::ShortQuote |
                               NYql::TAstPrintFlags::AdaptArbitraryContent;

const NSQLTranslationV1::TLexers& GetLexers() {
    static const NSQLTranslationV1::TLexers lexers = [] {
        NSQLTranslationV1::TLexers result;
        result.Antlr4 = NSQLTranslationV1::MakeAntlr4LexerFactory();
        result.Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiLexerFactory();
        return result;
    }();
    return lexers;
}

const NSQLTranslationV1::TParsers& GetParsers() {
    static const NSQLTranslationV1::TParsers parsers = [] {
        NSQLTranslationV1::TParsers result;
        result.Antlr4 = NSQLTranslationV1::MakeAntlr4ParserFactory();
        result.Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiParserFactory();
        return result;
    }();
    return parsers;
}

const NSQLTranslation::TTranslators& GetTranslators() {
    static const NSQLTranslation::TTranslators translators(
        nullptr,
        NSQLTranslationV1::MakeTranslator(GetLexers(), GetParsers()),
        nullptr);
    return translators;
}

TString QuoteSqlString(TStringBuf value) {
    TString result;
    result.reserve(value.size() + 2);
    result += '\'';
    for (unsigned char c : value) {
        if (c == '\'') {
            result += "''";
        } else if (c >= 0x20 && c < 0x7f) {
            result += static_cast<char>(c);
        } else {
            result += 'x';
        }
    }
    result += '\'';
    return result;
}

TString MakeIdentifier(TStringBuf value) {
    TString result = "fuzz";
    for (unsigned char c : value) {
        if (result.size() >= MaxIdentifierBytes) {
            break;
        }

        if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')) {
            result += static_cast<char>(c);
        } else {
            result += '_';
        }
    }
    return result;
}

NSQLTranslation::TTranslationSettings MakeSettings(FuzzedDataProvider& provider, google::protobuf::Arena& arena) {
    NSQLTranslation::TTranslationSettings settings;
    settings.Arena = &arena;
    settings.File = "fuzz.sql";
    settings.MaxErrors = provider.ConsumeIntegralInRange<size_t>(1, 8);

    static constexpr std::array<TStringBuf, 3> defaultClusters = {"plato", "ydb", "/Root"};
    settings.DefaultCluster = TString(defaultClusters[provider.ConsumeIntegralInRange<size_t>(0, defaultClusters.size() - 1)]);

    static constexpr std::array modes = {
        NSQLTranslation::ESqlMode::QUERY,
        NSQLTranslation::ESqlMode::LIMITED_VIEW,
        NSQLTranslation::ESqlMode::SUBQUERY,
        NSQLTranslation::ESqlMode::DISCOVERY,
    };
    settings.Mode = modes[provider.ConsumeIntegralInRange<size_t>(0, modes.size() - 1)];

    settings.ClusterMapping["plato"] = "plato";
    settings.ClusterMapping["hahn"] = "yt";
    settings.ClusterMapping["kikimr"] = "/Root";
    settings.ClusterMapping["ydb"] = "/Root";
    settings.ClusterMapping["/Root"] = "/Root";

    settings.SyntaxVersion = 1;
    settings.InferSyntaxVersion = false;
    settings.AnsiLexer = provider.ConsumeBool();
    settings.PgParser = false;
    settings.PGDisable = true;
    settings.V0Behavior = NSQLTranslation::EV0Behavior::Disable;
    settings.V0ForceDisable = true;
    settings.WarnOnV0 = false;

    settings.AssumeYdbOnClusterWithSlash = provider.ConsumeBool();
    settings.EndOfQueryCommit = provider.ConsumeBool();
    settings.UnicodeLiterals = provider.ConsumeBool();
    settings.PgSortNulls = provider.ConsumeBool();
    settings.AutoParametrizeEnabled = provider.ConsumeBool();
    settings.AutoParametrizeValuesStmt = provider.ConsumeBool();

    return settings;
}

void ConsumeYqlAst(const NYql::TAstParseResult& result) {
    if (!result.Root) {
        return;
    }

    const TString text = result.Root->ToString(AstPrintFlags);
    auto reparsed = NYql::ParseAst(text);
    Y_ABORT_UNLESS(reparsed.Root, "printed YQL AST does not reparse");
    (void)reparsed.Root->ToString(AstPrintFlags);
}

TMaybe<TString> TryTranslateToText(const TString& query, const NSQLTranslation::TTranslationSettings& settings) {
    try {
        auto result = NSQLTranslation::SqlToYql(GetTranslators(), query, settings);
        ConsumeYqlAst(result);
        if (!result.Root) {
            return Nothing();
        }

        return result.Root->ToString(AstPrintFlags);
    } catch (...) {
        return Nothing();
    }
}

bool TryParseProtoAst(const TString& query, const NSQLTranslation::TTranslationSettings& settings) {
    NYql::TIssues issues;
    ui16 actualSyntaxVersion = 0;
    google::protobuf::Message* protoAst = nullptr;
    try {
        protoAst = NSQLTranslation::SqlAST(
            GetTranslators(),
            query,
            settings.File,
            issues,
            settings.MaxErrors,
            settings,
            &actualSyntaxVersion);
    } catch (...) {
        return false;
    }

    if (!protoAst) {
        return false;
    }

    (void)actualSyntaxVersion;
    (void)protoAst->SerializeAsString();

    try {
        NSQLTranslation::TSQLHints hints;
        auto translated = NSQLTranslation::SqlASTToYql(GetTranslators(), query, *protoAst, hints, settings);
        ConsumeYqlAst(translated);
    } catch (...) {
    }

    return true;
}

void ExerciseLexer(const TString& query, const NSQLTranslation::TTranslationSettings& settings) {
    try {
        NYql::TIssues issues;
        ui16 actualSyntaxVersion = 0;
        auto lexer = NSQLTranslation::SqlLexer(GetTranslators(), query, issues, settings, &actualSyntaxVersion);
        if (!lexer) {
            return;
        }

        NSQLTranslation::TParsedTokenList tokens;
        if (NSQLTranslation::Tokenize(*lexer, query, settings.File, tokens, issues, settings.MaxErrors)) {
            TStringStream out;
            (void)NSQLTranslation::OutputTokens(out, tokens.begin(), tokens.end());
        }
    } catch (...) {
    }
}

void ExerciseStatements(const TString& query, const NSQLTranslation::TTranslationSettings& settings) {
    try {
        ui16 actualSyntaxVersion = 0;
        TVector<NYql::TStmtParseInfo> stmtInfo;
        auto statements = NSQLTranslation::SqlToAstStatements(
            GetTranslators(),
            query,
            settings,
            nullptr,
            &actualSyntaxVersion,
            &stmtInfo);

        (void)actualSyntaxVersion;
        for (const auto& statement : statements) {
            ConsumeYqlAst(statement);
        }
    } catch (...) {
    }
}

void CheckPrettyFormat(
    NSQLFormat::ISqlFormatter& formatter,
    const TString& query,
    const NSQLTranslation::TTranslationSettings& settings,
    bool originalProtoOk,
    const TMaybe<TString>& originalAstText)
{
    NYql::TIssues issues;
    TString formatted;
    bool formattedOk = false;
    try {
        formattedOk = formatter.Format(query, formatted, issues, NSQLFormat::EFormatMode::Pretty);
    } catch (...) {
        return;
    }

    if (!formattedOk) {
        return;
    }
    if (formatted.size() > MaxGeneratedQuerySize) {
        return;
    }

    NYql::TIssues secondIssues;
    TString formattedAgain;
    bool formattedAgainOk = false;
    try {
        formattedAgainOk = formatter.Format(formatted, formattedAgain, secondIssues, NSQLFormat::EFormatMode::Pretty);
    } catch (...) {
    }

    Y_ABORT_UNLESS(formattedAgainOk, "pretty-formatted SQL does not format again");
    Y_ABORT_UNLESS(formatted == formattedAgain, "SQL formatter is not deterministic");

    if (originalProtoOk) {
        Y_ABORT_UNLESS(TryParseProtoAst(formatted, settings), "pretty-formatted SQL does not reparse");
    }

    const auto formattedAstText = TryTranslateToText(formatted, settings);
    if (originalAstText.Defined() && formattedAstText.Defined()) {
        Y_ABORT_UNLESS(*originalAstText == *formattedAstText, "pretty formatter changed translated YQL AST");
    }
}

void ExerciseObfuscation(
    NSQLFormat::ISqlFormatter& formatter,
    const TString& query,
    const NSQLTranslation::TTranslationSettings& settings)
{
    NYql::TIssues issues;
    TString obfuscated;
    bool obfuscatedOk = false;
    try {
        obfuscatedOk = formatter.Format(query, obfuscated, issues, NSQLFormat::EFormatMode::Obfuscate);
    } catch (...) {
        return;
    }

    if (!obfuscatedOk) {
        return;
    }
    if (obfuscated.size() > MaxGeneratedQuerySize) {
        return;
    }

    Y_ABORT_UNLESS(TryParseProtoAst(obfuscated, settings), "obfuscated SQL does not reparse");

    NYql::TIssues prettyIssues;
    TString pretty;
    bool prettyOk = false;
    try {
        prettyOk = formatter.Format(obfuscated, pretty, prettyIssues, NSQLFormat::EFormatMode::Pretty);
    } catch (...) {
    }

    Y_ABORT_UNLESS(prettyOk, "obfuscated SQL does not pretty-format");
}

void ExerciseQuery(const TString& query, const NSQLTranslation::TTranslationSettings& settings) {
    if (query.size() > MaxGeneratedQuerySize) {
        return;
    }

    ExerciseLexer(query, settings);
    const bool protoOk = TryParseProtoAst(query, settings);
    const auto astText = TryTranslateToText(query, settings);
    ExerciseStatements(query, settings);

    auto formatter = NSQLFormat::MakeSqlFormatter(GetLexers(), GetParsers(), settings);
    CheckPrettyFormat(*formatter, query, settings, protoOk, astText);
    ExerciseObfuscation(*formatter, query, settings);

    try {
        const TString mutated = NSQLFormat::MutateQuery(GetLexers(), query, settings);
        if (mutated != query && mutated.size() <= MaxGeneratedQuerySize) {
            CheckPrettyFormat(*formatter, mutated, settings, TryParseProtoAst(mutated, settings), TryTranslateToText(mutated, settings));
        }
    } catch (...) {
    }
}

TVector<TString> MakeQueries(FuzzedDataProvider& provider) {
    const TString input = provider.ConsumeRemainingBytesAsString();
    const TStringBuf literalSource(input.data(), Min(input.size(), MaxLiteralBytes));
    const TString literal = QuoteSqlString(literalSource);
    const TString identifier = MakeIdentifier(literalSource);

    TVector<TString> queries;
    queries.push_back(input);
    queries.push_back(TStringBuilder() << "SELECT " << literal << " AS " << identifier << ";\n");
    queries.push_back(TStringBuilder() << "$" << identifier << " = " << literal << ";\nSELECT $" << identifier << ";\n");
    queries.push_back(TStringBuilder() << "USE plato;\nSELECT * FROM Input WHERE " << identifier << " = " << literal << ";\n");
    queries.push_back(TStringBuilder() << "USE ydb;\nCREATE TABLE " << identifier << " (id Uint64 NOT NULL, value String, PRIMARY KEY (id));\n");
    return queries;
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    if (size > MaxInputSize) {
        return 0;
    }

    google::protobuf::Arena arena;
    FuzzedDataProvider provider(data, size);
    const auto settings = MakeSettings(provider, arena);

    for (const auto& query : MakeQueries(provider)) {
        ExerciseQuery(query, settings);
    }

    return 0;
}
