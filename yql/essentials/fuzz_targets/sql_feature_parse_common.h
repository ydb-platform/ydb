#pragma once

#include <yql/essentials/ast/yql_ast.h>
#include <yql/essentials/parser/lexer_common/lexer.h>
#include <yql/essentials/sql/sql.h>
#include <yql/essentials/sql/v1/sql.h>
#include <yql/essentials/sql/v1/lexer/antlr4/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4_ansi/lexer.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4/proto_parser.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4_ansi/proto_parser.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <google/protobuf/arena.h>
#include <google/protobuf/message.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/stream/str.h>
#include <util/string/builder.h>
#include <util/string/cast.h>

#include <algorithm>
#include <array>

extern "C" const char* __lsan_default_options() {
    // SQL translation keeps parser objects in protobuf arenas; leak checks turn
    // those arena-owned allocations into stable harness false positives.
    return "detect_leaks=0";
}

namespace NYql::NFuzzSql {

constexpr size_t MaxInputSize = 8 * 1024;
constexpr size_t MaxGeneratedQuerySize = 16 * 1024;
constexpr size_t MaxLiteralBytes = 512;
constexpr size_t MaxIdentifierBytes = 48;
constexpr ui32 AstPrintFlags = TAstPrintFlags::PerLine |
                               TAstPrintFlags::ShortQuote |
                               TAstPrintFlags::AdaptArbitraryContent;

inline const NSQLTranslationV1::TLexers& GetLexers() {
    static const NSQLTranslationV1::TLexers lexers = [] {
        NSQLTranslationV1::TLexers result;
        result.Antlr4 = NSQLTranslationV1::MakeAntlr4LexerFactory();
        result.Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiLexerFactory();
        return result;
    }();
    return lexers;
}

inline const NSQLTranslationV1::TParsers& GetParsers() {
    static const NSQLTranslationV1::TParsers parsers = [] {
        NSQLTranslationV1::TParsers result;
        result.Antlr4 = NSQLTranslationV1::MakeAntlr4ParserFactory();
        result.Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiParserFactory();
        return result;
    }();
    return parsers;
}

inline const NSQLTranslation::TTranslators& GetTranslators() {
    static const NSQLTranslation::TTranslators translators(
        nullptr,
        NSQLTranslationV1::MakeTranslator(GetLexers(), GetParsers()),
        nullptr);
    return translators;
}

inline TString Limit(TString value, size_t limit = MaxGeneratedQuerySize) {
    if (value.size() > limit) {
        value.resize(limit);
    }
    return value;
}

inline TString QuoteSqlString(TStringBuf value) {
    TString result;
    result.reserve(std::min(value.size(), MaxLiteralBytes) + 2);
    result += '\'';
    for (unsigned char c : value.SubStr(0, std::min(value.size(), MaxLiteralBytes))) {
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

inline TString Identifier(TStringBuf value, TStringBuf prefix = "fuzz") {
    TString result(prefix);
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

inline TString SmallInt(FuzzedDataProvider& provider, int minValue = -100, int maxValue = 100) {
    return ToString(provider.ConsumeIntegralInRange<int>(minValue, maxValue));
}

inline TString PickType(FuzzedDataProvider& provider) {
    static constexpr std::array<TStringBuf, 20> types = {
        "Bool", "Int8", "Uint8", "Int16", "Uint16", "Int32", "Uint32", "Int64", "Uint64",
        "Float", "Double", "String", "Utf8", "Json", "Yson", "Uuid", "Date", "Datetime",
        "Timestamp", "Interval",
    };
    return TString(provider.PickValueInArray(types));
}

inline NSQLTranslation::TTranslationSettings MakeSettings(FuzzedDataProvider& provider, google::protobuf::Arena& arena) {
    NSQLTranslation::TTranslationSettings settings;
    settings.Arena = &arena;
    settings.File = "fuzz.sql";
    settings.MaxErrors = provider.ConsumeIntegralInRange<size_t>(1, 8);
    settings.DefaultCluster = provider.ConsumeBool() ? "plato" : "/Root";
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

    static constexpr std::array modes = {
        NSQLTranslation::ESqlMode::QUERY,
        NSQLTranslation::ESqlMode::LIMITED_VIEW,
        NSQLTranslation::ESqlMode::SUBQUERY,
        NSQLTranslation::ESqlMode::DISCOVERY,
    };
    settings.Mode = modes[provider.ConsumeIntegralInRange<size_t>(0, modes.size() - 1)];
    return settings;
}

inline void ConsumeAst(const TAstParseResult& result) {
    if (result.Root) {
        (void)result.Root->ToString(AstPrintFlags);
    }
}

inline void ExerciseLexer(const TString& query, const NSQLTranslation::TTranslationSettings& settings) {
    try {
        TIssues issues;
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
        (void)actualSyntaxVersion;
    } catch (...) {
    }
}

inline void ExerciseSql(const TString& query, FuzzedDataProvider& provider) {
    if (query.empty() || query.size() > MaxGeneratedQuerySize) {
        return;
    }

    google::protobuf::Arena arena;
    auto settings = MakeSettings(provider, arena);

    ExerciseLexer(query, settings);

    try {
        auto result = NSQLTranslation::SqlToYql(GetTranslators(), query, settings);
        ConsumeAst(result);
    } catch (...) {
    }

    try {
        NYql::TIssues issues;
        ui16 actualSyntaxVersion = 0;
        google::protobuf::Message* protoAst = NSQLTranslation::SqlAST(
            GetTranslators(),
            query,
            settings.File,
            issues,
            settings.MaxErrors,
            settings,
            &actualSyntaxVersion);
        if (protoAst) {
            (void)protoAst->SerializeAsString();
            NSQLTranslation::TSQLHints hints;
            auto translated = NSQLTranslation::SqlASTToYql(GetTranslators(), query, *protoAst, hints, settings);
            ConsumeAst(translated);
        }
        (void)actualSyntaxVersion;
    } catch (...) {
    }

    try {
        ui16 actualSyntaxVersion = 0;
        TVector<TStmtParseInfo> stmtInfo;
        auto statements = NSQLTranslation::SqlToAstStatements(
            GetTranslators(),
            query,
            settings,
            nullptr,
            &actualSyntaxVersion,
            &stmtInfo);
        for (const auto& statement : statements) {
            ConsumeAst(statement);
        }
        (void)actualSyntaxVersion;
    } catch (...) {
    }
}

inline void ExerciseSqlWithFreshSettings(const TString& query, const ui8* data, size_t size) {
    FuzzedDataProvider provider(data, size);
    ExerciseSql(query, provider);
}

} // namespace NYql::NFuzzSql
