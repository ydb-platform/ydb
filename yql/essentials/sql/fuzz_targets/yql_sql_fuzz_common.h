#pragma once

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <yql/essentials/parser/pg_wrapper/interface/parser.h>
#include <yql/essentials/sql/sql.h>
#include <yql/essentials/sql/v0/sql.h>
#include <yql/essentials/sql/v1/sql.h>
#include <yql/essentials/sql/v1/lexer/antlr4/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4_ansi/lexer.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4/proto_parser.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4_ansi/proto_parser.h>

#include <util/generic/strbuf.h>
#include <util/generic/string.h>
#include <util/system/types.h>

#include <array>

namespace NFuzzTargets {

inline TString ConsumeString(FuzzedDataProvider& fdp, size_t maxLen) {
    return fdp.ConsumeBytesAsString(fdp.ConsumeIntegralInRange<size_t>(0, maxLen));
}

inline const NSQLTranslationV1::TLexers& GetV1Lexers() {
    static const NSQLTranslationV1::TLexers Lexers = [] {
        NSQLTranslationV1::TLexers lexers;
        lexers.Antlr4 = NSQLTranslationV1::MakeAntlr4LexerFactory();
        lexers.Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiLexerFactory();
        return lexers;
    }();
    return Lexers;
}

inline const NSQLTranslationV1::TParsers& GetV1Parsers() {
    static const NSQLTranslationV1::TParsers Parsers = [] {
        NSQLTranslationV1::TParsers parsers;
        parsers.Antlr4 = NSQLTranslationV1::MakeAntlr4ParserFactory();
        parsers.Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiParserFactory();
        return parsers;
    }();
    return Parsers;
}

inline const NSQLTranslation::TTranslators& GetSqlTranslators() {
    static const NSQLTranslation::TTranslators Translators(
        NSQLTranslationV0::MakeTranslator(),
        NSQLTranslationV1::MakeTranslator(GetV1Lexers(), GetV1Parsers()),
        NSQLTranslationPG::MakeTranslator());
    return Translators;
}

inline const NSQLTranslation::TTranslators& GetV1OnlyTranslators() {
    static const NSQLTranslation::TTranslators Translators(
        nullptr,
        NSQLTranslationV1::MakeTranslator(GetV1Lexers(), GetV1Parsers()),
        nullptr);
    return Translators;
}

inline NSQLTranslation::TTranslationSettings MakeSqlSettings(FuzzedDataProvider& fdp, bool v1Only) {
    NSQLTranslation::TTranslationSettings settings;
    settings.MaxErrors = fdp.ConsumeIntegralInRange<size_t>(1, 16);
    settings.File = ConsumeString(fdp, 32);
    settings.PathPrefix = ConsumeString(fdp, 16);

    static constexpr std::array<TStringBuf, 3> DefaultClusters = {"", "plato", "/Root"};
    settings.DefaultCluster = TString(DefaultClusters[fdp.ConsumeIntegralInRange<size_t>(0, DefaultClusters.size() - 1)]);

    static constexpr std::array Modes = {
        NSQLTranslation::ESqlMode::QUERY,
        NSQLTranslation::ESqlMode::LIMITED_VIEW,
        NSQLTranslation::ESqlMode::LIBRARY,
        NSQLTranslation::ESqlMode::SUBQUERY,
        NSQLTranslation::ESqlMode::DISCOVERY,
    };
    settings.Mode = Modes[fdp.ConsumeIntegralInRange<size_t>(0, Modes.size() - 1)];

    settings.ClusterMapping["plato"] = "plato";
    settings.ClusterMapping["kikimr"] = "/Root";
    settings.ClusterMapping["ydb"] = "/Root";
    settings.AssumeYdbOnClusterWithSlash = fdp.ConsumeBool();
    settings.EndOfQueryCommit = fdp.ConsumeBool();
    settings.UnicodeLiterals = fdp.ConsumeBool();
    settings.PgSortNulls = fdp.ConsumeBool();
    settings.AutoParametrizeEnabled = fdp.ConsumeBool();
    settings.AutoParametrizeValuesStmt = fdp.ConsumeBool();

    if (fdp.ConsumeBool()) {
        settings.ApplicationName = ConsumeString(fdp, 16);
    }

    const size_t pgParamCount = fdp.ConsumeIntegralInRange<size_t>(0, 4);
    for (size_t i = 0; i < pgParamCount; ++i) {
        settings.PgParameterTypeOids.push_back(fdp.ConsumeIntegralInRange<ui32>(0, 5000));
    }

    if (v1Only) {
        settings.SyntaxVersion = 1;
        settings.InferSyntaxVersion = false;
        settings.AnsiLexer = fdp.ConsumeBool();
        settings.PgParser = false;
        settings.PGDisable = true;
        settings.V0Behavior = NSQLTranslation::EV0Behavior::Disable;
        settings.V0ForceDisable = true;
        settings.WarnOnV0 = false;
    } else {
        settings.SyntaxVersion = fdp.ConsumeIntegralInRange<ui16>(0, 1);
        settings.InferSyntaxVersion = fdp.ConsumeBool();
        settings.AnsiLexer = fdp.ConsumeBool();
        settings.PgParser = fdp.ConsumeBool();
        settings.PGDisable = fdp.ConsumeBool();

        static constexpr std::array Behaviors = {
            NSQLTranslation::EV0Behavior::Silent,
            NSQLTranslation::EV0Behavior::Report,
            NSQLTranslation::EV0Behavior::Disable,
        };
        settings.V0Behavior = Behaviors[fdp.ConsumeIntegralInRange<size_t>(0, Behaviors.size() - 1)];
        settings.V0ForceDisable = fdp.ConsumeBool();
        settings.WarnOnV0 = fdp.ConsumeBool();
    }

    return settings;
}

} // namespace NFuzzTargets
