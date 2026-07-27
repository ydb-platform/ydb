#include "sql2yql.h"

#include <yql/essentials/sql/settings/flags/flags.h>
#include <yql/essentials/sql/sql.h>
#include <yql/essentials/sql/v1/translation/sql.h>
#include <yql/essentials/sql/v1/lexer/antlr4/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4_ansi/lexer.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4/proto_parser.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4_ansi/proto_parser.h>
#include <yql/essentials/parser/pg_wrapper/interface/parser.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>
#include <yql/essentials/providers/common/proto/gateways_config.pb.h>
#include <yql/essentials/providers/common/gateways_utils/gateways_utils.h>

#include <util/generic/yexception.h>

#include <google/protobuf/text_format.h>

namespace NYqlLangModule {

namespace {

void ParseLangVersion(TStringBuf langVersion, NSQLTranslation::TTranslationSettings& settings) {
    if (langVersion.empty()) {
        return;
    }

    YQL_ENSURE(NYql::ParseLangVersion(langVersion, settings.LangVer));
}

void ParseGatewaysConfig(TStringBuf cfg, NSQLTranslation::TTranslationSettings& settings) {
    if (cfg.empty()) {
        return;
    }

    NYql::TGatewaysConfig config;
    if (!google::protobuf::TextFormat::ParseFromString(cfg, &config)) {
        ythrow yexception() << "Failed to parse gateways config";
    }

    NSQLTranslation::TExtendedSqlFlags sqlFlags = NYql::TGatewaySQLFlags::FromTesting(config).ToMap();

    GetClusterMappingFromGateways(config, settings.ClusterMapping);
    NSQLTranslation::ParseTranslationSettings(sqlFlags, settings);
}

void ParseTranslationSettings(const TSql2YqlInput& input, NSQLTranslation::TTranslationSettings& settings) {
    settings.SyntaxVersion = 1;
    ParseLangVersion(input.LangVersion, settings);
    ParseGatewaysConfig(input.GatewaysCfg, settings);
}

NSQLTranslation::TTranslators Translators(TMaybe<size_t> maxParseTreeDepth) {
    NSQLTranslationV1::TLexers lexers;
    lexers.Antlr4 = NSQLTranslationV1::MakeAntlr4LexerFactory();
    lexers.Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiLexerFactory();

    NSQLTranslationV1::TParsers parsers;
    parsers.Antlr4 = NSQLTranslationV1::MakeAntlr4ParserFactory(
        /*isAmbiguityError=*/false,
        /*isAmbiguityDebugging=*/false,
        maxParseTreeDepth);
    parsers.Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiParserFactory(
        /*isAmbiguityError=*/false,
        /*isAmbiguityDebugging=*/false,
        maxParseTreeDepth);

    NSQLTranslation::TTranslators translators(
        /*v0=*/nullptr,
        /*v1=*/NSQLTranslationV1::MakeTranslator(lexers, parsers),
        /*pg=*/NSQLTranslationPG::MakeTranslator());

    return translators;
}

} // namespace

TSql2YqlOutput Sql2Yql(const TSql2YqlInput& input) noexcept try {
    NSQLTranslation::TTranslationSettings settings;
    ParseTranslationSettings(input, settings);

    google::protobuf::Arena arena;
    settings.Arena = &arena;

    NYql::TAstParseResult res = NSQLTranslation::SqlToYql(
        Translators(settings.MaxParseTreeDepth), input.Query, settings);

    TVector<TString> issues(Reserve(res.Issues.Size()));
    for (const auto& issue : res.Issues) {
        issues.push_back(issue.ToString(/*oneLine=*/true));
    }

    return {
        .IsOk = res.IsOk(),
        .Issues = std::move(issues),
    };
} catch (...) {
    NYql::TIssue issue(CurrentExceptionMessage());
    issue.SetCode(NYql::UNEXPECTED_ERROR, NYql::TSeverityIds::S_FATAL);

    return {
        .IsOk = false,
        .Issues = {issue.ToString()},
    };
}

} // namespace NYqlLangModule
