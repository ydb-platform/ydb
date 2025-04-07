#include "check_runner.h"
#include <yql/essentials/sql/v1/sql.h>
#include <yql/essentials/sql/v1/lexer/antlr4/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4_ansi/lexer.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4/proto_parser.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4_ansi/proto_parser.h>
#include <yql/essentials/sql/settings/translation_settings.h>
#include <yql/essentials/parser/pg_wrapper/interface/parser.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>

namespace NYql {
namespace NFastCheck {

namespace {

class TTranslatorRunner : public ICheckRunner {
public:
    TString GetCheckName() const final {
        return "translator";
    }

    TCheckResponse Run(const TChecksRequest& request) final {
        switch (request.Syntax) {
        case ESyntax::SExpr:
            return RunSExpr(request);
        case ESyntax::PG:
            return RunPg(request);
        case ESyntax::YQL:
            return RunYql(request);
        }
    }

private:
    TCheckResponse RunSExpr(const TChecksRequest& request) {
        Y_UNUSED(request);
        // no separate check for translator here
        return TCheckResponse{.CheckName = GetCheckName(), .Success = true};
    }

    TCheckResponse RunPg(const TChecksRequest& request) {
        google::protobuf::Arena arena;
        NSQLTranslation::TTranslationSettings settings;
        settings.Arena = &arena;
        settings.PgParser = true;
        FillClusters(request, settings);

        auto astRes = NSQLTranslationPG::PGToYql(request.Program, settings);
        return TCheckResponse{
            .CheckName = GetCheckName(),
            .Success = astRes.IsOk(),
            .Issues = astRes.Issues
        };
    }

    TCheckResponse RunYql(const TChecksRequest& request) {
        TCheckResponse res {.CheckName = GetCheckName()};
        google::protobuf::Arena arena;
        NSQLTranslation::TTranslationSettings settings;
        settings.Arena = &arena;
        settings.File = request.File;
        FillClusters(request, settings);
        settings.EmitReadsForExists = true;
        settings.Antlr4Parser = true;
        settings.AnsiLexer = request.IsAnsiLexer;
        settings.SyntaxVersion = request.SyntaxVersion;
        settings.Flags.insert({
            "AnsiOrderByLimitInUnionAll",
            "DisableCoalesceJoinKeysOnQualifiedAll",
            "AnsiRankForNullableKeys",
            "DisableUnorderedSubqueries",
            "DisableAnsiOptionalAs",
            "FlexibleTypes",
            "CompactNamedExprs",
            "DistinctOverWindow"
        });

        switch (request.Mode) {
        case EMode::Default:
            settings.AlwaysAllowExports = true;
            break;
        case EMode::Library:
            settings.Mode = NSQLTranslation::ESqlMode::LIBRARY;
            break;
        case EMode::Main:
            break;
        case EMode::View:
            settings.Mode = NSQLTranslation::ESqlMode::LIMITED_VIEW;
            break;
        }

        if (!ParseTranslationSettings(request.Program, settings, res.Issues)) {
            return res;
        }

        NSQLTranslationV1::TLexers lexers;
        lexers.Antlr4 = NSQLTranslationV1::MakeAntlr4LexerFactory();
        lexers.Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiLexerFactory();
        NSQLTranslationV1::TParsers parsers;
        parsers.Antlr4 = NSQLTranslationV1::MakeAntlr4ParserFactory();
        parsers.Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiParserFactory();

        auto astRes = NSQLTranslationV1::SqlToYql(lexers, parsers, request.Program, settings);
        res.Success = astRes.IsOk();
        res.Issues = astRes.Issues;
        return res;
    }

    void FillClusters(const TChecksRequest& request, NSQLTranslation::TTranslationSettings& settings) {
        if (!request.ClusterSystem.empty()) {
            Y_ENSURE(AnyOf(Providers, [&](const auto& p) { return p == request.ClusterSystem; }),
                "Invalid ClusterSystem value: " + request.ClusterSystem);
        }

        switch (request.ClusterMode) {
        case EClusterMode::Many:
            for (const auto& x : request.ClusterMapping) {
                Y_ENSURE(AnyOf(Providers, [&](const auto& p) { return p == x.second; }),
                    "Invalid system: " + x.second);
            }

            settings.ClusterMapping = request.ClusterMapping;
            settings.DynamicClusterProvider = request.ClusterSystem;
            break;
        case EClusterMode::Single:
            Y_ENSURE(!request.ClusterSystem.empty(), "Missing ClusterSystem parameter");
            settings.DefaultCluster = "single";
            settings.ClusterMapping["single"] = request.ClusterSystem;
            settings.DynamicClusterProvider = request.ClusterSystem;
            break;
        case EClusterMode::Unknown:
            settings.DefaultCluster = "single";
            settings.ClusterMapping["single"] = UnknownProviderName;
            settings.DynamicClusterProvider = UnknownProviderName;
            break;
        }
    }
};

}

std::unique_ptr<ICheckRunner> MakeTranslatorRunner() {
    return std::make_unique<TTranslatorRunner>();
}

}
}
