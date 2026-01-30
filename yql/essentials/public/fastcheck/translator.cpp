#include "check_runner.h"

#include "settings.h"
#include "utils.h"

#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_graph_transformer.h>
#include <yql/essentials/core/type_ann/type_ann_expr.h>
#include <yql/essentials/sql/v1/sql.h>
#include <yql/essentials/sql/v1/lexer/antlr4/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4_ansi/lexer.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4/proto_parser.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4_ansi/proto_parser.h>
#include <yql/essentials/sql/settings/translation_settings.h>
#include <yql/essentials/parser/pg_wrapper/interface/parser.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>
#include <yql/essentials/providers/config/yql_config_provider.h>

#include <library/cpp/resource/resource.h>

namespace NYql {
namespace NFastCheck {

namespace {

NJson::TJsonValue LoadJsonResource(TStringBuf filename) {
    TString text;
    Y_ENSURE(NResource::FindExact(filename, &text), filename);
    return NJson::ReadJsonFastTree(text);
}

TUdfFilter LoadDefaultUdfFilter() {
    auto json = LoadJsonResource("udfs_basic.json");
    return ParseUdfFilter(json);
}

struct TDefaultUdfFilterLoader {
    TDefaultUdfFilterLoader()
        : Data(LoadDefaultUdfFilter())
    {
    }

    TUdfFilter Data;
};

const TUdfFilter& GetDefaultUdfFilter() {
    return Singleton<TDefaultUdfFilterLoader>()->Data;
}

class TTranslatorRunner: public TCheckRunnerBase {
public:
    TString GetCheckName() const final {
        return "translator";
    }

    TCheckResponse DoRun(const TChecksRequest& request) final {
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
        if (!request.WithTypeCheck) {
            // no separate check for translator here
            return TCheckResponse{.CheckName = GetCheckName(), .Success = true};
        }

        TIssues issues;
        TAstParseResult res = ParseAst(request.Program);
        issues.AddIssues(res.Issues);
        bool success = res.IsOk();
        if (success && request.WithTypeCheck) {
            success = DoTypeCheck(res.Root, request.LangVer, issues);
        }

        return TCheckResponse{
            .CheckName = GetCheckName(),
            .Success = success,
            .Issues = issues};
    }

    TCheckResponse RunPg(const TChecksRequest& request) {
        google::protobuf::Arena arena;
        NSQLTranslation::TTranslationSettings settings;
        settings.Arena = &arena;
        settings.PgParser = true;
        FillClusters(request, settings);

        TIssues issues;
        auto res = NSQLTranslationPG::PGToYql(request.Program, settings);
        issues.AddIssues(res.Issues);
        bool success = res.IsOk();
        if (success && request.WithTypeCheck) {
            success = DoTypeCheck(res.Root, request.LangVer, issues);
        }

        return TCheckResponse{
            .CheckName = GetCheckName(),
            .Success = success,
            .Issues = issues};
    }

    TCheckResponse RunYql(const TChecksRequest& request) {
        google::protobuf::Arena arena;
        NSQLTranslation::TTranslationSettings settings;
        settings.Arena = &arena;
        settings.File = request.File;
        FillClusters(request, settings);
        settings.EmitReadsForExists = true;
        settings.Antlr4Parser = true;
        settings.AnsiLexer = request.IsAnsiLexer;
        settings.SyntaxVersion = request.SyntaxVersion;
        settings.Flags = TranslationFlags();
        settings.LangVer = request.LangVer;
        if (!request.UdfFilter) {
            settings.UdfFilter = &GetDefaultUdfFilter().Modules;
        } else {
            settings.UdfFilter = &request.UdfFilter->Modules;
        }

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

        TIssues issues;
        if (!ParseTranslationSettings(request.Program, settings, issues)) {
            return TCheckResponse{
                .CheckName = GetCheckName(),
                .Success = false,
                .Issues = issues};
        }

        NSQLTranslationV1::TLexers lexers;
        lexers.Antlr4 = NSQLTranslationV1::MakeAntlr4LexerFactory();
        lexers.Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiLexerFactory();
        NSQLTranslationV1::TParsers parsers;
        parsers.Antlr4 = NSQLTranslationV1::MakeAntlr4ParserFactory();
        parsers.Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiParserFactory();

        auto res = NSQLTranslationV1::SqlToYql(lexers, parsers, request.Program, settings);
        issues.AddIssues(res.Issues);
        bool success = res.IsOk();
        if (success && request.WithTypeCheck) {
            success = DoTypeCheck(res.Root, request.LangVer, issues);
        }

        return TCheckResponse{
            .CheckName = GetCheckName(),
            .Success = success,
            .Issues = issues};
    }

    bool DoTypeCheck(TAstNode* astRoot, TLangVersion langver, TIssues& issues) {
        return PartialAnnonateTypes(astRoot, langver, issues, [](TTypeAnnotationContext& newTypeCtx) {
            return CreateConfigProvider(newTypeCtx, nullptr, "", {}, /*forPartialTypeCheck=*/true);
        });
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

} // namespace

std::unique_ptr<ICheckRunner> MakeTranslatorRunner() {
    return std::make_unique<TTranslatorRunner>();
}

} // namespace NFastCheck
} // namespace NYql
