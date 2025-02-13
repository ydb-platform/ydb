#include "check_runner.h"
#include <yql/essentials/sql/v1/sql.h>
#include <yql/essentials/sql/settings/translation_settings.h>
#include <yql/essentials/parser/pg_wrapper/interface/parser.h>

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
        settings.ClusterMapping = request.ClusterMapping;
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
        settings.ClusterMapping = request.ClusterMapping;
        settings.EmitReadsForExists = true;
        settings.Antlr4Parser = true;
        settings.AnsiLexer = request.IsAnsiLexer;
        settings.SyntaxVersion = request.SyntaxVersion;
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

        auto astRes = NSQLTranslationV1::SqlToYql(request.Program, settings);
        res.Success = astRes.IsOk();
        res.Issues = astRes.Issues;
        return res;
    }
};

}

std::unique_ptr<ICheckRunner> MakeTranslatorRunner() {
    return std::make_unique<TTranslatorRunner>();
}

}
}
