#include "check_runner.h"
#include "check_state.h"

#include "settings.h"
#include "utils.h"

#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_graph_transformer.h>
#include <yql/essentials/core/type_ann/type_ann_expr.h>
#include <yql/essentials/parser/pg_wrapper/interface/parser.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>
#include <yql/essentials/providers/config/yql_config_provider.h>

namespace NYql::NFastCheck {

namespace {

class TTypecheckRunner: public TCheckRunnerBase {
public:
    TString GetCheckName() const final {
        return "typecheck";
    }

    TCheckResponse DoRun(const TChecksRequest& request, TCheckState& state) final {
        switch (state.GetEffectiveSyntax()) {
            case ESyntax::SExpr:
                return RunSExpr(request, state);
            case ESyntax::PG:
                return RunPg(request, state);
            case ESyntax::YQL:
                return RunYql(request, state);
        }
    }

private:
    TCheckResponse RunSExpr(const TChecksRequest& request, TCheckState& state) {
        TCheckResponse res{.CheckName = GetCheckName()};

        const auto* astResult = state.TranslateSExpr(res.Issues);
        if (!astResult || !astResult->IsOk()) {
            res.Success = false;
            return res;
        }

        res.Success = DoTypeCheck(astResult->Root, request.LangVer, res.Issues);

        return res;
    }

    TCheckResponse RunPg(const TChecksRequest& request, TCheckState& state) {
        TCheckResponse res{.CheckName = GetCheckName()};

        const auto* astResult = state.TranslatePg(res.Issues);
        if (!astResult || !astResult->IsOk()) {
            res.Success = false;
            return res;
        }

        res.Success = DoTypeCheck(astResult->Root, request.LangVer, res.Issues);

        return res;
    }

    TCheckResponse RunYql(const TChecksRequest& request, TCheckState& state) {
        TCheckResponse res{.CheckName = GetCheckName()};

        const auto* astResult = state.TranslateSql(res.Issues);
        if (!astResult || !astResult->IsOk()) {
            res.Success = false;
            return res;
        }

        res.Success = DoTypeCheck(astResult->Root, request.LangVer, res.Issues);

        return res;
    }

    bool DoTypeCheck(TAstNode* astRoot, TLangVersion langver, TIssues& issues) {
        return PartialAnnonateTypes(astRoot, langver, issues, [](TTypeAnnotationContext& newTypeCtx) {
            return CreateConfigProvider(newTypeCtx, nullptr, "", {}, /*forPartialTypeCheck=*/true);
        });
    }
};

} // namespace

std::unique_ptr<ICheckRunner> MakeTypecheckRunner() {
    return std::make_unique<TTypecheckRunner>();
}

} // namespace NYql::NFastCheck
