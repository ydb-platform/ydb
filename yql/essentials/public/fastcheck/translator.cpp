#include "check_runner.h"
#include "check_state.h"

#include "settings.h"
#include "utils.h"

#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/parser/pg_wrapper/interface/parser.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>

namespace NYql::NFastCheck {

namespace {

class TTranslatorRunner: public TCheckRunnerBase {
public:
    TString GetCheckName() const final {
        return "translator";
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
        Y_UNUSED(request);
        TCheckResponse res{.CheckName = GetCheckName()};

        const auto* astResult = state.TranslateSExpr(res.Issues);
        res.Success = astResult && astResult->IsOk();

        return res;
    }

    TCheckResponse RunPg(const TChecksRequest& request, TCheckState& state) {
        Y_UNUSED(request);
        TCheckResponse res{.CheckName = GetCheckName()};

        const auto* astResult = state.TranslatePg(res.Issues);
        res.Success = astResult && astResult->IsOk();

        return res;
    }

    TCheckResponse RunYql(const TChecksRequest& request, TCheckState& state) {
        Y_UNUSED(request);
        TCheckResponse res{.CheckName = GetCheckName()};

        const auto* astResult = state.TranslateSql(res.Issues);
        res.Success = astResult && astResult->IsOk();

        return res;
    }
};

} // namespace

std::unique_ptr<ICheckRunner> MakeTranslatorRunner() {
    return std::make_unique<TTranslatorRunner>();
}

} // namespace NYql::NFastCheck
