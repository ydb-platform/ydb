#include "check_runner.h"
#include "check_state.h"

#include <yql/essentials/ast/yql_ast.h>
#include <yql/essentials/parser/pg_wrapper/interface/raw_parser.h>

namespace NYql::NFastCheck {

namespace {

class TParserRunner: public TCheckRunnerBase {
public:
    TString GetCheckName() const final {
        return "parser";
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

        const auto* astResult = state.ParseSExpr(&res.Issues);
        res.Success = astResult && astResult->IsOk();

        return res;
    }

    TCheckResponse RunPg(const TChecksRequest& request, TCheckState& state) {
        Y_UNUSED(request);
        TCheckResponse res{.CheckName = GetCheckName()};
        res.Success = state.ParsePg(&res.Issues) != nullptr;

        return res;
    }

    TCheckResponse RunYql(const TChecksRequest& request, TCheckState& state) {
        Y_UNUSED(request);
        TCheckResponse res{.CheckName = GetCheckName()};

        auto* msg = state.ParseSql(&res.Issues);
        if (msg) {
            res.Success = true;
        }

        return res;
    }
};

} // namespace

std::unique_ptr<ICheckRunner> MakeParserRunner() {
    return std::make_unique<TParserRunner>();
}

} // namespace NYql::NFastCheck
