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

        const auto* astResult = state.ParseSExpr(res.Issues);
        res.Success = astResult && astResult->IsOk();

        return res;
    }

    class TPGParseEventsHandler: public IPGParseEvents {
    public:
        explicit TPGParseEventsHandler(TCheckResponse& res)
            : Res_(res)
        {
        }

        void OnResult(const List* raw) final {
            Y_UNUSED(raw);
            Res_.Success = true;
        }

        void OnError(const TIssue& issue) final {
            Res_.Issues.AddIssue(issue);
        }

    private:
        TCheckResponse& Res_;
    };

    TCheckResponse RunPg(const TChecksRequest& request, TCheckState& state) {
        Y_UNUSED(request);
        TCheckResponse res{.CheckName = GetCheckName()};

        const auto* pgResult = state.ParsePg(res.Issues);
        if (pgResult) {
            TPGParseEventsHandler handler(res);
            pgResult->Visit(handler);
        }

        return res;
    }

    TCheckResponse RunYql(const TChecksRequest& request, TCheckState& state) {
        Y_UNUSED(request);
        TCheckResponse res{.CheckName = GetCheckName()};

        auto* msg = state.ParseSql(res.Issues);
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
