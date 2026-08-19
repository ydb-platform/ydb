#include "check_runner.h"
#include "check_state.h"

#include <yql/essentials/sql/v1/lexer/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4_ansi/lexer.h>
#include <yql/essentials/sql/settings/translation_settings.h>

namespace NYql::NFastCheck {

namespace {

class TLexerRunner: public TCheckRunnerBase {
public:
    ECheckName GetCheckName() const final {
        return ECheckName::Lexer;
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
        Y_UNUSED(state);
        // no separate check for lexer here
        return TCheckResponse{.CheckName = ToString(GetCheckName()), .Success = true};
    }

    TCheckResponse RunPg(const TChecksRequest& request, TCheckState& state) {
        Y_UNUSED(request);
        Y_UNUSED(state);
        // no separate check for lexer here
        return TCheckResponse{.CheckName = ToString(GetCheckName()), .Success = true};
    }

    TCheckResponse RunYql(const TChecksRequest& request, TCheckState& state) {
        Y_UNUSED(request);
        TCheckResponse res{.CheckName = ToString(GetCheckName())};
        res.Success = state.CheckLexer(res.Issues);
        return res;
    }
};

} // namespace

std::unique_ptr<ICheckRunner> MakeLexerRunner() {
    return std::make_unique<TLexerRunner>();
}

} // namespace NYql::NFastCheck
