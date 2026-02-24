#include "linter.h"
#include "check_runner.h"
#include "check_state.h"

#include <yql/essentials/ast/yql_ast.h>
#include <yql/essentials/ast/yql_expr.h>

#include <util/string/split.h>

#include <functional>

namespace NYql::NFastCheck {

namespace {

class TCheckRunnerFactory: public ICheckRunnerFactory {
public:
    using TRunnerFactoryFunction = std::function<std::unique_ptr<ICheckRunner>()>;

    TCheckRunnerFactory() {
        Registry_.emplace("lexer", MakeLexerRunner);
        Registry_.emplace("parser", MakeParserRunner);
        Registry_.emplace("format", MakeFormatRunner);
        Registry_.emplace("translator", MakeTranslatorRunner);
        Registry_.emplace("typecheck", MakeTypecheckRunner);
        for (const auto& x : Registry_) {
            CheckNames_.emplace(x.first);
        }
    }

    const TSet<TString>& ListChecks() const {
        return CheckNames_;
    }

    std::unique_ptr<ICheckRunner> MakeRunner(const TString& checkName) const final {
        auto ptr = Registry_.FindPtr(checkName);
        if (!ptr) {
            return {};
        }

        return (*ptr)();
    }

private:
    THashMap<TString, TRunnerFactoryFunction> Registry_;
    TSet<TString> CheckNames_;
};

TSet<TString> GetEnabledChecks(const TSet<TString>& allChecks, const TMaybe<TVector<TCheckFilter>>& filters) {
    auto usedFilters = filters.GetOrElse(TVector<TCheckFilter>(1, TCheckFilter{.Include = true, .CheckNameGlob = "*"}));
    TSet<TString> enabledChecks;
    for (const auto& f : usedFilters) {
        if (f.CheckNameGlob == "*") {
            if (f.Include) {
                enabledChecks = allChecks;
            } else {
                enabledChecks.clear();
            }
        } else {
            // TODO full support of glob (* and ?)
            Y_ENSURE(f.CheckNameGlob.find('*') == TString::npos);
            Y_ENSURE(f.CheckNameGlob.find('?') == TString::npos);
            if (f.Include) {
                if (allChecks.contains(f.CheckNameGlob)) {
                    enabledChecks.insert(f.CheckNameGlob);
                }
            } else {
                enabledChecks.erase(f.CheckNameGlob);
            }
        }
    }

    return enabledChecks;
}

TCheckRunnerFactory& GetCheckRunnerFactory() {
    return *Singleton<TCheckRunnerFactory>();
};

} // namespace

TVector<TCheckFilter> ParseChecks(const TString& checks) {
    TVector<TCheckFilter> res;
    for (TStringBuf one : StringSplitter(checks).SplitByString(",")) {
        TCheckFilter f;
        TStringBuf afterPrefix = one;
        if (one.AfterPrefix("-", afterPrefix)) {
            f.Include = false;
        }

        f.CheckNameGlob = afterPrefix;
        res.push_back(f);
    }

    return res;
}

TSet<TString> ListChecks(const TMaybe<TVector<TCheckFilter>>& filters) {
    return GetEnabledChecks(GetCheckRunnerFactory().ListChecks(), filters);
}

TChecksResponse RunChecks(const TChecksRequest& request) {
    auto enabledChecks = GetEnabledChecks(GetCheckRunnerFactory().ListChecks(), request.Filters);
    TChecksResponse res;

    // Create shared state for caching intermediate results between checks
    TCheckState state(request);

    for (const auto& c : enabledChecks) {
        auto checkRunner = GetCheckRunnerFactory().MakeRunner(c);
        if (checkRunner) {
            try {
                res.Checks.push_back(checkRunner->Run(request, state));
            } catch (const std::exception& e) {
                TCheckResponse r;
                r.CheckName = c;
                r.Success = false;
                r.Issues.AddIssue(ExceptionToIssue(e));
                CheckFatalIssues(r.Issues, request.IssueReportTarget);
                res.Checks.push_back(std::move(r));
            }
        }
    }

    return res;
}

} // namespace NYql::NFastCheck
