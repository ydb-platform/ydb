#include "linter.h"
#include "check_runner.h"

#include <yql/essentials/ast/yql_ast.h>
#include <yql/essentials/ast/yql_expr.h>

#include <functional>

namespace NYql {
namespace NFastCheck {

class TCheckRunnerFactory : public ICheckRunnerFactory {
public:
    using TRunnerFactoryFunction = std::function<std::unique_ptr<ICheckRunner>()>;

    TCheckRunnerFactory() {
        Registry_.emplace("lexer", MakeLexerRunner);
        Registry_.emplace("parser", MakeParserRunner);
        Registry_.emplace("format", MakeFormatRunner);
        Registry_.emplace("translator", MakeTranslatorRunner);
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

TCheckRunnerFactory& GetCheckRunnerFactory() {
    return *Singleton<TCheckRunnerFactory>();
};

const TSet<TString>& ListChecks() {
    return GetCheckRunnerFactory().ListChecks();
}

TChecksResponse RunChecks(const TChecksRequest& request) {
    auto filters = request.Filters.GetOrElse(TVector<TCheckFilter>(1, TCheckFilter{.Include = true, .CheckNameGlob = "*"}));
    const auto& fullChecks = ListChecks();
    TSet<TString> enabledChecks;
    for (const auto& f : filters) {
        if (f.CheckNameGlob == "*") {
            if (f.Include) {
                enabledChecks = fullChecks;
            } else {
                enabledChecks.clear();
            }
        } else {
            // TODO full support of glob (* and ?)
            Y_ENSURE(f.CheckNameGlob.find('*') == TString::npos);
            Y_ENSURE(f.CheckNameGlob.find('?') == TString::npos);
            if (f.Include) {
                if (fullChecks.contains(f.CheckNameGlob)) {
                    enabledChecks.insert(f.CheckNameGlob);
                }
            } else {
                enabledChecks.erase(f.CheckNameGlob);
            }
        }
    }

    TChecksResponse res;
    for (const auto& c : enabledChecks) {
        auto checkRunner = GetCheckRunnerFactory().MakeRunner(c);
        if (checkRunner) {
            res.Checks.push_back(checkRunner->Run(request));
        }
    }

    return res;
}

}
}
