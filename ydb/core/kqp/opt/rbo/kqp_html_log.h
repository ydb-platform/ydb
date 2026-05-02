#pragma once

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NKikimr {
namespace NKqp {

/// Collects per-stage rule applications and writes a self-contained HTML trace (for debugging the new RBO).
class TKqpOptimizerTraceHtml {
public:
    void AddStage(const TString& name);
    void AddRule(const TString& ruleName, const TString& planText);
    bool GenerateHtml(const TString& filename) const;

private:
    struct TRuleApplication {
        TString RuleName;
        TString PlanText;
    };
    struct TStage {
        TString Name;
        TVector<TRuleApplication> Rules;
    };

    TVector<TStage> Stages_;

    static TString Esc(TStringBuf s);
};

} // namespace NKqp
} // namespace NKikimr
