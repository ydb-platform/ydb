#pragma once

#include <yql/essentials/public/fastcheck/fastcheck.h>
#include <yql/essentials/public/fastcheck/linter.h>

namespace NYql::NFastCheck {

bool CheckProgramImpl(const TString& program, const TOptions& options, TIssues& errors);
TSet<TString> ListChecksImpl(const TMaybe<TVector<TCheckFilter>>& filters);
TChecksResponse RunChecksImpl(const TChecksRequest& request);

} // namespace NYql::NFastCheck
