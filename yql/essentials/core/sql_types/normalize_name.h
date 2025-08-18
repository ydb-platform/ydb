#pragma once

#include <yql/essentials/core/issue/yql_issue.h>

#include <util/generic/string.h>
#include <util/generic/fwd.h>

namespace NYql {

    TMaybe<TIssue> NormalizeName(TPosition position, TString& name);

    TString NormalizeName(const TStringBuf& name);

} // namespace NYql
