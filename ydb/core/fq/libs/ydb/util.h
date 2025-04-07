#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status/status.h>
#include <yql/essentials/public/issue/yql_issue_id.h>

#include <util/generic/fwd.h>

namespace NFq {

////////////////////////////////////////////////////////////////////////////////

TString JoinPath(const TString& basePath, const TString& path);

} // namespace NFq
