#pragma once

#include <ydb/public/sdk/cpp/client/ydb_types/status/status.h>
#include <ydb/library/yql/public/issue/yql_issue_id.h>

#include <util/generic/fwd.h>

namespace NFq {

////////////////////////////////////////////////////////////////////////////////

TString JoinPath(const TString& basePath, const TString& path);

} // namespace NFq
