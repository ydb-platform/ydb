#pragma once

#include <ydb/public/api/protos/draft/fq.pb.h>
#include <yql/essentials/public/issue/yql_issue.h>

namespace NFq {

NYql::TIssues ValidateExternalService(const FederatedQuery::ExternalService& service, bool disableCurrentIam);

} // namespace NFq
