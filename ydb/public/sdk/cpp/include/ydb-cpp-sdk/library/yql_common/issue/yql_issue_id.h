#pragma once

#include <ydb/public/sdk/cpp/src/library/yql_common/issue/protos/issue_severity.pb.h>

namespace NYql {

using TIssueCode = uint32_t;
using ESeverity = NYql::TSeverityIds::ESeverityId;
const TIssueCode DEFAULT_ERROR = 0;
const TIssueCode UNEXPECTED_ERROR = 1;

std::string SeverityToString(ESeverity severity);

}
