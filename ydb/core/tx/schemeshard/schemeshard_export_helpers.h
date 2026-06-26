#pragma once

#include <yql/essentials/public/issue/protos/issue_severity.pb.h>

namespace NKikimrSchemeOp {
    enum EPathType : int;
}

namespace NKikimrScheme {
    class TEvDescribeSchemeResult;
}

namespace NKikimr::NSchemeShard {

template <typename T>
void AddIssue(T& response, const TString& message, NYql::TSeverityIds::ESeverityId severity = NYql::TSeverityIds::S_ERROR) {
    auto& issue = *response.AddIssues();
    issue.set_severity(severity);
    issue.set_message(message);
}

NKikimrSchemeOp::EPathType GetPathType(const NKikimrScheme::TEvDescribeSchemeResult& describeResult);

} // namespace NKikimr::NSchemeShard

