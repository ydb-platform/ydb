#include "fastcheck.h"

#include <yql/essentials/public/fastcheck/abi/interface/fastcheck.h>
#include <yql/essentials/public/fastcheck/abi/serialization/result.h>
#include <yql/essentials/public/fastcheck/abi/serialization/serialization.h>
#include <yql/essentials/public/issue/yql_issue_message.h>

namespace NYql::NFastCheck {

bool CheckProgram(const TString& program, const TOptions& options, TIssues& errors) {
    NAbi::NProto::TFastCheckRequest request;
    NAbi::Encode(options, request);
    request.SetProgram(program);
    NAbi::EncodeMap(options.ClusterMapping, *request.MutableClusterMapping());
    NAbi::EncodeMap(options.SqlLibs, *request.MutableSqlLibs());
    const TString data = NAbi::SerializeMessage(request);
    const auto result = NAbi::ReadResult<NAbi::NProto::TFastCheckResult>(
        YqlFastCheckV1(data.data(), data.size()), YqlFastCheckFreeV1,
        YqlFastCheckResultV1, YqlFastCheckErrorV1);
    for (const auto& issue : result.GetIssues()) {
        errors.AddIssue(IssueFromMessage(issue));
    }
    return result.GetSuccess();
}

} // namespace NYql::NFastCheck
