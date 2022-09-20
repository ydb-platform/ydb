#pragma once

#include <ydb/library/yql/public/issue/yql_issue.h>

#include <util/generic/string.h>

namespace NYql::NDq {

bool ParseS3ErrorResponse(const TString& response, TString& errorCode, TString& message);
TIssues BuildIssues(long httpCode, const TString& s3ErrorCode, const TString& message);

}
