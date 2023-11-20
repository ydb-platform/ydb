#include "aws_credentials.h"

#include <util/string/builder.h>

namespace NYql {

static const TStringBuf AwsCredentialsInternalHeader = "X-Aws-Credentials-Internal-Using:";
static const TStringBuf AwsPrefix = "AWS ";
static const TString AwsCredentialsInternalPrefix = TString{AwsCredentialsInternalHeader} + AwsPrefix;

// string value after AWS prefix should be suitable for passing it to curl as CURLOPT_USERPWD, see details here:
// https://curl.se/libcurl/c/CURLOPT_AWS_SIGV4.html
// CURLOPT_USERPWD = "MY_ACCESS_KEY:MY_SECRET_KEY"
TString ConvertBasicToAwsToken(const TString& accessKey, const TString& accessSecret) {
    return TStringBuilder{} << AwsPrefix << accessKey << ":" << accessSecret;
}

TString PrepareAwsHeader(const TString& token) {
    if (token.StartsWith(AwsPrefix)) {
        return TString(AwsCredentialsInternalHeader) + token;
    }

    return {};
}

TString ExtractAwsCredentials(TSmallVec<TString>& headers) {
    TString result;
    auto it = FindIf(headers, [](const auto& r) { return r.StartsWith(AwsCredentialsInternalPrefix); });
    if (it != headers.end()) {
        result = it->substr(AwsCredentialsInternalPrefix.size());
        headers.erase(it);
    }
    return result;
}

}
