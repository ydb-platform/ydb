#include "credentials.h"

#include <ydb/library/yql/providers/common/structured_token/yql_token_builder.h>
#include <ydb/library/yql/providers/s3/proto/credentials.pb.h>

namespace NYql {

TS3Credentials::TS3Credentials(ISecuredServiceAccountCredentialsFactory::TPtr factory, const TString& structuredTokenJson, bool addBearerToToken)
    : StructuredTokenJson(structuredTokenJson)
{
    if (NYql::IsStructuredTokenJson(structuredTokenJson)) {
        NYql::TStructuredTokenParser parser = NYql::CreateStructuredTokenParser(structuredTokenJson);
        if (parser.HasBasicAuth()) {
            TString serializedParams;
            TString awsAccessSecret;
            parser.GetBasicAuth(serializedParams, awsAccessSecret);
            NS3::TAwsParams params;
            if (params.ParseFromString(serializedParams)) {
                AuthInfo.AwsAccessKey = params.GetAwsAccessKey();
                AuthInfo.AwsAccessSecret = awsAccessSecret;
                AuthInfo.AwsRegion = params.GetAwsRegion();
                return;
            }
        }
    }

    auto providerFactory = CreateCredentialsProviderFactoryForStructuredToken(factory, structuredTokenJson, addBearerToToken);
    CredentialsProvider = providerFactory->CreateProvider();  // Heavy operation, BLOCKs thread until TA reply
}

TS3Credentials::TAuthInfo TS3Credentials::GetAuthInfo() const {
    if (CredentialsProvider) {
        return TS3Credentials::TAuthInfo{.Token = CredentialsProvider->GetAuthInfo()};
    }
    return AuthInfo;
}

bool TS3Credentials::operator<(const TS3Credentials& other) const {
    return StructuredTokenJson < other.StructuredTokenJson;
}

IOutputStream& operator<<(IOutputStream& stream, const TS3Credentials& credentials) {
    const auto& authInfo = credentials.AuthInfo;
    return stream << "TS3Credentials{.ServiceAccountAuth=" << static_cast<bool>(credentials.CredentialsProvider)
                  << ",.AwsUserPwd=<some token with length" << authInfo.GetAwsUserPwd().length() << ">"
                  << ",.AwsSigV4=<some sig with length" << authInfo.GetAwsSigV4().length() << ">}";
}

// string value after AWS prefix should be suitable for passing it to curl as CURLOPT_USERPWD, see details here:
// https://curl.se/libcurl/c/CURLOPT_AWS_SIGV4.html
// CURLOPT_USERPWD = "MY_ACCESS_KEY:MY_SECRET_KEY"
TString TS3Credentials::TAuthInfo::GetAwsUserPwd() const {
    return AwsAccessKey && AwsAccessSecret ? AwsAccessKey + ":" + AwsAccessSecret : TString{};
}


// string value after AWS prefix should be suitable for passing it to curl as CURLOPT_AWS_SIGV4, see details here:
// https://curl.se/libcurl/c/CURLOPT_AWS_SIGV4.html
// CURLOPT_AWS_SIGV4 = "provider1[:provider2[:region[:service]]]"
TString TS3Credentials::TAuthInfo::GetAwsSigV4() const {
    return AwsRegion ? "aws:amz:" + AwsRegion + ":s3" : TString{};
}

TString TS3Credentials::TAuthInfo::GetToken() const {
    return Token;
}

TS3Credentials::TAuthInfo GetAuthInfo(ISecuredServiceAccountCredentialsFactory::TPtr factory, const TString& structuredTokenJson) {
    const TS3Credentials credentials(factory, structuredTokenJson);
    return credentials.GetAuthInfo();
}

}
