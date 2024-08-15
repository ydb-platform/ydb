#pragma once

#include <ydb/library/yql/providers/common/token_accessor/client/factory.h>
#include <ydb/public/sdk/cpp/client/ydb_types/credentials/credentials.h>

namespace NYql {

struct TS3Credentials {
    struct TAuthInfo {
    public:
        TString GetAwsUserPwd() const;
        TString GetAwsSigV4() const;
        TString GetToken() const;

    public:
        TString Token;
        TString AwsAccessKey;
        TString AwsAccessSecret;
        TString AwsRegion;
    };

    TS3Credentials() = default;
    TS3Credentials(ISecuredServiceAccountCredentialsFactory::TPtr factory, const TString& structuredTokenJson, bool addBearerToToken = false);

    TAuthInfo GetAuthInfo() const;

    bool operator<(const TS3Credentials& other) const;
    friend IOutputStream& operator<<(IOutputStream& stream, const TS3Credentials& credentials);

private:
    TString StructuredTokenJson;
    NYdb::TCredentialsProviderPtr CredentialsProvider;
    TS3Credentials::TAuthInfo AuthInfo;
};

TS3Credentials::TAuthInfo GetAuthInfo(ISecuredServiceAccountCredentialsFactory::TPtr factory, const TString& structuredTokenJson);

}
