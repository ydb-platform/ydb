#pragma once

#include <ydb/library/yql/providers/common/structured_token/yql_structured_token.h>

namespace NYql {

class TStructuredTokenBuilder {
public:
    TStructuredTokenBuilder();
    explicit TStructuredTokenBuilder(const TStructuredToken& data);
    TStructuredTokenBuilder(TStructuredTokenBuilder&&) = default;

    TStructuredTokenBuilder& SetServiceAccountIdAuth(const TString& accountId, const TString& accountIdSignature);
    TStructuredTokenBuilder& SetBasicAuth(const TString& login, const TString& password);
    TStructuredTokenBuilder& SetIAMToken(const TString& token);
    TStructuredTokenBuilder& SetNoAuth();

    TString ToJson() const;

private:
    TStructuredToken Data;
};

class TStructuredTokenParser {
public:
    explicit TStructuredTokenParser(TStructuredToken&& data);
    bool HasServiceAccountIdAuth() const;
    bool GetServiceAccountIdAuth(TString& accountId, TString& accountIdSignature) const;
    bool HasBasicAuth() const;
    bool GetBasicAuth(TString& login, TString& password) const;
    bool HasIAMToken() const;
    TString GetIAMToken() const;
    bool IsNoAuth() const;

    TStructuredTokenBuilder ToBuilder() const;

private:
    const TStructuredToken Data;
};

TStructuredTokenParser CreateStructuredTokenParser(const TString& content);
TString ComposeStructuredTokenJsonForServiceAccount(const TString& serviceAccountId, const TString& serviceAccountIdSignature, const TString& token);
}
