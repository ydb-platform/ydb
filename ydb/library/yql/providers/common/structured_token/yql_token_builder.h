#pragma once

#include <ydb/library/yql/providers/common/structured_token/yql_structured_token.h>

#include <util/generic/set.h>

namespace NYql {

class TStructuredTokenBuilder {
public:
    TStructuredTokenBuilder();
    explicit TStructuredTokenBuilder(const TStructuredToken& data);
    TStructuredTokenBuilder(TStructuredTokenBuilder&&) = default;

    TStructuredTokenBuilder& SetServiceAccountIdAuth(const TString& accountId, const TString& accountIdSignature);
    TStructuredTokenBuilder& SetServiceAccountIdAuthWithSecret(const TString& accountId, const TString& accountIdSignatureReference, const TString& accountIdSignature);
    TStructuredTokenBuilder& SetBasicAuth(const TString& login, const TString& password);
    TStructuredTokenBuilder& SetBasicAuthWithSecret(const TString& login, const TString& passwordReference);
    TStructuredTokenBuilder& SetTokenAuthWithSecret(const TString& tokenReference, const TString& token);
    TStructuredTokenBuilder& SetIAMToken(const TString& token);
    TStructuredTokenBuilder& SetNoAuth();
    TStructuredTokenBuilder& ReplaceReferences(const std::map<TString, TString>& secrets);
    TStructuredTokenBuilder& RemoveSecrets();

    TString ToJson() const;

private:
    TStructuredToken Data;
};

class TStructuredTokenParser {
public:
    explicit TStructuredTokenParser(TStructuredToken&& data);
    bool HasServiceAccountIdAuth() const;
    bool GetServiceAccountIdAuth(TString& accountId, TString& accountIdSignature) const;
    bool GetServiceAccountIdAuth(TString& accountId, TString& accountIdSignature, TString& accountIdSignatureReference) const;
    bool HasBasicAuth() const;
    bool GetBasicAuth(TString& login, TString& password) const;
    bool GetBasicAuth(TString& login, TString& password, TString& passwordReference) const;
    bool HasIAMToken() const;
    TString GetIAMToken() const;
    bool IsNoAuth() const;
    void ListReferences(TSet<TString>& references) const;

    TStructuredTokenBuilder ToBuilder() const;

private:
    const TStructuredToken Data;
};

TStructuredTokenParser CreateStructuredTokenParser(const TString& content);
TString ComposeStructuredTokenJsonForServiceAccount(const TString& serviceAccountId, const TString& serviceAccountIdSignature, const TString& token);
TString ComposeStructuredTokenJsonForServiceAccountWithSecret(const TString& serviceAccountId, const TString& serviceAccountIdSignatureSecretName, const TString& serviceAccountIdSignature);
TString ComposeStructuredTokenJsonForBasicAuthWithSecret(const TString& login, const TString& passwordSecretName, const TString& password);
TString ComposeStructuredTokenJsonForTokenAuthWithSecret(const TString& tokenSecretName, const TString& token);
}
