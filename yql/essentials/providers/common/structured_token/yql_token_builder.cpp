#include "yql_token_builder.h"

namespace NYql {
TStructuredTokenBuilder::TStructuredTokenBuilder() {

}

TStructuredTokenBuilder::TStructuredTokenBuilder(const TStructuredToken& data)
    : Data_(data)
{
}

TStructuredTokenBuilder& TStructuredTokenBuilder::SetServiceAccountIdAuth(const TString& accountId, const TString& accountIdSignature) {
    Data_.SetField("sa_id", accountId);
    Data_.SetField("sa_id_signature", accountIdSignature);
    return *this;
}

TStructuredTokenBuilder& TStructuredTokenBuilder::SetServiceAccountIdAuthWithSecret(const TString& accountId, const TString& accountIdSignatureReference, const TString& accountIdSignature) {
    Data_.SetField("sa_id", accountId);
    Data_.SetField("sa_id_signature_ref", accountIdSignatureReference);
    Data_.SetField("sa_id_signature", accountIdSignature);
    return *this;
}

TStructuredTokenBuilder& TStructuredTokenBuilder::SetBasicAuth(const TString& login, const TString& password) {
    Data_.SetField("basic_login", login);
    Data_.SetField("basic_password", password);
    return *this;
}

TStructuredTokenBuilder& TStructuredTokenBuilder::SetBasicAuthWithSecret(const TString& login, const TString& passwordReference) {
    Data_.SetField("basic_login", login);
    Data_.SetField("basic_password_ref", passwordReference);
    return *this;
}

TStructuredTokenBuilder& TStructuredTokenBuilder::SetTokenAuthWithSecret(const TString& tokenReference, const TString& token) {
    Data_.SetField("token_ref", tokenReference);
    Data_.SetField("token", token);
    return *this;
}

TStructuredTokenBuilder& TStructuredTokenBuilder::SetIAMToken(const TString& token) {
    Data_.SetField("token", token);
    return *this;
}

TStructuredTokenBuilder& TStructuredTokenBuilder::SetNoAuth() {
    Data_.SetField("no_auth", {});
    return *this;
}

TStructuredTokenBuilder& TStructuredTokenBuilder::ReplaceReferences(const std::map<TString, TString>& secrets) {
    if (Data_.HasField("basic_password_ref")) {
        auto reference = Data_.GetField("basic_password_ref");
        Data_.ClearField("basic_password_ref");
        Data_.SetField("basic_password", secrets.at(reference));
    }
    if (Data_.HasField("sa_id_signature_ref")) {
        auto reference = Data_.GetField("sa_id_signature_ref");
        Data_.ClearField("sa_id_signature_ref");
        Data_.SetField("sa_id_signature", secrets.at(reference));
    }
    if (Data_.HasField("token_ref")) {
        auto reference = Data_.GetField("token_ref");
        Data_.ClearField("token_ref");
        Data_.SetField("token", secrets.at(reference));
    }
    return *this;
}

TStructuredTokenBuilder& TStructuredTokenBuilder::RemoveSecrets() {
    Data_.ClearField("basic_password");
    Data_.ClearField("sa_id_signature");
    Data_.ClearField("token");
    return *this;
}

TString TStructuredTokenBuilder::ToJson() const {
    return Data_.ToJson();
}

///////////////////////////////////////////////////////////////////

TStructuredTokenParser::TStructuredTokenParser(TStructuredToken&& data)
    : Data_(std::move(data))
{
}

bool TStructuredTokenParser::HasServiceAccountIdAuth() const {
    return Data_.HasField("sa_id");
}

bool TStructuredTokenParser::GetServiceAccountIdAuth(TString& accountId, TString& accountIdSignature) const  {
    TString accountIdSignatureReference;
    return GetServiceAccountIdAuth(accountId, accountIdSignature, accountIdSignatureReference);
}

bool TStructuredTokenParser::GetServiceAccountIdAuth(TString& accountId, TString& accountIdSignature, TString& accountIdSignatureReference) const {
    accountId = Data_.GetField("sa_id");
    accountIdSignature = Data_.GetFieldOrDefault("sa_id_signature", "");
    accountIdSignatureReference = Data_.GetFieldOrDefault("sa_id_signature_ref", "");
    return true;
}

bool TStructuredTokenParser::HasBasicAuth() const {
    return Data_.HasField("basic_login");
}

bool TStructuredTokenParser::GetBasicAuth(TString& login, TString& password) const {
    TString passwordReference;
    return GetBasicAuth(login, password, passwordReference);
}

bool TStructuredTokenParser::GetBasicAuth(TString& login, TString& password, TString& passwordReference) const {
    login = Data_.GetField("basic_login");
    password = Data_.GetFieldOrDefault("basic_password", "");
    passwordReference = Data_.GetFieldOrDefault("basic_password_ref", "");
    return true;
}

bool TStructuredTokenParser::HasIAMToken() const {
    return Data_.HasField("token");
}

TString TStructuredTokenParser::GetIAMToken() const {
    return Data_.GetField("token");
}

bool TStructuredTokenParser::IsNoAuth() const {
    return Data_.HasField("no_auth");
}

void TStructuredTokenParser::ListReferences(TSet<TString>& references) const {
    if (Data_.HasField("basic_password_ref")) {
        references.insert(Data_.GetField("basic_password_ref"));
    }
    if (Data_.HasField("sa_id_signature_ref")) {
        references.insert(Data_.GetField("sa_id_signature_ref"));
    }
    if (Data_.HasField("token_ref")) {
        references.insert(Data_.GetField("token_ref"));
    }
}

TStructuredTokenBuilder TStructuredTokenParser::ToBuilder() const {
    return TStructuredTokenBuilder(Data_);
}

TStructuredTokenParser CreateStructuredTokenParser(const TString& content = {}) {
    return content ? TStructuredTokenParser(ParseStructuredToken(content)) : TStructuredTokenParser(TStructuredToken({}));
}

TString ComposeStructuredTokenJsonForServiceAccount(const TString& serviceAccountId, const TString& serviceAccountIdSignature, const TString& token) {
    TStructuredTokenBuilder result;

    if (serviceAccountId && serviceAccountIdSignature) {
        result.SetServiceAccountIdAuth(serviceAccountId, serviceAccountIdSignature);
        return result.ToJson();
    }

    if (token) {
        result.SetIAMToken(token);
        return result.ToJson();
    }

    result.SetNoAuth();
    return result.ToJson();
}

TString ComposeStructuredTokenJsonForServiceAccountWithSecret(const TString& serviceAccountId, const TString& serviceAccountIdSignatureSecretName, const TString& serviceAccountIdSignature) {
    TStructuredTokenBuilder result;

    if (serviceAccountId && serviceAccountIdSignatureSecretName && serviceAccountIdSignature) {
        result.SetServiceAccountIdAuthWithSecret(serviceAccountId, serviceAccountIdSignatureSecretName, serviceAccountIdSignature);
        return result.ToJson();
    }

    result.SetNoAuth();
    return result.ToJson();
}

TString ComposeStructuredTokenJsonForBasicAuthWithSecret(const TString& login, const TString& passwordSecretName, const TString& password) {
    TStructuredTokenBuilder result;

    if (login && passwordSecretName && password) {
        result.SetBasicAuth(login, password).SetBasicAuthWithSecret(login, passwordSecretName);
        return result.ToJson();
    }

    result.SetNoAuth();
    return result.ToJson();
}

TString ComposeStructuredTokenJsonForTokenAuthWithSecret(const TString& tokenSecretName, const TString& token) {
    TStructuredTokenBuilder result;

    if (tokenSecretName && token) {
        result.SetTokenAuthWithSecret(tokenSecretName, token);
        return result.ToJson();
    }

    result.SetNoAuth();
    return result.ToJson();
}

}
