#include "yql_token_builder.h" 
 
namespace NYql { 
TStructuredTokenBuilder::TStructuredTokenBuilder() { 
 
} 
 
TStructuredTokenBuilder::TStructuredTokenBuilder(const TStructuredToken& data) 
    : Data(data) 
{ 
} 
 
TStructuredTokenBuilder& TStructuredTokenBuilder::SetServiceAccountIdAuth(const TString& accountId, const TString& accountIdSignature) { 
    Data.SetField("sa_id", accountId); 
    Data.SetField("sa_id_signature", accountIdSignature); 
    return *this; 
} 
 
TStructuredTokenBuilder& TStructuredTokenBuilder::SetBasicAuth(const TString& login, const TString& password) { 
    Data.SetField("basic_login", login); 
    Data.SetField("basic_password", password); 
    return *this; 
} 
 
TStructuredTokenBuilder& TStructuredTokenBuilder::SetIAMToken(const TString& token) { 
    Data.SetField("token", token); 
    return *this; 
} 
 
TStructuredTokenBuilder& TStructuredTokenBuilder::SetNoAuth() { 
    Data.SetField("no_auth", {}); 
    return *this; 
} 
 
TString TStructuredTokenBuilder::ToJson() const { 
    return Data.ToJson(); 
} 
 
/////////////////////////////////////////////////////////////////// 
 
TStructuredTokenParser::TStructuredTokenParser(TStructuredToken&& data) 
    : Data(std::move(data)) 
{ 
} 
 
bool TStructuredTokenParser::HasServiceAccountIdAuth() const { 
    return Data.HasField("sa_id"); 
} 
 
bool TStructuredTokenParser::GetServiceAccountIdAuth(TString& accountId, TString& accountIdSignature) const  { 
    accountId = Data.GetField("sa_id"); 
    accountIdSignature = Data.GetField("sa_id_signature"); 
    return true; 
} 
 
bool TStructuredTokenParser::HasBasicAuth() const { 
    return Data.HasField("basic_login"); 
} 
 
bool TStructuredTokenParser::GetBasicAuth(TString& login, TString& password) const { 
    login = Data.GetField("basic_login"); 
    password = Data.GetField("basic_password"); 
    return true; 
} 
 
bool TStructuredTokenParser::HasIAMToken() const { 
    return Data.HasField("token"); 
} 
 
TString TStructuredTokenParser::GetIAMToken() const { 
    return Data.GetField("token"); 
} 
 
bool TStructuredTokenParser::IsNoAuth() const { 
    return Data.HasField("no_auth"); 
} 
 
TStructuredTokenBuilder TStructuredTokenParser::ToBuilder() const { 
    return TStructuredTokenBuilder(Data); 
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
 
} 
