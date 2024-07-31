#pragma once
#include <ydb/public/sdk/cpp/client/ydb_types/credentials/credentials.h>

#include <util/generic/string.h>

#include <vector>

namespace NYdb {

// Lists supported algorithms for creation of OAuth 2.0 token exchange provider via config file
std::vector<TString> GetSupportedOauth2TokenExchangeJwtAlgorithms();

// Creates OAuth 2.0 token exchange credentials provider factory that exchanges token using standard protocol
// https://www.rfc-editor.org/rfc/rfc8693
//
// Config file must be a valid json file
//
// Fields of json file
//     grant-type:           [string] Grant type option (default: see TOauth2TokenExchangeParams)
//     res:                  [string | list of strings] Resource option (optional)
//     aud:                  [string | list of strings] Audience option for token exchange request (optional)
//     scope:                [string | list of strings] Scope option (optional)
//     requested-token-type: [string] Requested token type option (default: see TOauth2TokenExchangeParams)
//     subject-credentials:  [creds_json] Subject credentials options (optional)
//     actor-credentials:    [creds_json] Actor credentials options (optional)
//     token-endpoint:       [string] Token endpoint. Can be overritten with tokenEndpoint param (if it is not empty)
//
// Fields of creds_json (JWT):
//     type:                 [string] Token source type. Set JWT
//     alg:                  [string] Algorithm for JWT signature. Supported algorithms can be listed with GetSupportedOauth2TokenExchangeJwtAlgorithms()
//     private-key:          [string] (Private) key in PEM format for JWT signature
//     kid:                  [string] Key id JWT standard claim (optional)
//     iss:                  [string] Issuer JWT standard claim (optional)
//     sub:                  [string] Subject JWT standard claim (optional)
//     aud:                  [string | list of strings] Audience JWT standard claim (optional)
//     jti:                  [string] JWT ID JWT standard claim (optional)
//     ttl:                  [string] Token TTL (default: see TJwtTokenSourceParams)
//
// Fields of creds_json (FIXED):
//     type:                 [string] Token source type. Set FIXED
//     token:                [string] Token value
//     token-type:           [string] Token type value. It will become subject_token_type/actor_token_type parameter in token exchange request (https://www.rfc-editor.org/rfc/rfc8693)
//
std::shared_ptr<ICredentialsProviderFactory> CreateOauth2TokenExchangeFileCredentialsProviderFactory(const TString& configFilePath, const TString& tokenEndpoint = {});

} // namespace NYdb
