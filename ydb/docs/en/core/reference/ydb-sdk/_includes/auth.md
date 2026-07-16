# Authentication in SDK

As described in the article about [connecting to the {{ ydb-short-name }} server](../../../concepts/connect.md), the client must send an [authentication token](../../../security/authentication.md) with each request. The authentication token is verified by the server; if authentication is successful, the request is authorized and executed; otherwise, an `Unauthenticated` error is returned.

The {{ ydb-short-name }} SDK uses an object responsible for generating such tokens. The SDK provides built-in ways to obtain such an object:

1. Methods with explicit parameter passing, each implementing one of the [authentication modes](../../../security/authentication.md).
2. A method for determining the authentication mode and required parameters from environment variables.

Typically, the token generation object is created before initializing the {{ ydb-short-name }} driver and passed as a parameter to its constructor. The C++ and Go SDKs additionally allow working with multiple databases and token generation objects through a single driver.

If the token generation object is not defined, the driver will not add any authentication information to requests. This approach allows successfully connecting to locally deployed {{ ydb-short-name }} clusters without mandatory authentication configured. If mandatory authentication is configured, requests to the database without an authentication token will be rejected with an authentication error.

## Methods for creating a token generation object {#auth-provider}

You can click on the methods below to go to the source code of the relevant example in the repository. You can also check out the [Authentication code recipes](../../../recipes/ydb-sdk/auth.md).

{% list tabs %}

- C++

  | Mode | Method |
  | --- | --- |
  | Anonymous | [NYdb::CreateInsecureCredentialsProviderFactory()](https://github.com/ydb-platform/ydb-cpp-sdk/blob/main/include/ydb-cpp-sdk/client/types/credentials/credentials.h) |
  | Access Token | [NYdb::TDriverConfig::SetAuthToken(token)](https://github.com/ydb-platform/ydb-cpp-sdk/blob/main/include/ydb-cpp-sdk/client/driver/driver.h) or [NYdb::CreateOAuthCredentialsProviderFactory(token)](https://github.com/ydb-platform/ydb-cpp-sdk/blob/main/include/ydb-cpp-sdk/client/types/credentials/credentials.h) |
  | Metadata | [NYdb::CreateIamCredentialsProviderFactory(NYdb::TIamHost{...})](https://github.com/ydb-platform/ydb-cpp-sdk/blob/main/include/ydb-cpp-sdk/client/iam/iam.h) |
  | Service Account Key | [NYdb::CreateIamJwtFileCredentialsProviderFactory(NYdb::TIamJwtFilename{...})](https://github.com/ydb-platform/ydb-cpp-sdk/blob/main/include/ydb-cpp-sdk/client/iam/iam.h) or [NYdb::CreateIamJwtParamsCredentialsProviderFactory(NYdb::TIamJwtContent{...})](https://github.com/ydb-platform/ydb-cpp-sdk/blob/main/include/ydb-cpp-sdk/client/iam/iam.h), additionally [NYdb::CreateFromSaKeyFile(saKeyFile, connectionString)](https://github.com/ydb-platform/ydb-cpp-sdk/blob/main/include/ydb-cpp-sdk/client/helpers/helpers.h) |
  | Static Credentials | [NYdb::CreateLoginCredentialsProviderFactory(NYdb::TLoginCredentialsParams{...})](https://github.com/ydb-platform/ydb-cpp-sdk/blob/main/include/ydb-cpp-sdk/client/types/credentials/credentials.h) |
  | OAuth 2.0 token exchange | [NYdb::CreateOauth2TokenExchangeCredentialsProviderFactory(NYdb::TOauth2TokenExchangeParams{...})](https://github.com/ydb-platform/ydb-cpp-sdk/blob/main/include/ydb-cpp-sdk/client/types/credentials/oauth2_token_exchange/credentials.h), [NYdb::CreateOauth2TokenExchangeFileCredentialsProviderFactory(configFilePath, tokenEndpoint)](https://github.com/ydb-platform/ydb-cpp-sdk/blob/main/include/ydb-cpp-sdk/client/types/credentials/oauth2_token_exchange/from_file.h) |
  | Determined by environment variables | [NYdb::CreateFromEnvironment(connectionString)](https://github.com/ydb-platform/ydb-cpp-sdk/blob/main/include/ydb-cpp-sdk/client/helpers/helpers.h) |

- Python

  | Mode | Method |
  | --- | --- |
  | Anonymous | [ydb.AnonymousCredentials()](https://github.com/yandex-cloud/ydb-python-sdk/tree/master/examples/anonymous-credentials) |
  | Access Token | [ydb.AccessTokenCredentials(token)](https://github.com/yandex-cloud/ydb-python-sdk/tree/master/examples/access-token-credentials) |
  | Metadata | [ydb.iam.MetadataUrlCredentials()](https://github.com/yandex-cloud/ydb-python-sdk/tree/master/examples/metadata-credentials) |
  | Service Account Key | [ydb.iam.ServiceAccountCredentials.from_file(<br/>key_file, iam_endpoint=None, iam_channel_credentials=None)](https://github.com/yandex-cloud/ydb-python-sdk/tree/master/examples/service-account-credentials) |
  | Static Credentials | [ydb.StaticCredentials.from_user_password(user, password)](https://github.com/ydb-platform/ydb-python-sdk/blob/main/examples/static-credentials/example.py) |
  | OAuth 2.0 token exchange | [ydb.oauth2_token_exchange.Oauth2TokenExchangeCredentials()](https://github.com/ydb-platform/ydb-python-sdk/blob/main/ydb/oauth2_token_exchange/token_exchange.py),<br/>[ydb.oauth2_token_exchange.Oauth2TokenExchangeCredentials.from_file(cfg_file, iam_endpoint=None)](https://github.com/ydb-platform/ydb-python-sdk/blob/main/ydb/oauth2_token_exchange/token_exchange.py) |
  | Determined by environment variables | `ydb.credentials_from_env_variables()` |

- Go

  | Mode | Package | Method |
  | --- | --- | --- |
  | Anonymous | [ydb-go-sdk/v3](https://github.com/ydb-platform/ydb-go-sdk/) | [ydb.WithAnonymousCredentials()](https://github.com/ydb-platform/ydb-go-examples/tree/master/auth/anonymous_credentials) |
  | Access Token | [ydb-go-sdk/v3](https://github.com/ydb-platform/ydb-go-sdk/) | [ydb.WithAccessTokenCredentials(token)](https://github.com/ydb-platform/ydb-go-examples/tree/master/auth/access_token_credentials) |
  | Metadata | [ydb-go-yc](https://github.com/ydb-platform/ydb-go-yc/) | [yc.WithMetadataCredentials(ctx)](https://github.com/ydb-platform/ydb-go-examples/tree/master/auth/metadata_credentials) |
  | Service Account Key | [ydb-go-yc](https://github.com/ydb-platform/ydb-go-yc/) | [yc.WithServiceAccountKeyFileCredentials(key_file)](https://github.com/ydb-platform/ydb-go-examples/tree/master/auth/service_account_credentials) |
  | Static Credentials | [ydb-go-sdk/v3](https://github.com/ydb-platform/ydb-go-sdk/) | [ydb.WithStaticCredentials(user, password)](https://github.com/ydb-platform/ydb-go-examples/tree/master/auth/static_credentials) |
  | OAuth 2.0 token exchange | [ydb-go-sdk/v3](https://github.com/ydb-platform/ydb-go-sdk/) | [ydb.WithOauth2TokenExchangeCredentials(options...)](https://github.com/ydb-platform/ydb-go-sdk/blob/master/options.go),<br/>[ydb.WithOauth2TokenExchangeCredentialsFile(configFilePath)](https://github.com/ydb-platform/ydb-go-sdk/blob/master/options.go) |
  | Determined by environment variables | [ydb-go-sdk-auth-environ](https://github.com/ydb-platform/ydb-go-sdk-auth-environ/) | [environ.WithEnvironCredentials(ctx)](https://github.com/ydb-platform/ydb-go-examples/tree/master/auth/environ) |

- Java

  | Mode | Method |
  | --- | --- |
  | Anonymous | [tech.ydb.core.auth.NopAuthProvider.INSTANCE](https://github.com/ydb-platform/ydb-java-examples/tree/master/auth/anonymous_credentials) |
  | Access Token | [new tech.ydb.core.auth.TokenAuthProvider(accessToken);](https://github.com/ydb-platform/ydb-java-examples/tree/master/auth/access_token_credentials) |
  | Metadata | [tech.ydb.auth.iam.CloudAuthHelper.getMetadataAuthProvider();](https://github.com/ydb-platform/ydb-java-examples/tree/master/auth/metadata_credentials) |
  | Service Account Key | [tech.ydb.auth.iam.CloudAuthHelper.getServiceAccountFileAuthProvider(saKeyFile);](https://github.com/ydb-platform/ydb-java-examples/tree/master/auth/service_account_credentials) |
  | OAuth 2.0 token exchange | [tech.ydb.auth.OAuth2TokenExchangeProvider.fromFile(cfgFile);](https://github.com/ydb-platform/ydb-java-sdk/blob/master/auth-providers/oauth2-provider/src/main/java/tech/ydb/auth/OAuth2TokenExchangeProvider.java) |
  | Determined by environment variables | [tech.ydb.auth.iam.CloudAuthHelper.getAuthProviderFromEnviron();](https://github.com/ydb-platform/ydb-java-examples/tree/master/auth/environ) |

- C#

  | Mode | Method |
  | --- | --- |
  | Anonymous | Nothing needs to be passed for this mode |
  | Access Token | [new TokenProvider(accessToken)](https://github.com/ydb-platform/ydb-dotnet-sdk/blob/main/src/Ydb.Sdk/src/Auth/TokenProvider.cs) |
  | Metadata | [new Ydb.Sdk.Auth.MetadataProvider()](https://github.com/ydb-platform/ydb-dotnet-yc/blob/main/src/Ydb.Sdk.Yc.Auth/src/MetadataProvider.cs) |
  | Service Account Key | [new Ydb.Sdk.Auth.ServiceAccountProvider(saKeyFile);](https://github.com/ydb-platform/ydb-dotnet-yc/blob/main/src/Ydb.Sdk.Yc.Auth/src/ServiceAccountProvider.cs) |
  | OAuth 2.0 token exchange | Not supported |
  | Determined by environment variables | Not supported |

- JavaScript

  | Mode | Method |
  | --- | --- |
  | Anonymous | [AnonymousCredentialsProvider()](https://github.com/ydb-platform/ydb-js-sdk/blob/main/packages/auth/src/anonymous.ts) |
  | Access Token | [AccessTokenCredentialsProvider({ token })](https://github.com/ydb-platform/ydb-js-sdk/blob/main/packages/auth/src/access-token.ts) |
  | Metadata | [MetadataCredentialsProvider()](https://github.com/ydb-platform/ydb-js-sdk/blob/main/packages/auth/src/metadata.ts) |
  | Service Account Key | [ServiceAccountCredentialsProvider.fromFile(saKeyFile)](https://github.com/ydb-platform/ydb-js-sdk/tree/main/examples/auth-yandex-cloud) |
  | Static Credentials | [StaticCredentialsProvider({ username, password }, endpoint)](https://github.com/ydb-platform/ydb-js-sdk/blob/main/packages/auth/src/static.ts) |
  | Determined by environment variables | [EnvironCredentialsProvider(connectionString)](https://github.com/ydb-platform/ydb-js-sdk/tree/main/examples/environ) |

- Rust

  | Mode | Method |
  | --- | --- |
  | Anonymous | ydb::AnonymousCredentials or ydb::AccessTokenCredentials::from("") |
  | Access Token | [ydb::AccessTokenCredentials::from("token")](https://github.com/ydb-platform/ydb-rs-sdk/blob/master/ydb/examples/auth-token.rs) |
  | Metadata | [ydb::MetadataUrlCredentials](https://github.com/ydb-platform/ydb-rs-sdk/blob/master/ydb/examples/auth-ycloud-metadata.rs) |
  | Service Account Key | [ydb::ServiceAccountCredentials](https://github.com/ydb-platform/ydb-rs-sdk/blob/master/ydb/examples/auth-ycloud-serviceaccount.rs) |
  | Static Credentials | [ydb::StaticCredentialsAuth](https://github.com/ydb-platform/ydb-rs-sdk/blob/master/ydb/examples/auth-static-credentials.rs) |
  | Determined by environment variables | [ydb::FromEnvCredentials](https://github.com/ydb-platform/ydb-rs-sdk/blob/master/ydb/examples/auth-ycloud-serviceaccount.rs) |
  | External command execution | [ydb::CommandLineCredentials](https://github.com/ydb-platform/ydb-rs-sdk/blob/master/ydb/examples/auth-yc-cmdline.rs) (for example, for authorization using an [IAM token](https://cloud.yandex.ru/docs/iam/concepts/authorization/iam-token) {{ yandex-cloud }} from a developer's computer ```ydb::CommandLineCredentials.from_cmd("yc iam create-token")```) |

- PHP

  | Mode | Method |
  | --- | --- |
  | Anonymous | [AnonymousAuthentication()](https://github.com/ydb-platform/ydb-php-sdk#anonymous) |
  | Access Token | [AccessTokenAuthentication($accessToken)](https://github.com/ydb-platform/ydb-php-sdk#access-token) |
  | Oauth Token | [OAuthTokenAuthentication($oauthToken)](https://github.com/ydb-platform/ydb-php-sdk#oauth-token) |
  | Metadata | [MetadataAuthentication()](https://github.com/ydb-platform/ydb-php-sdk#metadata-url) |
  | Service Account Key | [JwtWithJsonAuthentication($jsonFilePath)](https://github.com/ydb-platform/ydb-php-sdk#jwt--json-file) or [JwtWithPrivateKeyAuthentication($key_id, $service_account_id, $privateKeyFile)](https://github.com/ydb-platform/ydb-php-sdk#jwt--private-key) |
  | Determined by environment variables | [EnvironCredentials()](https://github.com/ydb-platform/ydb-php-sdk#determined-by-environment-variables) |
  | Static Credentials | [StaticAuthentication($user, $password)](https://github.com/ydb-platform/ydb-php-sdk#static-credentials) |

{% endlist %}

## Procedure for determining the authentication mode and parameters from the environment {#env}

The following algorithm, common to all SDKs, is executed:

1. If the environment variable `YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS` is set, the **System Account Key** authentication mode is used, and the key is loaded from the file specified in this variable.
2. Otherwise, if the environment variable `YDB_ANONYMOUS_CREDENTIALS` is set to 1, the anonymous authentication mode is used.
3. Otherwise, if the environment variable `YDB_METADATA_CREDENTIALS` is set to 1, the **Metadata** authentication mode is used.
4. Otherwise, if the environment variable `YDB_ACCESS_TOKEN_CREDENTIALS` is set, the **Access token** authentication mode is used, and the value of this variable is passed to it.
5. Otherwise, if the environment variable `YDB_OAUTH2_KEY_FILE` is set, the **OAuth 2.0 token exchange** authentication mode is used, and the settings are loaded from the [JSON file](#oauth2-key-file-format) specified in this variable.
6. Otherwise, the **Metadata** authentication mode is used.

Having the **Metadata** mode as the last item in the algorithm allows you to deploy a working application on virtual machines and in Cloud Functions {{ yandex-cloud }} without setting any environment variables.

## Configuration file format for the OAuth 2.0 token exchange authentication mode {#oauth2-key-file-format}

Description of the fields of the JSON configuration file for the **OAuth 2.0 token exchange** authentication method. The set of fields depends on the type of the source token, `JWT` or `FIXED`.

In the table below, `creds_json` denotes a JSON with parameters for the source token that is exchanged for an access token.

Fields not described in this table are ignored.

| Field | Type | Description | Default value / optionality |
| :---: | :---: | :---: | :---: |
| `grant-type` | string | Grant type | `urn:ietf:params:oauth:grant-type:token-exchange` |
| `res` | string \| list of strings | Resource | optional |
| `aud` | string \| list of strings | Audience option for the [token exchange request](https://www.rfc-editor.org/rfc/rfc8693) | optional |
| `scope` | string \| list of strings | Scope | optional |
| `requested-token-type` | string | Type of the received token | `urn:ietf:params:oauth:token-type:access_token` |
| `subject-credentials` | creds_json | Subject credentials | optional |
| `actor-credentials` | creds_json | Actor credentials | optional |
| `token-endpoint` | string | Token endpoint. In the case of {{ ydb-short-name }} CLI, it is overwritten by the `--iam-endpoint` option. | optional |
| **Description of `creds_json` fields (JWT)** |  |  |  |
| `type` | string | Token source type. Set the constant `JWT` |  |
| `alg` | string | JWT signing algorithm. The following algorithms are supported: ES256, ES384, ES512, HS256, HS384, HS512, PS256, PS384, PS512, RS256, RS384, RS512 |  |
| `private-key` | string | (Private) key in PEM format (for `ES*`, `PS*`, `RS*` algorithms) or Base64 (for `HS*` algorithms) for signing |  |
| `kid` | string | Standard JWT field `kid` (key id) | optional |
| `iss` | string | Standard JWT field `iss` (issuer) | optional |
| `sub` | string | Standard JWT field `sub` (subject) | optional |
| `aud` | string | Standard JWT field `aud` (audience) | optional |
| `jti` | string | Standard JWT field `jti` (JWT id) | optional |
| `ttl` | string | JWT token lifetime | `1h` |
| **Description of `creds_json` fields (FIXED)** |  |  |  |
| `type` | string | Token source type. Set the constant `FIXED` |  |
| `token` | string | Token value |  |
| `token-type` | string | Token type value. This value will be passed to the `subject_token_type/actor_token_type` parameter in the [token exchange request](https://www.rfc-editor.org/rfc/rfc8693) |  |

### Example

Example for JWT token exchange


```json
{
   "subject-credentials": {
      "type": "JWT",
      "alg": "RS256",
      "private-key": "-----BEGIN RSA PRIVATE KEY-----\n...-----END RSA PRIVATE KEY-----\n",
      "kid": "my_key_id",
      "sub": "account_id"
   }
}
```


## Features of {{ ydb-short-name }} Python SDK v2 (deprecated version)

{% note warning %}

The behavior of {{ ydb-short-name }} Python SDK v2 (deprecated version) differs from the one described above.

{% endnote %}

* Operation algorithm of the `construct_credentials_from_environ()` function in {{ ydb-short-name }} Python SDK v2:

  - If the environment variable `USE_METADATA_CREDENTIALS` is set to 1, the **Metadata** authentication mode is used
  - Otherwise, if the environment variable `YDB_TOKEN` is set, the **Access Token** authentication mode is used, and the value of this variable is passed to it
  - Otherwise, if the environment variable `SA_KEY_FILE` is set, the **System Account Key** authentication mode is used, and the key is loaded from the file whose name is specified in this variable
  - Otherwise, no authentication information will be added to requests.
* If no object responsible for token generation is passed during driver initialization, the [general procedure](#env) for reading environment variable values is applied.
