# Authentication in the SDK

As we discussed in the [{{ ydb-short-name }} server connection](../../../concepts/connect.md) article, the client must add an [authentication token](../../../concepts/auth.md) to each request. The authentication token is checked by the server. If the authentication is successful, the request is authorized and executed. Otherwise, the `Unauthenticated` error returns.

The {{ ydb-short-name }} SDK uses an object that is responsible for generating these tokens. SDK provides built-in methods for getting such an object:

1. The methods that pass parameters explicitly, with each method implementing a certain [authentication mode](../../../concepts/auth.md).
2. The method that determines the authentication mode and relevant parameters based on environmental variables.

Usually, you create a token generation object before you initialize the {{ ydb-short-name }} driver, and you pass the object to the driver constructor as a parameter. The C++ and Go SDKs additionally let you work with multiple databases and token generation objects through a single driver.

If the token generation object is not defined, the driver won't add any authentication data to the requests. This approach enables you to successfully connect to locally deployed {{ ydb-short-name }} clusters without enabling mandatory authentication. If you enable mandatory authentication, database requests without an authentication token will be rejected with an authentication error.

## Methods for creating token generation objects {#auth-provider}

You can click any of the methods below to go to the source code of an example in the repository. You can also learn about the [authentication code recipes](../../../recipes/ydb-sdk/auth.md).

{% list tabs %}

- Python

  | Mode | Method |
  | ----- | ----- |
  | Anonymous | [ydb.AnonymousCredentials()](https://github.com/yandex-cloud/ydb-python-sdk/tree/master/examples/anonymous-credentials) |
  | Access Token | [ydb.AccessTokenCredentials(token)](https://github.com/yandex-cloud/ydb-python-sdk/tree/master/examples/access-token-credentials) |
  | Metadata | [ydb.iam.MetadataUrlCredentials()](https://github.com/yandex-cloud/ydb-python-sdk/tree/master/examples/metadata-credentials) |
  | Service Account Key | [ydb.iam.ServiceAccountCredentials.from_file(<br/>key_file, iam_endpoint=None, iam_channel_credentials=None)](https://github.com/yandex-cloud/ydb-python-sdk/tree/master/examples/service-account-credentials) |
  | OAuth 2.0 token exchange | [ydb.oauth2_token_exchange.Oauth2TokenExchangeCredentials()](https://github.com/ydb-platform/ydb-python-sdk/blob/main/ydb/oauth2_token_exchange/token_exchange.py),<br/>[ydb.oauth2_token_exchange.Oauth2TokenExchangeCredentials.from_file(cfg_file, iam_endpoint=None)](https://github.com/ydb-platform/ydb-python-sdk/blob/main/ydb/oauth2_token_exchange/token_exchange.py) |
  | Determined by environment variables | `ydb.credentials_from_env_variables()` |

- Go

  | Mode | Package | Method |
  | ----- | ----- | ---- |
  | Anonymous | [ydb-go-sdk/v3](https://github.com/ydb-platform/ydb-go-sdk/) | [ydb.WithAnonymousCredentials()](https://github.com/ydb-platform/ydb-go-examples/tree/master/auth/anonymous_credentials) |
  | Access Token | [ydb-go-sdk/v3](https://github.com/ydb-platform/ydb-go-sdk/) | [ydb.WithAccessTokenCredentials(token)](https://github.com/ydb-platform/ydb-go-examples/tree/master/auth/access_token_credentials) |
  | Metadata | [ydb-go-yc](https://github.com/ydb-platform/ydb-go-yc/) | [yc.WithMetadataCredentials(ctx)](https://github.com/ydb-platform/ydb-go-examples/tree/master/auth/metadata_credentials) |
  | Service Account Key | [ydb-go-yc](https://github.com/ydb-platform/ydb-go-yc/) | [yc.WithServiceAccountKeyFileCredentials(key_file)](https://github.com/ydb-platform/ydb-go-examples/tree/master/auth/service_account_credentials) |
  | Static Credentials | [ydb-go-sdk/v3](https://github.com/ydb-platform/ydb-go-sdk/) | [ydb.WithStaticCredentials(user, password)](https://github.com/ydb-platform/ydb-go-examples/tree/master/auth/static_credentials) |
  | OAuth 2.0 token exchange | [ydb-go-sdk/v3](https://github.com/ydb-platform/ydb-go-sdk/) | [ydb.WithOauth2TokenExchangeCredentials(options...)](https://github.com/ydb-platform/ydb-go-sdk/blob/master/options.go),<br/>[ydb.WithOauth2TokenExchangeCredentialsFile(configFilePath)](https://github.com/ydb-platform/ydb-go-sdk/blob/master/options.go) |
  | Determined by environment variables | [ydb-go-sdk-auth-environ](https://github.com/ydb-platform/ydb-go-sdk-auth-environ/) | [environ.WithEnvironCredentials(ctx)](https://github.com/ydb-platform/ydb-go-examples/tree/master/auth/environ) |

- Java

  | Mode | Method |
  | ----- | ----- |
  | Anonymous | [tech.ydb.core.auth.NopAuthProvider.INSTANCE](https://github.com/ydb-platform/ydb-java-examples/tree/master/auth/anonymous_credentials) |
  | Access Token | [new tech.ydb.core.auth.TokenAuthProvider(accessToken);](https://github.com/ydb-platform/ydb-java-examples/tree/master/auth/access_token_credentials) |
  | Metadata | [tech.ydb.auth.iam.CloudAuthHelper.getMetadataAuthProvider();](https://github.com/ydb-platform/ydb-java-examples/tree/master/auth/metadata_credentials) |
  | Service Account Key | [tech.ydb.auth.iam.CloudAuthHelper.getServiceAccountFileAuthProvider(saKeyFile);](https://github.com/ydb-platform/ydb-java-examples/tree/master/auth/service_account_credentials) |
  | OAuth 2.0 token exchange | [tech.ydb.auth.OAuth2TokenExchangeProvider.fromFile(cfgFile);](https://github.com/ydb-platform/ydb-java-sdk/blob/master/auth-providers/oauth2-provider/src/main/java/tech/ydb/auth/OAuth2TokenExchangeProvider.java) |
  | Determined by environment variables | [tech.ydb.auth.iam.CloudAuthHelper.getAuthProviderFromEnviron();](https://github.com/ydb-platform/ydb-java-examples/tree/master/auth/environ) |

- Node.js

  | Mode | Method |
  | ----- | ----- |
  | Anonymous | [AnonymousAuthService()](https://github.com/ydb-platform/ydb-nodejs-sdk/tree/main/examples/auth/anonymous-credentials) |
  | Access Token | [TokenAuthService(accessToken, database)](https://github.com/ydb-platform/ydb-nodejs-sdk/tree/main/examples/auth/access-token-credentials) |
  | Metadata | [MetadataAuthService(database)](https://github.com/ydb-platform/ydb-nodejs-sdk/tree/main/examples/auth/metadata-credentials) |
  | Service Account Key | [getSACredentialsFromJson(saKeyFile)](https://github.com/ydb-platform/ydb-nodejs-sdk/tree/main/examples/auth/service-account-credentials) |
  | Static Credentials | [StaticCredentialsAuthService(user, password, endpoint)](https://github.com/ydb-platform/ydb-nodejs-sdk/tree/main/examples/auth/static-credentials) |
  | Determined by environment variables | [getCredentialsFromEnv(entryPoint, database, logger)](https://github.com/ydb-platform/ydb-nodejs-sdk/tree/main/examples/auth/environ) |

- Rust

  | Mode | Method |
  | ----- | ----- |
  | Anonymous | ydb::StaticToken("") |
  | Access Token | ydb::StaticToken(token) |
  | Metadata | ydb::GCEMetadata, ydb::YandexMetadata |
  | Static Credentials | [ydb::StaticCredentialsAuth](https://github.com/ydb-platform/ydb-rs-sdk/blob/master/ydb/examples/auth-static-credentials.rs) |
  | Service Account Key | not supported |
  | Determined by environment variables | not supported |
  | Execution of an external command | ydb.CommandLineYcToken (for example, for authentication using a {{ yandex-cloud }} [IAM token]{% if lang == "ru"%}(https://cloud.yandex.ru/docs/iam/concepts/authorization/iam-token){% endif %}{% if lang == "en" %}(https://cloud.yandex.com/en/docs/iam/concepts/authorization/iam-token){% endif %} from the developer's computer ```ydb::CommandLineYcToken.from_cmd("yc iam create-token")```) |

- PHP

  | Mode | Method |
  | ----- | ----- |
  | Anonymous | [AnonymousAuthentication()](https://github.com/ydb-platform/ydb-php-sdk#anonymous) |
  | Access Token | [AccessTokenAuthentication($accessToken)](https://github.com/ydb-platform/ydb-php-sdk#access-token) |
  | Oauth Token | [OAuthTokenAuthentication($oauthToken)](https://github.com/ydb-platform/ydb-php-sdk#oauth-token) |
  | Metadata | [MetadataAuthentication()](https://github.com/ydb-platform/ydb-php-sdk#metadata-url) |
  | Service Account Key | [JwtWithJsonAuthentication($jsonFilePath)](https://github.com/ydb-platform/ydb-php-sdk#jwt--json-file) or [JwtWithPrivateKeyAuthentication($key_id, $service_account_id, $privateKeyFile)](https://github.com/ydb-platform/ydb-php-sdk#jwt--private-key) |
  | Determined by environment variables | [EnvironCredentials()](https://github.com/ydb-platform/ydb-php-sdk#determined-by-environment-variables) |
  | Static Credentials | [StaticAuthentication($user, $password)](https://github.com/ydb-platform/ydb-php-sdk#static-credentials) |

{% endlist %}

## Procedure for determining the authentication mode and parameters from the environment {#env}

The following algorithm that is the same for all SDKs applies:

1. If the value of the `YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS` environment variable is set, the **System Account Key** authentication mode is used and the key is taken from the file whose name is specified in this variable.
2. Otherwise, if the value of the `YDB_ANONYMOUS_CREDENTIALS` environment variable is set to 1, the anonymous authentication mode is used.
3. Otherwise, if the value of the `YDB_METADATA_CREDENTIALS` environment variable is set to 1, the **Metadata** authentication mode is used.
4. Otherwise, if the value of the `YDB_ACCESS_TOKEN_CREDENTIALS` environment variable is set, the **Access token** authentication mode is used, where the this variable value is passed.
5. Otherwise, if the value of the `YDB_OAUTH2_KEY_FILE` environment variable is set, the **OAuth 2.0 token exchange** authentication mode is used, and the parameters are taken from the [JSON file](#oauth2-key-file-format) specified in this variable.
6. Otherwise, the **Metadata** authentication mode is used.

If the last step of the algorithm is selecting the **Metadata** mode, you can deploy a working application on VMs and in {{ yandex-cloud }} Cloud Functions without setting any environment variables.

## File format for OAuth 2.0 token exchange authentication mode parameters {#oauth2-key-file-format}

Description of fields of JSON file with **OAuth 2.0 token exchange** authentication mode parameters. The set of fields depends on the original token type, `JWT` and `FIXED`.

In the table below, `creds_json` means a JSON with parameters for exchanging the original token for an access token.

Fields not described in this table are ignored.

| Field | Type | Description | Default value/optionality |
|:-----:|:----:|:-----------:|:-------------------------:|
|`grant-type`|string|Grant type|`urn:ietf:params:oauth:grant-type:token-exchange`|
|`res`|string \| list of strings|Resource|optional|
|`aud`|string \| list of strings|Audience option for [token exchange request](https://www.rfc-editor.org/rfc/rfc8693)|optional|
|`scope`|string \| list of strings|Scope|optional|
|`requested-token-type`|string|Requested token type|`urn:ietf:params:oauth:token-type:access_token`|
|`subject-credentials`|creds_json|Subject credentials|optional|
|`actor-credentials`|creds_json|Actor credentials|optional|
|`token-endpoint`|string|Token endpoint. In the case of {{ ydb-short-name }} CLI, it is overridden by the `--iam-endpoint` option.|optional|
|**Description of fields of `creds_json` (JWT)**|||||
|`type`|string|Token source type. Set `JWT`||
|`alg`|string|Algorithm for JWT signature. Supported algorithms: ES256, ES384, ES512, HS256, HS384, HS512, PS256, PS384, PS512, RS256, RS384, RS512||
|`private-key`|string|(Private) key in PEM format (for algorithms `ES*`, `PS*`, `RS*`) or Base64 format (for algorithms `HS*`) for JWT signature||
|`kid`|string|`kid` JWT standard claim (key id)|optional|
|`iss`|string|`iss` JWT standard claim (issuer)|optional|
|`sub`|string|`sub` JWT standard claim (subject)|optional|
|`aud`|string|`aud` JWT standard claim (audience)|optional|
|`jti`|string|`jti` JWT standard claim (JWT id)|optional|
|`ttl`|string|JWT token TTL|`1h`|
|**Description of fields of `creds_json` (FIXED)**|||||
|`type`|string|Token source type. Set `FIXED`||
|`token`|string|Token value||
|`token-type`|string|Token type value. It will become `subject_token_type/actor_token_type` parameter in [token exchange request](https://www.rfc-editor.org/rfc/rfc8693).||

### Example

An example for JWT token exchange

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

## Peculiarities of {{ ydb-short-name }} Python SDK v2 (deprecated version)

{% note warning %}

The behavior of the {{ ydb-short-name }} Python SDK v2 (deprecated version) differs from the above-described version.

{% endnote %}

* The algorithm of the `construct_credentials_from_environ()` function from the {{ ydb-short-name }} Python SDK v2:
  - If the value of the `USE_METADATA_CREDENTIALS` environment variable is set to 1, the **Metadata** authentication mode is used.
  - Otherwise, if the value of the `YDB_TOKEN` environment variable is set, the **Access Token** authentication mode is used, where this variable value is passed.
  - Otherwise, if the value of the `SA_KEY_FILE` environment variable is set, the **System Account Key** authentication mode is used and the key is taken from the file whose name is specified in this variable.
  - Or else, no authentication information is added to requests.
* If no object responsible for generating tokens is passed when initializing the driver, the [general procedure](#env) for reading environment variables applies.