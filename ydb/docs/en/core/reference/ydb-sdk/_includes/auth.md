# Authentication in the SDK

As described in the article on [connecting to the {{ ydb-short-name }} server](../../../concepts/connect.md), with each request, the client should send an [authentication token](../../../concepts/connect.md#auth). The server verifies the authentication token and, if the authentication is successful, the request is authorized and executed, otherwise the `Unauthenticated` error is returned.

The {{ ydb-short-name }} SDK uses an object that is responsible for generating these tokens. The SDK provides built-in methods for obtaining this object:

1. Methods with parameters passed explicitly, each of the methods implements a certain [authentication mode](../../../concepts/connect.md#auth-modes).
2. A method that determines the authentication mode and the necessary parameters from environment variables.

Usually, you create a token generation object before you initialize the {{ ydb-short-name }} driver, and you pass the object to the driver constructor as a parameter. The C++ and Go SDKs additionally let you work with multiple databases and token generation objects through a single driver.

If a token generation object is not defined, the driver won't add any authentication information to requests. This may let you successfully connect to locally deployed {{ ydb-short-name }} clusters with no mandatory authentication configured. If it is configured, DB requests without an authentication token will be rejected with an authentication error returned.

## Methods for creating token generation objects {#auth-provider}

You can click on any of the methods described below to go to the source code of the relevant example in the repository. You can also learn about the [authentication code recipes](../recipes/auth/index.md).

{% list tabs %}

- Python

  | [Mode](../../../concepts/connect.md#auth-modes) | Method |
  | ----- | ----- |
  | Anonymous | [`ydb.AnonymousCredentials()`](https://github.com/yandex-cloud/ydb-python-sdk/tree/master/examples/anonymous-credentials) |
  | Access Token | [`ydb.AccessTokenCredentials( token )`](https://github.com/yandex-cloud/ydb-python-sdk/tree/master/examples/access-token-credentials) |
  | Metadata | [`ydb.iam.MetadataUrlCredentials()`](https://github.com/yandex-cloud/ydb-python-sdk/tree/master/examples/metadata-credentials) |
  | Service Account Key | [`ydb.iam.ServiceAccountCredentials.from_file(`</br> `key_file, iam_endpoint=None, iam_channel_credentials=None )`](https://github.com/yandex-cloud/ydb-python-sdk/tree/master/examples/service-account-credentials) |
  | Determined by environment variables | `ydb.construct_credentials_from_environ()` |

- Go

  | [Mode](../../../concepts/connect.md#auth-modes) | Package | Method |
  | ----- | ----- | ---- |
  | Anonymous | [ydb-go-sdk/v3](https://github.com/ydb-platform/ydb-go-sdk/blob/master/go.mod) | [`ydb.WithAnonymousCredentials()`](https://github.com/ydb-platform/ydb-go-examples/tree/master/cmd/auth/anonymous_credentials) |
  | Access Token | [ydb-go-sdk/v3](https://github.com/ydb-platform/ydb-go-sdk/blob/master/go.mod) | [`ydb.WithAccessTokenCredentials( token )`](https://github.com/ydb-platform/ydb-go-examples/tree/master/cmd/auth/access_token_credentials) |
  | Metadata | [ydb-go-yc](https://github.com/ydb-platform/ydb-go-yc/blob/master/go.mod) | [`yc.WithMetadataCredentials( ctx )`](https://github.com/ydb-platform/ydb-go-examples/tree/master/cmd/auth/metadata_credentials) |
  | Service Account Key | [ydb-go-yc](https://github.com/ydb-platform/ydb-go-yc/blob/master/go.mod) | [`yc.WithServiceAccountKeyFileCredentials( key_file )`](https://github.com/ydb-platform/ydb-go-examples/tree/master/cmd/auth/service_account_credentials) |
  | Determined by environment variables | [ydb-go-sdk-auth-environ](https://github.com/ydb-platform/ydb-go-sdk-auth-environ/blob/master/go.mod) | [`environ.WithEnvironCredentials(ctx)`](https://github.com/ydb-platform/ydb-go-examples/tree/master/cmd/auth/environ) |

- Java

  | [Mode](../../../concepts/connect.md#auth-modes) | Method |
  | ----- | ----- |
  | Anonymous | [`com.yandex.ydb.core.auth.NopAuthProvider.INSTANCE`](https://github.com/yandex-cloud/ydb-java-sdk/tree/master/examples/auth/anonymous_credentials) |
  | Access Token | [`com.yandex.ydb.auth.iam.CloudAuthProvider.newAuthProvider(`</br> `yandex.cloud.sdk.auth.provider.IamTokenCredentialProvider`</br> `.builder()`</br> `.token(accessToken)`</br> `.build()`</br>`);`](https://github.com/yandex-cloud/ydb-java-sdk/tree/master/examples/auth/access_token_credentials) |
  | Metadata | [`com.yandex.ydb.auth.iam.CloudAuthProvider.newAuthProvider(`</br> `yandex.cloud.sdk.auth.provider.ComputeEngineCredentialProvider`</br> `.builder()`</br> `.build()`</br>`);`](https://github.com/yandex-cloud/ydb-java-sdk/tree/master/examples/auth/metadata_credentials) |
  | Service Account Key | [`com.yandex.ydb.auth.iam.CloudAuthProvider.newAuthProvider(`</br> `yandex.cloud.sdk.auth.provider.ApiKeyCredentialProvider`</br> `.builder()`</br> `.fromFile(Paths.get(saKeyFile))`</br> `.build()`</br>`);`](https://github.com/yandex-cloud/ydb-java-sdk/tree/master/examples/auth/service_account_credentials) |
  | Determined by environment variables | [`com.yandex.ydb.auth.iam.CloudAuthHelper.getAuthProviderFromEnviron();`](https://github.com/yandex-cloud/ydb-java-sdk/tree/master/examples/auth/environ/src/main/java/com/yandex/ydb/example) |

- Node.js

  | [Mode](../../../concepts/connect.md#auth-modes) | Method |
  | ----- | ----- |
  | Anonymous | [`new 'ydb-sdk'.AnonymousAuthService()`](https://github.com/ydb-platform/ydb-nodejs-sdk/tree/main/examples/auth/anonymous-credentials) |
  | Access Token | [`new 'ydb-sdk'.TokenAuthService( accessToken, database )`](https://github.com/ydb-platform/ydb-nodejs-sdk/tree/main/examples/auth/access-token-credentials) |
  | Metadata | [`new 'ydb-sdk'.MetadataAuthService( database )`](https://github.com/ydb-platform/ydb-nodejs-sdk/tree/main/examples/auth/metadata-credentials) |
  | Service Account Key | [`new 'ydb-sdk'.getSACredentialsFromJson( saKeyFile )`](https://github.com/ydb-platform/ydb-nodejs-sdk/tree/main/examples/auth/service-account-credentials) |
  | Determined by environment variables | [`new 'ydb-sdk'.getCredentialsFromEnv( entryPoint, database, logger )`](https://github.com/ydb-platform/ydb-nodejs-sdk/tree/main/examples/auth/environ) |

- Rust

  [Mode](../../../concepts/connect.md#auth-modes) | Method
    ----- | -----
  Anonymous | ydb::StaticToken("")
  Access Token | ydb::StaticToken(token)
  Metadata | ydb::GCEMetadata, ydb::YandexMetadata
  Service Account Key | не поддерживается
  Determined by environment variables | не поддерживается
  Execute external command | ydb.CommandLineYcToken (for example: ```ydb::CommandLineYcToken.from_cmd("yc iam create-token")```)

{% endlist %}

## Procedure for determining the authentication mode and parameters from the environment {#env}

The following algorithm that is the same for all SDKs applies:

1. If the value of the `YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS` environment variable is set, the **System Account Key** authentication mode is used and the key is taken from the file whose name is specified in this variable.
2. Otherwise, if the value of the `YDB_ANONYMOUS_CREDENTIALS` environment variable is set to 1, the anonymous authentication mode is used.
3. Otherwise, if the value of the `YDB_METADATA_CREDENTIALS` environment variable is set to 1, the **Metadata** authentication mode is used.
4. Otherwise, if the value of the `YDB_ACCESS_TOKEN_CREDENTIALS` environment variable is set, the **Access token** authentication mode is used, where the this variable value is passed.
5. Otherwise, the **Metadata** authentication mode is used.

If the last step of the algorithm is selecting the **Metadata** mode, you can deploy a working application on VMs and in Cloud Functions in {{ yandex-cloud }} without setting any environment variables.

## Python SDK specifics

{% note warning %}

The behavior of the Python SDK differs from the one described above.

{% endnote %}

1. The algorithm for determining the authentication mode and the necessary parameters from the environment variables in the `construct_credentials_from_environ()` method differs from the one used in other SDKs:
   - If the value of the `USE_METADATA_CREDENTIALS` environment variable is set to 1, the **Metadata** authentication mode is used.
   - Otherwise, if the value of the `YDB_TOKEN` environment variable is set, the **Access Token** authentication mode is used, where this variable value is passed.
   - Otherwise, if the value of the `SA_KEY_FILE` environment variable is set, the **System Account Key** authentication mode is used and the key is taken from the file whose name is specified in this variable.
   - Or else, no authentication information is added to requests.
2. If no object responsible for generating tokens is passed when initializing the driver, the [general procedure](#env) for reading environment variable values applies.

