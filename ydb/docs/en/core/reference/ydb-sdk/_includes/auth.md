# Authentication in the SDK

As we discussed in the [{{ ydb-short-name }} server connection](../../../concepts/connect.md) article, the client must add an [authentication token](../../../concepts/auth.md) to each request. The authentication token is checked by the server. If the authentication is successful, the request is authorized and executed. Otherwise, the `Unauthenticated` error returns.

The {{ ydb-short-name }} SDK uses an object that is responsible for generating these tokens. SDK provides built-in methods for getting such an object:

1. The methods that pass parameters explicitly, with each method implementing a certain [authentication mode](../../../concepts/auth.md).
2. The method that determines the authentication mode and relevant parameters based on environmental variables.

Usually, you create a token generation object before you initialize the {{ ydb-short-name }} driver, and you pass the object to the driver constructor as a parameter. The C++ and Go SDKs additionally let you work with multiple databases and token generation objects through a single driver.

If the token generation object is not defined, the driver won't add any authentication data to the requests. This approach enables you to successfully connect to locally deployed {{ ydb-short-name }} clusters without enabling mandatory authentication. If you enable mandatory authentication, database requests without an authentication token will be rejected with an authentication error.

## Methods for creating token generation objects {#auth-provider}

You can click any of the methods below to go to the source code of an example in the repository. You can also learn about the [authentication code recipes](../recipes/auth.md).

{% list tabs %}

- Python

   | Mode | Method |
   ----- | -----
   | Anonymous | [ydb.AnonymousCredentials()](https://github.com/yandex-cloud/ydb-python-sdk/tree/master/examples/anonymous-credentials) |
   | Access Token | [ydb.AccessTokenCredentials( token )](https://github.com/yandex-cloud/ydb-python-sdk/tree/master/examples/access-token-credentials) |
   | Metadata | [ydb.iam.MetadataUrlCredentials()](https://github.com/yandex-cloud/ydb-python-sdk/tree/master/examples/metadata-credentials) |
   | Service Account Key | [ydb.iam.ServiceAccountCredentials.from_file(</br>key_file, iam_endpoint=None, iam_channel_credentials=None )](https://github.com/yandex-cloud/ydb-python-sdk/tree/master/examples/service-account-credentials) |
   | Determined by environment variables | `ydb.construct_credentials_from_environ()` |

- Go

   | Mode | Package | Method |
   ----- | ----- | ----
   | Anonymous | [ydb-go-sdk/v3](https://github.com/ydb-platform/ydb-go-sdk/blob/master/go.mod) | [ydb.WithAnonymousCredentials()](https://github.com/ydb-platform/ydb-go-examples/tree/master/cmd/auth/anonymous_credentials) |
   | Access Token | [ydb-go-sdk/v3](https://github.com/ydb-platform/ydb-go-sdk/blob/master/go.mod) | [ydb.WithAccessTokenCredentials( token )](https://github.com/ydb-platform/ydb-go-examples/tree/master/cmd/auth/access_token_credentials) |
   | Metadata | [ydb-go-yc](https://github.com/ydb-platform/ydb-go-yc/blob/master/go.mod) | [yc.WithMetadataCredentials( ctx )](https://github.com/ydb-platform/ydb-go-examples/tree/master/cmd/auth/metadata_credentials) |
   | Service Account Key | [ydb-go-yc](https://github.com/ydb-platform/ydb-go-yc/blob/master/go.mod) | [yc.WithServiceAccountKeyFileCredentials( key_file )](https://github.com/ydb-platform/ydb-go-examples/tree/master/cmd/auth/service_account_credentials) |
   | Determined by environment variables | [ydb-go-sdk-auth-environ](https://github.com/ydb-platform/ydb-go-sdk-auth-environ/blob/master/go.mod) | [environ.WithEnvironCredentials(ctx)](https://github.com/ydb-platform/ydb-go-examples/tree/master/cmd/auth/environ) |

- Java

   | Mode | Method |
   ----- | -----
   | Anonymous | [tech.ydb.core.auth.NopAuthProvider.INSTANCE](https://github.com/ydb-platform/ydb-java-examples/tree/master/auth/anonymous_credentials) |
   | Access Token | [new tech.ydb.core.auth.TokenAuthProvider(accessToken);](https://github.com/ydb-platform/ydb-java-examples/tree/master/auth/access_token_credentials) |
   | Metadata | [tech.ydb.auth.iam.CloudAuthHelper.getMetadataAuthProvider();](https://github.com/ydb-platform/ydb-java-examples/tree/master/auth/metadata_credentials) |
   | Service Account Key | [tech.ydb.auth.iam.CloudAuthHelper.getServiceAccountFileAuthProvider(saKeyFile);](https://github.com/ydb-platform/ydb-java-examples/tree/master/auth/service_account_credentials) |
   | Determined by environment variables | [tech.ydb.auth.iam.CloudAuthHelper.getAuthProviderFromEnviron();](https://github.com/ydb-platform/ydb-java-examples/tree/master/auth/environ) |

- Node.js

   ```js
   import { AnonymousAuthService, TokenAuthService,
     MetadataAuthService, getSACredentialsFromJson,
     getCredentialsFromEnv } from 'ydb-sdk';
   ```

   | Mode | Method |
   ----- | -----
   | Anonymous | [AnonymousAuthService()](https://github.com/ydb-platform/ydb-nodejs-sdk/tree/main/examples/auth/anonymous-credentials) |
   | Access Token | [TokenAuthService( accessToken, database )](https://github.com/ydb-platform/ydb-nodejs-sdk/tree/main/examples/auth/access-token-credentials) |
   | Metadata | [MetadataAuthService( database )](https://github.com/ydb-platform/ydb-nodejs-sdk/tree/main/examples/auth/metadata-credentials) |
   | Service Account Key | [getSACredentialsFromJson( saKeyFile )](https://github.com/ydb-platform/ydb-nodejs-sdk/tree/main/examples/auth/service-account-credentials) |
   | User+Password | [StaticCredentialsAuthService( user, password, endpoint )](https://github.com/ydb-platform/ydb-nodejs-sdk/tree/main/examples/auth/static-credentials) |
   | Determined by environment variables | [getCredentialsFromEnv( entryPoint, database, logger )](https://github.com/ydb-platform/ydb-nodejs-sdk/tree/main/examples/auth/environ) |

- Rust

   | Mode | Method |
   ----- | -----
   | Anonymous | ydb::StaticToken("") |
   | Access Token | ydb::StaticToken(token) |
   | Metadata | ydb::GCEMetadata, ydb::YandexMetadata |
   | Service Account Key | not supported |
   | Determined by environment variables | not supported |
   | Execution of an external command | ydb.CommandLineYcToken (for example, for authentication using a {{ yandex-cloud }} [IAM token]{% if lang == "ru"%}(https://cloud.yandex.ru/docs/iam/concepts/authorization/iam-token){% endif %}{% if lang == "en" %}(https://cloud.yandex.com/en/docs/iam/concepts/authorization/iam-token){% endif %} from the developer's desktop ```ydb::CommandLineYcToken.from_cmd("yc iam create-token")```) |

{% endlist %}

## Procedure for determining the authentication mode and parameters from the environment {#env}

The following algorithm that is the same for all SDKs applies:

1. If the value of the `YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS` environment variable is set, the **System Account Key** authentication mode is used and the key is taken from the file whose name is specified in this variable.
2. Otherwise, if the value of the `YDB_ANONYMOUS_CREDENTIALS` environment variable is set to 1, the anonymous authentication mode is used.
3. Otherwise, if the value of the `YDB_METADATA_CREDENTIALS` environment variable is set to 1, the **Metadata** authentication mode is used.
4. Otherwise, if the value of the `YDB_ACCESS_TOKEN_CREDENTIALS` environment variable is set, the **Access token** authentication mode is used, where the this variable value is passed.
5. Otherwise, the **Metadata** authentication mode is used.

If the last step of the algorithm is selecting the **Metadata** mode, you can deploy a working application on VMs and in {{ yandex-cloud }} Cloud Functions without setting any environment variables.

## Python SDK specifics

{% note warning %}

The behavior of the Python SDK differs from the one described above.

{% endnote %}

1. The algorithm for determining the authentication mode and the necessary parameters from the environment variables in the `construct_credentials_from_environ()` method differs from the one used in other SDKs:
   - If the value of the `USE_METADATA_CREDENTIALS` environment variable is set to 1, the **Metadata** authentication mode is used.
   - Otherwise, if the value of the `YDB_TOKEN` environment variable is set, the **Access Token** authentication mode is used, where this variable value is passed.
   - Otherwise, if the value of the `SA_KEY_FILE` environment variable is set, the **System Account Key** authentication mode is used and the key is taken from the file whose name is specified in this variable.
   - Or else, no authentication information is added to requests.
2. If no object responsible for generating tokens is passed when initializing the driver, the [general procedure](#env) for reading environment variables applies.
