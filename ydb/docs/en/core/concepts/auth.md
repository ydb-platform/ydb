# Authentication

Once a network connection is established, the server starts to accept client requests with authentication information for processing. The server uses it to identify the client's account and to verify access to execute the query.

The following authentication modes are supported:

* Anonymous access is enabled by default and available immediately when you [install the cluster](../devops/index.md).
* [Authentication through a third-party IAM provider](#iam), for example, [Yandex Identity and Access Management]{% if lang == "en" %}(https://cloud.yandex.com/en/docs/iam/){% endif %}{% if lang == "ru" %}(https://cloud.yandex.ru/docs/iam/){% endif %}.
* Authentication by [username and password](#static-credentials).

## Authentication through a third-party IAM provider {#iam}

* **Anonymous**: Empty token passed in a request.
* **Access Token**: Fixed token set as a parameter for the client (SDK or CLI) and passed in requests.
* **Refresh Token**: [OAuth token](https://auth0.com/blog/refresh-tokens-what-are-they-and-when-to-use-them/) of a user's personal account set as a parameter for the client (SDK or CLI), which the client periodically sends to the IAM API in the background to rotate a token (obtain a new one) to pass in requests.
* **Service Account Key**: Service account attributes and a signature key set as parameters for the client (SDK or CLI), which the client periodically sends to the IAM API in the background to rotate a token (obtain a new one) to pass in requests.
* **Metadata**: Client (SDK or CLI) periodically accesses a local service to rotate a token (obtain a new one) to pass in requests.
* **OAuth 2.0 token exchange** - The client (SDK or CLI) exchanges a token of another type for an access token using the [OAuth 2.0 token exchange protocol](https://www.rfc-editor.org/rfc/rfc8693), then it uses the access token in {{ ydb-short-name }} API requests.

Any owner of a valid token can get access to perform operations; therefore, the principal objective of the security system is to ensure that a token remains private and to protect it from being compromised.

Authentication modes with token rotation, such as **Refresh Token** and **Service Account Key**, provide a higher level of security compared to the **Access Token** mode that uses a fixed token, since only secrets with a short validity period are transmitted to the {{ ydb-short-name }} server over the network.

The highest level of security and performance is provided when using the **Metadata** mode, since it eliminates the need to work with secrets when deploying an application and allows accessing the IAM system and caching a token in advance, before running the application.

When choosing the authentication mode among those supported by the server and environment, follow the recommendations below:

* **You would normally use Anonymous** on self-deployed local {{ ydb-short-name }} clusters that are inaccessible over the network.
* **You would use Access Token** when other modes are not supported on server side or for setup/debugging purposes. It does not require that the client access IAM. However, if the IAM system supports an API for token rotation, fixed tokens issued by this IAM usually have a short validity period, which makes it necessary to update them manually in the IAM system on a regular basis.
* **Refresh Token** can be used when performing one-time manual operations under a personal account, for example, related to DB data maintenance, performing ad-hoc operations in the CLI, or running applications from a workstation. You can manually obtain this token from IAM once to have it last a long time and save it in an environment variable on a personal workstation to use automatically and with no additional authentication parameters on CLI launch.
* **Service Account Key** is mainly used for applications designed to run in environments where the **Metadata** mode is supported, when testing them outside these environments (for example, on a workstation). It can also be used for applications outside these environments, working as an analog of **Refresh Token** for service accounts. Unlike a personal account, service account access objects and roles can be restricted.
* **Metadata** is used when deploying applications in clouds. Currently, this mode is supported on virtual machines and in {{ sf-name }} {{ yandex-cloud }}.

The token to specify in request parameters can be obtained in the IAM system that the specific {{ ydb-short-name }} deployment is associated with. In particular, {{ ydb-short-name }} in {{ yandex-cloud }} uses Yandex.Passport OAuth and {{ yandex-cloud }} service accounts. When using {{ ydb-short-name }} in a corporate context, a company's standard centralized authentication system may be used.

When using modes in which the {{ ydb-short-name }} client accesses the IAM system, the IAM URL that provides an API for issuing tokens can be set additionally. By default, existing SDKs and CLIs attempt to access the {{ yandex-cloud }} IAM API hosted at `iam.api.cloud.yandex.net:443`.

{% include [overlay/auth_choose.md](_includes/connect_overlay/auth_choose.md) %}

## Authenticating by username and password {#static-credentials}

Authentication by username and password includes the following steps:

1. The client accesses the database and presents their username and password to the {{ ydb-short-name }} authentication service.

   You can only use lower case Latin letters and digits in usernames. No restrictions apply to passwords (empty passwords can be used).
1. The authentication service passes authentication data to the {{ ydb-short-name }} authentication component.
1. The component validates authentication data. If the data matches, it generates a token and returns it to the authentication service.

   Tokens accelerate authentication and strengthen security. Tokens have a default lifetime of 12 hours. YDB SDK rotates tokens by accessing the authentication service.

   The username and hashed password are stored in the table inside the authentication component. The password is hashed by the [Argon2]{% if lang == "en" %}(https://en.wikipedia.org/wiki/Argon2){% endif %}{% if lang == "ru" %}(https://ru.wikipedia.org/wiki/Argon2){% endif %} method. In authentication mode, only the system administrator can use a username/password pair to access the table.
1. The authentication system returns the token to the client.
1. The client accesses the database, presenting their token as authentication data.

To enable username/password authentication, use `true` in the `enforce_user_token_requirement` key of the cluster's [configuration file](../deploy/configuration/config.md#auth).

To learn how to manage roles and users, see [{#T}](../security/access-management.md).

<!-- ### API получения токенов IAM {#token-refresh-api}

Для ротации токенов {{ ydb-short-name }} SDK и CLI используют gRPC-запрос к {{ yandex-cloud }} IAM API [IamToken - create]{% if lang == "en" %}(https://cloud.yandex.com/en/docs/iam/api-ref/grpc/iam_token_service#Create){% endif %}{% if lang == "ru" %}(https://cloud.yandex.ru/docs/iam/api-ref/grpc/iam_token_service#Create){% endif %}. В режиме **Refresh Token** заданный параметром OAuth токен передается в атрибуте `yandex_passport_oauth_token`. В режиме **Service Account Key** на основании заданных атрибутов сервисной учетной записи и ключа шифрования формируется короткоживущий JWT-токен и передается в атрибуте `jwt`. Исходный код формирования JWT-токена доступен в составе SDK (например, метод `get_jwt()` в [коде на Python](https://github.com/ydb-platform/ydb-python-sdk/blob/main/ydb/iam/auth.py)).

{{ ydb-short-name }} SDK и CLI позволяют подменить хост для обращения к API получения токенов, что дает возможность реализовать аналогичный API в корпоративных контекстах. -->
