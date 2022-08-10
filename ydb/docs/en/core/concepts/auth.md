# Authentication

Once a network connection is established, the server starts to accept client requests with authentication information for processing. The server uses it to identify the client's account and to verify access to execute the query.

The following authentication modes are supported:

* Anonymous access is enabled by default and available immediately when you [install the cluster](../deploy/index.md).
* Authentication through a third-party IAM provider, for example, [Yandex Identity and Access Management]{% if lang == "en" %}(https://cloud.yandex.com/en/docs/iam/){% endif %}{% if lang == "ru" %}(https://cloud.yandex.ru/docs/iam/){% endif %}.
* Authentication by [username and password](#static-credentions).

{% include [overlay/auth_choose.md](_includes/connect_overlay/auth_choose.md) %}

## Authenticating by username and password {#static-credentions}

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

To learn how to manage roles and users, see [{#T}](../cluster/access.md).

<!-- ### API получения токенов IAM {#token-refresh-api}

Для ротации токенов {{ ydb-short-name }} SDK и CLI используют gRPC-запрос к {{ yandex-cloud }} IAM API [IamToken - create]{% if lang == "en" %}(https://cloud.yandex.com/en/docs/iam/api-ref/grpc/iam_token_service#Create){% endif %}{% if lang == "ru" %}(https://cloud.yandex.ru/docs/iam/api-ref/grpc/iam_token_service#Create){% endif %}. В режиме **Refresh Token** заданный параметром OAuth токен передается в атрибуте `yandex_passport_oauth_token`. В режиме **Service Account Key** на основании заданных атрибутов сервисной учетной записи и ключа шифрования формируется короткоживущий JWT-токен и передается в атрибуте `jwt`. Исходный код формирования JWT-токена доступен в составе SDK (например, метод `get_jwt()` в [коде на Python](https://github.com/ydb-platform/ydb-python-sdk/blob/main/ydb/iam/auth.py)).

{{ ydb-short-name }} SDK и CLI позволяют подменить хост для обращения к API получения токенов, что дает возможность реализовать аналогичный API в корпоративных контекстах. -->
