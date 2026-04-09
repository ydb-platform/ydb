  * С помощью [токена](../../../../reference/ydb-sdk/auth.md):

    * `TOKEN_SECRET_PATH` — [секрет](../../../../concepts/datamodel/secrets.md), содержащий токен.

  * С помощью [логина и пароля](../../../../reference/ydb-sdk/auth.md):

    * `USER` — имя пользователя.
    * `PASSWORD_SECRET_PATH` — [секрет](../../../../concepts/datamodel/secrets.md), содержащий пароль.

  * С помощью [делегированного сервисного аккаунта](https://yandex.cloud/ru/docs/iam/concepts/service-control):

    * `SERVICE_ACCOUNT_ID` — идентификатор сервисного аккаунта.
    * `INITIAL_TOKEN_SECRET_PATH` — [секрет](../../../../concepts/datamodel/secrets.md), содержащий токен от сервисного аккаунта. Используется для первоначальной инициализации.
