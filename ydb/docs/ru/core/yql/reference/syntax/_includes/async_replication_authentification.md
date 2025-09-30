  * С помощью [токена](../../../../recipes/ydb-sdk/auth-access-token.md):

    * `TOKEN_SECRET_NAME` — имя [секрета](../../../../concepts/datamodel/secrets.md), содержащего токен.

  * С помощью [логина и пароля](../../../../recipes/ydb-sdk/auth-static.md):

    * `USER` — имя пользователя.
    * `PASSWORD_SECRET_NAME` — имя [секрета](../../../../concepts/datamodel/secrets.md), содержащего пароль.

  * С помощью [делегированного сервисного аккаунта](https://yandex.cloud/ru/docs/iam/concepts/service-control):

    * `SERVICE_ACCOUNT_ID` — идентификатор сервисного аккаунта.
    * `INITIAL_TOKEN_SECRET_NAME` — имя [секрета](../../../../concepts/datamodel/secrets.md), содержащего токен от сервисного аккаунта. Используется для первоначальной инициализации.
