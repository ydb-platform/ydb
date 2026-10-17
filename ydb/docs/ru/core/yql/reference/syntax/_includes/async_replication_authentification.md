  * С помощью [токена](../../../../recipes/ydb-sdk/auth-access-token.md):

    * `TOKEN_SECRET_PATH` — [секрет](../../../../concepts/datamodel/secrets.md), содержащий токен.

  * С помощью [логина и пароля](../../../../recipes/ydb-sdk/auth-static.md):

    * `USER` — имя пользователя.
    * `PASSWORD_SECRET_PATH` — [секрет](../../../../concepts/datamodel/secrets.md), содержащий пароль.

  * С помощью [сервисного агента](https://yandex.cloud/ru/docs/iam/concepts/service-control?utm_referrer=about%3Ablank#service-agent):

    * `SERVICE_ACCOUNT_ID` — идентификатор сервисного аккаунта.
    * `INITIAL_TOKEN_SECRET_PATH` — [секрет](../../../../concepts/datamodel/secrets.md), содержащий токен от сервисного аккаунта. Используется для первоначальной инициализации.
