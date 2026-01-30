* Using a [token](../../../../recipes/ydb-sdk/auth-access-token.md):

    * `TOKEN_SECRET_NAME` — the name of the [secret](../../../../concepts/datamodel/secrets.md) that contains the token.

* Using a [username and password](../../../../recipes/ydb-sdk/auth-static.md):

    * `USER` — the username.
    * `PASSWORD_SECRET_NAME` — the name of the [secret](../../../../concepts/datamodel/secrets.md) that contains the password.

* Using a [delegated service account](https://yandex.cloud/ru/docs/iam/concepts/service-control):

    * `SERVICE_ACCOUNT_ID` — the identificator of the service account.
    * `INITIAL_TOKEN_SECRET_NAME` — the name of the [secret](../../../../concepts/datamodel/secrets.md) that contains the account's token. It is used for initial authentication.