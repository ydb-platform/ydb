# Kafka API Authentication

## When is authentication enabled and when is it disabled?

Authentication is always enabled when using the [Kafka API in Yandex Cloud](https://yandex.cloud/docs/data-streams/kafkaapi/auth).

Authentication is disabled by default when using the local database (e.g. Docker image).
To enable it, set [`enforce_user_token_requirement` configuration option](../configuration/index.md#auth).

## How does authentication work in the Kafka API?

In the Kafka API, authentication is conducted through the SASL_PLAINTEXT/PLAIN or SASL_SSL/PLAIN mechanisms.

The following variables are required for authentication:

* `<user-name>` — the username. For information about user management, refer to the section [Access Management](../../security/access-management.md).
* `<password>` — the user's password. For information about user management, refer to the section [Access Management](../../security/access-management.md).
* `<database>` — [the database path](../../concepts/connect#database).

These parameters form the following:

* `<sasl.username> = <user-name>@<database>`
* `<sasl.password> = <password>`

{% note warning %}

Please note, the logic for forming `<sasl.username>` and `<sasl.password>` is different. See [examples](./examples#authentication-in-cloud-examples) for details.

{% endnote %}

For examples of authentication, see [Kafka API Usage examples](./examples.md).