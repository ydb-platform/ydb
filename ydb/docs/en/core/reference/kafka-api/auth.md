# Kafka API Authentication

## Enabling authentication

Authentication is always enabled when using the [Kafka API in Yandex Cloud](https://yandex.cloud/docs/data-streams/kafkaapi/auth).

When you run YDB cluster [by your own](../../quickstart.md), [anonymous authentication](../../security/authentication.md#anonymous) is used by default.
It doesn't require username and password.
To enable required authentication, set [`enforce_user_token_requirement` configuration option](../configuration/index.md#auth).

## How does authentication work in the Kafka API?

In the Kafka API, authentication is conducted through the SASL_PLAINTEXT/PLAIN or SASL_SSL/PLAIN mechanisms.

The following variables are required for authentication:

* `<user-name>` — the username. For information about user management, refer to the section [Authorization](../../security/authorization.md#user).
* `<password>` — the user's password. For information about user management, refer to the section [Authorization](../../security/authorization.md#user).
* `<database>` — [the database path](../../concepts/connect#database).

These parameters form the following variables you can use in `sasl.jaas.config` kafka client property:

* `<sasl.username> = <user-name>@<database>`
* `<sasl.password> = <password>`

{% note warning %}

Please note, the logic for forming `<sasl.username>` and `<sasl.password>` is different. See [examples](./examples#authentication-in-cloud-examples) for details.

{% endnote %}

For examples of authentication, see [Kafka API Usage examples](./examples.md).