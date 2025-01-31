# Kafka API Authentication

## Enabling authentication

Authentication is always enabled when using the [Kafka API in Yandex Cloud](https://yandex.cloud/en/docs/data-streams/kafkaapi/auth).

When you run YDB cluster [by your own](../../quickstart.md), [anonymous authentication](../../security/authentication.md#anonymous) is used by default.
It doesn't require a username and password.
To require authentication see [Authentication](../../security/authentication#static-credentials).

## How does authentication work in the Kafka API?

The Kafka API uses the SASL_PLAINTEXT/PLAIN or SASL_SSL/PLAIN authentication mechanism.

The following variables are required for authentication:

* `<user-name>` — the username. For information about user management, refer to the section [Authorization](../../security/authorization.md#user).
* `<password>` — the user's password. For information about user management, refer to the section [Authorization](../../security/authorization.md#user).
* `<database>` — [the database path](../../concepts/connect#database).

These parameters form the following variables you can use in `sasl.jaas.config` kafka client property:

* `<sasl.username> = <user-name>@<database>`
* `<sasl.password> = <password>`

{% note info %}

The `<sasl.username>` and `<sasl.password>` parameters are formed differently. See [examples](./examples#authentication-in-cloud-examples) for details.

{% endnote %}

For authentication examples, see [Kafka API Usage examples](./examples.md).