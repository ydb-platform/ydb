# CREATE OBJECT (TYPE SECRET_ACCESS)

{% include [deprecated_secrets_warning](./_includes/deprecated_secrets_warning.md) %}

All rights to use a secret belong to the secret's creator. The creator can grant another user read access to the secret through secret access management.

Access to secrets is managed using special `SECRET_ACCESS` objects. To grant permission to use the secret `secret_name` to the user `user_name`, create a `SECRET_ACCESS` object named `secret_name:user_name`.

```yql
CREATE OBJECT `secret_name:user_name` (TYPE SECRET_ACCESS);
```

Where:

* `secret_name` — the name of the [secret](create-object-type-secret.md).
* `user_name` — the name of the user who receives access.

## Example

The following statement grants access to the secret `MySecretName` to the user `another_user`:

```yql
CREATE OBJECT `MySecretName:another_user` (TYPE SECRET_ACCESS);
```
