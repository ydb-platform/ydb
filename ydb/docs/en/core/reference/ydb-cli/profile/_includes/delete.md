# Deleting a profile

Currently, you can only delete profiles interactively with the following command:

```bash
{{ ydb-cli }} config profile delete <profile_name>
```

where `<profile_name>` is the profile name.

The {{ ydb-short-name }} CLI will request confirmation to delete the profile:

```text
Profile "<profile_name>" will be permanently removed. Continue? (y/n):
```

Choose `y` (Yes) to delete the profile.

## Example {#example}

Deleting the `mydb1` profile:

```bash
$ {{ ydb-cli }} config profile delete mydb1
Profile "mydb1" will be permanently removed. Continue? (y/n): y
Profile "mydb1" was removed.
```

## Deleting a profile without interactive input {#non-interactive}

Although this mode is not supported by the {{ ydb-short-name }} CLI, if necessary, you can use input redirection in your OS to automatically respond `y` to the request to confirm the deletion:

```bash
echo y | {{ ydb-cli }} config profile delete my_profile
```

The efficiency of this method is not guaranteed in any way.

