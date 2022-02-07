# Deleting a profile

{% include [profile-list](profile-list.md) %}

Delete the `example` profile:

```bash
{{ ydb-cli }} config profile delete example
```

Result:

```text
Profile "example" will be permanently removed. Continue? (y/n): 
```

Confirm the deletion. Result:

```text
Profile "example" was removed.
```

