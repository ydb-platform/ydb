# Getting profile information

To specify a profile, use its name. You can get the profile name from the list of profiles.

## Getting a list of profiles {#profile-list}

Get the list of profiles:

```bash
{{ ydb-cli }} config profile list
```

Result:

```text
example (active)
```

## Getting detailed profile information {#profile-get}

Get details about the profile named `example`:

```bash
{{ ydb-cli }} config profile get example
```

