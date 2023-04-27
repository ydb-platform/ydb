# Authentication

The `discovery whoami` information command lets you check the account on behalf of which the server actually accepts requests:

```bash
{{ ydb-cli }} [connection options] discovery whoami [-g]
```

{% include [conn_options_ref.md](conn_options_ref.md) %}

The response includes the account name (User SID) and, if the `-g` option is specified, the information whether the account belongs to groups.

If authentication is not enabled on the {{ ydb-short-name }} server (for example, in the case of an independent local deployment), the command will fail with an error.

Support for the `-g` option depends on the server configuration. If disabled, you'll receive `User has no groups` in response, regardless of the actual inclusion of your account in any groups.

## Example

```bash
$ ydb -p quickstart discovery whoami -g
User SID: aje5kkjdgs0puc18976co@as

User has no groups
```

