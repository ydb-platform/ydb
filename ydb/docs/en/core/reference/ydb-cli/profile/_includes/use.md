# Using a profile

## Connection based on a selected profile {#explicit}

A profile can be applied when running a {{ ydb-short-name }} CLI command with the `--profile` or the `-p` option:

```bash
{{ ydb-cli }} -p <profile_name> <command and command options>
```

For example:

```bash
{{ ydb-cli }} -p mydb1 scheme ls -l
```

In this case, all DB connection parameters are taken from the profile. At the same time, if the authentication parameters are not specified in the profile, the {{ ydb-short-name }} CLI will try to define them based on environment variables, as described in [Connecting to and authenticating with a database — Environment variable](../../connect.md#env).

## Connection based on a selected profile and specified command line parameters {#explicit-and-pars}

The `--profile` (`-p`) option doesn't need to be the only connection setting specified on the command line. For example:

```bash
{{ ydb-cli }} -p mydb1 -d /local2 scheme ls -l
```

```bash
{{ ydb-cli }} -p mydb1 --user alex scheme ls -l
```

In this case, the connection parameters specified on the command line have priority over those stored in the profile. This format lets you reuse profiles to connect to different databases or under different accounts. In addition, specifying the authentication parameter on the command line (such as `--user alex` in the example above) disables environment variable checks regardless of their presence in the profile.

## Connection based on an activated profile {#implicit}

If the `--profile` (`-p`) option is not specified on the command line, the {{ ydb-short-name }} CLI will attempt to take all the connection parameters that it couldn't otherwise determine (from the command-line options or environment variables, as described in [Connecting to and authenticating with a database](../../connect.md)) from the currently activated profile.

Implicit use of the activated profile may cause errors, so we recommend that you read the [Activated profile](../activate.md) article before using this mode.