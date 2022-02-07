## Getting detailed information about the subcommands of the {{ ydb-short-name }} CLI {#one}

You can also get a more detailed description for any subcommand with a list of available parameters:

```bash
{{ ydb-cli }} discovery whoami --help
```

Result:

```text
Usage: ydb [global options...] discovery whoami [options...]

Description: Who am I?

Global options:
  {-e|--endpoint}, {-d|--database}, {-v|--verbose}, --ca-file, --iam-token-file, --yc-token-file, --use-metadata-credentials, --sa-key-file, --iam-endpoint, --profile, --license, --credits
  To get full description of these options run 'ydb --help'.

Options:
  {-?|-h|--help}      print usage
  --client-timeout ms Operation client timeout
  {-g|--groups}       With groups (default: 0)
```

There are two types of passed parameters:

* `Global options`: Global, specified after `ydb`.
* `Options`: Options of the subcommand, specified after the subcommand.

