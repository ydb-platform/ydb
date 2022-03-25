If several of the above options are set simultaneously in the command line, the CLI returns an error asking you to specify only one:

```
$ {{ ydb-cli }} --use-metadata-credentials --iam-token-file ~/.ydb/token scheme ls
More than one auth method were provided via options. Choose exactly one of them
Try "--help" option for more info.
```

