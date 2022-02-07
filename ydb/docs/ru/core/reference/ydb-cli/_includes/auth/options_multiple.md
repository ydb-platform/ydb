Если в командной строке задано одновременно несколько из перечисленных выше опций, то CLI выдаст ошибку с просьбой указать ровно одну:

```
$ {{ ydb-cli }} --use-metadata-credentials --iam-token-file ~/.ydb/token scheme ls
More than one auth method were provided via options. Choose exactly one of them
Try "--help" option for more info.
```
