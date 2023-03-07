# Why this file was created

[YDB Python tests](https://github.com/ydb-platform/ydb/tree/main/ydb/tests) import and use symbols from [YDB Python SDK](https://github.com/ydb-platform/ydb-python-sdk) in their source code. YDB Python SDK is availible as pip package, but its sources are also [shipped with YDB](https://github.com/ydb-platform/ydb/tree/main/ydb/public/sdk/python/ydb), so, when the tests are run, the developer is sure that he or she is using the most recent version of SDK.

When tests are run in YDB developers private infrastructure, some tweaks are applied, and SDK sources are put at top-level, so its symbols should be imported via `import ydb`.

However, when sources are published in GitHub, they are not moved prior to tests launch and reside in *ydb/public/sdk/python/ydb*, so symbols should be imported via `from ydb.public.sdk.python import ydb`.

We choose the type of the import based on environment variable value.
