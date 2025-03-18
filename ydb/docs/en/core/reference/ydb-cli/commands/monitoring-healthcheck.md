# Database self check

YDB has a built-in self-diagnostic system, which can be used to get a brief report on the database status and information about existing issues.

General command format:

```bash
ydb [global options...] monitoring healthcheck [options...]
```

* `global options` — [global options](global-options.md),
* `options` — [subcommand options](#options).

## Subcommand options {#options}

#|
|| Name | Description ||
||`--timeout` | The time within which the operation should be completed on the server, ms.||
||`--format` | Output format. Available options:

* `pretty` — short human readable output,
* `json` — detailed JSON output.

Default — `pretty`.||
|#

## Examples {#examples}

Response structure and description are provided in the [Health Check API](../../ydb-sdk/health-check-api.md#response-structure) documentation.
