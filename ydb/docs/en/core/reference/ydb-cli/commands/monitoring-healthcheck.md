# Health check

{{ ydb-short-name }} has a built-in self-diagnostic system that provides a brief report on the cluster status and information about existing issues. This report can be obtained via {{ ydb-short-name }} CLI using the command explained below.

General command format:

```bash
ydb [global options...] monitoring healthcheck [options...]
```

* `global options` — [global options](global-options.md),
* `options` — [subcommand options](#options).

## Subcommand options {#options}

#|
|| Name | Description ||
|| `--timeout` | The time, in milliseconds, within which the operation should be completed on the server. ||
|| `--format` | Output format. Available options:

* `pretty` — short, human-readable output
* `json` — detailed JSON output

Default: `pretty`. ||
|#

## Examples {#examples}

The response structure and description are provided in the [Health Check API](../../ydb-sdk/health-check-api.md#response-structure) documentation.
