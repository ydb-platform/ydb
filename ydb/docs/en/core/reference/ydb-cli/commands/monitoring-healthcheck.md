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

* `pretty` — overall database status. Possible values are provided in the [table](../../ydb-sdk/health-check-api.md#selfcheck-result).
* `json` — a detailed JSON response containing a hierarchical list of detected problems. Possible issues are listed in the [Healthcheck API](../../ydb-sdk/health-check-api.md#issues) documentation.

Default: `pretty`. ||
|#

## Examples {#examples}

### Health check result in pretty format {#example-pretty}

```bash
{{ ydb-cli }} --profile quickstart monitoring healthcheck --format pretty
```

Database is in good condition:

```bash
Healthcheck status: GOOD
```

Database is degraded:

```bash
Healthcheck status: DEGRADED
```

### Health check result in JSON format {#example-json}


```bash
{{ ydb-cli }} --profile quickstart monitoring healthcheck --format json
```

Database is in good condition:

```json
{
 "self_check_result": "GOOD",
 "location": {
  "id": 51059,
  "host": "my-host.net",
  "port": 19001
 }
}
```

Database is degraded:

```json
{
 "self_check_result": "DEGRADED",
 "issue_log": [
  {
   "id": "YELLOW-b3c0-70fb",
   "status": "YELLOW",
   "message": "Database has multiple issues",
   "location": {
    "database": {
     "name": "/my-cluster/my-database"
    }
   },
   "reason": [
    "YELLOW-b3c0-1ba8",
    "YELLOW-b3c0-1c83"
   ],
   "type": "DATABASE",
   "level": 1
  },
  {
   "id": "YELLOW-b3c0-1ba8",
   "status": "YELLOW",
   "message": "Compute is overloaded",
   "location": {
    "database": {
     "name": "/my-cluster/my-database"
    }
   },
   "reason": [
    "YELLOW-b3c0-343a-51059-User"
   ],
   "type": "COMPUTE",
   "level": 2
  },
  {
   "id": "YELLOW-b3c0-343a-51059-User",
   "status": "YELLOW",
   "message": "Pool usage is over than 99%",
   "location": {
    "compute": {
     "node": {
      "id": 51059,
      "host": "my-host.net",
      "port": 31043
     },
     "pool": {
      "name": "User"
     }
    },
    "database": {
     "name": "/my-cluster/my-database"
    }
   },
   "type": "COMPUTE_POOL",
   "level": 4
  },
  {
   "id": "YELLOW-b3c0-1c83",
   "status": "YELLOW",
   "message": "Storage usage over 75%",
   "location": {
    "database": {
     "name": "/my-cluster/my-database"
    }
   },
   "type": "STORAGE",
   "level": 2
  }
 ],
 "location": {
  "id": 117,
  "host": "my-host.net",
  "port": 19001
 }
}
```




