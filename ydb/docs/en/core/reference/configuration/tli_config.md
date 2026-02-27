# tli_config

The `tli_config` section contains configuration parameters for [transaction lock invalidation](../../concepts/glossary.md#tli) (TLI) diagnostics.

TLI is a situation where one transaction (the breaker) breaks the optimistic locks of another transaction (the victim), forcing the victim to roll back and retry. For more information about TLI diagnostics, see [{#T}](../../troubleshooting/performance/queries/transaction-lock-invalidation.md).

## Configuration parameters

| Parameter | Type | Default | Description |
|:----------|:-----|:--------|:------------|
| `ignored_table_regexes` | repeated string | `[]` | List of table path regular expressions excluded from TLI diagnostics |

### ignored_table_regexes

Allows excluding specific tables from TLI logging and statistics. If all tables involved in a lock conflict match at least one of the specified regular expressions, no TLI log entry is generated for that conflict.

Changes take effect for new sessions without restarting nodes.

Typical use cases:

- reducing log volume for system or auxiliary tables where conflicts are expected;
- excluding high-conflict queue tables that do not require diagnostics.

Regular expressions are applied to the full table path, e.g. `/Root/mydb/mytable`. The syntax follows [ECMAScript regex](https://en.cppreference.com/w/cpp/regex/ecmascript).

## Configuration example

```yaml
tli_config:
  ignored_table_regexes:
    - "/Root/.*/queue_.*"
    - "/Root/system/.*"
```

In this example, the following are excluded from TLI diagnostics:

- tables whose name starts with `queue_` in any database;
- all tables in the `/Root/system/` directory.

## See also

- [{#T}](../../troubleshooting/performance/queries/transaction-lock-invalidation.md)
- [{#T}](log_config.md)
