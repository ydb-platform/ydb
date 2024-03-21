# {{ ydb-short-name }} data source for Grafana

The [{{ ydb-short-name }} data source plugin](https://grafana.com/grafana/plugins/ydbtech-ydb-datasource/) allows you to use [Grafana](https://grafana.com) to query and visualize data from {{ ydb-short-name }}.

## Installation

Prerequisites: the plugin requires `v9.2` and higher of Grafana.

Follow the Grafana's [plugin installation docs](https://grafana.com/docs/grafana/latest/plugins/installation/) to install a plugin named `ydb-grafana-datasource-plugin`.

## Configuration

### {{ ydb-short-name }} user for the data source

Set up an {{ ydb-short-name }} user account with **read-only** permissions [(more about permissions)](../cluster/access.md) and access to databases and tables you want to query. 

{% note warning %}

Please note that Grafana does not validate that queries are safe. Queries can contain any SQL statements including data modification instructions.

{% endnote %}

### Data transfer protocol support

The plugin supports [gRPC and gRPCS](https://grpc.io/) transport protocols. If self-signed certificates are used on your {{ ydb-short-name }} cluster, specify the [Certificate Authority](https://en.wikipedia.org/wiki/Certificate_authority) certificate, through which they were released.

### Configuration via UI

Once the plugin is installed on your Grafana instance, follow [these instructions](https://grafana.com/docs/grafana/latest/datasources/add-a-data-source/) to add a new {{ ydb-short-name }} data source, and enter configuration options.

### Configuration with provisioning system

Alternatively, it is possible to configure data sources using configuration files with Grafana’s provisioning system. To read about how it works, including all the settings that you can set for this data source, refer to [Provisioning Grafana data sources](https://grafana.com/docs/grafana/latest/administration/provisioning/#data-sources) documentation.

### Authentication

The Grafana plugin supports various authentication [authentication methods](../reference/ydb-sdk/auth.md).

Below is an example config for authenticating a {{ ydb-short-name }} data source using username and password:

```yaml
apiVersion: 1
datasources:
  - name: YDB
    type: ydbtech-ydb-datasource
    jsonData:
      authKind: '<password>'
      endpoint: 'grpcs://<hostname>:2135'
      dbLocation: '<location_to_db>'
      user: '<username>'
    secureJsonData:
      password: '<userpassword>'
      certificate: |
        <overall content of *.pem file>
```

Here are fields that are supported in connection configuration:

| Name  | Description         |         Type          |
| :---- | :------------------ | :-------------------: |
| authKind | Authentication type |       `"Anonymous"`, `"ServiceAccountKey"`, `"AccessToken"`, `"UserPassword"`, `"MetaData"`        |
| endpoint | Database endpoint  | `string` |
| dbLocation | Database location  | `string` |
| user | User name  | `string` |
| serviceAccAuthAccessKey | Service account access key  | `string` (secured) |
| accessToken | OAuth access token  | `string` (secured) |
| password | User password  | `string` (secured) |
| certificate | If self-signed certificates are used on your {{ ydb-short-name }} cluster, specify the [Certificate Authority](https://en.wikipedia.org/wiki/Certificate_authority) certificate, through which they were issued.  | `string` (secured) |

## Building queries

{{ ydb-short-name }} is queried with a SQL dialect named [YQL](../yql/reference/index.md).
Queries can contain macros which simplify syntax and allow for dynamic parts. There are two kinds of macros - [Grafana-level](#macros) and {{ ydb-short-name }}-level. The plugin will parse query text and, before sending it to {{ ydb-short-name }}, substitute variables and Grafana-level macros with particular values. After that {{ ydb-short-name }}-level macroses will be treated by {{ ydb-short-name }} server-side. 
The query editor allows to get data in different representations: time series, table or logs.

### Time series { #time-series }

Time series visualization options are selectable if the query returns at least one field with `Date`, `Datetime`, or `Timestamp` type (for now work with time supported only in UTC format) and at least one field with `Int64`, `Int32`, `Int16`, `Int8`, `Uint64`, `Uint32`, `Uint16`, `Uint8`, `Double` or `Float` type. Then you can select time series visualization options. Grafana interprets timestamp rows without an explicit time zone as UTC. Any other column is treated as a value column.

![Time-series](../_assets/grafana/time-series.png)

#### Multi-line time series

To create multi-line time series, the query must return at least 3 fields in the following order:

- field 1: `Date`, `Datetime` or `Timestamp`
- field 2: value to group by
- field 3+: the metric values

For example:

```sql
SELECT `timestamp`, `requestTime`, AVG(`responseStatus`) AS `avgRespStatus`
FROM `/database/endpoint/my-logs`
GROUP BY `requestTime`, `timestamp`
ORDER BY `timestamp`
```

### Tables { #tables }

Table visualizations will always be available for any valid {{ ydb-short-name }} query with exactly one result set.


![Table](../_assets/grafana/table.png)

### Visualizing logs with the Logs Panel { #visual-logs }

To use the Logs panel your query must return a `Date`, `Datetime` or `Timestamp` and `String` values. You can select logs visualizations using the visualization options.

By default, only the first text field will be represented as log line. This can be customized using the query builder.

![Logs](../_assets/grafana/logs.png)

### Macros

To simplify syntax and to allow for dynamic parts, like date range filters, the query can contain macros.

Here is an example of a query with a macro that will use Grafana's time filter:

```sql
SELECT `timeCol`
FROM `/database/endpoint/my-logs`
WHERE $__timeFilter(`timeCol`)
```

```sql
SELECT `timeCol`
FROM `/database/endpoint/my-logs`
WHERE $__timeFilter(`timeCol` + INTERVAL("PT24H"))
```

| Macro                                        | Description                                                                                                                      | Output example                                                                                  |
| -------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------- |
| `$__timeFilter(columnName)`                | Replaced by a conditional that filters the data (using the provided column or expression) based on the time range of the panel in microseconds | `foo >= CAST(1636717526371000 AS TIMESTAMP) AND foo <=  CAST(1668253526371000 AS TIMESTAMP)' )` |
| `$__fromTimestamp`                         | Replaced by the starting time of the range of the panel casted to Timestamp                                                      | `CAST(1636717526371000 AS TIMESTAMP)`                                                           |
| `$__toTimestamp`                           | Replaced by the ending time of the range of the panel casted to Timestamp                                                        | `CAST(1636717526371000 AS TIMESTAMP)`                                                           |
| `$__varFallback(condition, $templateVar)` | Replaced by the first parameter when the template variable in the second parameter is not provided.                              | `$__varFallback('foo', $bar)` `foo` if variable `bar` is not provided, or `$bar`'s value                                                               |

### Templates and variables

To add a new {{ ydb-short-name }} query variable, refer to [Add a query variable](https://grafana.com/docs/grafana/latest/variables/variable-types/add-query-variable/).
After creating a variable, you can use it in your {{ ydb-short-name }} queries by using [Variable syntax](https://grafana.com/docs/grafana/latest/variables/syntax/).
For more information about variables, refer to [Templates and variables](https://grafana.com/docs/grafana/latest/variables/).

## Learn more

- Add [Annotations](https://grafana.com/docs/grafana/latest/dashboards/annotations/).
- Configure and use [Templates and variables](https://grafana.com/docs/grafana/latest/variables/).
- Add [Transformations](https://grafana.com/docs/grafana/latest/panels/transformations/).
- Set up alerting; refer to [Alerts overview](https://grafana.com/docs/grafana/latest/alerting/).
