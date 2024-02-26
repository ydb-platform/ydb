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

The plugin supports [gRPC and gRPCS](https://grpc.io/) transport protocols. A TLS/SSL certificate needs to be provided when using `grpcs://`.

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
      authKind: "UserPassword"
      endpoint: 'grpcs://<hostname>:2135'
      dbLocation: 'location'
      user: '<username>'
    secureJsonData:
      password: '<userpassword>'
      certificate: 'certificate'
```

Here are fields that are supported in connection configuration:

```typescript
    jsonData:
      authKind: "Anonymous" | "ServiceAccountKey" | "AccessToken" | "UserPassword" | "MetaData";
      endpoint: string;
      dbLocation: string;
      user?: string;
    secureJsonData:
      serviceAccAuthAccessKey?: string;
      accessToken?: string;
      password?: string;
      certificate?: string;
```

## Building queries

[YQL dialect](../yql/reference/index.md) is used to query YDB.
Queries can contain macros which simplify syntax and allow for dynamic parts.
The query editor allows you to get data in different representation: time series, table or logs.

### Time series

Time series visualization options are selectable after adding to your query one field with `Date`, `Datetime` or `Timestamp` type and at least one field with `number` type. You can select time series visualizations using the visualization options. Grafana interprets timestamp rows without explicit time zone as UTC. Any other column is treated as a value column.

#### Multi-line time series

To create multi-line time series, the query must return at least 3 fields in the following order:

- field 1: time field
- field 2: value to group by
- field 3+: the metric values

For example:

```sql
SELECT `timestamp`, `requestTime`, AVG(`responseStatus`) AS `avgRespStatus`
FROM `/database/endpoint/my-logs`
GROUP BY `requestTime`, `timestamp`
ORDER BY `timestamp`
```

### Tables

Table visualizations will always be available for any valid YDB query.

### Visualizing logs with the Logs Panel

To use the Logs panel your query must return a time and string values. You can select logs visualizations using the visualization options.

By default only the first text field will be represented as log line, but this can be customized using query builder.

### Macros

To simplify syntax and to allow for dynamic parts, like date range filters, the query can contain macros.

Here is an example of a query with a macro that will use Grafana's time filter:

```sql
SELECT `timeCol`
FROM `/database/endpoint/my-logs`
WHERE $__timeFilter(`timeCol`)
```

| Macro                                        | Description                                                                                                                      | Output example                                                                                  |
| -------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------- |
| _$\_\_timeFilter(columnName)_                | Replaced by a conditional that filters the data (using the provided column) based on the time range of the panel in microseconds | `foo >= CAST(1636717526371000 AS TIMESTAMP) AND foo <=  CAST(1668253526371000 AS TIMESTAMP)' )` |
| _$\_\_fromTimestamp_                         | Replaced by the starting time of the range of the panel casted to Timestamp                                                      | `CAST(1636717526371000 AS TIMESTAMP)`                                                           |
| _$\_\_toTimestamp_                           | Replaced by the ending time of the range of the panel casted to Timestamp                                                        | `CAST(1636717526371000 AS TIMESTAMP)`                                                           |
| _$\_\_varFallback(condition, \$templateVar)_ | Replaced by the first parameter when the template variable in the second parameter is not provided.                              | `condition` or `templateVarValue`                                                               |

### Templates and variables

To add a new YDB query variable, refer to [Add a query variable](https://grafana.com/docs/grafana/latest/variables/variable-types/add-query-variable/).
After creating a variable, you can use it in your YDB queries by using [Variable syntax](https://grafana.com/docs/grafana/latest/variables/syntax/).
For more information about variables, refer to [Templates and variables](https://grafana.com/docs/grafana/latest/variables/).

## Learn more

- Add [Annotations](https://grafana.com/docs/grafana/latest/dashboards/annotations/).
- Configure and use [Templates and variables](https://grafana.com/docs/grafana/latest/variables/).
- Add [Transformations](https://grafana.com/docs/grafana/latest/panels/transformations/).
- Set up alerting; refer to [Alerts overview](https://grafana.com/docs/grafana/latest/alerting/).
