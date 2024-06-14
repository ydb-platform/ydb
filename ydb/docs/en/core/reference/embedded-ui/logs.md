# Logs

## Logging levels {#log_levels}

| Level | Numeric value | Value |
| --- | --- | --- |
| TRACE | 8 | Very detailed debugging information. |
| DEBUG | 7 | Debugging information for developers. |
| INFO | 6 | Debugging information for collecting statistics. |
| NOTICE | 5 | An event essential for the system or the user has occurred. |
| WARN | 4 | This is a warning, it should be responded to and fixed unless it's temporary. |
| ERROR | 3 | A non-critical error. |
| CRIT | 2 | A critical state. |
| ALERT | 1 | System degradation is possible, system components may fail. |
| EMERG | 0 | System outage (for example, cluster failure) is possible. |

The logging level for different {{ ydb-short-name }} components can be configured individually. For each component, either an explicitly set value or a default logging level value can be applied. The default logging level value can also be changed.

## Changing the logging level {#change_log_level}

To change the logging level:

1. Follow the link in the format

    ```bash
    http://<endpoint>:8765/cms
    ```

    The `Cluster Management System` page opens.

1. On the **Configs** tab, click on the `LogConfigItems` line. The `Create new item` button will show up along with a list of already created configuration elements.

1. Click `Create new item` to create a new configuration item (or click the pencil button to edit a previously created item).

1. To change the default logging level, select the desired logging level from the `Level` drop-down list under `Default log settings`.

1. To change the logging level for individual components, use the table under `Component log settings`. In the line with the name of the component whose logging level you want to change, in the `Component` column, select the desired logging level from the drop-down list in the `Log level` column.

1. To save changes, click `Submit`

