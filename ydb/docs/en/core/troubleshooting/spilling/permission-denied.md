# Permission denied

Insufficient access permissions to the spilling directory prevent {{ ydb-short-name }} from writing data to disk during spilling operations. This can cause queries to fail when they require spilling to handle large data volumes.

## Diagnostics

Check if the spilling directory exists and has proper permissions:

- Verify that the spilling directory exists (see [Spilling Configuration](../../reference/configuration/table_service_config.md#root) for information on how to find the spilling directory)
- Ensure the directory has write and read permissions for the user under which ydbd is running
- Check access permissions to the spilling directory
- Verify that the user under which `ydbd` runs can read and write to the directory

## Recommendations

If permissions are incorrect:

1. Change the directory owner to the user under which `ydbd` runs
2. Ensure read/write permissions are set for the directory owner
3. Restart the `ydbd` process to apply the changes

{% note info %}

The spilling directory is automatically created by {{ ydb-short-name }} when the process starts. If the directory doesn't exist, check that the `root` parameter in the spilling configuration is set correctly.

{% endnote %}
