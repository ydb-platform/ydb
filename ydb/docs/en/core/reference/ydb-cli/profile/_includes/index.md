# Managing profiles

A profile is a named set of DB connection parameters stored in a configuration file in the local file system. With profiles, you can reuse data about DB location and authentication parameters, making a CLI call much shorter:

- Calling the `scheme ls` command without a profile:
   ```bash
   {{ ydb-cli }} \
   -e grpsc://some.host.in.some.domain:2136 \
   -d /some_long_identifier1/some_long_identifier2/database_name \
   --yc-token-file ~/secrets/token_database1 \
   scheme ls
   ```

- Calling the same `scheme ls` command using a profile:
   ```bash
   {{ ydb-cli }} -p quickstart scheme ls
   ```

## Profile management commands {#commands}

- [Creating a profile](../create.md)
- [Using a profile](../use.md)
- [Getting a list of profiles and profile parameters](../list-and-get.md)
- [Deleting a profile](../delete.md)
- [Activating a profile and using the activated profile](../activate.md)

## Where profiles are stored {#location}

Profiles are stored locally in a file named `~/ydb/config/config.yaml`.

{% include [location_overlay.md](location_overlay.md) %}
