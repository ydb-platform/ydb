# Custom initialization scripts

The {{ ydb-short-name }} Docker container supports custom initialization scripts that allow you to automate database setup tasks.

## Script directories

There are two directories for placing custom scripts:

| Directory | Description |
|---|---|
| `/preinit.d` | Scripts in this directory are executed on every container start, **before** the {{ ydb-short-name }} server starts. |
| `/init.d` | Scripts in this directory are executed only **once** after a successful {{ ydb-short-name }} server start. A marker file is created to prevent re-execution on subsequent container restarts. |

### Understanding preinit vs init

- **preinit.d**: Executed on every container start, before the {{ ydb-short-name }} server starts. Useful for setting environment variables, configuring logging, or other preparatory tasks that might need to be done each time the container starts.
- **init.d**: Executed only once after a successful {{ ydb-short-name }} server start. Example: creating database structure (tables, indexes) and inserting initial data.

## Supported file types

### Pre-init scripts (`/preinit.d`)

| Extension | Description |
|---|---|
| `.sh` | Shell scripts. If the script is executable, it is run directly. Otherwise, it is [sourced](https://bash.cyberciti.biz/guide/Source_command), allowing it to modify environment variables for subsequent scripts. |

### Init scripts (`/init.d`)

| Extension | Description |
|---|---|
| `.sh` | Shell scripts. If the script is executable, it is run directly. Otherwise, it is [sourced](https://bash.cyberciti.biz/guide/Source_command). |
| `.sql` | SQL files. The contents are executed using the YDB YQL command-line interface. |
| `.sql.gz` | Gzip-compressed SQL files. The contents are decompressed and executed. |

## Execution order

Scripts are executed in alphabetical order within each directory. Use numeric prefixes to control the execution sequence:

```text
/init.d/
├── 01-create-tables.sh
├── 02-create-indexes.sql
└── 03-insert-data.sql.gz
```

## Environment variables

You can customize the script directories using environment variables:

| Variable | Default | Description |
|---|---|---|
| `YDB_PREINITSCRIPTS_DIR` | `/preinit.d` | Path to the pre-init scripts directory |
| `YDB_INITSCRIPTS_DIR` | `/init.d` | Path to the init scripts directory |

## Error handling

If any script fails (exits with a non-zero status), the container stops execution and exits with an error. This ensures that initialization errors are immediately visible and prevents the container from running with an incomplete setup.

## Examples

### Using shell scripts

Create a shell script to configure database settings:

```bash
#!/bin/bash
# /init.d/01-setup.sh
echo "Setting up database..."
/ydb -e grpc://localhost:2136 -d /local --no-discovery sql -s "CREATE TABLE test (id Uint64, PRIMARY KEY (id));"
```

Mount the script when starting the container:

```bash
docker run -d \
    --name ydb-local \
    -v $(pwd)/init.d:/init.d \
    {{ ydb_local_docker_image }}:{{ ydb_local_docker_image_tag }}
```

### Using SQL files

Create a SQL file with your table definitions:

```sql
-- /init.d/01-create-tables.sql
CREATE TABLE users (
    id Uint64,
    name Utf8,
    email Utf8,
    PRIMARY KEY (id)
);

CREATE TABLE orders (
    id Uint64,
    user_id Uint64,
    amount Double,
    PRIMARY KEY (id)
);
```

Mount the init directory:

```bash
docker run -d \
    --name ydb-local \
    -v $(pwd)/init.d:/init.d \
    {{ ydb_local_docker_image }}:{{ ydb_local_docker_image_tag }}
```

### Using pre-init scripts

Create a pre-init script to set environment variables:

```bash
# /preinit.d/01-set-env.sh
# This script will be sourced, so exported variables will be available
export YDB_DEFAULT_LOG_LEVEL=INFO
```

Mount the pre-init directory:

```bash
docker run -d \
    --name ydb-local \
    -v $(pwd)/preinit.d:/preinit.d \
    {{ ydb_local_docker_image }}:{{ ydb_local_docker_image_tag }}
```

### Restoring from a backup using init scripts

Create a script in the `/init.d` directory to perform the restoration:

```bash
# /init.d/01-restore-backup.sh
#!/bin/bash
if [ -d "/backup" ] && [ -n "$(ls -A /backup)" ]; then
    /ydb -e grpc://localhost:2136 -d /local --no-discovery tools restore -p . -i /backup
fi
```

Mount the init directory, as well as the volume with your backup:

```bash
docker run -d \
    --name ydb-local \
    -v $(pwd)/init.d:/init.d \
    -v $(pwd)/backup:/backup \
    {{ ydb_local_docker_image }}:{{ ydb_local_docker_image_tag }}
```
