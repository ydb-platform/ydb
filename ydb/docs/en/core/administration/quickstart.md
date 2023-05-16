# Getting started

In this guide, you will [deploy](#install) a single-node local [{{ ydb-short-name }} cluster](../concepts/databases.md#cluster) and [execute](#queries) simple queries against your [database](../concepts/databases.md#database).

## Deploy a {{ ydb-short-name }} cluster {#install}

To deploy your {{ ydb-short-name }} cluster, use an archive with an executable or a Docker image.

{% list tabs %}

- Bin

   {% note info %}

   Currently, only a Linux build is supported. We'll add builds for Windows and macOS later.

   {% endnote %}

   1. Create a working directory and change to it:

      ```bash
      mkdir ~/ydbd && cd ~/ydbd
      ```

   1. Download and run the installation script:

      ```bash
      curl https://binaries.ydb.tech/local_scripts/install.sh | bash
      ```

      This will download and unpack the archive including the `idbd` executable, libraries, configuration files, and scripts needed to start and stop the cluster.

   1. Start the cluster in one of the following storage modes:

      * In-memory data:

         ```bash
         ./start.sh ram
         ```

         When data is stored in-memory, it is lost when the cluster is stopped.

      * Data on disk:

         ```bash
         ./start.sh disk
         ```

         The first time you run the script, an 80GB `ydb.data` file will be created in the working directory. Make sure there's enough disk space to create it.

      Result:

      ```text
      Starting storage process...
      Initializing storage ...
      Registering database ...
      Starting database process...

      Database started. Connection options for YDB CLI:

      -e grpc://localhost:2136 -d /Root/test
      ```

- Docker

   1. Pull the current version of the Docker image:

      ```bash
      docker pull {{ ydb_local_docker_image }}:{{ ydb_local_docker_image_tag }}
      ```

      Make sure that the pull operation was successful:

      ```bash
      docker image list | grep {{ ydb_local_docker_image }}
      ```

      Result:

      ```text
      cr.yandex/yc/yandex-docker-local-ydb   latest    c37f967f80d8   6 weeks ago     978MB
      ```

   1. Run the Docker container:

      ```bash
      docker run -d --rm --name ydb-local -h localhost \
        -p 2135:2135 -p 2136:2136 -p 8765:8765 \
        -v $(pwd)/ydb_certs:/ydb_certs -v $(pwd)/ydb_data:/ydb_data \
        -e YDB_DEFAULT_LOG_LEVEL=NOTICE \
        -e GRPC_TLS_PORT=2135 -e GRPC_PORT=2136 -e MON_PORT=8765 \
        {{ ydb_local_docker_image}}:{{ ydb_local_docker_image_tag }}
      ```

      If the container starts successfully, you'll see the container's ID. The container might take a few minutes to initialize. The database will not be available until the initialization completes.

{% endlist %}

## Connect to the DB {#connect}

To connect to the YDB database, use the cluster's Embedded UI or the [{{ ydb-short-name }} CLI command-line interface](../reference/ydb-cli/index.md).

{% list tabs %}

- YDB UI

   1. In your browser, open the page:

      ```http
      http://localhost:8765
      ```

   1. Under **Database list**, select the database:

      * `/Root/test`: If you used an executable to deploy your cluster.
      * `/local`: If you deployed your cluster from a Docker image.

- YDB CLI

   1. Install the {{ ydb-short-name }} CLI:

      * For Linux or macOS:

         ```bash
         curl -sSL https://storage.yandexcloud.net/yandexcloud-ydb/install.sh | bash
         ```

         {% note info %}

         The script will update the `PATH` variable only if you run it in the bash or zsh command shell. If you run the script in a different shell, add the CLI path to the `PATH` variable yourself.

         {% endnote %}

      * For Windows:

         ```cmd
         @"%SystemRoot%\System32\WindowsPowerShell\v1.0\powershell.exe" -Command "iex ((New-Object System.Net.WebClient).DownloadString('https://storage.yandexcloud.net/yandexcloud-ydb/install.ps1'))"
         ```

         Specify whether to add the executable file path to the `PATH` environment variable:

         ```text
         Add ydb installation dir to your PATH? [Y/n]
         ```

         {% note info %}

         Some {{ ydb-short-name }} CLI commands may use Unicode characters in their results. If these characters aren't displayed correctly in the Windows console, switch the encoding to UTF-8:

         ```cmd
         chcp 65001
         ```

         {% endnote %}

      To update the environment variables, restart the command shell session.

   1. Save the DB connection parameters in the [{{ ydb-short-name }} CLI profile](../reference/ydb-cli/profile/index.md):

      ```bash
      ydb config profile create quickstart --endpoint grpc://localhost:2136 --database <path_database>
      ```

      * `path_database`: Database path. Specify one of these values:

         * `/Root/test`: If you used an executable to deploy your cluster.
         * `/local`: If you deployed your cluster from a Docker image.

   1. Check your database connection:

      ```bash
      ydb --profile quickstart scheme ls
      ```

      Result:

      ```text
      .sys_health .sys
      ```

{% endlist %}

## Run queries against the database {#queries}

Use the cluster's YDB Embedded UI or the  [{{ ydb-short-name }} CLI](../reference/ydb-cli/index.md) to execute queries against the database.

{% list tabs %}

- YDB UI

   1. Create tables in the database:

      Enter the query text under **Query**:

      {% include [create-tables](../_includes/create-tables.md) %}

      Click **Run Script**.

   1. Populate the resulting tables with data:

      Enter the query text under **Query**:

      {% include [upsert](../_includes/upsert.md) %}

      Click **Run Script**.

   1. Select data from the `series` table:

      Enter the query text under **Query**:

      {% include [upsert](../_includes/select.md) %}

      Click **Run Script**.

      You'll see the query result below:

      ```text
      series_id	series_title	release_date
      1	IT Crowd	13182
      2	Silicon Valley	16166
      ```

   1. Delete data from the `episodes` table.

      Enter the query text under **Query**:

      {% include [upsert](../_includes/delete.md) %}

      Click **Run Script**.

- YDB CLI

   1. Create tables in the database:

      Write the query text to the `create-table.sql` file:

      {% include [create-tables](../_includes/create-tables.md) %}

      Run the following query:

      ```bash
      ydb --profile quickstart yql --file create-table.sql
      ```

   1. Populate the resulting tables with data:

      Write the query text to the `upsert.sql` file:

      {% include [upsert](../_includes/upsert.md) %}

      Run the following query:

      ```bash
      ydb --profile quickstart yql --file upsert.sql
      ```

   1. Select data from the `series` table:

      Write the query text to the `select.sql` file:

      {% include [select](../_includes/select.md) %}

      Run the following query:

      ```bash
      ydb --profile quickstart yql --file select.sql
      ```

      Result:

      ```text
      ┌───────────┬──────────────────┬──────────────┐
      | series_id | series_title     | release_date |
      ├───────────┼──────────────────┼──────────────┤
      | 1         | "IT Crowd"       | "2006-02-03" |
      ├───────────┼──────────────────┼──────────────┤
      | 2         | "Silicon Valley" | "2014-04-06" |
      └───────────┴──────────────────┴──────────────┘
      ```

   1. Delete data from the `episodes` table.

      Write the query text to the `delete.sql` file:

      {% include [delete](../_includes/delete.md) %}

      Run the following query:

      ```bash
      ydb --profile quickstart yql --file delete.sql
      ```

      View the `episodes` table:

      ```bash
      ydb --profile quickstart yql --script "SELECT * FROM episodes;"
      ```

      Result:

      ```text
      ┌──────────────┬────────────┬───────────┬───────────┬──────────────────────────┐
      | air_date     | episode_id | season_id | series_id | title                    |
      ├──────────────┼────────────┼───────────┼───────────┼──────────────────────────┤
      | "2006-02-03" | 1          | 1         | 1         | "Yesterday's Jam"        |
      ├──────────────┼────────────┼───────────┼───────────┼──────────────────────────┤
      | "2006-02-03" | 2          | 1         | 1         | "Calamity Jen"           |
      ├──────────────┼────────────┼───────────┼───────────┼──────────────────────────┤
      | "2014-04-06" | 1          | 1         | 2         | "Minimum Viable Product" |
      └──────────────┴────────────┴───────────┴───────────┴──────────────────────────┘
      ```

      You've deleted The Cap Table series row from the table.

{% endlist %}

## Stop the cluster {#stop}

Stop the {{ ydb-short-name }} cluster when done:

{% list tabs %}

- Bin

   To stop your cluster, change to the `~/ydbd` directory, then run this command:

   ```bash
   ./stop.sh
   ```

- Docker

   To stop the Docker container with the cluster, run this command:

   ```bash
   docker kill ydb-local
   ```

{% endlist %}

## What's next {#advanced}

* Read about [{{ ydb-short-name }} concepts](../concepts/index.md).
* Learn more about these and other methods of [{{ ydb-short-name }} deployment](../deploy/index.md).
* Find out how to access your {{ ydb-short-name }} databases over the [SDK](../reference/ydb-sdk/index.md).
* Learn more about the [YQL](../yql/reference/index.md) query language.
