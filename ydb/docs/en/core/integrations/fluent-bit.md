# Log records collection using FluentBit

This section describes the integration between {{ ydb-short-name }} and the log capture tool [FluentBit](https://fluentbit.io) to save and analyze the log records in {{ ydb-short-name }}.


## Overview

FluentBit is a tool that can collect text data, manipulate it (modify, transform, combine), and send it to various repositories for further processing. A custom plugin library for FluentBit has been developed to support saving the log records into {{ ydb-short-name }}. The library's source code is available in the [fluent-bit-ydb repository](https://github.com/ydb-platform/fluent-bit-ydb).

Deploying a log delivery scheme using FluentBit and {{ ydb-short-name }} as the destination database includes the following steps:

1. Create {{ ydb-short-name }} tables for the log data storage
2. Deploy FluentBit and {{ ydb-short-name }} plugin for FluentBit
3. Configure FluentBit to collect and process the logs
4. Configure FluentBit to send the logs to {{ ydb-short-name }} tables


## Creating tables for log data

Tables for log data storage must be created in the chosen {{ ydb-short-name }} database. The structure of the tables is determined by a set of fields of a specific log supplied using FluentBit. Depending on the requirements, different log types may be saved to different tables. Normally, the table for log data contains the following fields:

* timestamp
* log level
* hostname
* service name
* message text or its semi-structural representation as JSON document

{{ ydb-short-name }} tables must have a primary key, uniquely identifying each table's row. A timestamp does not always uniquely identify messages coming from a particular source because messages might be generated simultaneously. To enforce the uniqueness of the primary key, a hash value can be added to the table. The hash value is computed using the [CityHash64](https://github.com/google/cityhash) algorithm over the log record data.

Row-based and columnar tables can both be used for log data storage. Columnar tables are recommended, as they support more efficient data scans for log data retrieval.

Example of the row-based table for log data storage:

```sql
CREATE TABLE `fluent-bit/log` (
    `timestamp`         Timestamp NOT NULL,
    `hostname`          Text NOT NULL,
    `input`             Text NOT NULL,
    `datahash`          Uint64 NOT NULL,
    `level`             Text NULL,
    `message`           Text NULL,
    `other`             JsonDocument NULL,
    PRIMARY KEY (
         `datahash`, `timestamp`, `hostname`, `input`
    )
);
```

Example of the columnar table for log data storage:

```sql
CREATE TABLE `fluent-bit/log` (
    `timestamp`         Timestamp NOT NULL,
    `hostname`          Text NOT NULL,
    `input`             Text NOT NULL,
    `datahash`          Uint64 NOT NULL,
    `level`             Text NULL,
    `message`           Text NULL,
    `other`             JsonDocument NULL,
    PRIMARY KEY (
         `timestamp`, `hostname`, `input`, `datahash`
    )
) PARTITION BY HASH(`timestamp`, `hostname`, `input`)
  WITH (STORE = COLUMN);
```

The command that creates the columnar table differs in the following details:

* it specifies the columnar storage type and the table's partitioning key in the last two lines;
* the `timestamp` column is the first column of the primary key, which is optimal and recommended for columnar, but not for row-based tables. See the specific guidelines for choosing the primary key [for columnar tables](../dev/primary-key/column-oriented.md) and [for row-based tables](../dev/primary-key/row-oriented.md).

[TTL configuration](../concepts/ttl.md) can be optionally applied to the table, limiting the data storage period and enabling the automatic removal of obsolete data. Enabling TTL requires an extra setting in the `WITH` section of the table creation command. For example, `TTL = Interval("P14D") ON timestamp` sets the storage period to 14 days, based on the `timestamp` field's value.


## FluentBit deployment and configuration

FluentBit deployment should be performed according to [its documentation](https://docs.fluentbit.io/manual/installation/getting-started-with-fluent-bit).

{{ ydb-short-name }} plugin for FluentBit is available in the source code form in the [repository](https://github.com/ydb-platform/fluent-bit-ydb), along with the build instructions. A docker image is provided for container-based deployments: `ghcr.io/ydb-platform/fluent-bit-ydb`.

General logic, configuration syntax and procedures to set up the receiving, processing, and delivering logs in the FluentBit environment are defined in the corresponding [FluentBit documentation](https://docs.fluentbit.io/manual/concepts/key-concepts).


## Writing logs to {{ ydb-short-name }} tables

Before using the {{ ydb-short-name }} output plugin, it needs to be enabled in the FluentBit settings. The list of the enabled FluentBit plugins is configured in a file (for example, `plugins.conf`), which is referenced through the `plugins_file` parameter in the `SERVICE` section of the main FluentBit configuration file. Below is the example of such a file with {{ ydb-short-name }} plugin enabled (plugin library path may be different depending on your setup):

```text
# plugins.conf
[PLUGINS]
    Path /usr/lib/fluent-bit/out_ydb.so
```

The table below lists the configuration parameters supported by the {{ ydb-short-name }} output plugin for FluentBit.

| **Parameter** | **Description** |
|----------|--------------|
| `Name`   | Plugin type, should be value `ydb` |
| `Match`  | (optional) [Tag matching expression](https://docs.fluentbit.io/manual/concepts/data-pipeline/router) to select log records which should be routed to {{ ydb-short-name }} |
| `ConnectionURL` | YDB connection URL, including the protocol, endpoint, and database path (see the [documentation](../concepts/connect.md)) |
| `TablePath` | Table path starting from database root (example: `fluent-bit/log`) |
| `Columns` | JSON structure mapping the fields of FluentBit record to the columns of the target YDB table. May include the pseudo-columns listed below |
| `CredentialsAnonymous` | Configured as `1` for anonymous {{ ydb-short-name }} authentication |
| `CredentialsToken` | Token value, to use the token authentication {{ ydb-short-name }} mode |
| `CredentialsYcMetadata` | Configure as `1` for virtual machine metadata {{ ydb-short-name }} authentication |
| `CredentialsStatic` | Username and password for {{ ydb-short-name }} authentication, specified in the following format: `username:password@` |
| `CredentialsYcServiceAccountKeyFile` | Path of a file containing the service account (SA) key, to use the SA key {{ ydb-short-name }} authentication |
| `CredentialsYcServiceAccountKeyJson` | JSON data of the service account key to be used instead of the filename (useful in K8s environment) |
| `Certificates` | Path to the certificate authority (CA) trusted certificates file, or the literal trusted CA certificate value |
| `LogLevel` | Plugin-specific logging level should be one of `disabled` (default), `trace`, `debug`, `info`, `warn`, `error`, `fatal` or `panic` |

The following pseudo-columns are available, in addition to the actual FluentBit log record fields, to be used as source values in the column map (`Columns` parameter):

* `.timestamp` - log record timestamp (mandatory)
* `.input` - log input stream name (mandatory)
* `.hash` - uint64 hash code, computed over the log record fields (optional)
* `.other` - JSON document containing all log record fields that were not explicitly mapped to any table column (optional)

Example of `Columns` parameter value:

```json
{".timestamp": "timestamp", ".input": "input", ".hash": "datahash", "log": "message", "level": "level", "host": "hostname", ".other": "other"}
```

## Collecting logs in a Kubernetes cluster

FluentBit is often used to collect logs in the Kubernetes environment. Below is the schema of the log delivery process, implemented using FluentBit and {{ ydb-short-name }}, for applications running in the Kubernetes cluster:

![FluentBit in Kubernetes cluster](../_assets/fluent-bit-ydb-scheme.png)
<small>Figure 1 —  Interaction diagram between FluentBit and {{ ydb-short-name }} in the Kubernetes cluster</small>

In this diagram:

* Application pods write logs to stdout/stderr
* Text from stdout/stderr is saved as files on Kubernetes worker nodes
* Pod with FluentBit:
  * Mounts a folder with log files for itself
  * Reads the contents from the log files
  * Enriches log records with additional metadata
  * Saves records to {{ ydb-short-name }} database

### Table to store Kubernetes logs

Below is the {{ ydb-short-name }} table structure to store the Kubernetes logs:

```sql
CREATE TABLE `fluent-bit/log` (
    `timestamp`         Timestamp NOT NULL,
    `file`              Text NOT NULL,
    `pipe`              Text NOT NULL,
    `message`           Text NULL,
    `datahash`          Uint64 NOT NULL,
    `message_parsed`    JSON NULL,
    `kubernetes`        JSON NULL,

    PRIMARY KEY (
         `timestamp`, `file`, `datahash`
    )
) PARTITION BY HASH(`timestamp`, `file`)
  WITH (STORE = COLUMN, TTL = Interval("P14D") ON `timestamp`);
```

Columns purpose:

* `timestamp` – the log record timestamp;
* `file` – the name of the source from which the log was read. In the case of Kubernetes, this will be the name of the file on the worker node in which the logs of a specific pod are written;
* `pipe` – stdout or stderr stream where application-level writing was done;
* `datahash` – hash code computed over the log record;
* `message` – the textual part of the log record;
* `message_parsed` – log record fields in the structured form, if it could be parsed using the configured FluentBit parsers from the textual part;
* `kubernetes` – information about the pod, including name, namespace, and annotations.

Optionally, TTL can be configured for the table, as shown in the example.

### FluentBit configuration

In order to deploy FluentBit in the Kubernetes environment, a configuration file with the log collection and processing parameters must be prepared (typical file name: `values.yaml`). This section provides the necessary comments on this file's content with the examples.

It is necessary to replace the repository and image version of the FluentBit container:

```yaml
image:
  repository: ghcr.io/ydb-platform/fluent-bit-ydb
  tag: latest
```

In this image, a plugin library has been added that implements {{ ydb-short-name }} support.

The following lines define the rules for mounting log folders in FluentBit pods:

```yaml
volumeMounts:
  - name: config
    mountPath: /fluent-bit/etc/conf

daemonSetVolumes:
  - name: varlog
    hostPath:
      path: /var/log
  - name: varlibcontainers
    hostPath:
      path: /var/lib/containerd/containers
  - name: etcmachineid
    hostPath:
      path: /etc/machine-id
      type: File

daemonSetVolumeMounts:
  - name: varlog
    mountPath: /var/log
  - name: varlibcontainers
    mountPath: /var/lib/containerd/containers
    readOnly: true
  - name: etcmachineid
    mountPath: /etc/machine-id
    readOnly: true
```

FluentBit startup parameters should be configured as shown below:

```yaml
command:
  - /fluent-bit/bin/fluent-bit

args:
  - --workdir=/fluent-bit/etc
  - --plugin=/fluent-bit/lib/out_ydb.so
  - --config=/fluent-bit/etc/conf/fluent-bit.conf
```

The FluentBit pipeline for collecting, converting, and delivering logs should be defined according to the example:

```yaml
config:
  inputs: |
    [INPUT]
        Name tail
        Path /var/log/containers/*.log
        multiline.parser cri
        Tag kube.*
        Mem_Buf_Limit 5MB
        Skip_Long_Lines On

  filters: |
    [FILTER]
        Name kubernetes
        Match kube.*
        Keep_Log On
        Merge_Log On
        Merge_Log_Key log_parsed
        K8S-Logging.Parser On
        K8S-Logging.Exclude On

    [FILTER]
        Name modify
        Match kube.*
        Remove time
        Remove _p

  outputs: |
    [OUTPUT]
        Name ydb
        Match kube.*
        TablePath fluent-bit/log
        Columns {".timestamp":"timestamp",".input":"file",".hash":"datahash","log":"message","log_parsed":"message_structured","stream":"pipe","kubernetes":"metadata"}
        ConnectionURL ${OUTPUT_YDB_CONNECTION_URL}
        CredentialsToken ${OUTPUT_YDB_CREDENTIALS_TOKEN}
```

Configuration blocks description:

* `inputs` - this block specifies where to read and how to parse logs. In this case, `*.log` files will be read from the `/var/log/containers/` folder, which is mounted from the host
* `filters` - this block specifies how the logs will be processed. In this case, for each log record, the corresponding metadata is added (using the Kubernetes filter), and unused fields (`_p`, `time`) are cut out
* `outputs` - this block specifies where the logs will be sent. In this case, logs are saved into the `fluent-bit/log` table in the {{ ydb-short-name }} database. Database connection parameters (in the shown example, `ConnectionURL` and `CredentialsToken`) are defined using the environment variables – `OUTPUT_YDB_CONNECTION_URL`, `OUTPUT_YDB_CREDENTIALS_TOKEN`. Authentication parameters and the set of corresponding environment variables are updated depending on the configuration of the {{ ydb-short-name }} cluster being used.

Environment variables are defined as shown below:

```yaml
env:
  - name: OUTPUT_YDB_CONNECTION_URL
    value: grpc://ydb-endpoint:2135/path/to/database
  - name: OUTPUT_YDB_CREDENTIALS_TOKEN
    valueFrom:
      secretKeyRef:
        key: token
        name: fluent-bit-ydb-plugin-token
```

Authentication data should be stored as the secret object in the Kubernetes cluster configuration. Example command to create a Kubernetes secret:

```sh
kubectl create secret -n ydb-fluent-bit-integration generic fluent-bit-ydb-plugin-token --from-literal=token=<YDB TOKEN>
```

### Deploying FluentBit in a Kubernetes cluster

[HELM](https://helm.sh) is a tool to package and install applications in a Kubernetes cluster. To deploy FluentBit, the corresponding chart repository (containing the installation scenario) should be added using the following command:

```sh
helm repo add fluent https://fluent.github.io/helm-charts
```

After that, FluentBit can be deployed to a Kubernetes cluster with the following command:

```sh
helm upgrade --install fluent-bit fluent/fluent-bit \
  --version 0.37.1 \
  --namespace ydb-fluent-bit-integration \
  --create-namespace \
  --values values.yaml
```

The argument `--values` in the example command shown above references the file containing the FluentBit settings.

### Installation verification

Check that FluentBit has started by reading its logs (there should be no `[error]` level entries):

```sh
kubectl logs -n ydb-fluent-bit-integration -l app.kubernetes.io/instance=fluent-bit
```

Check that there are records in the {{ ydb-short-name }} table (they will appear approximately a few minutes after launching FluentBit):

```sql
SELECT * FROM `fluent-bit/log` LIMIT 10 ORDER BY `timestamp` DESC
```

### Resources cleanup

To remove FluentBit, it is sufficient to delete the Kubernetes namespace which was used for the installation:

```sh
kubectl delete namespace ydb-fluent-bit-integration
```

After uninstalling FluentBit, the log storage table can be dropped from the {{ ydb-short-name }} database:

```sql
DROP TABLE `fluent-bit/log`
```
