# Configuring S3 backups for YDB EM

This guide describes how to configure database backups to an S3-compatible storage through {{ ydb-short-name }} Enterprise Manager (YDB EM). Backups are configured in the Control Plane configuration file. In the examples below, the path to this file is shown as `<ydb-em-cp-config.yaml>`.

S3 backup configuration consists of three parts:

* `backup_targets` — where to store backups;
* `secret_key` — which master key is used to encrypt S3 access keys;
* `locations[].default_backup_config` — when to run backups and how long to retain them.

{% note info %}

This article describes automated backup configuration through YDB EM. For general information about backup options in {{ ydb-short-name }}, see [{#T}](../../concepts/backup.md) and [{#T}](../backup-and-recovery.md).

{% endnote %}

## Configuring S3 storage {#backup-targets}

Add the `backup_targets` block to the Control Plane configuration:


```yaml
backup_targets:
  - target_id: "target-em"
    tags:
      locations:
        - "em"
    settings:
      s3:
        endpoint: s3.example.net
        bucket: ydb-em-backups
        access_key: "<encrypted access key>"
        secret_key: "<encrypted secret key>"
        compression: "zstd"
```


| Parameter | Description |
| --- | --- |
| `target_id` | Unique backup target ID. |
| `tags.locations` | List of `location_id` values this target applies to. A database is backed up to this target if its `location_id` is included in the list. For YDB EM, this is usually `em`, matching `locations[].database_location_id` and `meta_location_id`. |
| `settings.s3.endpoint` | S3-compatible storage endpoint. |
| `settings.s3.bucket` | Bucket name for backups. |
| `settings.s3.scheme` | Connection protocol: `1` for HTTP, `2` for HTTPS. If omitted, HTTPS is used. |
| `settings.s3.access_key` | Encrypted [S3 access key](#master-key). |
| `settings.s3.secret_key` | Encrypted [S3 secret key](#master-key). |
| `settings.s3.compression` | Compression algorithm for exported data. The default is `zstd`. Remove this parameter if compression is not required. |

A **location** is a logical group of databases in YDB EM that each database belongs to through its `location_id`. Typically, a location corresponds to a single placement zone of databases — for example, a data center or availability zone where they run — but a location can also be used for logical separation, such as by environment (`prod`, `test`). For example, if databases are deployed across two data centers, you can assign them different locations and send backups from each data center to its own S3 storage. A typical YDB EM installation uses a single location with the value `em`, which matches the `locations[].database_location_id` and `meta_location_id` fields in the Control Plane configuration.

`tags.locations` is useful when one Control Plane manages databases from several locations and different locations must use different backup storage targets. The worker selects a target by the database `location_id`: if the database `location_id` is included in `tags.locations`, this target is used for backups. For example, this lets you send backups from different zones or environments to different S3 buckets.

{% note info %}

Instead of binding a target to `location_id`, you can use `explicit_backup_targets` with `by_database_path` or `by_cloud_id` rules if a specific database or cloud must be assigned to a specific target.

{% endnote %}

## Configuring the master key {#master-key}

At the top level of the configuration file, specify the path to the master key file:


```yaml
secret_key: configs/em/secret_key
```


This master key is used to encrypt and decrypt `settings.s3.access_key` and `settings.s3.secret_key` values. The master key file must be available to Control Plane processes at the path specified in the `secret_key` parameter.

{% note warning %}

Do not store `access_key` and `secret_key` in the configuration as plaintext. YDB EM expects encrypted values and decrypts them at runtime.

{% endnote %}

## Encrypting S3 access keys {#encrypt-s3-keys}

Encrypt the S3 access keys with the same master key specified in the `secret_key` parameter. Use the `ydb-em-cp` Control Plane binary, which is included in the YDB EM package as `bin/ydb-em-cp` and is placed on the Control Plane host during [initial deployment](initial-deployment.md#download):


```bash
ydb-em-cp admin crypto encrypt --body '<plaintext access key>' --cfg-file <ydb-em-cp-config.yaml>
ydb-em-cp admin crypto encrypt --body '<plaintext secret key>' --cfg-file <ydb-em-cp-config.yaml>
```


Copy the resulting encrypted strings to `settings.s3.access_key` and `settings.s3.secret_key`.

To check that a value can be decrypted with the same master key, run:


```bash
ydb-em-cp admin crypto decrypt --body '<encrypted value>' --cfg-file <ydb-em-cp-config.yaml>
```


## Configuring schedule and retention {#schedule-and-ttl}

The target defines where backups are stored. The schedule and retention period are configured separately in `locations[].default_backup_config`. This is the default backup configuration for databases in the specified location. If an individual backup configuration is set for a specific database in YDB EM, it can differ from these defaults.


```yaml
locations:
  - database_location_id: em
    default_backup_config:
      backup_settings:
        - name: daily
          type: SYSTEM
          backup_schedule:
            daily_backup_schedule:
              execute_time:
                hours: 20
          backup_time_to_live: "604800s"
```


| Parameter | Description |
| --- | --- |
| `backup_settings[].name` | Backup setting name. |
| `backup_settings[].type` | Backup setting type. Use `SYSTEM` for a system schedule. |
| `backup_schedule.daily_backup_schedule.execute_time.hours` | Hour of the daily backup start time in UTC. |
| `backup_time_to_live` | Backup retention period in seconds. For example, `604800s` means 7 days. |

## Applying the configuration {#apply}

To apply the settings:

1. Place the updated configuration file in the Control Plane working directory. For example, for an installation under `/opt/ydb-em`, this can be `/opt/ydb-em/ydb-em-cp/cfg/config.yaml`.
2. Make sure the master key file specified in `secret_key` is available to Control Plane processes.
3. Check that `settings.s3.access_key` and `settings.s3.secret_key` are encrypted with the same master key.
4. Restart the Control Plane service:


   ```bash
   sudo systemctl restart ydb-em-cp
   ```

After restart, Control Plane resolves the target by `location_id` according to the schedule, configures the S3 storage, and starts data export to the specified bucket.

## Checklist {#checklist}

Before starting backups, check that:

* `backup_targets[].tags.locations` contains the `location_id` values of the databases that require backups;
* `endpoint`, `bucket`, and `scheme` point to the correct S3 storage;
* `access_key` and `secret_key` are encrypted with `ydb-em-cp admin crypto encrypt`;
* the top-level `secret_key` parameter points to the correct master key file;
* `locations[].default_backup_config` defines the schedule and retention period;
* the updated configuration file is placed in the Control Plane working directory;
* the Control Plane service is restarted after the configuration change.
