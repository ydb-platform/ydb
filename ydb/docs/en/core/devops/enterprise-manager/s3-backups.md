# Configuring S3 backup for YDB EM

This guide describes how to configure backup of databases to an S3-compatible storage via {{ ydb-short-name }} Enterprise Manager (YDB EM). Backup is configured in the Control Plane configuration file. In the examples below, the path to this file is denoted as `<ydb-em-cp-config.yaml>`.

You need three configuration blocks to configure S3 backups:

* `backup_targets` — where to save backups.
* `secret_key` — the master key used to encrypt the S3 access keys.
* `locations[].default_backup_config` — when to run backups and how long to keep copies.

{% note info %}

This article describes how to configure automatic backup via YDB EM. For general information about backup methods in {{ ydb-short-name }}, see sections [{#T}](../../concepts/backup.md) and [{#T}](../backup-and-recovery/index.md).

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
| `target_id` | Unique identifier of the target backup storage. |
| `tags.locations` | List of `location_id` to which this target applies. A database will be backed up to this target if its `location_id` is in the list. For YDB EM, the value `em` is typically used, corresponding to `locations[].database_location_id` and `meta_location_id`. |
| `settings.s3.endpoint` | Address of the S3-compatible storage. |
| `settings.s3.bucket` | Bucket name for backups. |
| `settings.s3.scheme` | Connection protocol: `1` — HTTP, `2` — HTTPS. If the parameter is not set, HTTPS is used. |
| `settings.s3.access_key` | Encrypted [S3 access key](#master-key). |
| `settings.s3.secret_key` | Encrypted [S3 secret access key](#master-key). |
| `settings.s3.compression` | Compression algorithm for exported data. By default, `zstd` is used. Remove the parameter if compression is not required. |

**Location** is a logical group of databases in YDB EM to which each database is linked via the `location_id` identifier. Typically, one location corresponds to one database placement zone — for example, a data center or an availability zone where they run — but location can also be used for logical separation, for example by environments (`prod`, `test`). For instance, if databases are deployed in two data centers, you can assign them different locations and route backups from each data center to its own S3 storage. For a typical YDB EM installation, a single location with the value `em` is used, which corresponds to the `locations[].database_location_id` and `meta_location_id` fields in the Control Plane configuration.

`tags.locations` is needed when a single Control Plane manages databases from multiple locations and different backup storages need to be used for different locations. The worker selects a target based on the database's `location_id`: if the database's `location_id` is included in `tags.locations`, this target is used for backup. This way, for example, you can route backups from different zones or environments to different S3 buckets.

{% note info %}

Instead of binding a target to `location_id`, you can use `explicit_backup_targets` with `by_database_path` or `by_cloud_id` rules if a specific database or cloud needs to be assigned to a particular target.

{% endnote %}

## Configuring the master key {#master-key}

At the top level of the configuration file, specify the path to the master key file:


```yaml
secret_key: configs/em/secret_key
```


This master key is used to encrypt and decrypt the values of `settings.s3.access_key` and `settings.s3.secret_key`. The master key file must be accessible to Control Plane processes at the path specified in the `secret_key` parameter.

{% note warning %}

Do not save `access_key` and `secret_key` in plain text in the configuration. YDB EM expects encrypted values and decrypts them at runtime.

{% endnote %}

## Encrypting S3 access keys {#encrypt-s3-keys}

Encrypt the S3 access keys with the same master key that is specified in the `secret_key` parameter. To encrypt, use the Control Plane binary `ydb-em-cp`, which is included in the YDB EM package as `bin/ydb-em-cp` and is placed on the Control Plane host during [initial deployment](initial-deployment.md#download):


```bash
ydb-em-cp admin crypto encrypt --body '<plaintext access key>' --cfg-file <ydb-em-cp-config.yaml>
ydb-em-cp admin crypto encrypt --body '<plaintext secret key>' --cfg-file <ydb-em-cp-config.yaml>
```


Copy the resulting encrypted strings to the `settings.s3.access_key` and `settings.s3.secret_key` parameters.

To verify that the value can be decrypted with the same master key, run:


```bash
ydb-em-cp admin crypto decrypt --body '<encrypted value>' --cfg-file <ydb-em-cp-config.yaml>
```


## Configuring schedule and retention period {#schedule-and-ttl}

The target defines where to save backups. The schedule and retention period are set separately, in the `locations[].default_backup_config` block. This is the default backup configuration for databases in the specified location. If a specific database in YDB EM has an individual backup configuration, it may differ from these default values.


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
| `backup_settings[].name` | Name of the backup setting. |
| `backup_settings[].type` | Setting type. For a system schedule, use `SYSTEM`. |
| `backup_schedule.daily_backup_schedule.execute_time.hours` | Start hour of daily backup in UTC. |
| `backup_time_to_live` | Backup retention period in seconds. For example, `604800s` is 7 days. |

## Applying the configuration {#apply}

To apply the settings:

1. Place the updated configuration file in the Control Plane working directory. For example, for an installation in `/opt/ydb-em`, this could be the file `/opt/ydb-em/ydb-em-cp/cfg/config.yaml`.
2. Make sure the master key file specified in `secret_key` is accessible to the Control Plane processes.
3. Verify that `settings.s3.access_key` and `settings.s3.secret_key` are encrypted with the same master key.
4. Restart the Control Plane service:


   ```bash
   sudo systemctl restart ydb-em-cp
   ```

After the restart, the Control Plane determines the target by `location_id` according to the schedule, configures the S3 storage, and starts data export to the specified bucket.

## Checklist {#checklist}

Before starting, check:

* `backup_targets[].tags.locations` contains `location_id` databases for which backup needs to be enabled.
* `endpoint`, `bucket`, and `scheme` point to the required S3 storage.
* `access_key` and `secret_key` are encrypted with the `ydb-em-cp admin crypto encrypt` command.
* The top-level parameter `secret_key` points to the correct master key file.
* `locations[].default_backup_config` has the schedule and retention period set.
* The updated configuration file is placed in the Control Plane working directory.
* The Control Plane service is restarted after the configuration change.
