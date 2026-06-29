# Data export and import

The {{ ydb-short-name }} CLI includes a set of commands for exporting and importing data and descriptions of data schema objects. Data export can be used both for creating backups for subsequent recovery and for other purposes.

- [Export file structure](../file-structure.md), used both when exporting to a file system and when exporting to an S3-compatible object storage.
- [Exporting cluster metadata to a file system using `admin cluster dump`](../tools-dump.md#cluster)
- [Loading cluster metadata from a file system using `admin cluster restore`](../tools-restore.md#cluster)
- [Exporting database data and metadata to a file system using `admin database dump`](../tools-dump.md#db)
- [Loading database data and metadata from a file system using `admin database restore`](../tools-restore.md#db)
- [Exporting individual schema objects to a file system using `tools dump`](../tools-dump.md#schema-objects)
- [Loading individual schema objects from a file system using `tools restore`](../tools-restore.md#schema-objects)
- [Connection and authentication when working with an S3-compatible object storage](../auth-s3.md)
- [Export to an S3-compatible object storage `export s3`](../export-s3.md)
- [Loading from an S3-compatible object storage `import s3`](../import-s3.md)
- [Configuring NFS for backup](../../../../recipes/nfs-backup/nfs-backup.md)
- [Export to NFS `export nfs`](../export-nfs.md)
- [Loading from NFS `import nfs`](../import-nfs.md)

{% include [_includes/options_overlay.md](options_overlay.md) %}
