# Backup and restore via NFS

This recipe describes preparing NFS for the server commands **`export nfs`** and **`import nfs`**: cluster requirements, access rights, step-by-step configuration of the NFS server and clients, feature flags, performance, and diagnostics. See the reference for command syntax: [export to NFS](../../reference/ydb-cli/export-import/export-nfs.md), [import from NFS](../../reference/ydb-cli/export-import/import-nfs.md).

## Prerequisites {#nfs-prerequisites}

The `export nfs` / `import nfs` operations are performed by **server processes** {{ ydb-short-name }}, not by the CLI client. This means that the file system must be accessible from each node of the cluster:

- The NFS directory must be mounted on **all nodes** of the {{ ydb-short-name }} cluster at the same absolute path.
- The {{ ydb-short-name }} process must have **write** permissions for export and **read** permissions for import to this directory.
- The `--fs-path` path must be absolute. Relative paths, paths containing `..` or `.`, and paths with null bytes will be rejected by the server.
- The functionality must be enabled using the [feature flag](#nfs-feature-flags) `enable_fs_backups`.

{% note warning %}

This functionality is compatible with the NFSv4 network file system protocol.

{% endnote %}

## Access rights {#nfs-permissions}

File operations are performed by server processes {{ ydb-short-name }}, not by the CLI client. Therefore, access rights to the NFS directory must be granted to the OS user under which the `ydbd` process is running.

**POSIX permissions:**

- For **export** (writing files): `rwx` on the directory (read for traversal, write for creating files and subdirectories, execute for navigation) and `rw` on files.
- For **import** (reading files): `rx` on the directory (read for traversal, execute for navigation) and `r` on files.

**NFSv4 ACL:**

NFSv4 uses an extended permission model instead of POSIX `rwx`. The same letter in an NFSv4 ACE has different meanings for files and directories:

#|
|| **Letter** | **For files** | **For directories** ||
|| `r` | `read-data` — reading content | `list-directory` — viewing content ||
|| `w` | `write-data` — writing data | `create-file` — creating files ||
|| `a` | `append-data` — appending data | `create-subdirectory` — creating subdirectories ||
|| `x` | `execute` — execution | `change-directory` — navigation ||
|| `D` | — | `delete-child` — deleting nested objects ||
|| `t` | `read-attributes` | `read-attributes` ||
|#

Permissions on the NFS directory for **export** (creating files and subdirectories):

- On the directory itself: `w` (create-file), `a` (create-subdirectory), `x` (change-directory), `t` (read-attributes), `D` (delete-child).
- On created files: `w` (write-data), `a` (append-data), `t` (read-attributes).

Permissions on the NFS directory for **import** (reading files):

- On the directory itself: `r` (list-directory), `x` (change-directory), `t` (read-attributes).
- On files: `r` (read-data), `t` (read-attributes).

Since files are created by the `ydbd` process during export, permissions on them cannot be set in advance. In NFSv4, **ACE inheritance** is used for this — the `f` (file-inherit) and `d` (directory-inherit) flags on the parent directory's ACE. When a new file or subdirectory is created, the ACE with these flags is automatically copied to the new object.

{% cut "Step-by-step setup NFS-server and clients" %}

Below are instructions for configuring the NFS server and {{ ydb-short-name }} client nodes for two permission management options: standard POSIX permissions and extended NFSv4 ACLs.

### Option 1: POSIX permissions

#### Configuring the NFS server

1. Install the NFS server package:


   ```bash
   sudo apt install nfs-kernel-server
   ```

2. Create a directory for export and assign the owner:


   ```bash
   sudo mkdir -p /mnt/ydb-backup
   sudo chown ydb:ydb /mnt/ydb-backup
   sudo chmod 755 /mnt/ydb-backup
   ```


   Options of the `chmod` command:

   - `7` (rwx) — the owner (`ydb`) can read, write, and enter the directory.
   - `5` (r-x) — the group and others can read and enter, but not write.
3. Add the directory to `/etc/exports`:


   ```text
   /mnt/ydb-backup ydb-node-*.example.com(rw,sync,root_squash)
   ```


   Export options:

   - `rw` — allow reading and writing.
   - `sync` — synchronous write (data is written to disk before confirming to the client).
   - `root_squash` — map `root` on the client to an anonymous user.
4. Apply the changes and start the NFS server:


   ```bash
   sudo exportfs -arv
   sudo systemctl enable --now nfs-server
   ```


   Options of `exportfs`:

   - `-a` — export all directories from `/etc/exports`.
   - `-r` — re-export all directories (synchronization with `/etc/exports`).
   - `-v` — verbose output.

#### Configuring clients ({{ ydb-short-name }} nodes)

1. Install the NFS client package:


   ```bash
   sudo apt install nfs-common
   ```

2. Create a mount point:


   ```bash
   sudo mkdir -p /mnt/ydb-backup
   ```

3. Mount the NFS directory:


   ```bash
   sudo mount -t nfs4 -o rw nfs-server.example.com:/mnt/ydb-backup /mnt/ydb-backup
   ```


   Mount options:

   - `-t nfs4` — NFSv4 file system type.
   - `-o rw` — mount with read and write permissions.

   In this example, the host server does not belong to the {{ ydb-short-name }} cluster, but backup and restore will work correctly if one of the cluster hosts is designated as the NFS server.
4. For automatic mounting at boot, add a line to `/etc/fstab`:


   ```text
   nfs-server.example.com:/mnt/ydb-backup /mnt/ydb-backup nfs4 rw,hard,timeo=600,retrans=2,_netdev 0 0
   ```


   Options of `fstab`:

   - `rw` — read and write.
   - `hard` — retry requests indefinitely if the server is unavailable (do not lose data).
   - `timeo=600` — request timeout in tenths of a second (60 seconds).
   - `retrans=2` — number of retries before reporting an error.
   - `_netdev` — mount only after network initialization.

#### Verifying functionality

On the server (or any client), create a test file as user `ydb`:


```bash
sudo -u ydb touch /mnt/ydb-backup/test-file
```


On all client nodes, verify that the file is visible:


```bash
sudo -u ydb ls /mnt/ydb-backup/test-file
```


After verification, delete the test file:


```bash
sudo -u ydb rm /mnt/ydb-backup/test-file
```


### Option 2: NFSv4 ACL

NFSv4 ACLs allow you to set more granular access permissions and configure permission inheritance for newly created files and directories.

#### Configuring the NFS server

1. Install the NFS server package and utilities for working with NFSv4 ACLs:


   ```bash
   sudo apt install nfs-kernel-server nfs4-acl-tools
   ```

2. Create a directory for export:


   ```bash
   sudo mkdir -p /mnt/ydb-backup-server
   ```

3. Add the directory to `/etc/exports`:


   ```text
   /mnt/ydb-backup-server ydb-node-*.example.com(rw,sync,no_subtree_check,root_squash)
   ```

4. Apply the changes:


   ```bash
   sudo exportfs -arv
   sudo systemctl enable --now nfs-server
   ```

#### Configuring clients ({{ ydb-short-name }} nodes)

Unlike POSIX permissions, which are set on the NFS server, NFSv4 ACLs are configured on each client after mounting the directory. If the NFS server is also a {{ ydb-short-name }} cluster node, all the steps described below must be performed on it.

1. Install the NFS client packages and utilities for working with NFSv4 ACLs:


   ```bash
   sudo apt install nfs-common nfs4-acl-tools
   ```

2. Create a mount point:


   ```bash
   sudo mkdir -p /mnt/ydb-backup
   ```

3. Mount the NFS directory using the NFSv4 protocol:


   ```bash
   sudo mount -t nfs4 -o rw nfs-server.example.com:/mnt/ydb-backup /mnt/ydb-backup
   ```


   Options:

   - `-t nfs4` — explicitly specify the NFSv4 protocol (required for correct ACL operation).
   - `-o rw` — mount with read and write permissions.
4. For automatic mounting, add a line to `/etc/fstab`:


   ```text
   nfs-server.example.com:/mnt/ydb-backup-server /mnt/ydb-backup nfs4 rw,hard,timeo=600,retrans=2,_netdev 0 0
   ```

5. Determine the UID of user `ydb`:


   ```bash
   id -u ydb
   ```


   Example output: `1871`
6. Configure NFSv4 ACL on the directory:


   ```bash
   sudo nfs4_setfacl -s A::<uid>:rwaxtD,A:fi:OWNER@:rwat,A:di:OWNER@:rwaxtD /mnt/ydb-backup
   ```


   ACE structure (`A:flags:who:permissions`):

   - `A` — ACE type: Allow.
   - Inheritance flags:

     - `f` (file-inherit) — inherit to created files.
     - `d` (directory-inherit) — inherit to created subdirectories.
     - `i` (inherit-only) — apply only to descendants, not to the directory itself.
   - `who` — who is granted permissions:

     - `1871` — UID of user `ydb`.
     - `OWNER@` — owner of the object.
   - Permissions:

     - `r` — read-data / list-directory.
     - `w` — write-data / create-file.
     - `a` — append-data / create-subdirectory.
     - `t` — read-attributes.
     - `D` - delete-child.

   This command sets:

   - Permissions for user `ydb` on the directory itself.
   - Inherited permissions for files and subdirectories (`fi`, `di`) that will apply only to descendants.
   - Owner permissions (`OWNER@`) with inheritance to files and directories.

#### Verification

Check the configured ACLs:


```bash
nfs4_getfacl /mnt/ydb-backup
```


Create a test file as user `ydb`:


```bash
sudo -u ydb touch /mnt/ydb-backup/test-file
```


Check that the file inherited the ACL:


```bash
nfs4_getfacl /mnt/ydb-backup/test-file
```


On all client nodes, check access:


```bash
sudo -u ydb ls /mnt/ydb-backup/test-file
```


After verification, delete the test file:


```bash
sudo -u ydb rm /mnt/ydb-backup/test-file
```

{% endcut %}

## Feature flags {#nfs-feature-flags}

To use export and import via NFS, you need to enable the feature flag `enable_fs_backups` in the [configuration](../../reference/configuration/feature_flags.md) of the cluster. By default, this flag is **disabled**.

## Performance configuration {#nfs-performance}

The performance of export and import operations via NFS is affected by two mechanisms: the **resource broker** (manages concurrency of data scan and load tasks) and the **actor system IO pool** (handles file write and read operations).

### Resource broker

Export and import via NFS use the same [resource broker](../../reference/configuration/resource_broker_config.md) queues as S3: `queue_backup` for export and `queue_restore` for import. Increasing limits speeds up operations but increases CPU load. Default values and configuration method are described in the [{#T}](../../reference/configuration/resource_broker_config.md) section.

### Actor system IO pool

File operations (write during export, read during import) are performed by actors in the pool of the [actor system](../../reference/configuration/actor_system_config.md#tuneconfig). By default, this pool contains `1` thread. Since file system operations are blocking, the `IO` pool can become a bottleneck during intensive export or import.

To increase the number of threads in the IO pool, change the `threads` parameter in the [actor system configuration](../../reference/configuration/actor_system_config.md#tuneconfig):


```yaml
actor_system_config:
  executor:
  - name: IO
    threads: 3
    time_per_mailbox_micro_secs: 100
    type: IO
```


{% note tip %}

Recommendations for choosing the number of IO pool threads when using NFS export/import:

- Start with `2` threads and monitor the load.
- Keep in mind that the IO pool handles not only NFS operations but also other blocking tasks (for example, log writing).
- Monitor the pool load via the [Embedded UI](../../reference/embedded-ui/ydb-monitoring.md#node_list_page).

{% endnote %}

## Logging {#nfs-logging}

The progress of export and import operations is reflected in the {{ ydb-short-name }} server logs. Main logging components:

- **`EXPORT`** — logging of the export process on SchemeShard: creating directories, copying tables, starting backup tasks, loading the schema.
- **`IMPORT`** — logging of the import process on SchemeShard: reading metadata, creating tables, restoring data.
- **`FS_WRAPPER`** — logging of file operations on each node. This component records I/O errors (file unavailability, insufficient space, locking errors), which helps identify issues with NFS mounting on specific nodes.

When diagnosing issues, it is recommended to:

- Check the operation status via `{{ ydb-cli }} operation get`.
- Search server logs for entries with components `EXPORT` or `IMPORT` and the operation ID.
- In case of file write or read errors, search logs for entries with component `FS_WRAPPER` — they contain the file path and error description, which allows determining on which host and with which file the problem occurred.
- Check that the NFS directory is available and has enough free space on **all** cluster nodes.

## Limitations {#nfs-limitations}

- Export and import via NFS support the same object types as export to S3. A detailed list of supported objects is provided in the [export command](../../reference/ydb-cli/export-import/export-nfs.md) documentation.
- Import always creates objects from scratch — you cannot import into existing tables.
- The `--list` parameter for listing export objects is currently only available for import from S3.
- The operation is not supported on the Windows platform.
