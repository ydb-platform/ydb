# Backup and recovery via NFS

This recipe describes how to prepare NFS for the server-side **`export nfs`** and **`import nfs`** commands: cluster requirements, permissions, step-by-step NFS server and client setup, feature flags, performance, and diagnostics. For command syntax, see [Exporting to NFS](../../reference/ydb-cli/export-import/export-nfs.md) and [Importing from NFS](../../reference/ydb-cli/export-import/import-nfs.md).

## Prerequisites {#nfs-prerequisites}

The `export nfs` / `import nfs` operations are performed by **server processes** of {{ ydb-short-name }}, not by the CLI client. This means the filesystem must be available from every cluster node:

- The NFS directory must be mounted on **all nodes** of the {{ ydb-short-name }} cluster at the same absolute path.
- The {{ ydb-short-name }} process must have **write** permission for export and **read** permission for import on that directory.
- The `--fs-path` value must be absolute. Relative paths, paths containing `..` or `.`, and paths with null bytes are rejected by the server.
- The feature must be enabled with the [feature flag](#nfs-feature-flags) `enable_fs_backups`.

{% note warning %}

This functionality is compatible with the NFSv4 network file system protocol.

{% endnote %}

## Permissions {#nfs-permissions}

File operations are performed by {{ ydb-short-name }} server processes, not by the CLI client. Therefore, permissions on the NFS directory must be granted to the OS user under which the `ydbd` process runs.

**POSIX permissions:**

- For **export** (writing files): `rwx` on the directory (read to traverse, write to create files and subdirectories, execute to navigate) and `rw` on files.
- For **import** (reading files): `rx` on the directory (read to traverse, execute to navigate) and `r` on files.

**NFSv4 ACL:**

NFSv4 uses an extended permission model instead of POSIX `rwx`. The same letter in an NFSv4 ACE has different meaning for files and directories:

#|
|| **Letter** | **For files** | **For directories** ||
|| `r` | `read-data` — read contents | `list-directory` — list contents ||
|| `w` | `write-data` — write data | `create-file` — create files ||
|| `a` | `append-data` — append data | `create-subdirectory` — create subdirectories ||
|| `x` | `execute` — execute | `change-directory` — navigate ||
|| `D` | — | `delete-child` — delete child objects ||
|| `t` | `read-attributes` | `read-attributes` ||
|#

Permissions on the NFS directory for **export** (creating files and subdirectories):

- On the directory itself: `w` (create-file), `a` (create-subdirectory), `x` (change-directory), `t` (read-attributes), `D` (delete-child).
- On files being created: `w` (write-data), `a` (append-data), `t` (read-attributes).

Permissions on the NFS directory for **import** (reading files):

- On the directory itself: `r` (list-directory), `x` (change-directory), `t` (read-attributes).
- On files: `r` (read-data), `t` (read-attributes).

Because files are created by the `ydbd` process during export, file permissions cannot be set in advance. In NFSv4, **ACE inheritance** is used — flags `f` (file-inherit) and `d` (directory-inherit) on the parent directory ACE. When a new file or subdirectory is created, ACEs with these flags are automatically copied to the new object.

{% cut "Step-by-step NFS-server and clients setting" %}

Below are instructions for setting up an NFS server and {{ ydb-short-name }} client nodes for two permission management options: standard POSIX permissions and extended NFSv4 ACL.

### Option 1: POSIX permissions

#### NFS server setup

1. Install the NFS server package:


   ```bash
   sudo apt install nfs-kernel-server
   ```

2. Create the export directory and set the owner:


   ```bash
   sudo mkdir -p /mnt/ydb-backup
   sudo chown ydb:ydb /mnt/ydb-backup
   sudo chmod 755 /mnt/ydb-backup
   ```


   `chmod` options:

   - `7` (rwx) — the owner (`ydb`) can read, write, and enter the directory.
   - `5` (r-x) — the group and others can read and enter, but not write.
3. Add the directory to `/etc/exports`:


   ```text
   /mnt/ydb-backup ydb-node-*.example.com(rw,sync,root_squash)
   ```


   Export options:

   - `rw` — allow read and write.
   - `sync` — synchronous writes (data is written to disk before the client is acknowledged).
   - `root_squash` — map client `root` to an anonymous user.
4. Apply changes and start the NFS server:


   ```bash
   sudo exportfs -arv
   sudo systemctl enable --now nfs-server
   ```


   `exportfs` options:

   - `-a` — export all directories from `/etc/exports`.
   - `-r` — re-export all directories (sync with `/etc/exports`).
   - `-v` — verbose output.

#### Client setup ({{ ydb-short-name }} nodes)

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

   - `-t nfs4` — NFSv4 filesystem type.
   - `-o rw` — mount read-write.

   In this example the NFS server host is not part of the {{ ydb-short-name }} cluster; backup and recovery still work correctly if one of the cluster hosts acts as the NFS server.
4. For automatic mount at boot, add a line to `/etc/fstab`:


   ```text
   nfs-server.example.com:/mnt/ydb-backup /mnt/ydb-backup nfs4 rw,hard,timeo=600,retrans=2,_netdev 0 0
   ```


   `fstab` options:

   - `rw` — read and write.
   - `hard` — if the server is unavailable, retry requests indefinitely (do not lose data).
   - `timeo=600` — request timeout in tenths of a second (60 seconds).
   - `retrans=2` — number of retransmissions before reporting an error.
   - `_netdev` — mount only after the network is initialized.

#### Verification

On the server (or any client), create a test file as user `ydb`:


```bash
sudo -u ydb touch /mnt/ydb-backup/test-file
```


On all client nodes, verify that the file is visible:


```bash
sudo -u ydb ls /mnt/ydb-backup/test-file
```


After verification, remove the test file:


```bash
sudo -u ydb rm /mnt/ydb-backup/test-file
```


### Option 2: NFSv4 ACL

NFSv4 ACL allows more granular access control and inheritance for newly created files and directories.

#### NFS server setup

1. Install the NFS server package and NFSv4 ACL utilities:


   ```bash
   sudo apt install nfs-kernel-server nfs4-acl-tools
   ```

2. Create the export directory:


   ```bash
   sudo mkdir -p /mnt/ydb-backup-server
   ```

3. Add the directory to `/etc/exports`:


   ```text
   /mnt/ydb-backup-server ydb-node-*.example.com(rw,sync,no_subtree_check,root_squash)
   ```

4. Apply changes:


   ```bash
   sudo exportfs -arv
   sudo systemctl enable --now nfs-server
   ```

#### Client setup ({{ ydb-short-name }} nodes)

Unlike POSIX permissions, which are set on the NFS server, NFSv4 ACL are configured on each client after mounting the directory. If the NFS server is also a {{ ydb-short-name }} cluster node, perform all steps below on it as well.

1. Install the NFS client package and NFSv4 ACL utilities:


   ```bash
   sudo apt install nfs-common nfs4-acl-tools
   ```

2. Create a mount point:


   ```bash
   sudo mkdir -p /mnt/ydb-backup
   ```

3. Mount the NFS directory using NFSv4:


   ```bash
   sudo mount -t nfs4 -o rw nfs-server.example.com:/mnt/ydb-backup /mnt/ydb-backup
   ```


   Options:

   - `-t nfs4` — explicitly specify NFSv4 (required for ACL to work correctly).
   - `-o rw` — mount read-write.
4. For automatic mount, add a line to `/etc/fstab`:


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
   - `who` — who receives permissions:

     - `1871` — UID of user `ydb`.
     - `OWNER@` — object owner.
   - Permissions:

     - `r` — read-data / list-directory.
     - `w` — write-data / create-file.
     - `a` — append-data / create-subdirectory.
     - `t` — read-attributes.
     - `D` — delete-child.

   This command sets:

   - Permissions for user `ydb` on the directory itself.
   - Inherited permissions for files and subdirectories (`fi`, `di`) that apply only to descendants.
   - Owner (`OWNER@`) permissions with inheritance on files and directories.

#### Verification

Check the configured ACL:


```bash
nfs4_getfacl /mnt/ydb-backup
```


Create a test file as user `ydb`:


```bash
sudo -u ydb touch /mnt/ydb-backup/test-file
```


Verify that the file inherited the ACL:


```bash
nfs4_getfacl /mnt/ydb-backup/test-file
```


On all client nodes, verify access:


```bash
sudo -u ydb ls /mnt/ydb-backup/test-file
```


After verification, remove the test file:


```bash
sudo -u ydb rm /mnt/ydb-backup/test-file
```

{% endcut %}

## Feature flags {#nfs-feature-flags}

To use export and import via NFS, enable the `enable_fs_backups` feature flag in the [cluster configuration](../../reference/configuration/feature_flags.md). By default, this flag is **disabled**.

## Performance configuration {#nfs-performance}

The performance of NFS export and import is affected by two mechanisms: the **resource broker** (controls parallelism of scan and data upload tasks) and the **actor system IO pool** (handles file read and write operations).

### Resource broker

NFS export and import use the same [resource broker](../../reference/configuration/resource_broker_config.md) queues as S3: `queue_backup` for export and `queue_restore` for import. Increasing limits speeds up operations but increases CPU load. Default values and configuration are described in [{#T}](../../reference/configuration/resource_broker_config.md).

### Actor system IO pool

File operations (writes during export, reads during import) are performed by actors in the `IO` pool of the [actor system](../../reference/configuration/actor_system_config.md#tuneconfig). By default, this pool has `1` thread. Because filesystem operations are blocking, under intensive export or import the IO pool can become a bottleneck.

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

Recommendations for choosing the IO pool thread count when using NFS export/import:

- Start with `2` threads and monitor load.
- Keep in mind that the IO pool serves not only NFS operations but also other blocking tasks (for example, log writes).
- Monitor pool utilization via [Embedded UI](../../reference/embedded-ui/ydb-monitoring.md#node_list_page).

{% endnote %}

## Logging {#nfs-logging}

Export and import progress is reflected in {{ ydb-short-name }} server logs. Main logging components:

- **`EXPORT`** — export process logging on SchemeShard: creating directories, copying tables, starting backup tasks, uploading schema.
- **`IMPORT`** — import process logging on SchemeShard: reading metadata, creating tables, restoring data.
- **`FS_WRAPPER`** — file operation logging on each node. This component records I/O errors (file unavailable, out of space, lock errors), which helps identify NFS mount issues on specific nodes.

When diagnosing problems, it is recommended to:

- Check operation status with `{{ ydb-cli }} operation get`.
- Search server logs for entries with `EXPORT` or `IMPORT` components and the operation ID.
- For file read or write errors, search logs for `FS_WRAPPER` entries — they contain the file path and error description, which helps identify the host and file involved.
- Verify that the NFS directory is accessible and has enough free space on **all** cluster nodes.

## Limitations {#nfs-limitations}

- NFS export and import support the same object types as S3 export. See [export command documentation](../../reference/ydb-cli/export-import/export-nfs.md) for the full list of supported objects.
- Import always creates objects from scratch — you cannot import into existing tables.
- The `--list` parameter for listing export objects is currently available only for import from S3.
- The operation is not supported on the Windows platform.
