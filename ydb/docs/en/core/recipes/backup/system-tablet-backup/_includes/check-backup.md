```bash
ls /path/to/backup/directory/hive/72057594037968897/backup_20251007T193502_g214_s1222/snapshot/
```

```text
manifest.json
schema.json
Tablet.json
TabletFollowerGroup.json
...
```

Check that the backup files contain data:

```bash
ls -lh /path/to/backup/directory/hive/72057594037968897/backup_20251007T193502_g214_s1222/snapshot
```

```text
total 128K
-rw-r--r-- 1 ydb disk  591 May 27 11:34 manifest.json
-rw-r--r-- 1 ydb disk  12K May 27 11:34 schema.json
-rw-r--r-- 1 ydb disk  47K May 27 11:34 Tablet.json
-rw-r--r-- 1 ydb disk  41K May 27 11:34 TabletFollowerGroup.json
...
```

Calculate the checksum of the `changelog.json` file and verify it against the checksum recorded in `changelog.json.sha256`:

```bash
sha256sum /path/to/backup/directory/hive/72057594037968897/backup_20251007T193502_g214_s1222/changelog.json
```

```text
ea4bdc2f7afaf7b6d35adbf13b5360e4e4a19046f742effdf1b4f0bb9c449185
```

```bash
cat /path/to/backup/directory/hive/72057594037968897/backup_20251007T193502_g214_s1222/changelog.json.sha256
```

```text
ea4bdc2f7afaf7b6d35adbf13b5360e4e4a19046f742effdf1b4f0bb9c449185
```
