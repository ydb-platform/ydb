### Загрузка в корень базы данных {#example-full-db}

Загрузка в корень базы данных содержимого директории `/mnt/nfs/backups/export1` на файловой системе:

```bash
{{ ydb-cli }} -p quickstart import nfs \
  --fs-path /mnt/nfs/backups/export1
```

### Загрузка нескольких директорий {#example-specific-dirs}

Загрузка объектов из директорий `dir1` и `dir2` выгрузки, расположенной в `/mnt/nfs/backups/export1` на файловой системе, в одноименные директории базы данных:

```bash
{{ ydb-cli }} -p quickstart import nfs \
  --fs-path /mnt/nfs/backups/export1 \
  --include dir1 --include dir2
```

### Перечисление объектов в существующей зашифрованной выгрузке {#example-list}

Перечисление путей всех объектов в существующей зашифрованной выгрузке, расположенной в `/mnt/nfs/backups/export1` на файловой системе, с использованием секретного ключа из файла `~/my_secret_key`.

```bash
{{ ydb-cli }} -p quickstart import nfs \
  --fs-path /mnt/nfs/backups/export1 \
  --encryption-key-file ~/my_secret_key \
  --list
```

### Загрузка зашифрованной выгрузки {#example-encryption}

Загрузка одной таблицы, которая была выгружена по пути `dir/my_table`, в путь `dir1/dir/my_table` из зашифрованной выгрузки, расположенной в `/mnt/nfs/backups/export1` на файловой системе, с использованием секретного ключа из файла `~/my_secret_key`.

```bash
{{ ydb-cli }} -p quickstart import nfs \
  --fs-path /mnt/nfs/backups/export1 --destination-path dir1 \
  --include dir/my_table \
  --encryption-key-file ~/my_secret_key
```
