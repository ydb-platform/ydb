### Выгрузка базы данных {#example-full-db}

Выгрузка всех несистемных объектов базы данных в директорию `/mnt/nfs/backups/export1` на файловой системе:

```bash
{{ ydb-cli }} -p quickstart export nfs \
  --fs-path /mnt/nfs/backups/export1
```

### Выгрузка нескольких директорий {#example-specific-dirs}

Выгрузка объектов из директорий `dir1` и `dir2` базы данных в директорию `/mnt/nfs/backups/export1` на файловой системе:

```bash
{{ ydb-cli }} -p quickstart export nfs \
  --fs-path /mnt/nfs/backups/export1 \
  --include dir1 --include dir2
```

Либо с использованием альтернативного способа:

```bash
{{ ydb-cli }} -p quickstart export nfs \
  --fs-path /mnt/nfs/backups \
  --item src=dir1,dst=export1/dir1 --item src=dir2,dst=export1/dir2
```

### Выгрузка с шифрованием {#example-encryption}

Выгрузка всей базы данных с шифрованием:
- С использованием алгоритма шифрования `AES-128-GCM`
- С генерацией случайного ключа утилитой `openssl` в файл `~/my_secret_key`
- С чтением сгенерированного ключа из файла `~/my_secret_key`
- В директорию `/mnt/nfs/backups/export1` на файловой системе

```bash
openssl rand -out ~/my_secret_key 16
{{ ydb-cli }} -p quickstart export nfs \
  --fs-path /mnt/nfs/backups/export1 \
  --encryption-algorithm AES-128-GCM --encryption-key-file ~/my_secret_key
```

Выгрузка директории `dir1` базы данных с шифрованием:
- С использованием алгоритма шифрования `AES-256-GCM`
- С генерацией случайного ключа утилитой `openssl` в переменную окружения `YDB_ENCRYPTION_KEY`
- С чтением сгенерированного ключа из переменной окружения `YDB_ENCRYPTION_KEY`
- В директорию `/mnt/nfs/backups/export1` на файловой системе

```bash
export YDB_ENCRYPTION_KEY=$(openssl rand -hex 32)
{{ ydb-cli }} -p quickstart export nfs \
  --root-path dir1 \
  --fs-path /mnt/nfs/backups/export1 \
  --encryption-algorithm AES-256-GCM
```
