# Утилита ydb-dstool

Утилита ```ydb-dstool``` помогает реализовывать различные сценарии управления распределенным хранилищем кластера.

## Как запускать ydb-dstool

### Склонировать репозиторий

```bash
user@host:~$ mkdir github
user@host:~$ cd github
user@host:~$ git clone https://github.com/ydb-platform/ydb.git
```

### Установить библиотеку grpc_tools

Выполните шаги, которые описаны на странице https://grpc.io/docs/languages/python/quickstart.

### Скомпилировать proto файлы

```bash
user@host:~$ cd ~/github/ydb
user@host:~/github/ydb$ ydb_root=$(pwd)
user@host:~/github/ydb$ chmod +x ydb/apps/dstool/compile_protos.py
user@host:~/github/ydb$ ./ydb/apps/dstool/compile_protos.py --ydb-root ${ydb_root} 2>/dev/null
```

### Настроить параметры запуска

```bash
user@host:~$ cd ~/github/ydb
user@host:~/github/ydb$ export PATH=$PATH:${ydb_root}/ydb/apps/dstool
user@host:~/github/ydb$ export PYTHONPATH=$PYTHONPATH:${ydb_root}
user@host:~/github/ydb$ chmod +x ./ydb/apps/dstool/ydb-dstool.py
```
### Теперь можно выполнять команды

```bash
user@host:~/github/ydb$ ydb-dstool.py -e ydb.endpoint cluster list
```
