# Остановка кластера YDB

## Требования

- предустановленный кластер с любой конфигурацией

## Остановка кластера

```bash
ansible-playbook ydb_platform.ydb.stop_cluster
```

## Запуск кластера

Чтобы запустить кластер после остановки, используйте:

```bash
ansible-playbook ydb_platform.ydb.start_cluster
```

## Остановка конкретного хоста

Чтобы остановить YDB на конкретном хосте:

```bash
ansible-playbook ydb_platform.ydb.stop_cluster -l static-node-1.ydb-cluster.com
```

## Остановка только определенных типов узлов

Вы можете остановить только определенные типы узлов, используя теги:

- Остановить только узлы хранения:

  ```bash
  ansible-playbook ydb_platform.ydb.stop_cluster -t storage
  ```

- Остановить только динамические узлы:

  ```bash
  ansible-playbook ydb_platform.ydb.stop_cluster -t dynamic
  ```