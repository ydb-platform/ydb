### Кластер {{ ydb-short-name }}

| Требование | Рекомендация |
| --- | --- |
| Endpoint | Рабочий gRPC endpoint, например `grpc://localhost:2136`, база `/local` |
| Доступ | Учётная запись с правами на создание таблиц и запись данных |
| Кодировка | UTF-8 для текстовых полей |

### {{ ydb-short-name }} CLI

CLI нужен для проверки подключения, создания таблиц (если не создаёт их инструмент) и верификации результата.

```bash
curl -sSL https://install.ydb.tech/cli | bash
ydb version
```

Подробнее: [Установка YDB CLI](../../../reference/ydb-cli/install.md).

Проверка подключения:

```bash
ydb -e grpc://localhost:2136 -d /local scheme ls
```
