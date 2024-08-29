# Настройка ydbops

{% include [warning.md](_includes/warning.md) %}

`ydbops` можно запустить, указав все необходимые аргументы командной строки при вызове команды.
Однако, есть две функции, которые позволяют избежать повторения часто используемых аргументов:


- [Файл конфигурации](#config-file)
- [Переменные окружения](#environment-variables)

## Файл конфигурации {#config-file}

Файл конфигурации для `ydbops` — это файл в формате YAML, содержащий несколько профилей. Профили для `ydbops` работают так же, как и профили в [{{ ydb-short-name }} CLI](../ydb-cli/profile/index.md), однако они не взаимозаменяемы, поскольку могут содержать параметры, которые актуальны для `ydbops`, но не имеют смысла для {{ ydb-short-name }} CLI, и наоборот.

Расположение файла конфигурации по умолчанию следует той же конвенции, что и {{ ydb-short-name }} CLI, и находится в той же папке в подкаталоге `ydbops`. Для сравнения:

- файл конфигурации по умолчанию для {{ ydb-short-name }} CLI : `$HOME/ydb/config/config.yaml`
- файл конфигурации по умолчанию для CLI `ydbops` находится в той же папке, в подкаталоге `ydbops`: `$HOME/ydb/ydbops/config/config.yaml`

Некоторые параметры командной строки могут быть записаны в файл конфигурации вместо того, чтобы указывать их каждый раз напрямую при вызове `ydbops`.

### Примеры

Вызов команды `ydbops restart` без профиля:

```bash
ydbops restart \
 -e grpc://<hostname>:2135 \
 --kubeconfig ~/.kube/config \
 --k8s-namespace <k8s-namespace> \
 --user admin \
 --password-file ~/<password-file> \
 --tenant --tenant-list=<tenant-name>
```

Вызов той же команды `ydbops restart` с включенными параметрами профиля делает команду намного короче:

```bash
ydbops restart \
 --config-file ./config.yaml \
 --tenant --tenant-list=<tenant-name>
```

Для указанного выше вызова предполагается наличие следующего `config.yaml`:

```yaml
current-profile: my-profile
my-profile:
  endpoint: grpc://<hostname>:2135
  user: admin
  password-file: ~/<password-file>
  k8s-namespace: <k8s-namespace>
  kubeconfig: ~/.kube/config
```

### Команды управления профилями

В настоящее время `ydbops` не поддерживает создание, изменение и активацию профилей через команды CLI [так, как это делает {{ ydb-short-name }} CLI](../ydb-cli/profile/index.md#commands).

Файл конфигурации необходимо создать и редактировать вручную.

### Максимально полный файл конфигурации

Пример файла конфигурации со всеми возможными параметрами и примерами значений (скорее всего, не все опции будут нужны одновременно):

```yaml
# специальный ключ `current-profile` может быть указан для
# использования в качестве активного профиля по умолчанию при вызове CLI
current-profile: my-profile

my-profile:
  endpoint: grpc://your.ydb.cluster.fqdn:2135

  # расположение файла CA при использовании grpcs в endpoint
  ca-file: /path/to/custom/ca/file

  # имя пользователя и файл пароля, если используется аутентификация при помощи логина и пароля:
  user: your-ydb-user-name
  password-file: /path/to/password-file

  # если используется аутентификация при помощи access token
  token-file: /path/to/ydb/token

  # если идет работа с YDB кластерами под Kubernetes, можно указать путь к kubeconfig:
  kubeconfig: /path/to/kube/config
  k8s-namespace: <k8s-namespace>
```

## Переменные окружения {#environment-variables}

Кроме того, можно указать несколько переменных окружения вместо передачи аргументов командной строки или использования [файлов конфигурации](#config-files).


Для справки о приоритете при определении опции в нескольких местах, вызовите `ydbops --help`.

- `YDB_TOKEN` может быть передана вместо флага `--token-file` или параметра профиля `token-file`.
- `YDB_PASSWORD` может быть передана вместо флага `--password-file` или параметра профиля `password-file`.
- `YDB_USER` может быть передана вместо флага `--user` или параметра профиля `user`.

## Смотрите также

- [{#T}](index.md)
- [{#T}](install.md)
- [{#T}](rolling-restart-scenario.md)
