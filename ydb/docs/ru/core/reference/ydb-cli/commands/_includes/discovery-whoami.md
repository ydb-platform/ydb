# Проверка аутентификации

Информационная команда `discovery whoami` позволяет проверить, от имени какой учетной записи сервер фактически принимает запросы:

```bash
{{ ydb-cli }} [connection options] discovery whoami [-g|--groups] [-l|--access-list] [-a|--all]
```

{% include [conn_options_ref.md](conn_options_ref.md) %}

В ответ выводится имя учетной записи (User SID). Дополнительную информацию можно запросить с помощью следующих опций:

- `-g`, `--groups` — показать группы, в которые входит учетная запись;
- `-l`, `--access-list` — показать уровни доступа, выданные учетной записи (administration, monitoring, viewer, database, register node, bootstrap);
- `-a`, `--all` — показать всю дополнительную информацию (эквивалентно одновременному указанию `-g` и `-l`).

Уровни доступа, отображаемые при использовании опций `-l` или `-a`, соответствуют [иерархической конфигурации управления доступом](../../../configuration/security_config.md#security-access-levels). Подробная информация о списках уровней доступа и их иерархии приведена в разделе [Списки уровней доступа](../../../../security/authorization.md#access-level-lists) документации по авторизации.

Выводятся только те уровни доступа, которые действительно выданы пользователю:

- **Database** (наличие в `database_allowed_sids`) — даёт право работать с Embedded UI только как «пользователь базы данных»: можно открыть UI и видеть данные в рамках конкретной базы, но нельзя просматривать общекластерные данные или выполнять операции уровня кластера.
- **Viewer** (наличие в `viewer_allowed_sids`) — даёт право просматривать Embedded UI без возможности вносить изменения.
- **Monitoring** (наличие в `monitoring_allowed_sids`) — даёт право выполнять в Embedded UI действия, изменяющие состояние системы.
- **Administration** (наличие в `administration_allowed_sids`) — даёт право выполнять административные действия с базами данных или кластером.
- **Register node** (наличие в `register_dynamic_node_allowed_sids`) — даёт право регистрировать динамические узлы в кластере.
- **Bootstrap** (наличие в `bootstrap_allowed_sids`) — даёт право выполнять операции начальной инициализации кластера.

Если на сервере {{ ydb-short-name }} не включена аутентификация (что может применяться, например, при самостоятельном локальном развертывании), выполнение команды завершится ошибкой.

Поддержка опции `-g` зависит от конфигурации сервера. Если она отключена, в ответ всегда будет возвращаться `User has no groups`, независимо от фактического членства вашей учетной записи в каких-либо группах.

## Примеры

### Базовое использование

```bash
$ ydb -p quickstart discovery whoami
User SID: aje5kkjdgs0puc18976co@as
```

### С выводом групп

```bash
$ ydb -p quickstart discovery whoami -g
User SID: aje5kkjdgs0puc18976co@as

User has no groups
```

### С выводом списка уровней доступа

```bash
$ ydb -p quickstart discovery whoami -l
User SID: user1@builtin

Access levels:
Database
Viewer
```

### С выводом всей информации

```bash
$ ydb -p quickstart discovery whoami -a
User SID: admin@builtin

Group SIDs:
all-users@well-known
ADMINS

Access levels:
Database
Viewer
Monitoring
Administration
```

