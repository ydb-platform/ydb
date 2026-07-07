## Управление кластером в режиме bridge

{% include [feature_enterprise.md](../../../_includes/feature_enterprise.md) %}

Ниже приведены типовые операции для кластера в [режиме bridge](../../../concepts/bridge.md) с использованием [соответствующих команд {{ ydb-short-name }} CLI](../../../reference/ydb-cli/commands/bridge/index.md).

### Посмотреть текущее состояние {#list}

Показывает текущее состояние каждого pile, настроенного на кластере {{ ydb-short-name }}.

```bash
{{ ydb-cli }} admin cluster bridge list
```

Пример вывода:

```bash
pile-a: PRIMARY
pile-b: SYNCHRONIZED
```

### Плановая смена `PRIMARY` (switchover) {#switchover}

Если известно, что в обозримом будущем запланированы плановые работы в датацентре или на оборудовании, на котором работает текущий `PRIMARY` pile, то рекомендуется заранее переключить кластер на использование другого pile в роли `PRIMARY`. Выберите другой pile в состоянии `SYNCHRONIZED`, чтобы переключить его в состояние `PRIMARY` следующей командой:

```bash
{{ ydb-cli }} admin cluster bridge switchover --new-primary <pile>
```

Переключение выполняется плавно: роли проходят через `PRIMARY/PROMOTED` и завершаются в состоянии `SYNCHRONIZED/PRIMARY`.

### Плановое отключение pile (takedown) {#takedown}

Если плановые работы приведут к недоступности одного из pile, его необходимо вывести из кластера перед их началом с помощью следующей команды:

```bash
{{ ydb-cli }} admin cluster bridge takedown --pile <pile>
# если отключаете текущий PRIMARY:
{{ ydb-cli }} admin cluster bridge takedown --pile <current-primary> --new-primary <synchronized-pile>
```

При выполнении операции pile переводится в `SUSPENDED`, затем — в `DISCONNECTED`; дальнейшие операции с кластером выполняются без участия отключённого pile.

Если отключается текущий `PRIMARY` и не было возможности [сменить его заранее](#switchover), то эти операции можно совместить, указав новый `PRIMARY` в аргументе `--new-primary`, который должен быть в состоянии `SYNCHRONIZED`.

```bash
{{ ydb-cli }} admin cluster bridge takedown --pile <pile>
# если отключаете текущий PRIMARY:
{{ ydb-cli }} admin cluster bridge takedown --pile <current-primary> --new-primary <synchronized-pile>
```

{% note warning %}

Перед началом плановых работ обязательно убедитесь через команду [list](#list), что операция вывода pile из кластера завершилась успешно и все pile находятся в ожидаемом состоянии.

{% endnote %}

### Аварийное отключение недоступного pile (failover) {#failover}

Так как между pile работает синхронная репликация, то при неожиданном выходе одного из них из строя работа кластера по умолчанию останавливается, и необходимо принять решение, продолжать ли работу кластера без этого pile. Это решение может принимать как человек (например, дежурный DevOps-инженер), так и внешняя по отношению к кластеру {{ ydb-short-name }} автоматизация.

В случае положительного решения о продолжении работы кластера необходимо выполнить следующую команду:

```bash
{{ ydb-cli }} admin cluster bridge failover --pile <unavailable-pile>
```

Если недоступен текущий `PRIMARY`, необходимо добавить параметр `--new-primary` с указанием имени pile в состоянии `SYNCHRONIZED`. Если параметр не указан или указан некорректно, команда завершится с ошибкой без изменений в кластере.

```bash
{{ ydb-cli }} admin cluster bridge failover --pile <unavailable-pile>
# если недоступен текущий PRIMARY:
{{ ydb-cli }} admin cluster bridge failover --pile <unavailable-primary> --new-primary <synchronized-pile>
```

Недоступный pile будет переведён в состояние `DISCONNECTED`, а при указании нового `PRIMARY` произойдёт переключение этой роли. Если остальные pile находятся в состояниях, отличных от `SYNCHRONIZED`, аварийное отключение также может быть выполнено. Допустимые переходы зависят от текущей пары состояний и приведены на [диаграмме состояний](../../../concepts/bridge.md#pile-states) и в [таблице переходов](../../../concepts/bridge.md#transitions-between-states).

{% note warning %}

Если на кластере включена обязательная аутентификация, при выполнении failover в аварийном состоянии аутентификация по логину и паролю не работает — используйте аутентификацию по клиентским сертификатам. Подробнее см. [{#T}](#emergency-auth).

{% endnote %}

### Вернуть pile в кластер (rejoin) {#rejoin}

После завершения плановых работ или устранения причин отказа ранее отключённые pile необходимо явным образом вводить обратно в эксплуатацию следующей командой:

```bash
{{ ydb-cli }} admin cluster bridge rejoin --pile <pile>
```

Сразу после запуска операции pile переходит в состояние `NOT_SYNCHRONIZED` и запускается фоновый процесс синхронизации данных; по её завершении pile автоматически становится `SYNCHRONIZED`. Дождавшись этого состояния, при необходимости можно [переключить роль `PRIMARY` на данный pile](#switchover).

### Особенности аутентификации в аварийном состоянии кластера {#emergency-auth}

Команды управления кластером в режиме bridge требуют административных прав. На кластере с включённой обязательной аутентификацией ([`enforce_user_token_requirement: true`](../../../reference/configuration/security_config.md)) на исправном кластере работают все способы аутентификации, включая [аутентификацию по логину и паролю](../../../security/authentication.md#static-credentials).

Однако при отказе pile кластер приостанавливает обслуживание запросов до выполнения [failover](#failover). В этом состоянии аутентификация по логину и паролю не работает: для проверки учётных данных нужен доступ к [SchemeShard](../../../concepts/glossary.md#scheme-shard), который недоступен на кластере в аварийном состоянии. Попытка входа завершается ошибкой:

```text
SchemeShard is unreachable
```

Единственный способ аутентификации, работающий в аварийном состоянии кластера, — [аутентификация по клиентским сертификатам](../../../reference/configuration/client_certificate_authorization.md). Клиентский сертификат проверяется локально узлом, принявшим запрос, поэтому такая проверка не зависит от доступности кластера в целом.

Аутентификация по клиентским сертификатам должна быть настроена на кластере заранее:

1. Секция [`client_certificate_authorization`](../../../reference/configuration/client_certificate_authorization.md) конфигурации кластера должна присваивать подключениям с доверенным клиентским сертификатом [SID](../../../concepts/glossary.md#access-sid) административной группы:

    ```yaml
    client_certificate_authorization:
      request_client_certificate: true
      client_certificate_definitions:
        - member_groups: ["ADMINS"]
          subject_terms:
          - short_name: "O"
            values: ["YDB"]
    ```

2. Этот SID должен входить в список `administration_allowed_sids` секции [`security_config`](../../../reference/configuration/security_config.md):

    ```yaml
    security_config:
      enforce_user_token_requirement: true
      administration_allowed_sids:
      - "root"
      - "ADMINS"
    ```

Если кластер развёрнут по [инструкции по развёртыванию](initial-deployment/deployment-configuration-v2.md), эти настройки уже включены в конфигурацию, а в качестве клиентского сертификата подойдут файлы `node.crt` и `node.key` из каталога `~/CA/certs/` на любом узле кластера.

Для аутентификации по клиентскому сертификату укажите его в глобальных опциях {{ ydb-short-name }} CLI `--client-cert-file` и `--client-cert-key-file`. Подключаться необходимо к узлу работоспособного pile. Пример выполнения failover:

```bash
{{ ydb-cli }} -e grpcs://<node.ydb.tech>:2135 \
    --ca-file ca.crt \
    --client-cert-file node.crt \
    --client-cert-key-file node.key \
    admin cluster bridge failover --pile <unavailable-pile> --new-primary <synchronized-pile>
```

где:

- `<node.ydb.tech>` — FQDN узла в работоспособном pile;
- `ca.crt` — сертификат доверенного центра сертификации кластера;
- `node.crt`, `node.key` — клиентский сертификат и приватный ключ к нему, удовлетворяющие требованиям секции `client_certificate_authorization`.

{% note warning %}

Проверьте работоспособность аутентификации по клиентским сертификатам заранее, до возникновения аварии: изменить конфигурацию кластера в аварийном состоянии невозможно.

{% endnote %}
