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

### Вернуть pile в кластер (rejoin) {#rejoin}

После завершения плановых работ или устранения причин отказа ранее отключённые pile необходимо явным образом вводить обратно в эксплуатацию следующей командой:

```bash
{{ ydb-cli }} admin cluster bridge rejoin --pile <pile>
```

Сразу после запуска операции pile переходит в состояние `NOT_SYNCHRONIZED` и запускается фоновый процесс синхронизации данных; по её завершении pile автоматически становится `SYNCHRONIZED`. Дождавшись этого состояния, при необходимости можно [переключить роль `PRIMARY` на данный pile](#switchover).  
