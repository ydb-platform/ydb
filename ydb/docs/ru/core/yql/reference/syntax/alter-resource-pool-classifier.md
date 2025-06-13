# ALTER RESOURCE POOL CLASSIFIER

`ALTER RESOURCE POOL CLASSIFIER` изменяет определение [классификатора пула ресурсов](../../../concepts/glossary.md#resource-pool-classifier.md).

## Синтаксис

### Изменение параметров

Синтаксис для изменения любого параметра классификатора пула ресурсов выглядит следующим образом:

```yql
ALTER RESOURCE POOL CLASSIFIER <name> SET (<key> = <value>);
```

`<key>` — имя параметра, `<value>` — его новое значение.

Например, такая команда изменит пользователя, для которого применяется правило:

```yql
ALTER RESOURCE POOL CLASSIFIER olap_classifier SET (MEMBER_NAME = "user2@domain");
```

### Сброс параметров

Команда для сброса параметра классификатора пула ресурсов выглядит следующим образом:

```yql
ALTER RESOURCE POOL CLASSIFIER <name> RESET (<key>);
```

`<key>` — имя параметра.

Например, такая команда сбросит настройку `MEMBER_NAME`:

```yql
ALTER RESOURCE POOL CLASSIFIER olap_classifier RESET (MEMBER_NAME);
```

## Разрешения

Требуется [разрешение](grant.md#permissions-list) `ALL` на базу данных, пример выдачи такого разрешения:

```yql
GRANT 'ALL' ON `/my_db` TO `user1@domain`;
```

## Параметры

* `RANK` (Int64) — опциональное поле, задающее порядок выбора классификатора пула ресурсов. Если значение не указано, берётся максимальный существующий `RANK` и к нему прибавляется 1000. Допустимые значения: уникальное число в диапазоне $[0, 2^{63}-1]$.
* `RESOURCE_POOL` (String) — обязательное поле, задающее имя пула ресурсов, в который будут отправлены запросы, удовлетворяющие критериям классификатора.
* `MEMBER_NAME` (String) — опциональное поле, определяющее, какой пользователь или группа пользователей будут отправлены в указанный пул ресурсов. Если поле не указано, классификатор игнорирует `MEMBER_NAME`, и классификация осуществляется по другим признакам.

## См. также

* [{#T}](../../../dev/resource-consumption-management.md)
* [{#T}](create-resource-pool-classifier.md)
* [{#T}](drop-resource-pool-classifier.md)