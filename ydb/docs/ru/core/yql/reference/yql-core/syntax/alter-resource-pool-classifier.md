# ALTER RESOURCE POOL CLASSIFIER

`ALTER RESOURCE POOL CLASSIFIER` изменяет определение [resource pool classifier](../../../../concepts/gloassary#resource-pool-classifier).

### Параметры

* `RANK` (Int) - опциональное поле которое задает порядок выбора resource pool classifier. Допустимые значения: уникальное число [0, MAX_INT64]
* `RESOURCE_POOL` (String) - обязательное поле задающее имя resource pool в который будут отправлены запросы удовлетворяющие критериям resource pool classifier.
* `MEMBERNAME` (String) - опциональное поле которое задает какой пользователь или группа должны быть отправлены в описанные классификатор

## Изменение параметров

Синтаксис для изменения любого параметра resource pool classifier выглядит следующим образом:

```sql
ALTER RESOURCE POOL CLASSIFIER <имя> SET (<key> = <value>);
```

```<key>``` — имя параметра, ```<value>``` — его новое значение.

Например, такая команда изменит пользователя для которого применяется правило:

```sql
ALTER RESOURCE POOL CLASSIFIER olap_classifier SET (MEMBERNAME = "user2@domain");
```

## Сброс параметров

Команда для сброса параметра resource pool classifier выглядит следующим образом:

```sql
ALTER RESOURCE POOL CLASSIFIER <имя> RESET (<key>);
```

```<key>``` — имя параметра.

Например, такая команда сбросит настройки `MEMBERNAME` для resource pool classifier:

```sql
ALTER RESOURCE POOL CLASSIFIER olap_classifier RESET (MEMBERNAME);
```

## Разрешения

Требуется разрешение `ALL` на базу данных

## См. также

* [CREATE RESOURCE POOL CLASSIFIER](create-resource-pool-classifier.md)
* [DROP RESOURCE POOL CLASSIFIER](drop-resource-pool-classifier.md)