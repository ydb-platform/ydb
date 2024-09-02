# CREATE RESOURCE POOL CLASSIFIER

`CREATE RESOURCE POOL CLASSIFIER` создаёт [resource pool classifier](../../../../concepts/gloassary#resource-pool-classifier).

## Синтаксис

```sql
CREATE RESOURCE POOL CLASSIFIER <имя>
WITH ( <имя_параметра> [= <значение_параметра>] [, ... ] )
```

### Параметры

* `имя` - имя создаваемого resource pool classifier. Должно быть уникально. Не допускается запись в виде пути (т.е. не должно содержать `/`).
* `WITH ( <имя_параметра> [= <значение_параметра>] [, ... ] )` позволяет задать значения параметров, определяющих поведение resource pool classifier. Поддерживаются следующие опции:
    * `RANK` (Int) - опциональное поле которое задает порядок выбора resource pool.
    * `RESOURCE_POOL` (String) - обязательное поле задающее имя resource pool в который будут отправлены запросы удовлетворяющие критериям resource pool classifier.
    * `MEMBERNAME` (String) - опциональное поле которое задает какой пользователь или группа должны быть отправлены в описанные классификатор

## Замечания {#замечания}

Если в DDL для создания resource pool classifier не указан `RANK`, то по умолчанию будет проставлен `RANK` = (максимальный из существующих resource pool classifier) + 1000. Все `RANK` должны быть уникальны, чтобы иметь строго детерминированный порядок выбора resource pool в случае конфликтующих условий. Такое поведение выбрано для возможности добавлять между уже созданными resource pool classifier другие resource pool classifier.

В базе данных есть ограничение на максимальное число пулов и оно равно 1000. Это ограничение выбрано специально в целях оптимизации производительности

Также возможно иметь resource pool classifer которые ссылаются на не существующий resource pool или те у которых нету доступов к нему, в этом случае они пропускаются.

## Примеры {#примеры}

```
CREATE RESOURCE POOL CLASSIFIER olap_classifier WITH (
    RANK=1000,
    RESOURCE_POOL="olap",
    MEMBERNAME="user1@domain"
)
```

В примере выше создается resource pool classifier с именем `olap_classifier`, который описывает перенаправление запросов от пользователя `user1@domain` в resource pool c именем `olap`, а запросы всех остальных пользователей будут отправлены в `default` resource pool при условии, что другие resource pool classifier не существуют

## Разрешения

Требуется разрешение `ALL` на базу данных

## См. также

* [ALTER RESOURCE POOL CLASSIFIER](alter-resource-pool-classifier.md)
* [DROP RESOURCE POOL CLASSIFER](drop-resource-pool-classifier.md)