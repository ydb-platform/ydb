# CREATE RESOURCE POOL CLASSIFIER

`CREATE RESOURCE POOL CLASSIFIER` создаёт [resource pool classifier](../../../../concepts/gloassary#resource-pool-classifier).

## Синтаксис

```sql
CREATE RESOURCE POOL CLASSIFIER <имя>
WITH ( <имя_параметра> [= <значение_параметра>] [, ... ] )
```

### Параметры

* `имя` - имя создаваемого resource pool classifier. Должно быть уникально. * `имя` - не должно содержать в себе запрещенные символы для схемных объектов.
* `WITH ( <имя_параметра> [= <значение_параметра>] [, ... ] )` позволяет задать значения параметров, определяющих поведение resource pool classifier. Поддерживаются следующие опции:
    * `RANK` (Int) - опциональное поле которое задает порядок выбора resource pool classifier. Допустимые значения: уникальное число [0, MAX_INT64]
    * `RESOURCE_POOL` (String) - обязательное поле задающее имя resource pool в который будут отправлены запросы удовлетворяющие критериям resource pool classifier.
    * `MEMBERNAME` (String) - опциональное поле которое задает какой пользователь или группа должны быть отправлены в описанные классификатор

## Замечания {#замечания}

Если в DDL для создания resource pool classifier не указан `RANK`, то по умолчанию будет проставлен `RANK` = (максимальный из существующих resource pool classifier) + 1000. Все `RANK` должны быть уникальны, чтобы иметь строго детерминированный порядок выбора resource pool в случае конфликтующих условий. Такое поведение выбрано для возможности добавлять между уже созданными resource pool classifier другие resource pool classifier.

В базе данных есть ограничение на максимальное число resource pool classifier и оно равно 1000. Это ограничение выбрано специально в целях оптимизации производительности. Ограничение на число resource pool соответствует ограничениям на число схемных объектов

Также возможно иметь resource pool classifer которые ссылаются на не существующий resource pool или же у пользователя может не быть доступа к нему, в этом случае они пропускаются.

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