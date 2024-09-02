# DROP RESOURCE POOL CLASSIFIER

`DROP RESOURCE POOL CLASSIFIER` удаляет [resource pool classifier](../../../../concepts/gloassary#resource-pool-classifier).

## Синтаксис

```sql
DROP RESOURCE POOL CLASSIFIER <имя>
```

### Параметры

* `имя` - имя resource pool classifier, подлежащего удалению.

## Замечания {#замечания}

* `имя` - не должно содержать в себе символ `/`, оно не привязано к иерархии схемы

## Разрешения

Требуется разрешение `ALL` на базу данных

## Примеры

Следующая команда удалит resource pool classifier с именем olap_classifier:

```sql
DROP RESOURCE POOL CLASSIFIER olap_classifier;
```

## См. также

* [CREATE RESOURCE POOL CLASSIFIER](create-resource-pool-classifier.md)
* [ALTER RESOURCE POOL CLASSIFIER](alter-resource-pool-classifier.md)