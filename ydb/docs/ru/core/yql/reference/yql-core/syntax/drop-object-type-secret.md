# DROP OBJECT (TYPE SECRET)

{% note warning %}

Текущий систаксис работы с секретами является временным, в будущих релизах {{ydb-full-name}} он будет изменен.

{% endnote %}


Удаляет указанный [секрет](../../../concepts/datamodel/secrets.md).

Если секрета с таким именем не существует, возвращается ошибка.

**Пример**

```yql
DROP OBJECT my_secret (TYPE SECRET);
```

