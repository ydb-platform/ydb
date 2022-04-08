{% if tech %}

## YDB

### `kikimr.IsolationLevel`

| Тип значения | По умолчанию |
| --- | --- |
| Serializable, ReadCommitted, ReadUncommitted или ReadStale. | Serializable |

Экспериментальная pragma, позволяет ослабить уровень изоляции текущей транзакции в YDB.

{% endif %}