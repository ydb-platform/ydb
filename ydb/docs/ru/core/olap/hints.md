# Optimizer Hints (Подсказки Оптимизатора)

Optimizer hints позволяют влиять на поведение cost-based optimizer при планировании выполнения SQL-запросов. Система поддерживает четыре типа подсказок для управления соединениями (joins) и статистикой.

## Использование через прагму

Подсказки задаются через прагму `PRAGMA ydb.OptimizerHints` в начале SQL-запроса:

```sql
PRAGMA ydb.OptimizerHints =
'
    Rows(R # 10e8)
    Rows(T # 1)
    Rows(R T # 1)
    JoinOrder( (R S) (T U) )
    JoinType(T U Broadcast)
';

SELECT * FROM
    R   INNER JOIN  S   on  R.id = S.id
        INNER JOIN  T   on  R.id = T.id
        INNER JOIN  U   on  T.id = U.id;
```

## Синтаксис

Подсказки передаются в виде строки, содержащей массив выражений одного из четырех типов:

```
[JoinType(...) | Rows(...) | Bytes(...) | JoinOrder(...)]
```

## Требования к CBO

**Важно:** Все подсказки (`Rows`, `Bytes`, `JoinOrder`) работают только с **включенным** Cost-Based Optimizer (CBO), кроме `JoinType` - его можно указывать и для выключенного CBO.

## Типы подсказок

### 1. JoinType - Алгоритм соединения

Позволяет принудительно задать алгоритм соединения для определенных таблиц.

**Синтаксис:**
```
JoinType(t1 t2 ... tn Broadcast | Shuffle | Lookup)
```

**Параметры:**
- `t1 t2 ... tn` - таблицы, участвующие в соединении
- Алгоритм:
  - `Broadcast` - выбрать алгоритм BroadcastJoin
  - `Shuffle` - выбрать алгоритм HashJoin
  - `Lookup` - выбрать алгоритм LookupJoin

**Примеры:**
```sql
-- Использовать Broadcast для соединения таблиц nation, region
JoinType(nation region Broadcast)

-- Использовать HashJoin для соединения, в поддереве которого будут только таблицы customers, orders, products
JoinType(customers orders products Shuffle)

JoinType(nation region Lookup)
```

### 2. Rows - Подсказки по кардинальности

Позволяет изменить ожидаемое количество строк (оценку оптимизатора) для соединения или отдельных таблиц.

**Синтаксис:**
```
Rows(t1 t2 ... tn (*|/|+|-|#) Number)
```

**Параметры:**
- `t1 t2 ... tn` - таблицы
- Операция:
  - `*` - умножить на значение
  - `/` - разделить на значение
  - `+` - прибавить значение
  - `-` - вычесть значение
  - `#` - заменить значение
- `Number` - числовое значение

**Примеры:**
```sql
-- Умножить ожидаемое количество строк на 2 для соединения в поддереве которого есть только таблицы users orders yandex
Rows(users orders yandex * 2.0)

-- Заменить кардинальность таблицы products на 1333337
Rows(products # 1333337)

-- Уменьшить ожидаемое количество строк в 228 раз
Rows(filtered_table / 228)

-- Добавить 5000 строк к ожидаемому результату
Rows(table1 table2 + 5000)
```

### 3. Bytes - Подсказки по размеру данных

Позволяет изменить ожидаемый размер данных в байтах для соединения или отдельных таблиц.

**Синтаксис:**
```
Bytes(t1 t2 ... tn (*|/|+|-|#) Number)
```

**Параметры аналогичны Rows, но применяются к размеру данных в байтах.**

**Примеры:**
```sql
-- Умножить ожидаемый размер данных на 1.5
Bytes(large_table * 1.5)

-- Заменить размер данных для соединения на 1GB
Bytes(table1 table2 # 1073741824)

-- Уменьшить ожидаемый размер в 2 раза
Bytes(compressed_table / 2)

-- Добавить 100MB к ожидаемому размеру
Bytes(temp_table + 104857600)
```

### 4. JoinOrder - Порядок соединений

Позволяет зафиксировать определенное поддерево соединений в общем дереве соединений.

**Синтаксис:**
```
JoinOrder((t1 t2) (t3 (t4 ...)))
```

**Параметры:**
- Вложенная структура скобок определяет порядок соединений
- `(t1 t2)` означает, что t1 и t2 должны быть соединены первыми
- Можно задавать произвольную глубину вложенности

**Примеры:**
```sql
-- Принудительно соединить сначала users с orders, затем с products
JoinOrder((users orders) products)

-- Более сложный порядок соединений
JoinOrder(((customers orders) products) shipping)

-- Группировка соединений
JoinOrder((table1 table2) (table3 table4))

-- Многоуровневая структура
JoinOrder((users (orders products)) (addresses phones))
```

## Комбинирование подсказок

Можно использовать несколько типов подсказок одновременно в рамках одной прагмы:

```sql
PRAGMA ydb.OptimizerHints =
'
    JoinType(users orders Broadcast)
    Rows(users orders * 0.5)
    JoinOrder((users orders) products)
    Bytes(products # 1073741824)
';
```

## Пример использования

```sql
PRAGMA ydb.OptimizerHints =
'
    JoinOrder((((R S) T) U) V)
    JoinType(R S Shuffle)
    JoinType(R S T Broadcast)
    JoinType(R S T U Shuffle)
    JoinType(R S T U V Broadcast)
';

SELECT * FROM
    R   INNER JOIN  S   on  R.id = S.id
        INNER JOIN  T   on  R.id = T.id
        INNER JOIN  U   on  T.id = U.id
        INNER JOIN  V   on  U.id = V.id;
```

Как можно увидеть - порядок соединений сохранился и для нужных поддеревьев выбрались запрошенные алгоритмы.

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│ Operation                                                                               │
├─────────────────────────────────────────────────────────────────────────────────────────┤
│ ┌> ResultSet                                                                            │
│ └─┬> InnerJoin (MapJoin) (U.id = V.id)                                                  │
│   ├─┬> InnerJoin (Grace) (T.id = U.id)                                                  │
│   │ ├─┬> HashShuffle (KeyColumns: ["T.id"], HashFunc: "HashV2")                         │
│   │ │ └─┬> InnerJoin (MapJoin) (R.id = T.id)                                            │
│   │ │   ├─┬> InnerJoin (Grace) (R.id = S.id)                                            │
│   │ │   │ ├─┬> HashShuffle (KeyColumns: ["id"], HashFunc: "HashV2")                     │
│   │ │   │ │ └──> TableFullScan (Table: R, ReadColumns: ["id (-∞, +∞)","payload1","ts"]) │
│   │ │   │ └─┬> HashShuffle (KeyColumns: ["id"], HashFunc: "HashV2")                     │
│   │ │   │   └──> TableFullScan (Table: S, ReadColumns: ["id (-∞, +∞)","payload2"])      │
│   │ │   └──> TableFullScan (Table: T, ReadColumns: ["id (-∞, +∞)","payload3"])          │
│   │ └─┬> HashShuffle (KeyColumns: ["id"], HashFunc: "HashV2")                           │
│   │   └──> TableFullScan (Table: U, ReadColumns: ["id (-∞, +∞)","payload4"])            │
│   └──> TableFullScan (Table: V, ReadColumns: ["id (-∞, +∞)","payload5"])                │
└─────────────────────────────────────────────────────────────────────────────────────────┘
```