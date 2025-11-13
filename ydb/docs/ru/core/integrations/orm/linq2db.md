# Провайдер {{ ydb-short-name }} для LinqToDB

## Введение {#overview}

Это руководство по использованию [LinqToDB](https://linq2db.github.io/) с {{ ydb-short-name }}.

LinqToDB — лёгкий и быстрый ORM/µ-ORM для .NET, предоставляющий типобезопасные [LINQ](https://learn.microsoft.com/en-us/dotnet/csharp/linq/) запросы и точный контроль над SQL. Провайдер {{ ydb-short-name }} формирует корректный YQL, поддерживает типы YDB, генерацию схемы и массовые операции (Bulk Copy).

## Установка провайдера {{ ydb-short-name }} {#install-provider}

Примеры добавления зависимостей:

{% list tabs %}

- dotnet CLI

  ```bash
  dotnet add package Community.Ydb.Linq2db
  dotnet add package linq2db
  ```

- csproj (PackageReference)

  ```xml
  <ItemGroup>
      <PackageReference Include="Community.Ydb.Linq2db" Version="$(CommunityYdbLinqToDbVersion)" />
      <PackageReference Include="linq2db" Version="$(LinqToDbVersion)" />
  </ItemGroup>
  ```

{% endlist %}

## Конфигурация провайдера {#configuration-provider}

Настройте LinqToDB на использование {{ ydb-short-name }} через код:

{% list tabs group=lang %}

- C#

  ```csharp
  // Вариант 1: быстрая инициализация по строке подключения
  using var db = YdbTools.CreateDataConnection(
      "Endpoint=grpcs://<host>:2135;Database=/path/to/database;Token=<...>"
  );
  DataConnection.AddProviderDetector(YdbTools.ProviderDetector);

  // Вариант 2: через DataOptions
  var options = new DataOptions()
      .UseConnectionString(YdbTools.GetDataProvider(),
          "Endpoint=grpcs://<host>:2135;Database=/path/to/database;Token=<...>")
  DataConnection.AddProviderDetector(YdbTools.ProviderDetector);
  using var db2 = new DataConnection(options);
  ```

{% endlist %}

---

## Использование {#using}

Используйте провайдер как и любой другой провайдер Linq To DB: сопоставляйте классы сущностей с таблицами и выполняйте запросы через `DataConnection`/`ITable<T>`. Ниже — типовая таблица соответствий и примеры генерации схемы из атрибутов.

### Таблица соответствия .NET типов с типами {{ ydb-short-name }} {#types}

| .NET тип(ы)                   | Linq To DB `DataType`                     | Тип в YDB          | Примечание                                                                                                                      |
|-------------------------------|-------------------------------------------|--------------------|---------------------------------------------------------------------------------------------------------------------------------|
| `bool`                        | `Boolean`                                 | `Bool`             | —                                                                                                                               |
| `string`                      | `NVarChar` / `VarChar` / `Char` / `NChar` | `Utf8`             | UTF-8 строка. Text и Utf8 — одно и то же; что выводить, решает генератор DDL                                                    |
| `byte[]`                      | `VarBinary` / `Binary` / `Blob`           | `String`           | Бинарные данные. Bytes и String в YDB равнозначны                                                                               |
| `Guid`                        | `Guid`                                    | `Uuid`             | 128-битный UUID (RFC 4122). Проверка версии (v1/v4/v7) не выполняется — нужную версию генерируйте в приложении                  |
| `DateOnly` / `DateTime`       | `Date`                                    | `Date`             | Дата в UTC (время отбрасывается). Диапазон 1970-01-01..2106-01-01                                                               |
| `DateTime`                    | `DateTime`                                | `Datetime`         | Точность до секунд, хранится как момент в UTC. Диапазон 1970-01-01..2106-01-01                                                  |
| `DateTime` / `DateTimeOffset` | `DateTime2`                               | `Timestamp`        | Точность до микросекунд, хранится как момент в UTC. Диапазон 1970-01-01..2106-01-01                                             |
| `TimeSpan`                    | `Interval`                                | `Interval`         | Длительность с точностью до микросекунды; значение округляется вниз до кратного 10 тикам.                                       |
| `decimal`                     | `Decimal`                                 | `Decimal(p,s)`     | По умолчанию — Decimal(22,9). Чтобы задать свои Precision/Scale, используйте Decimal(p,s). [Пример](#пример-кастомного-decimal) |
| `float`                       | `Single`                                  | `Float`            | —                                                                                                                               |
| `double`                      | `Double`                                  | `Double`           | —                                                                                                                               |
| `sbyte` / `byte`              | `SByte` / `Byte`                          | `Int8` / `Uint8`   | —                                                                                                                               |
| `short` / `ushort`            | `Int16` / `UInt16`                        | `Int16` / `Uint16` | —                                                                                                                               |
| `int` / `uint`                | `Int32` / `UInt32`                        | `Int32` / `Uint32` | —                                                                                                                               |
| `long` / `ulong`              | `Int64` / `UInt64`                        | `Int64` / `Uint64` | —                                                                                                                               |
| `string`                      | `Json`                                    | `Json`             | Текстовый JSON.                                                                                                                 |
| `byte[]`                      | `BinaryJson`                              | `JsonDocument`     | Бинарный JSON.                                                                                                                  |
| `DateOnly` / `DateTime`       | `Date`                                    | `Date32`           | Более широкий диапазон дат. Укажите DbType = "Date32". [Пример](#пример-кастомного-decimal)                                     |
| `DateTime`                    | `DateTime`                                | `Datetime64`       | Точность до секунд, расширенный диапазон. Укажите DbType = "Datetime64". [Пример](#пример-кастомного-decimal)                   |
| `DateTime` / `DateTimeOffset` | `DateTime2`                               | `Timestamp64`      | Точность до микросекунд, расширенный диапазон. Укажите DbType = "Timestamp64". [Пример](#пример-кастомного-decimal)             |
| `TimeSpan`                    | `Interval`                                | `Interval64`       | Более широкий диапазон интервалов. Укажите DbType = "Interval64". [Пример](#пример-кастомного-decimal)                          |

{% note tip %}

Точное `Precision`/`Scale` задаётся атрибутами: `[Column(DataType = DataType.Decimal, Precision = 22, Scale = 9)]`.

{% endnote %}

{% note info %}

По умолчанию (если на колонке не задан `DbType`) применяются базовые типы YDB: `Date`, `Datetime`, `Timestamp`, `Interval`.
Чтобы точечно включить расширенные типы, укажи `DbType`, например: `[Column(DbType = "Date32")]`. Обе семьи типов могут сосуществовать в одной таблице.

{% endnote %}

---

### Пример кастомного Decimal

```csharp
[Table("amounts")]
public sealed class AmountRow
{
    [PrimaryKey] public long Id { get; set; }

    // Custom precision & scale: Decimal(25,10)
    [Column("amount", DataType = DataType.Decimal, Precision = 25, Scale = 10), NotNull]
    public decimal Amount { get; set; }
}
```

### Пример кастомного DbType

```csharp
[Table("events")]
public sealed class EventRow
{
[PrimaryKey] public long Id { get; set; }

    // Extended-range timestamp
    [Column("happened_at", DbType = "Timestamp64"), NotNull]
    public DateTime HappenedAt { get; set; }

    // Extended-range date
    [Column("due_on", DbType = "Date32"), NotNull]
    public DateTime DueOn { get; set; }

    // Second precision date
    [Column("made_at", DbType = "Datetime64"), NotNull]
    public DateTime MadeAt { get; set; }

    // Extended-range interval
    [Column("duration", DbType = "Interval64"), NotNull]
    public TimeSpan Duration { get; set; }
}
```

### Генерация схемы из атрибутов

Опишите сущность с помощью атрибутов Linq To DB; провайдер создаст таблицу и индексы.

{% list tabs group=lang %}

- C#

  ```csharp
  using LinqToDB.Mapping;

  [Table(Name = "Groups")]
  [Index("GroupName", Name = "group_name_index")]
  public class Group
  {
      [PrimaryKey, Column("GroupId")]
      public int Id { get; set; }

      [Column("GroupName")]
      public string? Name { get; set; }
  }
  ```

{% endlist %}

**Сгенерированный DDL ({{ ydb-short-name }})**:

```yql
CREATE TABLE Groups (
    GroupId Int32 NOT NULL,
    GroupName Utf8,
    PRIMARY KEY (GroupId)
);

ALTER TABLE Groups
  ADD INDEX group_name_index GLOBAL
       ON (GroupName);
```

### Эволюция схемы (добавление поля)

Добавим поле `Department` к сущности `Group`:

{% list tabs group=lang %}

- C#

  ```csharp
  [Column] public string? Department { get; set; }
  ```

{% endlist %}

**Изменение схемы (DDL)**:

```yql
ALTER TABLE Groups
   ADD COLUMN Department Utf8;
```

{% note info %}

LinqToDB не управляет миграциями. DDL ниже — иллюстративный; применяйте его через Liquibase/Flyway (рекомендуется). Для быстрых локальных изменений можно выполнить его напрямую через db.Execute(...) или YDB CLI.

{% endnote %}

```csharp
using var db = new DataConnection("YDB", connectionString);

// Применяем изменение схемы (добавляем колонку):
db.Execute(@"
ALTER TABLE Groups
   ADD COLUMN Department Utf8;
");
```


### Индексы YDB: как задать параметры

Через атрибут [Index] вы задаёте имя, колонки и уникальность индекса. Провайдер создаёт вторичный индекс как GLOBAL.
Параметры ASYNC/SYNC и COVER(...) через атрибут не задаются — их добавляют отдельной DDL-командой после создания таблицы.

#### Вариант A — через атрибут (имя + Unique)

```csharp
[Table(Name = "Groups", IsColumnAttributeRequired = false)]
[Index("GroupName", Name = "group_name_index", Unique = true)]
public class Group
{
    [PrimaryKey, Column("GroupId")] public int Id { get; set; }
    [Column("GroupName")] public string? Name { get; set; }

    // Колонка, которую при желании можно добавить в COVER отдельной командой
    [Column] public string? Department { get; set; }
}

// При db.CreateTable<Group>() будет создан GLOBAL UNIQUE индекс по GroupName.
```

Сгенерированный DDL будет эквивалентен

```yql
CREATE TABLE Groups (
    GroupId Int32 NOT NULL,
    GroupName Utf8,
    Department Utf8,
    PRIMARY KEY (GroupId)
);

ALTER TABLE Groups
  ADD INDEX group_name_index GLOBAL UNIQUE
       ON (GroupName);
```

#### Вариант B — расширенные параметры (ASYNC, COVER) отдельной командой

```csharp
[Table(Name = "Groups", IsColumnAttributeRequired = false)]
public class Group
{
    [PrimaryKey, Column("GroupId")] public int Id { get; set; }
    [Column("GroupName")] public string? Name { get; set; }
    [Column] public string? Department { get; set; }
}

public static class Demo
{
    public static void Main()
    {
        using var db = new DataConnection("YDB", "Host=localhost;Port=2136;Database=/local;UseTls=false");

        // 1) Создаём таблицу из атрибутов
        db.CreateTable<Group>();

        // 2) Добавляем индекс с параметрами ASYNC и COVER
        db.Execute(@"
            ALTER TABLE Groups
                ADD INDEX group_name_index GLOBAL ASYNC
                ON (GroupName)
                COVER (Department);
        ");
    }
}

```

### Связи между сущностями

Опишите навигации через атрибуты `[Association]`. Пример «один‑ко‑многим» между `Group` и `Student`:

{% list tabs group=lang %}

- C#

  ```csharp
  [Table("Students")]
  public class Student
  {
      [PrimaryKey, Column("StudentId")]
      public int Id { get; set; }

      [Column("StudentName")]
      public string Name { get; set; } = null!;

      [Column]
      public int GroupId { get; set; }

      // многие-к-одному
      [Association(ThisKey = nameof(GroupId), OtherKey = nameof(Group.Id), CanBeNull = false)]
      public Group Group { get; set; } = null!;
  }

  [Table("Groups")]
  public class Group
  {
      [PrimaryKey, Column("GroupId")]
      public int Id { get; set; }

      [Column("GroupName")]
      public string? Name { get; set; }

      // один-ко-многим
      [Association(ThisKey = nameof(Id), OtherKey = nameof(Student.GroupId))]
      public IEnumerable<Student> Students { get; set; } = null!;
  }
  ```

{% endlist %}

**Создаваемые таблицы**:

```yql
CREATE TABLE Groups (
    GroupId Int32 NOT NULL,
    GroupName Utf8,
    PRIMARY KEY (GroupId)
);

CREATE TABLE Students (
    StudentId Int32 NOT NULL,
    StudentName Utf8,
    GroupId Int32,
    PRIMARY KEY (StudentId)
);
```

### Примеры сгенерированного YQL для выборок по связям

{% list tabs %}

- “Lazy” loading (two queries)

  ```csharp
    // 1) SELECT g.GroupId, g.GroupName FROM Groups AS g WHERE g.GroupName = 'M3439';
    var grp = db.GetTable<Group>()
    .Where(g => g.Name == "M3439")
    .Select(g => new { g.Id, g.Name })
    .FirstOrDefault();
    
    // 2) SELECT s.StudentId, s.StudentName, s.GroupId FROM Students AS s WHERE s.GroupId = ?;
    var students = grp == null
    ? new List<Student>()
    : db.GetTable<Student>()
    .Where(s => s.GroupId == grp.Id)
    .Select(s => new { s.Id, s.Name, s.GroupId })
    .ToList();
  ```

- “Eager” loading (JOIN)

  ```csharp
    var joined =
    (from g in db.GetTable<Group>()
    join s in db.GetTable<Student>() on g.Id equals s.GroupId
    where g.Name == "M3439"
    select new { GroupId = g.Id, GroupName = g.Name, StudentId = s.Id, StudentName = s.Name, s.GroupId })
    .ToList();
  ```

{% endlist %}

{% list tabs %}

- «Ленивая» загрузка (две выборки)

  ```yql
  SELECT
      g.GroupId,
      g.GroupName
  FROM
      Groups AS g
  WHERE
      g.GroupName = 'M3439';

  SELECT
      s.StudentId,
      s.StudentName,
      s.GroupId
  FROM
      Students AS s
  WHERE
      s.GroupId = ?;
  ```

- «Жадная» загрузка (JOIN)

  ```yql
  SELECT
      g.GroupId,
      g.GroupName,
      s.StudentId,
      s.StudentName,
      s.GroupId
  FROM
      Groups AS g
  JOIN
      Students AS s
    ON g.GroupId = s.GroupId
  WHERE
      g.GroupName = 'M3439';
  ```

{% endlist %}

### Пример «прикладной» сущности и генерируемого DDL

Этот раздел показывает практическую сущность. Он демонстрирует:

- реалистичный набор колонок (ФИО, e-mail, дата найма, зарплата, флаги, целочисленные значения);
- точные типы (например, Decimal(22,9) для денежных значений, Date для дат, Utf8/Bool/Int32/Int64);
- GLOBAL вторичный индекс по full_name для быстрых поисков и сортировки;
- сквозной сценарий: маппинг → создание таблицы → CRUD с сгенерированным YQL/DDL.

При вызове `db.CreateTable<Employee>()` Linq To DB создаёт таблицу и применяет атрибут [Index] как GLOBAL-индекс YDB.

{% list tabs group=lang %}

- C#

  ```csharp
  using System;
  using LinqToDB.Mapping;

  [Table("employee")]
  [Index("full_name", Name = "employee_full_name_idx")]
  public class Employee
  {
      [PrimaryKey] public long Id { get; set; }

      [Column("full_name"), NotNull] public string FullName { get; set; } = null!;

      [Column, NotNull] public string Email { get; set; } = null!;

      [Column("hire_date", DataType = DataType.Date), NotNull]
      public DateTime HireDate { get; set; }

      [Column(DataType = DataType.Decimal, Precision = 22, Scale = 9), NotNull]
      public decimal Salary { get; set; }

      [Column("is_active"), NotNull] public bool IsActive { get; set; }

      [Column, NotNull] public string Department { get; set; } = null!;

      [Column, NotNull] public int Age { get; set; }
  }
  ```

{% endlist %}

**Сгенерированный DDL**:

```yql
CREATE TABLE employee (
    Id Int64 NOT NULL,
    full_name Utf8 NOT NULL,
    Email Utf8 NOT NULL,
    hire_date Date NOT NULL,
    Salary Decimal(22,9) NOT NULL,
    is_active Bool NOT NULL,
    Department Utf8 NOT NULL,
    Age Int32 NOT NULL,
    PRIMARY KEY (Id)
);

ALTER TABLE employee
  ADD INDEX employee_full_name_idx GLOBAL
       ON (full_name);
```

Пример использования:

```csharp
using System;
using System.Linq;
using LinqToDB;
using LinqToDB.Data;

using var db = new DataConnection("YDB");

// INSERT
var employee = new Employee
{
    Id         = 1L,
    FullName   = "Example",
    Email      = "example@example.com",
    HireDate   = new DateTime(2023, 12, 20),
    Salary     = 500000.000000000m,
    IsActive   = true,
    Department = "YDB AppTeam",
    Age        = 23,
};
db.Insert(employee);

// SELECT по первичному ключу
var loaded = db.GetTable<Employee>()
               .FirstOrDefault(e => e.Id == employee.Id);

// Обновим Email/Department/Salary у сотрудника с заданным Id
db.GetTable<Employee>()
  .Where(e => e.Id == employee.Id)
  .Set(e => e.Email,      "example+updated@example.com")
  .Set(e => e.Department, "Analytics")
  .Set(e => e.Salary,     550000.000000000m)
  .Update();

// DELETE по первичному ключу
db.GetTable<Employee>()
  .Where(e => e.Id == employee.Id)
  .Delete();
```

**Примеры YQL, формируемые провайдером для простых операций:**

- Вставка одной записи

  ```yql
  INSERT INTO employee (Age,Department,Email,full_name,hire_date,is_active,Salary,Id)
  VALUES (?,?,?,?,?,?,?,?);
  ```

- Чтение по первичному ключу

  ```yql
  SELECT
      e.Id, e.full_name, e.Email, e.hire_date, e.Salary, e.is_active, e.Department, e.Age
  FROM employee AS e
  WHERE e.Id = ?;
  ```

- Обновление по первичному ключу

  ```yql
  UPDATE employee
  SET
      Email      = ?,
      Department = ?,
      Salary     = ?
  WHERE Id = ?;
  ```

{% note info %}

Провайдер выводит параметры (?), потому что значения и типы привязываются драйвером, а не объявляются в тексте запроса. Когда YQL требует типизированные параметры, провайдер автоматически добавляет соответствующие DECLARE. Для “нестандартных” паттернов вроде upsert-записей используйте UPSERT с параметризацией — провайдер генерирует такие выражения? как обычный YQL с параметрами.

{% endnote %}

- Удаление по первичному ключу

```yql
DELETE FROM employee WHERE Id = ?;
```

### Массовые операции: вставка, обновление и удаление

#### Массовая вставка (BulkCopy)

`BulkCopy` в провайдере YDB выполняется через нативный `API BulkUpsert` и не генерирует текстовый YQL. Данные отправляются в YDB как поток типизированных строк по двоичному протоколу SDK, поэтому не используются DECLARE и параметрические плейсхолдеры (?)

```csharp
var now  = DateTime.UtcNow;
var data = Enumerable.Range(0, 15_000).Select(i => new SimpleEntity
{
    Id      = i,
    IntVal  = i,
    DecVal  = 0m,
    StrVal  = $"Name {i}",
    BoolVal = (i & 1) == 0,
    DtVal   = now,
}); 
```

Массовое обновление (WHERE IN)

{% list tabs group=lang %}

- C#

  ```csharp
  var ids = Enumerable.Range(0, 15_000).ToArray();
  
  table.Where(t => ids.Contains(t.Id))
      .Set(_ => _.DecVal, 1.23m)
      .Set(_ => _.StrVal, "updated")
      .Set(_ => _.BoolVal, true)
      .Update();
  ```

- YQL

  ```yql
  DECLARE $Gen_List_Primitive_1 AS List<Int32>;
  UPDATE
      SimpleEntity
  SET
      DecVal = Decimal('1.23', 22, 9),
      StrVal = 'updated'u,
      BoolVal = true
  WHERE
      SimpleEntity.Id IN $Gen_List_Primitive_1
  ```

{% endlist %}

Массовое удаление (WHERE IN)

{% list tabs group=lang %}

- C#

  ```csharp
  table.Delete(t => ids.Contains(t.Id));
  ```

- YQL

  ```yql
  DECLARE $Gen_List_Primitive_1 AS List<Int32>;
  DELETE FROM
      SimpleEntity
  WHERE
      SimpleEntity.Id IN $Gen_List_Primitive_1
  ```

{% endlist %}

---