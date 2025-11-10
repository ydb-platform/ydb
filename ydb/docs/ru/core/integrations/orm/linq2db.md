# Провайдер {{ ydb-short-name }} для LinqToDB

## Введение {#overview}

Это руководство по использованию [LinqToDB](https://linq2db.github.io/) с {{ ydb-short-name }}.

LinqToDB — лёгкий и быстрый ORM/µ-ORM для .NET, предоставляющий типобезопасные LINQ‑запросы и точный контроль над SQL. Провайдер {{ ydb-short-name }} формирует корректный YQL, поддерживает типы YDB, генерацию схемы и массовые операции (Bulk Copy).

## Установка провайдера {{ ydb-short-name }} {#install-provider}

Примеры добавления зависимостей:

{% list tabs %}

- dotnet CLI

  ```bash
  dotnet add package Community.Ydb.Linq2db
  dotnet add package linq2db
  dotnet add package Ydb.Sdk
  ```

- csproj (PackageReference)

  ```xml
  <ItemGroup>
  <PackageReference Include="Community.Ydb.Linq2db" Version="$(CommunityYdbLinqToDbVersion)" />
  <PackageReference Include="linq2db" Version="$(LinqToDbVersion)" />
  <PackageReference Include="Ydb.Sdk" Version="$(YdbSdkVersion)" />  </ItemGroup>
  ```

{% endlist %}

Если вы используете конфигурационный файл провайдеров, имя конфигурации должно содержать `YDB`, чтобы провайдер определился автоматически (например, `"YDB"`).

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
      .UseOptions(new YdbOptions(
          BulkCopyType: BulkCopyType.ProviderSpecific,   // дефолтный режим BulkCopy
          UseParametrizedDecimal: true                   // Decimal(p,s) в DDL
      ));
  DataConnection.AddProviderDetector(YdbTools.ProviderDetector);
  using var db2 = new DataConnection(options);
  ```

{% endlist %}

---

## Использование {#using}

Используйте провайдер как и любой другой провайдер Linq To DB: сопоставляйте классы сущностей с таблицами и выполняйте запросы через `DataConnection`/`ITable<T>`. Ниже — типовая таблица соответствий и примеры генерации схемы из атрибутов.

### Таблица соответствия .NET типов с типами {{ ydb-short-name }} {#types}

| .NET тип(ы)                   | Linq To DB `DataType`                     | Тип в YDB          | Примечание                                |
| ----------------------------- | ----------------------------------------- | ------------------ |-------------------------------------------|
| `bool`                        | `Boolean`                                 | `Bool`             | —                                         |
| `string`                      | `NVarChar` / `VarChar` / `Char` / `NChar` | `Text`             | Строка UTF-8.                             |
| `byte[]`                      | `VarBinary` / `Binary` / `Blob`           | `Bytes`            | Бинарные данные.                          |
| `Guid`                        | `Guid`                                    | `Uuid`             | —                                         |
| `DateOnly` / `DateTime`       | `Date`                                    | `Date`             | Время игнорируется.                       |
| `DateTime`                    | `DateTime`                                | `Datetime`         | Секундная точность.                       |
| `DateTime` / `DateTimeOffset` | `DateTime2`                               | `Timestamp`        | Микросекунды.                             |
| `TimeSpan`                    | `Interval`                                | `Interval`         | Длительность.                             |
| `decimal`                     | `Decimal`                                 | `Decimal(p,s)`     | По умолчанию `Decimal(22,9)`              |
| `float`                       | `Single`                                  | `Float`            | —                                         |
| `double`                      | `Double`                                  | `Double`           | —                                         |
| `sbyte` / `byte`              | `SByte` / `Byte`                          | `Int8` / `Uint8`   | —                                         |
| `short` / `ushort`            | `Int16` / `UInt16`                        | `Int16` / `Uint16` | —                                         |
| `int` / `uint`                | `Int32` / `UInt32`                        | `Int32` / `Uint32` | —                                         |
| `long` / `ulong`              | `Int64` / `UInt64`                        | `Int64` / `Uint64` | —                                         |
| `string`                      | `Json`                                    | `Json`             | Текстовый JSON.                           |
| `byte[]`                      | `BinaryJson`                              | `JsonDocument`     | Бинарный JSON.                            |
| `DateOnly` / `DateTime`       | `Date`                                    | `Date32`           | Расширенный диапазон даты.                |
| `DateTime`                    | `DateTime`                                | `Datetime64`       | Секундная точность, расширенный диапазон. |
| `DateTime` / `DateTimeOffset` | `DateTime2`                               | `Timestamp64`      | Микросекунды, расширенный диапазон.       |
| `TimeSpan`                    | `Interval`                                | `Interval64`       | Расширенный диапазон интервалов.          |

> Точное `Precision`/`Scale` можно задать атрибутами: `[Column(DataType = DataType.Decimal, Precision = 22, Scale = 9)]` или глобально через `YdbOptions(UseParametrizedDecimal: true)`.
 
> Типы с таймзоной (`TzDate`/`TzDatetime`/`TzTimestamp`) **не используются как типы колонок**. При создании таблиц будут сведены к `Date`/`Datetime`/`Timestamp`. В выражениях/литералах допустимы.

> По умолчанию (если на колонке не задан DbType) провайдер создаёт и использует старые типы времени в YDB:
Date, Datetime, Timestamp, Interval. Чтобы включить новые типы точечно, укажите DbType на колонке. Например  `[Column(DbType = "Date32")]`
---

### Генерация схемы из атрибутов

Опишите сущность с помощью атрибутов Linq To DB; провайдер создаст таблицу и индексы.

{% list tabs group=lang %}

- C#

  ```csharp
  using LinqToDB.Mapping;

  [Table(Name = "Groups", IsColumnAttributeRequired = false)]
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

> Linq To DB не управляет миграциями — используйте Liquibase/Flyway или ручные DDL‑скрипты. Методы `CreateTable<T>()`/`DropTable<T>()` удобны для тестов и локальной разработки.

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

> Внешние ключи как ограничения на уровне БД не создаются провайдером автоматически. Следите за целостностью на прикладном уровне либо добавляйте проверки отдельными скриптами.

### Примеры сгенерированного YQL для выборок по связям

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

Создадим сущность `Employee` с типами колонок и индексом:

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
    Email      = "example@bk.com",
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
  .Set(e => e.Email,      "example+updated@bk.com")
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

> Примечание: провайдер специально снимает алиас с таблицы для UPDATE, чтобы соответствовать правилам YQL, — это делает оптимизатор SQL провайдера.

- Удаление по первичному ключу

```yql
DELETE FROM employee WHERE Id = ?;
```

### Массовые операции: вставка, обновление и удаление

Массовая вставка (BulkCopy)

{% list tabs group=lang %}

- C#

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

- YQL

  ```yql
  DECLARE $Gen_List_Primitive_1 AS List<Int32>;
  SELECT
      COUNT(*) as COUNT_1
  FROM
      SimpleEntity t
  WHERE
      t.Id IN $Gen_List_Primitive_1 AND
      (t.DecVal <> Decimal('1.23', 22, 9) OR t.StrVal <> 'updated'u OR t.StrVal IS NULL OR t.BoolVal = false)
  ```

{% endlist %}

Массовое обновление (WHERE IN)

{% list tabs group=lang %}

- C#

  ```csharp
  var ids = Enumerable.Range(0, 15_000).ToArray();
  
  table.Where(t => ids.Contains(t.Id))
      .Set(_ => _.DecVal,  _ => 1.23m)
      .Set(_ => _.StrVal,  _ => "updated")
      .Set(_ => _.BoolVal, _ => true)
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