# {{ ydb-short-name }} Provider for LinqToDB

## Overview {#overview}

This guide explains how to use [LinqToDB](https://linq2db.github.io/) with {{ ydb-short-name }}.

LinqToDB is a lightweight and fast ORM/µ-ORM for .NET that provides type-safe [LINQ](https://learn.microsoft.com/en-us/dotnet/csharp/linq/) queries and precise SQL control. The {{ ydb-short-name }} provider generates correct YQL, supports {{ ydb-short-name }} types, schema generation, and bulk operations (Bulk Copy).

## Installing the {{ ydb-short-name }} provider {#install-provider}

Add dependencies:

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

## Provider Configuration {#configuration-provider}

Configure LinqToDB to use {{ ydb-short-name }} in code:

{% list tabs group=lang %}

- C#

  ```csharp
  // Option 1: quick initialization via connection string
  using var db = YdbTools.CreateDataConnection(
      "Endpoint=grpcs://<host>:2135;Database=/path/to/database;Token=<...>"
  );
  DataConnection.AddProviderDetector(YdbTools.ProviderDetector);

  // Option 2: via DataOptions
  var options = new DataOptions()
      .UseConnectionString(YdbTools.GetDataProvider(),
          "Endpoint=grpcs://<host>:2135;Database=/path/to/database;Token=<...>")
  DataConnection.AddProviderDetector(YdbTools.ProviderDetector);
  using var db2 = new DataConnection(options);
  ```

{% endlist %}

## Usage {#using}

Use the provider like any other Linq To DB provider: map your entity classes to tables and run queries via `DataConnection`/`ITable<T>`. Below you’ll find the type mapping table and schema generation examples.

### Type Mapping between .NET and {{ ydb-short-name }} {#types}

| .NET type(s)                  | Linq To DB `DataType`                     | { ydb-short-name }} type | Notes                                                                                                                                                                                                                                              |
|-------------------------------|-------------------------------------------|--------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `bool`                        | `Boolean`                                 | `Bool`                   | —                                                                                                                                                                                                                                                  |
| `string`                      | `NVarChar` / `VarChar` / `Char` / `NChar` | `Text`                   | UTF-8 text. Text and Utf8 are the same in YDB; the DDL generator may output either                                                                                                                                                                 |
| `byte[]`                      | `VarBinary` / `Binary` / `Blob`           | `Bytes`                  | Binary data. Bytes and String are equivalent in YDB; DDL may show either                                                                                                                                                                           |
| `Guid`                        | `Guid`                                    | `Uuid`                   | 128-bit RFC 4122 UUID. No version enforcement (v1/v4/v7). Generate the desired version in the app                                                                                                                                                  |
| `DateOnly` / `DateTime`       | `Date`                                    | `Date`                   | Date-only value stored as YDB `Date` in UTC, range `1970-01-01`..`2106-01-01`. .NET `DateOnly`/`DateTime` support a wider range (`0001`..`9999`), but values outside the YDB range cannot be stored.                                               |
| `DateTime`                    | `DateTime`                                | `Datetime`               | YDB `Datetime` with second precision, UTC instant in the range `1970-01-01`..`2106-01-01`. .NET `DateTime` supports a wider range; values outside the YDB range are not supported. `DateTime.Kind` is not stored; the provider expects UTC values. |
| `DateTime` / `DateTimeOffset` | `DateTime2`                               | `Timestamp`              | YDB `Timestamp` with microsecond precision, UTC instant `1970-01-01`..`2106-01-01`. When writing `DateTimeOffset`, the value is converted to UTC; the offset/time zone is not persisted.                                                           |
| `TimeSpan`                    | `Interval`                                | `Interval`               | Duration with microsecond precision. .NET `TimeSpan` range is wider than YDB `Interval`; values outside the YDB range are not supported. Sub-microsecond ticks are truncated.                                                                      |
| `decimal`                     | `Decimal`                                 | `Decimal(p,s)`           | Default: Decimal(22,9). To use custom precision/scale, specify Decimal(p,s).  [Example](#custom-precision-scale-example)                                                                                                                           |
| `float`                       | `Single`                                  | `Float`                  | —                                                                                                                                                                                                                                                  |
| `double`                      | `Double`                                  | `Double`                 | —                                                                                                                                                                                                                                                  |
| `sbyte` / `byte`              | `SByte` / `Byte`                          | `Int8` / `Uint8`         | —                                                                                                                                                                                                                                                  |
| `short` / `ushort`            | `Int16` / `UInt16`                        | `Int16` / `Uint16`       | —                                                                                                                                                                                                                                                  |
| `int` / `uint`                | `Int32` / `UInt32`                        | `Int32` / `Uint32`       | —                                                                                                                                                                                                                                                  |
| `long` / `ulong`              | `Int64` / `UInt64`                        | `Int64` / `Uint64`       | —                                                                                                                                                                                                                                                  |
| `string`                      | `Json`                                    | `Json`                   | Text JSON.                                                                                                                                                                                                                                         |
| `byte[]`                      | `BinaryJson`                              | `JsonDocument`           | Binary JSON.                                                                                                                                                                                                                                       |
| `DateOnly` / `DateTime`       | `Date`                                    | `Date32`                 | Wider date range. Use `DbType = "Date32"`. [Example](#dbtype-override-example)                                                                                                                                                                     |
| `DateTime`                    | `DateTime`                                | `Datetime64`             | Second precision, wider range. `DbType = "Datetime64"`. [Example](#dbtype-override-example)                                                                                                                                                        |
| `DateTime` / `DateTimeOffset` | `DateTime2`                               | `Timestamp64`            | Microseconds, wider range. `DbType = "Timestamp64"`. [Example](#dbtype-override-example)                                                                                                                                                           |
| `TimeSpan`                    | `Interval`                                | `Interval64`             | Wider interval range. `DbType = "Interval64"`. [Example](#dbtype-override-example)                                                                                                                                                                 |

{% note tip %}

You can set exact `Precision`/`Scale` with attributes: `[Column(DataType = DataType.Decimal, Precision = 22, Scale = 9)]`.

{% endnote %}

{% note info %}

By default (when `DbType` is not specified), the provider uses legacy YDB temporal types: `Date`, `Datetime`, `Timestamp`, `Interval`. To opt in per column to extended types, set `DbType`, e.g. `[Column(DbType = "Date32")]`. Both families can coexist in the same table.

{% endnote %}

### Custom precision scale example

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

### DbType override example

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

### Schema Generation from Attributes

Describe an entity using Linq To DB attributes, then explicitly create the schema by calling db.CreateTable<YourEntity>(). At that moment the table is created, and any [Index] attributes are applied as database indexes.

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

**Generated DDL ({{ ydb-short-name }})**:

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

### Schema evolution (adding a column)

Add the `Department` column to the `Group` entity:

{% list tabs group=lang %}

- C#

  ```csharp
  [Column] public string? Department { get; set; }
  ```

{% endlist %}

**Schema change (DDL):**

```yql
ALTER TABLE Groups
   ADD COLUMN Department Utf8;
```

{% note info %}

Linq To DB doesn’t manage migrations. The DDL below is illustrative—apply it with Liquibase/Flyway (recommended). For quick local changes you can also run it directly with db.Execute(...) or the YDB CLI.

{% endnote %}

```csharp
using var db = new DataConnection("YDB", connectionString);

// Apply the schema change (adds the column):
db.Execute(@"ALTER TABLE Groups
   ADD COLUMN Department Utf8;");
```

### YDB Indexes how to set parameters

With the [Index] attribute you can set name, columns, and uniqueness. The provider creates a GLOBAL secondary index.
Parameters like ASYNC/SYNC and COVER(...) are not supported via the attribute; add them using a separate DDL statement after table creation.

#### Option A — attribute (name + Unique)

```csharp
[Table(Name = "Groups", IsColumnAttributeRequired = false)]
[Index("GroupName", Name = "group_name_index", Unique = true)]
public class Group
{
    [PrimaryKey, Column("GroupId")] public int Id { get; set; }
    [Column("GroupName")] public string? Name { get; set; }

    // A column you may include into COVER via separate DDL
    [Column] public string? Department { get; set; }
}

// When you call db.CreateTable<Group>(), a GLOBAL UNIQUE index on GroupName is created.
```

The generated DDL is effectively

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

#### Option B — extended parameters (ASYNC, COVER) via separate DDL

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
        using var db = new DataConnection("YDB", connectionString);

        // 1) Create table from attributes (avoid duplicate indexes if you plan to add them via DDL)
        db.CreateTable<Group>();

        // 2) Add an index with ASYNC and COVER parameters
        db.Execute(@"
ALTER TABLE Groups
  ADD INDEX group_name_index GLOBAL ASYNC
       ON (GroupName)
       COVER (Department);
");
    }
}
```

### Relationships between entities

Describe navigations using `[Association]` attributes. Example of one-to-many between `Group` and `Student`:

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

      // many-to-one
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

      // one-to-many
      [Association(ThisKey = nameof(Id), OtherKey = nameof(Student.GroupId))]
      public IEnumerable<Student> Students { get; set; } = null!;
  }
  ```

{% endlist %}

**Created tables**:

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

### Example generated YQL for relationship queries

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

- “Lazy” loading (two queries)

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

- “Eager” loading (JOIN)

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

### “Business” entity example and generated DDL

This section shows a practical, production-style entity from a typical domain. It demonstrates:

- a realistic column set (name, email, hire date, salary, flags, ints);
- precise types (e.g., Decimal(22,9) for money-like values, Date for dates, Utf8/Bool/Int32/Int64);
- a GLOBAL secondary index on full_name for lookups and ordering;
- end-to-end flow: mapping → table creation → CRUD with generated YQL/DDL.

When you call `db.CreateTable<Employee>()`, Linq To DB creates the table and applies the [Index] attribute as a YDB GLOBAL index.

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

**Generated DDL**:

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

Usage example

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

// SELECT by primary key
var loaded = db.GetTable<Employee>()
               .FirstOrDefault(e => e.Id == employee.Id);

// UPDATE Email/Department/Salary by primary key
db.GetTable<Employee>()
  .Where(e => e.Id == employee.Id)
  .Set(e => e.Email,      "example+updated@example.com")
  .Set(e => e.Department, "Analytics")
  .Set(e => e.Salary,     550000.000000000m)
  .Update();

// DELETE by primary key
db.GetTable<Employee>()
  .Where(e => e.Id == employee.Id)
  .Delete();
```

**YQL samples generated by the provider for simple operations:**

- Insert a single row

  ```yql
  INSERT INTO employee (Age,Department,Email,full_name,hire_date,is_active,Salary,Id)
  VALUES (?,?,?,?,?,?,?,?);
  ```

- Read by primary key

  ```yql
  SELECT
      e.Id, e.full_name, e.Email, e.hire_date, e.Salary, e.is_active, e.Department, e.Age
  FROM employee AS e
  WHERE e.Id = ?;
  ```

- Update by primary key

  ```yql
  UPDATE employee
  SET
      Email      = ?,
      Department = ?,
      Salary     = ?
  WHERE Id = ?;
  ```

{% note info %}

The provider emits parameters (?) because values and types are bound via the driver, not declared in the query text. When YQL requires typed parameters, the provider adds the necessary DECLARE statements automatically. For non-standard patterns such as upsert-style writes, use YDB’s UPSERT with parameter binding—the provider generates these statements as regular YQL with parameters.

{% endnote %}


- Delete by primary key

  ```yql
  DELETE FROM employee WHERE Id = ?;
  ```

### Bulk operations: insert, update and delete

#### BulkCopy

In the YDB provider, `BulkCopy` uses the native `BulkUpsert API` and does not generate textual YQL. Rows are sent to YDB as a stream of strongly-typed values over the SDK’s binary protocol, so there are no DECLARE statements or ? placeholders

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
  
  db.BulkCopy(data)
  ```

Massive Update (WHERE IN)

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

Mass deletion (WHERE IN)

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