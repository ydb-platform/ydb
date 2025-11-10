# {{ ydb-short-name }} Provider for LinqToDB

## Overview {#overview}

This guide explains how to use [LinqToDB](https://linq2db.github.io/) with {{ ydb-short-name }}.

LinqToDB is a lightweight and fast ORM/µ-ORM for .NET that provides type-safe LINQ queries and precise SQL control. The {{ ydb-short-name }} provider generates correct YQL, supports YDB types, schema generation, and bulk operations (Bulk Copy).

## Installing the {{ ydb-short-name }} provider {#install-provider}

Add dependencies:

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

If you use a providers configuration file, the configuration name must contain `YDB` so the provider is picked automatically (for example, `"YDB"`).

## Provider configuration {#configuration-provider}

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
      .UseOptions(new YdbOptions(
          BulkCopyType: BulkCopyType.ProviderSpecific,   // default BulkCopy mode
          UseParametrizedDecimal: true                   // Decimal(p,s) in DDL
      ));
  DataConnection.AddProviderDetector(YdbTools.ProviderDetector);
  using var db2 = new DataConnection(options);
  ```

{% endlist %}

---

## Usage {#using}

Use the provider like any other Linq To DB provider: map your entity classes to tables and run queries via `DataConnection`/`ITable<T>`. Below you’ll find the type mapping table and schema generation examples.

### .NET ↔ {{ ydb-short-name }} type mapping {#types}

| .NET type(s)                   | Linq To DB `DataType`               | {{ ydb-short-name }} type | Notes                                  |
|--------------------------------|-------------------------------------|---------------------------|----------------------------------------|
| `bool`                         | `Boolean`                           | `Bool`                    | —                                      |
| `string`                       | `NVarChar`/`VarChar`/`Char`/`NChar` | `Utf8`                    | UTF-8 string.                          |
| `byte[]`                       | `VarBinary`/`Binary`/`Blob`         | `String`                  | Binary data.                           |
| `Guid`                         | `Guid`                              | `Uuid`                    | —                                      |
| `DateOnly` / `DateTime`        | `Date`                              | `Date`                    | Time is ignored.                       |
| `DateTime`                     | `DateTime`                          | `Datetime`                | Second precision.                      |
| `DateTime`/`DateTimeOffset`    | `DateTime2`                         | `Timestamp`               | Microsecond precision.                 |
| `TimeSpan`                     | `Interval`                          | `Interval`                | Duration.                              |
| `decimal`                      | `Decimal`                           | `Decimal(p,s)`            | Defaults to `Decimal(22,9)`            |
| `float`                        | `Single`                            | `Float`                   | —                                      |
| `double`                       | `Double`                            | `Double`                  | —                                      |
| `sbyte` / `byte`               | `SByte` / `Byte`                    | `Int8` / `Uint8`          | —                                      |
| `short` / `ushort`             | `Int16` / `UInt16`                  | `Int16` / `Uint16`        | —                                      |
| `int` / `uint`                 | `Int32` / `UInt32`                  | `Int32` / `Uint32`        | —                                      |
| `long` / `ulong`               | `Int64` / `UInt64`                  | `Int64` / `Uint64`        | —                                      |
| `string`                       | `Json`                              | `Json`                    | Text JSON.                             |
| `byte[]`                       | `BinaryJson`                        | `JsonDocument`            | Binary JSON.                           |
| `DateOnly` / `DateTime`        | `Date`                              | `Date32`                  | Extended date range.                   |
| `DateTime`                     | `DateTime`                          | `Datetime64`              | Second precision, extended range.      |
| `DateTime` / `DateTimeOffset`  | `DateTime2`                         | `Timestamp64`             | Microsecond precision, extended range. |
| `TimeSpan`                     | `Interval`                          | `Interval64`              | Extended interval range.               |


> * You can set exact `Precision`/`Scale` with attributes:  
    >   `[Column(DataType = DataType.Decimal, Precision = 22, Scale = 9)]`  
    >   or globally via `YdbOptions(UseParametrizedDecimal: true)`.
> Time zone types (`TzDate`/`TzDatetime`/`TzTimestamp`) are **not used as column types**. When creating tables they will be reduced to `Date`/`Datetime`/`Timestamp`. They are allowed in expressions/literals.

---

### Schema generation from attributes

Describe an entity using Linq To DB attributes; the provider will create a table and indexes.

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

> Linq To DB does not manage migrations — use Liquibase/Flyway/your tool of choice. Methods like `CreateTable<T>()`/`DropTable<T>()` are convenient for tests and local development.

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

> Foreign keys as database-level constraints are not created by the provider. Implement referential logic at the application level or add checks with separate scripts.

### Example generated YQL for relationship queries

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

Create an `Employee` entity with column types and an index:

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
    Email      = "example@bk.com",
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
  .Set(e => e.Email,      "example+updated@bk.com")
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

> Note: The vendor specifically removes the alias from the update table to comply with the YQL rules. This is done by the provider's SQL optimizer.

- Delete by primary key

  ```yql
  DELETE FROM employee WHERE Id = ?;
  ```

### Bulk operations: insert, update and delete

BulkCopy

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

Massive Update (WHERE IN)

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