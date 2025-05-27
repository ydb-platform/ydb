# Использование Dapper

[Dapper](https://www.learndapper.com/) - это микро ORM (Object Relational Mapping), который предоставляет простой и гибкий способ для взаимодействия с базами данных. Он работает на основе стандарта [ADO.NET](../../reference/languages-and-apis/ado-net/index.md) и предлагает множество функций, которые облегчают работу с базой данных.

Чтобы начать работу требуется дополнительная зависимость [Dapper](https://www.nuget.org/packages/Dapper/).

Рассмотрим полный пример:

```c#
using Dapper;
using Ydb.Sdk.Ado;

await using var connection = await new YdbDataSource().OpenConnectionAsync();

await connection.ExecuteAsync("""
                              CREATE TABLE Users(
                                  Id Int32,
                                  Name Text,
                                  Email Text,
                                  PRIMARY KEY (Id)
                              );
                              """);

await connection.ExecuteAsync("INSERT INTO Users(Id, Name, Email) VALUES (@Id, @Name, @Email)",
    new User { Id = 1, Name = "Name", Email = "Email" });

Console.WriteLine(await connection.QuerySingleAsync<User>("SELECT * FROM Users WHERE Id = @Id", new { Id = 1 }));

await connection.ExecuteAsync("DROP TABLE Users");

internal class User
{
    public int Id { get; init; }
    public string Name { get; init; } = null!;
    public string Email { get; init; } = null!;

    public override string ToString()
    {
        return $"Id: {Id}, Name: {Name}, Email: {Email}";
    }
}
```

За дополнительной информацией обратитесь к официальной [документации](https://www.learndapper.com/).

## Важные аспекты

Для того чтобы Dapper интерпретировал `DateTime` как {{ ydb-short-name }} тип `Datetime`. Выполните следующий код:

```c#
SqlMapper.AddTypeMap(typeof(DateTime), DbType.DateTime);
```

По умолчанию, `DateTime` интерпретируется как `Timestamp`.
