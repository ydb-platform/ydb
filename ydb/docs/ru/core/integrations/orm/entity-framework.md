# {{ ydb-short-name }} Entity Framework Core Provider

{{ ydb-short-name }} предоставляет поставщика Entity Framework (EF) Core — объектно-реляционный модуль сопоставления (ORM), который позволяет разработчикам .NET работать с базой данных {{ ydb-short-name }} с помощью объектов .NET. Он ведёт себя так же, как и другие поставщики EF Core (например, SQL Server), поэтому здесь применима [общая документация по EF Core](https://docs.microsoft.com/ef/core/index). Если вы только начинаете работать с EF Core, эта документация — лучшее место для начала.

Разработка EF Core Provider ведётся в [открытом GitHub репозитории](https://github.com/ydb-platform/ydb-dotnet-sdk/tree/main), о всех проблемах следует сообщать там.

## Настройка поставщика {{ ydb-short-name }} Entity Framework Core

Чтобы начать работу, необходимо добавить необходимые пакеты NuGet в свой проект:

```dotnet
dotnet add package EntityFrameworkCore.Ydb
```

## Определение моделей и DbContext

Допустим, вы хотите хранить блоги и их записи в своей базе данных; вы можете смоделировать их как типы .NET следующим образом:

```c#
public class Blog
{
    public int BlogId { get; set; }
    public string Url { get; set; }

    public List<Post> Posts { get; set; }
}

public class Post
{
    public int PostId { get; set; }
    public string Title { get; set; }
    public string Content { get; set; }

    public int BlogId { get; set; }
    public Blog Blog { get; set; }
}
```

Затем вы определяете `DbContext`, который будете использовать для взаимодействия с базой данных:

{% list tabs %}

- OnConfiguring

  Использование `OnConfiguring()` для настройки контекста — самый простой способ начать работу, но для большинства приложений в рабочей среде он не рекомендуется:

  ```c#
  public class BloggingContext : DbContext
    {
    public DbSet<Blog> Blogs { get; set; }
    public DbSet<Post> Posts { get; set; }
    
        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
            => optionsBuilder.UseYdb("<connection string>");
    }
    
    // At the point where you need to perform a database operation:
    using var context = new BloggingContext();
    // Use the context...
  ```

- DbContext pooling

  ```c#
  var dbContextFactory = new PooledDbContextFactory<BloggingContext>(
    new DbContextOptionsBuilder<BloggingContext>()
        .UseYdb("<connection string>")
        .Options);

  // At the point where you need to perform a database operation:
  using var context = dbContextFactory.CreateDbContext();
  // Use the context...
  ```

- ASP.NET / DI

  ```c#
  var builder = WebApplication.CreateBuilder(args);

  builder.Services.AddDbContextPool<BloggingContext>(opt =>
      opt.UseYdb(builder.Configuration.GetConnectionString("BloggingContext")));
  
  public class BloggingContext(DbContextOptions<BloggingContext> options) : DbContext(options)
  {
      public DbSet<Blog> Blogs { get; set; }
      public DbSet<Post> Posts { get; set; }
  }
  ```

{% endlist %}

Для получения дополнительной информации о начале работы с EF обратитесь к [документации по началу работы](https://learn.microsoft.com/en-us/ef/core/get-started/overview/first-app?tabs=netcore-cli).

## Дополнительные конфигурации {{ ydb-short-name }}

Поставщик Entity Framework (EF) Core для {{ ydb-short-name }} имеет собственные дополнительные параметры конфигурации.

### Подключение ADO.NET к Yandex Cloud

Подробнее ознакомиться с различными способами аутентификации в Yandex Cloud можно в [документации по ADO.NET](../../reference/languages-and-apis/ado-net/yandex-cloud.md).

Ниже приведён пример того, как передать необходимые параметры для подключения к Yandex Cloud в Entity Framework:

```c#
.UseYdb(cmd.ConnectionString, builder => builder
    .WithCredentialsProvider(saProvider)
    .WithServerCertificates(YcCerts.GetYcServerCertificates())
)
```

### Миграция схемы

Для корректного выполнения миграций схемы базы данных необходимо отключить стратегию автоматического повтора запросов (`ExecutionStrategy`), которая по умолчанию активирована в пакете `EntityFrameworkCore.Ydb`.

Чтобы отключить `ExecutionStrategy` при выполнении миграций, следует явно переопределить интерфейс `IDesignTimeDbContextFactory` и воспользоваться методом `DisableRetryOnFailure()`.

Пример реализации фабрики контекста данных для миграций:

```с#
internal class BloggingContextFactory : IDesignTimeDbContextFactory<BloggingContext>
{
    public BloggingContext CreateDbContext(string[] args)
    {
        var optionsBuilder = new DbContextOptionsBuilder<BloggingContext>();

        return new BloggingContext(
            optionsBuilder.UseYdb("Host=localhost;Port=2136;Database=/local",
                builder => builder.DisableRetryOnFailure()
            ).Options
        );
    }
}
```

{% note info %}

Объект фабрики потребуется для выполнения команд миграции, например:  

```bash
dotnet ef migrations add MyMigration  
dotnet ef database update
```

{% endnote %}

## Примеры

Полные примеры использования [доступны на GitHub](https://github.com/ydb-platform/ydb-dotnet-sdk/tree/main/examples).
