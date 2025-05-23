# {{ ydb-short-name }} Entity Framework Core Provider

{{ ydb-short-name }} has an Entity Framework (EF) Core provider. It behaves like other EF Core providers (e.g. SQL Server), so the [general EF Core docs](https://docs.microsoft.com/ef/core/index) apply here as well. If you're just getting started with EF Core, those docs are the best place to start.

Development happens in the [ydb-dotnet-sdk](https://github.com/ydb-platform/ydb-dotnet-sdk/tree/main) repository, all issues should be reported there.

## Set up the {{ ydb-short-name }} Entity Framework Core provider

To get started, you need to add the necessary NuGet packages to your project:

```dotnet
dotnet add package EntityFrameworkCore.Ydb
```

## Defining a model and a DbContext

Let's say you want to store blogs and their posts in their database; you can model these as .NET types as follows:

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

You then define a `DbContext` type which you'll use to interact with the database:

{% list tabs %}

- OnConfiguring

  Using OnConfiguring() to configure your context is the easiest way to get started, but is discouraged for most production applications:

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

For more information on getting started with EF, consult the [getting started documentation](https://learn.microsoft.com/en-us/ef/core/get-started/overview/first-app?tabs=netcore-cli).

## Additional {{ ydb-short-name }} configuration

The Entity Framework (EF) Core provider for {{ ydb-short-name }} has its own additional configuration parameters.

### Connection ADO.NET to Yandex Cloud

You can find more information about different authentication methods for Yandex Cloud in the [ADO.NET documentation](../../reference/languages-and-apis/ado-net/yandex-cloud.md).

Below is an example of how to specify the necessary parameters for connecting to Yandex Cloud using Entity Framework:

```c#
.UseYdb(cmd.ConnectionString, builder =>
{
    builder.WithCredentialsProvider(saProvider);
    builder.WithServerCertificates(YcCerts.GetYcServerCertificates());
})
```
