using System.Diagnostics;
using Ydb.Sdk.Ado;

var mode = Environment.GetEnvironmentVariable("YDB_PROBE") ?? "scheme";
if (mode != "scheme")
{
    throw new ArgumentException($"Unsupported YDB_PROBE {mode}");
}
var endpoint = Environment.GetEnvironmentVariable("YDB_ENDPOINT")
    ?? throw new ArgumentException("YDB_ENDPOINT");
var database = Environment.GetEnvironmentVariable("YDB_DATABASE")
    ?? throw new ArgumentException("YDB_DATABASE");
var uri = new Uri($"http://{endpoint}");
var settings = new YdbConnectionStringBuilder
{
    Host = uri.Host,
    Port = uri.Port,
    Database = database,
    UseTls = false,
    ConnectTimeout = 8,
    EnableImplicitSession = true,
};
await using var connection = new YdbConnection(settings);
await connection.OpenAsync();

var retryWindow = TimeSpan.FromSeconds(8);
var lastSuccess = Stopwatch.StartNew();
Exception? lastError = null;
while (lastSuccess.Elapsed < retryWindow)
{
    var remaining = retryWindow - lastSuccess.Elapsed;
    using var timeout = new CancellationTokenSource(
        TimeSpan.FromMilliseconds(Math.Clamp(remaining.TotalMilliseconds, 0, 2000)));
    try
    {
        await connection.GetSchemaAsync("Tables", timeout.Token);
        Console.WriteLine("OK");
        lastSuccess.Restart();
    }
    catch (Exception error) when (error is YdbException or OperationCanceledException)
    {
        lastError = error;
        Console.Error.WriteLine(error.Message);
    }
    await Task.Delay(200);
}
throw new InvalidOperationException("ListDirectory did not succeed within the retry window", lastError);
