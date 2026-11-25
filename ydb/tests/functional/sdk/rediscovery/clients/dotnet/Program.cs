using System.Diagnostics;
using Ydb.Sdk.Ado;
using Ydb.Sdk.Topic.Reader;
using Ydb.Sdk.Topic.Writer;

var mode = Environment.GetEnvironmentVariable("YDB_PROBE") ?? "scheme";
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
if (Environment.GetEnvironmentVariable("YDB_LONG_POLL_DISCOVERY") == "1")
{
    settings["EnableLongPollDiscovery"] = true;
}
if (mode is "topic-write-on-demand" or "topic-read-on-demand")
{
    var topic = Environment.GetEnvironmentVariable("YDB_PROBE_TOPIC") ?? throw new ArgumentException("YDB_PROBE_TOPIC");
    if (mode == "topic-write-on-demand")
    {
        await using var writer = new WriterBuilder<string>(settings, topic) { ProducerId = "probe" }.Build();
        while (await Console.In.ReadLineAsync() is { } payload)
        {
            await writer.WriteAsync(payload).WaitAsync(TimeSpan.FromSeconds(20));
            Console.WriteLine($"OK {payload}");
        }
    }
    else
    {
        await using var reader = new ReaderBuilder<string>(settings)
        {
            ConsumerName = "probe", SubscribeSettings = { new SubscribeSettings(topic) }
        }.Build();
        while (await Console.In.ReadLineAsync() is not null)
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(20));
            var message = await reader.ReadAsync(timeout.Token);
            Console.WriteLine($"OK {message.Data}");
        }
    }
    return;
}
if (mode != "scheme")
{
    throw new ArgumentException($"Unsupported YDB_PROBE {mode}");
}
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
