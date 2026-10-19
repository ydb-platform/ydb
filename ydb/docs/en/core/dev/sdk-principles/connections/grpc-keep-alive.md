# Optimal gRPC keep-alive settings for {{ ydb-short-name }}

## The problem

gRPC keep-alive parameters control how often the client driver checks that the connection to {{ ydb-short-name }} is alive, and how long it waits for a reply. The check is there so the driver can notice a dropped connection and the server does not keep an already dead one. If the client pings too often, the server closes the connection with `too_many_pings`.

### Common problems

1. **Pings that are too frequent** (`too_many_pings`) — when the client sends keep-alive too often and the server closes the connection with `GOAWAY received; Error code: 11; Debug Text: too_many_pings`.

2. **Slow driver startup** — when endpoint resolution or keep-alive settings stretch the connection to {{ ydb-short-name }} to 10 seconds or more.

3. **Poor load balancing** — when the default settings do not account for many driver instances at once.

4. **Network timeouts** — when the client runs in a serverless environment or the network is unstable.

## The solution

Optimal gRPC keep-alive settings keep the connection stable and the load balanced.
The default settings suit most users.

**Recommended settings**:

| Parameter | Recommended value | Description |
|-----------|-------------------|-------------|
| Keep-alive interval | 10 seconds | How often ping requests are sent |
| Keep-alive timeout | 10 seconds | How long to wait for a ping reply |
| Permit ping without data | true | Sending a ping when there is no traffic |

The examples differ by one line: in the bad example `grpc_keep_alive_timeout` is 1 second, in the good example the default is used — 10 seconds.

{% cut "Bad example" %}

```python
# Bad: keep-alive settings are too aggressive
import ydb
import os

def get_driver():
    driver_config = ydb.DriverConfig(
        endpoint=os.getenv("YDB_ENDPOINT"),
        database=os.getenv("YDB_DATABASE"),
        credentials=ydb.credentials_from_env_variables(),
        grpc_keep_alive_timeout=1000,  # Value is too small - 1 second
    )
    driver = ydb.Driver(driver_config)
    try:
        driver.wait(timeout=5)
    except TimeoutError:
        raise RuntimeError("YDB connection timeout")
    return driver
```

{% endcut %}

{% cut "Good example" %}

```python
# Good: default keep-alive settings
import ydb
import os

def get_driver():
    driver_config = ydb.DriverConfig(
        endpoint=os.getenv("YDB_ENDPOINT"),
        database=os.getenv("YDB_DATABASE"),
        credentials=ydb.credentials_from_env_variables(),
        # grpc_keep_alive_timeout=10000, 10 seconds - the default value
    )
    driver = ydb.Driver(driver_config)
    try:
        driver.wait(timeout=5)
    except TimeoutError:
        raise RuntimeError("YDB connection timeout")
    return driver
```

{% endcut %}


### Key benefits of the right settings

- **Stable connections** — avoids `too_many_pings` errors
- **Fast startup** — the driver usually connects to the cluster in under a second
- **Efficient resource use** — a reasonable load on the client and the server
- **Tolerance of network issues** — temporary failures are handled correctly

### Monitoring and diagnostics

To watch the state of connections:

- Monitor connection metrics in the application logs
- Use tracing to investigate connection problems
- Keep the SDK up to date
- Check that {{ ydb-short-name }} endpoints are reachable

### Special cases

**Serverless environments**

In a serverless environment, increase the interval.

```python
driver_config = ydb.DriverConfig(
    endpoint=os.getenv("YDB_ENDPOINT"),
    database=os.getenv("YDB_DATABASE"),
    credentials=ydb.credentials_from_env_variables(),
    grpc_keep_alive_timeout=20000,  # 20 seconds to reduce the load
)
```

**High-load systems**

Under high load, set a shorter interval and timeout.

```go
keepAliveParams := keepalive.ClientParameters{
    Time:                5 * time.Second,
    Timeout:             5 * time.Second,
    PermitWithoutStream: true,
}
```

> **Important**: If your case needs different settings, test them in conditions as close to production as possible before you settle on the values.
