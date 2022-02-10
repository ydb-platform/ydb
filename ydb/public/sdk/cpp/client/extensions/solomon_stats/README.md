# YDB C++ SDK Metrics

You can plug in YDB C++ SDK extension to monitor how your application interacts with YDB. In particular you can monitor:
- Transport errors;
- Per host messages in fligh;
- How database nodes discover works;
- Session pool size;
- Number of sessions per host;
- Number of server endpoints available;
- Query size, latency, etc.

You can get these metrics via http server provided by YDB C++ SDK or implement your own MetricRegistry if it more convenient.

## Setting up Solomon Monitoring

> This is Yandex specific section for setting up internal monitoring called Solomon

### Setup Solomon Environment
TSolomonStatPullExtension class allows you to quickly setup you application monitoring. You need to prepare a Solomon project that can accept your metrics beforehand.
[Create project, cluster and service, and connect them.](https://wiki.yandex-team.ru/solomon/howtostart/).
Add hostnames which run your application to **Cluster hosts** (use hostnames without "http://"). Solomon's fetcher will use **Port** field for all added hosts.
Fill in the following params:
- **Monitoring model** : **PULL**;
- **URL Path** : **/stats**;
- **Interval** : **10**;
- **Port** : a port of http server YDB SDK runs.

### Setup TSolomonStatPullExtension in Your Application Code

After creating NYdb::TDriver you need to add Solomon Monitoring extension. If you set up incorrect hostname or port **TSystemError** exception will be thrown.

> **Important**: you must plug in monitoring before driver creation.

```cl
#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>
#include <ydb/public/sdk/cpp/client/extensions/solomon_stats/pull_client.h>

...

{
    auto config = NYdb::TDriverConfig();
    NYdb::TDriver driver(config);
    try {

        const TString host = ...    // use hostname without http://
        const ui64 port = ...
        const TString project = ... // Solomon's project id
        const TString service = ... // Solomon's service id
        const TString cluster = ... // Solomon's cluster id
        NSolomonStatExtension::TSolomonStatPullExtension::TParams params(host, port, project, service, cluster);
        driver.AddExtension<NSolomonStatExtension::TSolomonStatPullExtension>(params);

    } catch (TSystemError& error) {
        ...
    }
}

```

## Setup Monitoring of Your Choice

Implementing NMonitoring::IMetricRegistry provides more flexibility. You can deliver application metrics to Prometheus or any other system of your choice, just register your specific NMonitoring::IMetricRegistry implementation via AddMetricRegistry function.

> **Important**: you must plug in monitoring before driver creation.

Select a method which is right for you:
```cl
#include <ydb/public/sdk/cpp/client/extensions/solomon_stats/pull_connector.h>

...

void AddMetricRegistry(NYdb::TDriver& driver, NMonitoring::IMetricRegistry* ptr);
void AddMetricRegistry(NYdb::TDriver& driver, std::shared_ptr<NMonitoring::IMetricRegistry> ptr);
void AddMetricRegistry(NYdb::TDriver& driver, TAtomicSharedPtr<NMonitoring::IMetricRegistry> ptr);
```

If you provide a raw pointer, it's your responsibility to delete the registry. You must shutdown the SDK driver before destroying the registry.

## Metrics Description

The most valuable metrics are the following:

- Grpc/InFlightByYdbHost - queries in flight for a selected YDB hostname
- Request/Latency - Query execution latency histogram from the client side, ms (without RetryOperation)
- Request/ParamsSize - Query params size histogram in bytes
- Request/QuerySize - Query text size histogram in bytes
- Request/ResultSize - Query response size histogram in bytes
- SessionBalancer/Variation - Coefficient of variation in the distribution of sessions across database hosts
- Sessions/InPool - Number of sessions in a pool
- Sessions/SessionsLimitExceeded - Rate of events for exceeding the limit on the number of sessions on the client side
- SessionsByYdbHost - Number of sessions per YDB hosts

