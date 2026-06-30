# Prefer a pile with a specific state

Below is an example of setting the balancing algorithm option "prefer a [pile](../../concepts/glossary.md#pile) with a specific [state](../../concepts/bridge.md#pile-states)" in the {{ ydb-short-name }} SDK.

If the state is not specified when setting the option, the SDK prefers the PRIMARY [pile](../../concepts/glossary.md#pile).

This option only makes sense if the cluster is in [bridge mode](../../concepts/bridge.md). If this is not the case, the SDK will use [uniform random balancing](./balancing-random-choice.md).

{% list tabs %}

- C++

  {% list tabs %}

  - Native SDK

    ```cpp
    #include <ydb-cpp-sdk/client/driver/driver.h>

    int main() {
      auto connectionString = std::string(std::getenv("YDB_CONNECTION_STRING"));

      auto driverConfig = NYdb::TDriverConfig(connectionString)
        .SetBalancingPolicy(NYdb::TBalancingPolicy::UsePreferablePileState(NYdb::EPileState::PRIMARY));

      NYdb::TDriver driver(driverConfig);
      // ...
      driver.Stop(true);
      return 0;
    }
    ```

  - userver

    {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

  {% endlist %}

- Go

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- Python

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- C#

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- JavaScript

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- Java

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- Rust

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

  Track progress or vote for support in the Rust SDK: [ydb-rs-sdk#491](https://github.com/ydb-platform/ydb-rs-sdk/issues/491)

- PHP

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

{% endlist %}
