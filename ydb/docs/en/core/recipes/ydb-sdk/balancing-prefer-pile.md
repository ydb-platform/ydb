# Prefer a pile with a specific state

Below is an example of setting the balancing algorithm option that prefers a [pile](../../concepts/glossary.md#pile) in a given [state](../../concepts/bridge.md#pile-states) in the {{ ydb-short-name }} SDK.

If no state is specified when setting the option, the SDK prefers the PRIMARY pile.

This option only makes sense if the cluster is operating in [bridge mode](../../concepts/bridge.md). Otherwise, the SDK uses [random choice balancing](./balancing-random-choice.md).

{% list tabs %}

- C++

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

- Python

  Not supported at this time.

{% endlist %}
