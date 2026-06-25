# Prefer a pile with a specific state

Below is example of the code for setting the "prefer pile with a specific state" balancing algorithm in {{ ydb-short-name }} SDK.

If no state is specified when setting the option, the SDK prefers the PRIMARY pile.

This option only makes sense if the cluster is operating in bridge mode. If it is not, the SDK will use [random choice balancing algorithm](./balancing-random-choice.md).

{% list tabs %}

- Go

  This functionality is not currently supported.

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

  This functionality is not currently supported.

- JavaScript

  {% include [work-in-progress](../../_includes/work-in-progress.md) %}

- Java

  This functionality is not currently supported.

<<<<<<< HEAD
=======
- Rust

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

  Track progress or vote for Rust SDK support: [ydb-rs-sdk#491](https://github.com/ydb-platform/ydb-rs-sdk/issues/491)
>>>>>>> 7835ec47514 (docs: Rust basic query example in example-app + other Rust code snippets + Vector search article refactoring + removed OpenTracing from feature-parity table (#43637))

{% endlist %}
