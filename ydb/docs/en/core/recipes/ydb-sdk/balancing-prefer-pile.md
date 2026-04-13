# Prefer a pile with a specific state

Below is example of the code for setting the "prefer pile with a specific state" balancing algorithm in {{ ydb-short-name }} SDK.

If no state is specified when setting the option, the SDK prefers the PRIMARY pile.

This option only makes sense if the cluster is operating in bridge mode. If it is not, the SDK will use [random choice balancing algorithm](./balancing-random-choice.md).

{% list tabs %}

<<<<<<< HEAD
- С++
=======
- Go

  This functionality is not currently supported.

- C++
>>>>>>> 26186944f5a (DOCSUP-127029: [YDBDOCS-1972] docs: align RU YDB SDK docs with nested tab structure. Организация процесса перевода (1 архив) (1 шт.) (#37826))

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

<<<<<<< HEAD
=======
- Python

  This functionality is not currently supported.

- JavaScript

  {% include [work-in-progress](../../_includes/work-in-progress.md) %}

- Java

  This functionality is not currently supported.

>>>>>>> 26186944f5a (DOCSUP-127029: [YDBDOCS-1972] docs: align RU YDB SDK docs with nested tab structure. Организация процесса перевода (1 архив) (1 шт.) (#37826))
{% endlist %}
