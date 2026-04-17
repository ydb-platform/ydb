# Предпочитать пайл c конкретным состоянием

Ниже приведен пример кода установки опции алгоритма балансировки "предпочитать [пайл](../../concepts/glossary.md#pile) c конкретным [состоянием](../../concepts/bridge.md#pile-states)" в {{ ydb-short-name }} SDK.

Если при установке опции состояние не задано, SDK предпочитает PRIMARY [пайл](../../concepts/glossary.md#pile).

Данная опция имеет смысл, только если кластер находится в [bridge режиме](../../concepts/bridge.md). Если это неверно, SDK будет использовать [равномерную случайную балансировку](./balancing-random-choice.md).

{% list tabs %}

- Go

  Функциональность на данный момент не поддерживается.

- С++

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

<<<<<<< HEAD
  Функциональность на данный момент не поддерживается.
=======
  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- C#

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- C#

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}

- JavaScript

  {% include [feature-not-supported](../../_includes/feature-not-supported.md) %}
>>>>>>> 317adb799 (dev: update dotnet snippets (#38018))

- Java

  Функциональность на данный момент не поддерживается.

- JavaScript

  {% include [work-in-progress](../../_includes/work-in-progress.md) %}

- Java

  Функциональность на данный момент не поддерживается.

{% endlist %}
