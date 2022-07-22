# Аутентификация при помощи логина и пароля

{% include [work in progress message](../_includes/addition.md) %}

Ниже приведены примеры кода аутентификации при помощи логина и пароля в разных {{ ydb-short-name }} SDK.

{% list tabs %}

- C++

  ```c++
  auto driverConfig = NYdb::TDriverConfig()
      .SetEndpoint(endpoint)
      .SetDatabase(database)
      .SetCredentialsProviderFactory(NYdb::CreateLoginCredentialsProviderFactory({
          .User = "user",
          .Password = "password",
      }));

  NYdb::TDriver driver(driverConfig);
  ```

{% endlist %}
