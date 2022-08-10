# Username and password based authentication

{% include [work in progress message](../_includes/addition.md) %}

Below are examples of the code for authentication based on a username and token in different {{ ydb-short-name }} SDKs.

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
