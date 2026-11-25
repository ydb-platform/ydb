# C++ replica switch probe for streaming discovery

The C++ driver watches `StreamEndpoints` on its bootstrap connection. After a switch the balancer ends the old watch. The next watch receives the active replica’s endpoints. Fresh RPCs use the new assignment while calls already running on the healthy source can finish.

Enable this mode in a C++ application with `TDriverConfig::SetDiscoveryMode(EDiscoveryMode::Eds)`. To build and run the probe from the repository root, point it at a managed bootstrap endpoint serving the test database.

```sh
./ya make --build relwithdebinfo ydb/tests/functional/sdk/cpp/rediscovery
YDB_ENDPOINT=localhost:2136 YDB_DATABASE=/Root/tenant YDB_EDS_DISCOVERY=1 \
  YDB_PROBE=list-on-demand ydb/tests/functional/sdk/cpp/rediscovery/rediscovery-probe
```

Enter `list` before and after switching the bootstrap route. With a distinct directory on each replica, the returned names show which replica handled each fresh request on the same driver.

The native regression tests use gRPC servers to exercise discovery and requests held across a switch.

```sh
./ya make --build relwithdebinfo -DUSER_CXXFLAGS=-Werror -tA ydb/public/sdk/cpp/tests/unit/client/driver -F 'EndpointRediscovery::*'
```
