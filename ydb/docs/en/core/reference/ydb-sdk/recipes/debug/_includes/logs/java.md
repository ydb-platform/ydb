In the {{ ydb-short-name }} Java SDK, logging is done using the slf4j library. It lets you use different logging levels (`error`, `warn`, `info`, `debug`,  and `trace`) for one or more loggers. The following loggers are available in the current implementation:

* The `tech.ydb.core.grpc` logger provides  information about the internal implementation of gRPC.
  * At the `debug` level, all operations are logged using gRPC. This level is recommended for debugging only.
  * Use the `info` level by default.

* The `tech.ydb.table.impl` logger at the `debug` level lets you monitor the internal state of the ydb driver, including session pool performance.

* The `tech.ydb.table.SessionRetryContext` logger at the `debug` level provides information about the number of retries, the results of executed queries, the execution time of individual retries, and the total execution time of the entire operation.

* The `tech.ydb.table.Session` logger at the `debug` level provides information about the query text, response status, and execution time for various session operations.

Enabling and configuring Java SDK loggers depends on the `slf4j-api` implementation used.
Below is an example of the `log4j2` configuration for the `log4j-slf4j-impl` library.

```xml
<Configuration status="WARN">
    <Appenders>
        <Console name="Console" target="SYSTEM_OUT">
            <PatternLayout pattern="%d{HH:mm:ss.SSS} [%t] %-5level %logger{36} - %msg%n"/>
        </Console>
    </Appenders>

    <Loggers>
        <Logger name="io.netty" level="warn" additivity="false">
            <AppenderRef ref="Console"/>
        </Logger>
        <Logger name="io.grpc.netty" level="warn" additivity="false">
            <AppenderRef ref="Console"/>
        </Logger>
        <Logger name="tech.ydb.core.grpc" level="info" additivity="false">
            <AppenderRef ref="Console"/>
        </Logger>
        <Logger name="tech.ydb.table.impl" level="info" additivity="false">
            <AppenderRef ref="Console"/>
        </Logger>
        <Logger name="tech.ydb.table.SessionRetryContext" level="debug" additivity="false">
            <AppenderRef ref="Console"/>
        </Logger>
        <Logger name="tech.ydb.table.Session" level="debug" additivity="false">
            <AppenderRef ref="Console"/>
        </Logger>

        <Root level="debug" >
            <AppenderRef ref="Console"/>
        </Root>
    </Loggers>
</Configuration>
```

