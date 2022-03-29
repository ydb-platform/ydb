В {{ ydb-short-name }} Java SDK для логирования используется библиотека slf4j, которая позволяет использовать различные уровни логирования (`error`, `warn`, `info`, `debug`, `trace`) для одного или нескольких логгеров. В текущей реализации доступны следующие логгеры:

* Логгер `com.yandex.ydb.core.grpc` предоставляет информацию о внутренней реализации grpc протокола 
  * уровень `debug` логирует все операции по протоколу grpc, рекомедуется использовать только для отладки
  * уровень `info` рекомендуется использовать по умолчанию

* Логгер `com.yandex.ydb.table.impl` на уровне `debug` позволяет отслеживать внутреннее состояние драйвера ydb, в частности работу пула сессий.

* Логгер `com.yandex.ydb.table.SessionRetryContext` на уровне `debug` будет информировать о количестве ретраев, результатах выполненных запросов, времени выполнения отдельных ретраев и общем времени выполнения всей операции

* Логгер `com.yandex.ydb.table.Session` на уровне `debug` предоставляет информацию о тексте запроса, статусе ответа и времени выполнения для различных операций сессии


Включение и настройка логгеров Java SDK зависит от используемой реализации `slf4j-api`.
Здесь приведен пример конфигурации `log4j2` для библиотеки `log4j-slf4j-impl`

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
        <Logger name="com.yandex.ydb.core.grpc" level="info" additivity="false">
            <AppenderRef ref="Console"/>
        </Logger>
        <Logger name="com.yandex.ydb.table.impl" level="info" additivity="false">
            <AppenderRef ref="Console"/>
        </Logger>
        <Logger name="com.yandex.ydb.table.SessionRetryContext" level="debug" additivity="false">
            <AppenderRef ref="Console"/>
        </Logger>
        <Logger name="com.yandex.ydb.table.Session" level="debug" additivity="false">
            <AppenderRef ref="Console"/>
        </Logger>

        <Root level="debug" >
            <AppenderRef ref="Console"/>
        </Root>
    </Loggers>
</Configuration>
```
