# Сборка JDBC-драйвера

По умолчанию все тесты запускаются в локальном экземпляре {{ ydb-short-name }} в Docker (если на хосте установлен Docker или Docker Machine).

Чтобы отключить эти тесты, выполните команду `mvn test -DYDB_DISABLE_INTEGRATION_TESTS=true`.