# Быстрый старт

1. Скачайте [JDBC-драйвер для {{ ydb-short-name }}](https://github.com/ydb-platform/ydb-jdbc-driver/releases).

1. Скопируйте jar-файл в директорию, прописанную в переменной окружения `CLASSPATH`, или загрузите jar-файл в интегрированной среде разработки (IDE).

1. Установите соединение с {{ ydb-short-name }}. Примеры JDBC URL:

    {% include notitle [примеры](_includes/jdbc-url-examples.md) %}

1. Выполните проверочный запрос к базе данных {{ ydb-short-name }}. См. пример [YdbDriverExampleTest.java](https://github.com/ydb-platform/ydb-jdbc-driver/blob/master/jdbc/src/test/java/tech/ydb/jdbc/YdbDriverExampleTest.java).
