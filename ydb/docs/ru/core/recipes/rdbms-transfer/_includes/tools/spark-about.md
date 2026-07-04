## Apache Spark {#about}

**Apache Spark** — распределённый движок обработки данных с API на Scala, Python, Java и R. Для миграции Spark читает таблицу источника (через JDBC или файлы) в `DataFrame` и записывает в {{ ydb-short-name }} через [YDB Spark Connector](../../../../integrations/query-engines/spark.md).

### spark-shell, pyspark, spark-submit

| Инструмент | Назначение |
| --- | --- |
| **spark-shell** | Интерактивная Scala-консоль; удобна для отладки и разовых миграций |
| **pyspark** | То же для Python |
| **spark-submit** | Запуск готового JAR/Python-скрипта в production |

В рецептах ниже используется **spark-shell** — REPL, в котором можно по шагам выполнить чтение и запись.

### Системные требования

| Компонент | Требование |
| --- | --- |
| ОС | Linux (рекомендуется), macOS; для локального `local[*]` — та же машина, где установлен Spark |
| JDK | **11+** (OpenJDK или Temurin) |
| RAM | **≥ 8 ГБ** на машине; для `--conf spark.executor.memory=4g` — не менее 6–8 ГБ свободной RAM |
| Диск | ~500 МБ под дистрибутив Spark + кэш Maven (`--packages`) |
| Сеть | Доступ к Maven Central (или заранее скачанные JAR) и к источнику/JDBC |

### Установка Spark

1. Скачайте дистрибутив [Apache Spark 3.x](https://spark.apache.org/downloads.html) (тип **Pre-built for Apache Hadoop 3**).
2. Распакуйте архив, например в `/opt/spark`.
3. Добавьте в `PATH`:

   ```bash
   export SPARK_HOME=/opt/spark
   export PATH="$SPARK_HOME/bin:$PATH"
   java -version   # 11+
   spark-shell --version
   ```

Альтернатива — образ Docker `apache/spark` или managed Spark (Yandex Cloud, EMR, Dataproc): логика миграции та же, меняется только `--master`.

### YDB Spark Connector

Коннектор подтягивается через `--packages` (рекомендуется shaded-сборка со всеми зависимостями):

```bash
spark-shell --master 'local[*]' \
  --packages tech.ydb.spark:ydb-spark-connector-shaded:2.0.1 \
  --conf spark.executor.memory=4g
```

* **`--master 'local[*]'`** — все ядра CPU на одной машине; для кластера укажите URL вашего Spark master.
* **`spark.executor.memory=4g`** — рекомендация из [документации коннектора](../../../../integrations/query-engines/spark.md); при OOM увеличьте до 6–8g.

Репозиторий: [ydb-spark-connector](https://github.com/ydb-platform/ydb-spark-connector).

### JDBC-драйвер источника

Драйвер СУБД-источника добавляется вторым артефактом в `--packages` или через `--jars /path/to/driver.jar` (Oracle и часть enterprise-СУБД часто только так).
