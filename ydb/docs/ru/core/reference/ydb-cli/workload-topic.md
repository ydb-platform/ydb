# Topic нагрузка

Нагружает [топики](../../concepts/topic.md) {{ ydb-short-name }}, используя их в качестве очередей сообщений. Для имитации реальной нагрузки вы можете изменять различные входные параметры: число сообщений, размер сообщений, целевую скорость записи, число читателей и писателей.

В процессе работы на консоль выдаются результаты, которые включают количество записанных сообщений, скорость записи сообщений и пр.

Чтобы нагрузить топик:

1. [Инициализируйте нагрузку](#init).
1. Запустите один из доступных видов нагрузки:
    * [write](#run-write) — генерация сообщений и их запись в топик в асинхронном режиме;
    * [read](#run-read) — асинхронное чтение сообщений из топика;
    * [full](#run-full) — одновременное асинхронное чтение и запись сообщений.

{% include [ydb-cli-profile.md](../../_includes/ydb-cli-profile.md) %}

## Инициализация нагрузочного теста {#init}

Перед запуском нагрузки ее необходимо инициализировать. При инициализации будет создан топик `workload-topic` с указанными параметрами. Инициализация выполняется следующей командой:

```bash
{{ ydb-cli }} [global options...] workload topic init [options...]
```

* `global options` — [глобальные параметры](commands/global-options.md).
* `options` — параметры подкоманды.

Параметры подкоманды:

Имя параметра | Описание параметра
---|---
`--topic` | Имя топика.<br>Значение по умолчанию: `workload-topic`.
`--partitions`, `-p` | Количество партиций топика.<br>Значение по умолчанию: `128`.
`--consumers`, `-c` | Количество читателей топика.<br>Значение по умолчанию: `1`.
`--consumer-prefix` | Префикс имени читателей.<br/>Значение по умолчанию: `workload-consumer`.<br/>Например, если количество читателей `--consumers` равно `2` и префикс `--consumer-prefix` равен `workload-consumer`, то будут использованы следующие имена читателей: `workload-consumer-0`, `workload-consumer-1`.

>Чтобы создать топик с `256` партициями и `2` читателями, выполните команду:
>
>```bash
>{{ ydb-cli }} --profile quickstart workload topic init --partitions 256 --consumers 2
>```

## Нагрузка на запись {#run-write}

Этот вид нагрузки генерирует и записывает сообщения в топик в асинхронном режиме.

Общий вид команды для запуска нагрузки на запись:

```bash
{{ ydb-cli }} [global options...] workload topic run write [options...]
```

* `global options` — [глобальные параметры](commands/global-options.md).
* `options` — параметры подкоманды.

Посмотреть описание команды для запуска нагрузки на запись:

```bash
{{ ydb-cli }} workload topic run write --help
```

Параметры подкоманды:

Имя параметра | Описание параметра
---|---
`--seconds`, `-s` | Продолжительность теста в секундах.<br>Значение по умолчанию: `60`.
`--window`, `-w` | Длительность окна сбора статистики в секундах.<br>Значение по умолчанию: `1`.
`--quiet`, `-q` | Выводить только итоговый результат теста.
`--print-timestamp` | Печатать время вместе со статистикой каждого временного окна.
`--warmup` | Время прогрева теста в секундах.<br>В течение этого времени не вычисляется статистика, оно необходимо для устранения переходных процессов при старте.<br>Значение по умолчанию: `5`.
`--percentile` | Процентиль в выводе статистики.<br>Значение по умолчанию: `50`.
`--topic` | Имя топика.<br>Значение по умолчанию: `workload-topic`.
`--threads`, `-t` | Количество потоков писателя.<br>Значение по умолчанию: `1`.
`--message-size`, `-m` | Размер сообщения в байтах. Возможно задание в КБ, МБ, ГБ путем добавления суффиксов `K`, `M`, `G` соответственно.<br>Значение по умолчанию: `10K`.
`--message-rate` | Целевая суммарная скорость записи, сообщений в секунду. Исключает использование параметра `--byte-rate`.<br>Значение по умолчанию: `0` (нет ограничения).
`--byte-rate` | Целевая суммарная скорость записи, байт в секунду. Исключает использование параметра `--message-rate`. Возможно задание в КБ/с, МБ/с, ГБ/с путем добавления суффиксов `K`,`M`,`G` соответственно.<br>Значение по умолчанию: `0` (нет ограничения).
`--codec` | Кодек, используемый для сжатия сообщений на клиенте перед отправкой на сервер.<br>Сжатие увеличивает затраты CPU на клиенте при записи и чтении сообщений, но обычно позволяет уменьшить объем передаваемых по сети и хранимых данных. При последующем чтении сообщений подписчиками они автоматически разжимаются использованным при записи кодеком, не требуя указания каких-либо параметров.<br>Возможные значения: `RAW` - без сжатия (по умолчанию), `GZIP`, `ZSTD`.

>Чтобы записать в `100` потоков писателей с целевой скоростью `80` МБ/с в течение `10` секунд, выполните следующую команду:
>
>```bash
>{{ ydb-cli }} --profile quickstart workload topic run write --threads 100 --byte-rate 80M
>```
>
>В процессе работы будет выводиться статистика по промежуточным временным окнам, а по окончании теста — итоговая статистика за все время работы:
>
>```text
>Window  Write speed     Write time      Inflight
>#       msg/s   MB/s    percentile,ms   percentile,msg
>1       20      0       1079            72
>2       8025    78      1415            78
>3       7987    78      1431            79
>4       7888    77      1471            101
>5       8126    79      1815            116
>6       7018    68      1447            79
>7       8938    87      2511            159
>8       7055    68      1463            78
>9       7062    69      1455            79
>10      9912    96      3679            250
>Window  Write speed     Write time      Inflight
>#       msg/s   MB/s    percentile,ms   percentile,msg
>Total   7203    70      3023            250
>```
>
>* `Window` — порядковый номер временного окна сбора статистики;
>* `Write speed` — скорость записи сообщений, сообщений в секунду и МБ/с;
>* `Write time` — процентиль времени записи сообщения, мс.
>* `Inflight` — максимальное число сообщений, ожидающих подтверждения по всем партициям.

## Нагрузка на чтение {#run-read}

Этот вид нагрузки асинхронно читает сообщения из топика. Чтобы в топике появились сообщения, перед началом чтения запустите [нагрузку на запись](#run-write).

Общий вид команды для запуска нагрузки на чтение:

```bash
{{ ydb-cli }} [global options...] workload topic run read [options...]
```

* `global options` — [глобальные параметры](commands/global-options.md).
* `options` — параметры подкоманды.

Посмотреть описание команды для запуска нагрузки на чтение:

```bash
{{ ydb-cli }} workload topic run read --help
```

Параметры подкоманды:

Имя параметра | Описание параметра
---|---
`--seconds`, `-s` | Продолжительность теста в секундах.<br>Значение по умолчанию: `60`.
`--window`, `-w` | Длительность окна сбора статистики в секундах.<br>Значение по умолчанию: `1`.
`--quiet`, `-q` | Выводить только итоговый результат теста.
`--print-timestamp` | Печатать время вместе со статистикой каждого временного окна.
`--warmup` | Время прогрева теста в секундах.<br>В течение этого времени не вычисляется статистика, оно необходимо для устранения переходных процессов при старте.<br>Значение по умолчанию: `5`.
`--percentile` | Процентиль в выводе статистики.<br>Значение по умолчанию: `50`.
`--topic` | Имя топика.<br>Значение по умолчанию: `workload-topic`.
`--consumers`, `-c` | Количество читателей.<br>Значение по умолчанию: `1`.
`--consumer-prefix` | Префикс имени читателей.<br/>Значение по умолчанию: `workload-consumer`.<br/>Например, если количество читателей `--consumers` равно `2` и префикс `--consumer-prefix` равен `workload-consumer`, то будут использованы следующие имена читателей: `workload-consumer-0`, `workload-consumer-1`.
`--threads`, `-t` | Количество потоков читателя.<br>Значение по умолчанию: `1`.

>Чтобы выполнить чтение из топика с помощью `2` читателей, каждый из который имеет `100` потоков, выполните следующую команду:
>
>```bash
>{{ ydb-cli }} --profile quickstart workload topic run read --consumers 2 --threads 100
>```
>
>В процессе работы будет выводиться статистика по промежуточным временным окнам, а по окончании теста — итоговая статистика за все время работы:
>
>```text
>Window  Lag             Lag time        Read speed      Full time
>#       percentile,msg  percentile,ms   msg/s   MB/s    percentile,ms
>1       0               0               0       0       0
>2       30176           0               66578   650     0
>3       30176           0               68999   674     0
>4       30176           0               66907   653     0
>5       27835           0               67628   661     0
>6       30176           0               67938   664     0
>7       30176           0               71628   700     0
>8       20338           0               61367   599     0
>9       30176           0               61770   603     0
>10      30176           0               58291   569     0
>Window  Lag             Lag time        Read speed      Full time
>#       percentile,msg  percentile,ms   msg/s   MB/s    percentile,ms
>Total   30176           0               80267   784     0
>```
>
>* `Window` — порядковый номер временного окна сбора статистики.
>* `Lag` — максимальное в окне сбора статистики отставание читателя. Учитываются сообщения по всем партициям.
>* `Lag time` — процентиль времени задержки сообщений в мс.
>* `Read` — Скорость чтения сообщений читателем, сообщений в секунду и МБ/с.
>* `Full time` — процентиль времени полной обработки сообщения (от записи писателем до чтения читателем) в мс.

## Нагрузка на чтение и запись {#run-full}

Этот вид нагрузки одновременно асинхронно пишет и читает сообщения. Выполнение данной команды эквивалентно одновременному запуску нагрузок на чтение и запись.

Общий вид команды для запуска нагрузки на чтение и запись:

```bash
{{ ydb-cli }} [global options...] workload topic run full [options...]
```

* `global options` — [глобальные параметры](commands/global-options.md).
* `options` — параметры подкоманды.

Посмотреть описание команды для запуска нагрузки на чтение и запись:

```bash
{{ ydb-cli }} workload topic run full --help
```

Параметры подкоманды:

Имя параметра | Описание параметра
---|---
`--seconds`, `-s` | Продолжительность теста в секундах.<br>Значение по умолчанию: `60`.
`--window`, `-w` | Длительность окна сбора статистики в секундах.<br>Значение по умолчанию: `1`.
`--quiet`, `-q` | Выводить только итоговый результат теста.
`--print-timestamp` | Печатать время вместе со статистикой каждого временного окна.
`--warmup` | Время прогрева теста в секундах.<br>В течение этого времени не вычисляется статистика, оно необходимо для устранения переходных процессов при старте.<br>Значение по умолчанию: `5`.
`--percentile` | Процентиль в выводе статистики.<br>Значение по умолчанию: `50`.
`--topic` | Имя топика.<br>Значение по умолчанию: `workload-topic`.
`--producer-threads`, `-p` | Количество потоков писателя.<br>Значение по умолчанию: `1`.
`--message-size`, `-m` | Размер сообщения в байтах. Возможно задание в КБ, МБ, ГБ путем добавления суффиксов `K`, `M`, `G` соответственно.<br>Значение по умолчанию: `10K`.
`--message-rate` | Целевая суммарная скорость записи, сообщений в секунду. Исключает использование параметра `--message-rate`.<br>Значение по умолчанию: `0` (нет ограничения).
`--byte-rate` | Целевая суммарная скорость записи, байт в секунду. Исключает использование параметра `--byte-rate`. Возможно задание в КБ/с, МБ/с, ГБ/с путем добавления суффиксов `K`,`M`,`G` соответственно.<br>Значение по умолчанию: `0` (нет ограничения).
`--codec` | Кодек, используемый для сжатия сообщений на клиенте перед отправкой на сервер.<br>Сжатие увеличивает затраты CPU на клиенте при записи и чтении сообщений, но обычно позволяет уменьшить объем передаваемых по сети и хранимых данных. При последующем чтении сообщений подписчиками они автоматически разжимаются использованным при записи кодеком, не требуя указания каких-либо параметров.<br>Возможные значения: `RAW` - без сжатия (по умолчанию), `GZIP`, `ZSTD`.
`--consumers`, `-c` | Количество читателей.<br>Значение по умолчанию: `1`.
`--consumer-prefix` | Префикс имени читателей.<br/>Значение по умолчанию: `workload-consumer`.<br/>Например, если количество читателей `--consumers` равно `2` и префикс `--consumer-prefix` равен `workload-consumer`, то будут использованы следующие имена читателей: `workload-consumer-0`, `workload-consumer-1`.
`--threads`, `-t` | Количество потоков читателя.<br>Значение по умолчанию: `1`.

>Пример команды чтения с помощью `2` читателей в `50` потоков и записи `100` потоков писателей с целевой скоростью `80` МБ/с и длительностью `10` секунд:
>
>```bash
>{{ ydb-cli }} --profile quickstart workload topic run full --producer-threads 100 --consumers 2 --consumer-threads 50 --byte-rate 80M
>```
>
>В процессе работы будет выводиться статистика по промежуточным временным окнам, а по окончании теста — итоговая статистика за все время работы:
>
>```text
>Window  Write speed     Write time      Inflight        Lag             Lag time        Read speed      Full time
>#       msg/s   MB/s    percentile,ms   percentile,msg  percentile,msg  percentile,ms   msg/s   MB/s    percentile,ms
>1       0       0       0               0               0               0               0       0       0
>2       1091    10      2143            8               2076            20607           40156   392     30941
>3       1552    15      2991            12              7224            21887           41040   401     31886
>4       1733    16      3711            15              10036           22783           38488   376     32577
>5       1900    18      4319            15              10668           23551           34784   340     33372
>6       2793    27      5247            21              9461            24575           33267   325     34893
>7       2904    28      6015            22              12150           25727           34423   336     35507
>8       2191    21      5087            21              12150           26623           29393   287     36407
>9       1952    19      2543            10              7627            27391           33284   325     37814
>10      1992    19      2655            9               10104           28671           29101   284     38797
>Window  Write speed     Write time      Inflight        Lag             Lag time        Read speed      Full time
>#       msg/s   MB/s    percentile,ms   percentile,msg  percentile,msg  percentile,ms   msg/s   MB/s    percentile,ms
>Total   1814    17      5247            22              12150           28671           44827   438     40252
>```
>
>* `Window` — порядковый номер временного окна сбора статистики.
>* `Write speed` — скорость записи сообщений, сообщений в секунду и МБ/с.
>* `Write time` — процентиль времени записи сообщения в мс.
>* `Inflight` — максимальное число сообщений, ожидающих подтверждения по всем партициям.
>* `Lag` — максимальное число сообщений, ожидающих чтения, в окне сбора статистики. Учитываются сообщения по всем партициям.
>* `Lag time` — процентиль времени задержки сообщений в мс.
>* `Read` — Скорость чтения сообщений читателем, сообщений в секунду и МБ/с.
>* `Full time` — процентиль времени полной обработки сообщения, от записи писателем до чтения читателем в мс.

## Удаление топика {#clean}

После завершения работы можно удалить тестовый топик. Общий вид команды для удаления топика:

```bash
{{ ydb-cli }} [global options...] workload topic clean [options...]
```

* `global options` — [глобальные параметры](commands/global-options.md).
* `options` — параметры подкоманды.

Параметры подкоманды:

Имя параметра | Описание параметра
---|---
`--topic` | Имя топика.<br>Значение по умолчанию: `workload-topic`.

>Чтобы удалить тестовый топик `workload-topic`, выполните следующую команду:
>
>```bash
>{{ ydb-cli }} --profile quickstart workload topic clean
>```