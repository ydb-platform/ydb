|Имя параметра|Описание|Принимаемые значения|
|----|----|---|
|`file_pattern`|Шаблон имени файла|Строка шаблона имени. Поддерживаются подстановочные знаки `*`, `?`, `{ ... }`.|
|`data.interval.unit`|Единица измерения для парсинга типа `Interval`|`MICROSECONDS`, `MILLISECONDS`, `SECONDS`, `MINUTES`, `HOURS`, `DAYS`, `WEEKS`|
|`data.datetime.format_name`|Предопределенный формат, в котором записаны данные типа `Datetime`|`POSIX`, `ISO`|
|`data.datetime.format`|Шаблон, определяющий как записаны данные типа `Datetime`|Строка форматирования, например: `%Y-%m-%dT%H-%M`|
|`data.timestamp.format_name`|Предопределенный формат, в котором записаны данные типа `Timestamp`|`POSIX`, `ISO`, `UNIX_TIME_SECONDS`, `UNIX_TIME_MILLISECONDS`, `UNIX_TIME_MICROSECONDS`|
|`data.timestamp.format`|Шаблон, определяющий как записаны данные типа `Timestamp`|Строка форматирования, например: `%Y-%m-%dT%H-%M-%S`|
|`data.date.format`|Формат, в котором записаны данные типа `Date`|Строка форматирования, например: `%Y-%m-%d`|
|`csv_delimiter`|Разделитель данных в формате `csv_with_names`|Любой символ (UTF-8)|
