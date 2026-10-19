# Оптимальные настройки gRPC keep-alive для {{ ydb-short-name }}

## Проблема

Параметры gRPC keep-alive задают, как часто клиентский драйвер проверяет, что соединение с {{ ydb-short-name }} живо, и сколько ждёт ответ на эту проверку. Проверка нужна, чтобы заметить обрыв и не держать на сервере уже мёртвое соединение. Слишком частый ping сервер закрывает ошибкой `too_many_pings`.

### Основные проблемы

1. **Слишком частые пинги** (`too_many_pings`) — когда клиент отправляет keep-alive слишком часто и сервер закрывает соединение с ошибкой `GOAWAY received; Error code: 11; Debug Text: too_many_pings`.

2. **Долгая инициализация драйвера** — когда разрешение endpoint или настройки keep-alive затягивают подключение к {{ ydb-short-name }} до 10 секунд и дольше.

3. **Неоптимальная балансировка нагрузки** — когда настройки по умолчанию не учитывают работу сразу большого числа экземпляров драйвера.

4. **Проблемы с сетевыми таймаутами** — когда клиент работает в serverless-окружении или сеть нестабильна.

## Решение

Использование оптимальных настроек gRPC keep-alive обеспечивает стабильную работу и оптимальную балансировку нагрузки.
Настройки по умолчанию подходят большинству пользователей.

**Рекомендуемые настройки**:

| Параметр | Рекомендуемое значение | Описание |
|----------|------------------------|----------|
| Интервал keep-alive | 10 секунд | Периодичность отправки ping-запросов |
| Таймаут keep-alive | 10 секунд | Время ожидания ответа на ping |
| Разрешение ping без данных | true | Отправка ping при отсутствии трафика |

Примеры отличаются одной строкой: в плохом `grpc_keep_alive_timeout` равен 1 секунде, в хорошем используется значение по умолчанию — 10 секунд.

{% cut "Плохой пример" %}

```python
# Плохо: слишком агрессивные настройки keep-alive
import ydb
import os

def get_driver():
    driver_config = ydb.DriverConfig(
        endpoint=os.getenv("YDB_ENDPOINT"),
        database=os.getenv("YDB_DATABASE"),
        credentials=ydb.credentials_from_env_variables(),
        grpc_keep_alive_timeout=1000,  # Слишком маленькое значение - 1 секунда
    )
    driver = ydb.Driver(driver_config)
    try:
        driver.wait(timeout=5)
    except TimeoutError:
        raise RuntimeError("YDB connection timeout")
    return driver
```

{% endcut %}

{% cut "Хороший пример" %}

```python
# Хорошо: настройки keep-alive по умолчанию
import ydb
import os

def get_driver():
    driver_config = ydb.DriverConfig(
        endpoint=os.getenv("YDB_ENDPOINT"),
        database=os.getenv("YDB_DATABASE"),
        credentials=ydb.credentials_from_env_variables(),
        # grpc_keep_alive_timeout=10000, 10 секунд - значение по умолчанию
    )
    driver = ydb.Driver(driver_config)
    try:
        driver.wait(timeout=5)
    except TimeoutError:
        raise RuntimeError("YDB connection timeout")
    return driver
```

{% endcut %}

### Ключевые преимущества правильной настройки

- **Стабильные соединения** — избегаем ошибок `too_many_pings`
- **Быстрая инициализация** — драйвер быстро подключается к кластеру (обычно < 1 секунды)
- **Эффективное использование ресурсов** — оптимальная нагрузка на клиент и сервер
- **Устойчивость к сетевым проблемам** — корректная обработка временных сбоев

### Мониторинг и диагностика

Для контроля состояния соединений рекомендуется:

- Мониторинг метрик соединений в логах приложения
- Использование трассировки для анализа проблем с подключением
- Регулярное обновление SDK до актуальных версий
- Проверка сетевой доступности endpoint'ов {{ ydb-short-name }}

### Особые случаи

**Serverless-окружения**

В serverless-окружении увеличьте интервал.

```python
driver_config = ydb.DriverConfig(
    endpoint=os.getenv("YDB_ENDPOINT"),
    database=os.getenv("YDB_DATABASE"),
    credentials=ydb.credentials_from_env_variables(),
    grpc_keep_alive_timeout=20000,  # 20 секунд для снижения нагрузки
)
```

**Высоконагруженные системы**

При высокой нагрузке задайте более короткий интервал и таймаут.

```go
keepAliveParams := keepalive.ClientParameters{
    Time:                5 * time.Second,
    Timeout:             5 * time.Second,
    PermitWithoutStream: true,
}
```

> **Важно**: Если в вашем конкретном случае нужны другие настройки, обязательно тестируйте их в условиях, максимально приближенных к продакшену, чтобы найти оптимальные значения.
