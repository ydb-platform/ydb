from dataclasses import dataclass


@dataclass
class WorkloadConfig:
    """Конфигурация тестового workload-а для YDB Topics."""

    # Параметры статистики CLI
    STATS_WINDOW: int = 30  # Интервал между строками статистики (сек)

    # Таймауты и интервалы
    STATS_HANG_TIMEOUT: int = 180  # Таймаут зависания (сек)

    # Коммит транзакций
    TX_COMMIT_INTERVAL: str = "2000"  # Интервал коммита транзакций (все сценарии)

    # Скорость записи
    DEFAULT_BYTE_RATE: str = "100M"
    SMALL_BYTE_RATE: str = "10M"

    # Ограничения памяти
    MEMORY_LIMIT_PER_CONSUMER: str = "2M"
    MEMORY_LIMIT_PER_PRODUCER: str = "2M"

    # Ретеншн данных
    RETENTION: str = "1s"  # Меньше периода коммита (2 сек)

    # Период доступности консьюмеров (целочисленная арифметика)
    AVAILABILITY_PERIOD_NUMERATOR: int = 9  # Числитель для расчета периода
    AVAILABILITY_PERIOD_DENOMINATOR: int = 10  # Знаменатель для расчета периода (9/10 = 90%)

    # Параметры консьюмеров для config transactions
    CONFIG_CONSUMERS_COUNT: str = "500"

    # Параметры авто-партиционирования
    AUTO_PARTITIONING_WINDOW: str = "20"
    AUTO_PARTITIONING_UTILIZATION: str = "50"

    # Имена консьюмеров
    DATA_HOLDER_CONSUMER: str = "data_holder"


@dataclass
class TestConfig:
    """Конфигурация тестового сценария."""

    partitions: int
    partitions_per_tablet: int
    producers: int
    consumers: int
    consumer_threads: int
    byte_rate: str
