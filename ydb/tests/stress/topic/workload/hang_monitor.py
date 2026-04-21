import logging


class TransactionHangError(Exception):
    def __init__(self, message: str, timeout_seconds: int = 180):
        super().__init__(message)
        self.timeout_seconds = timeout_seconds


class TransactionHangMonitor:
    """Определяет завис тест или нет.

    Проверяет метрики из вывода `ydb workload topic run full`:
    - Write speed - msg/s
    - Read speed - msg/s

    Если одна из метрик равна 0 в течение заданного timeout, то считается что тест завис.
    """

    def __init__(self, hang_timeout: int = 180, window_interval: int = 5):
        """
            hang_timeout: время в секундах, после которого считаем что тест завис
            window_interval: интервал между строками статистики в секундах
        """
        self.hang_timeout = hang_timeout
        self.window_interval = window_interval
        self.required_zero_windows = hang_timeout // window_interval
        if self.required_zero_windows < 1:
            self.required_zero_windows = 1
        self.consecutive_zero_windows = 0
        self.logger = logging.getLogger(__name__)

    def reset(self) -> None:
        self.consecutive_zero_windows = 0

    def check_line(self, line: str) -> bool:
        """Проверяет строку лога. Возвращает True если долго не было записи в топик или чтения из топика.

        Формат строки с транзакциями (пример 11+ колонок):
        "1       1000    95      93              1               2988    284     100000          0               0               50"
        Колонки:
        - 0: Window #
        - 1: Write speed - msg/s
        - 5: Read speed - msg/s

        Формат строки без транзакций (пример <11 колонок):
        "1       998     95      41              0               699             100351          4365    416     100000"
        Колонки:
        - 0: Window #
        - 1: Write speed - msg/s
        - 7: Read speed - msg/s
        """
        parts = line.split()

        if len(parts) < 8 or not parts[0].isdigit():
            return False

        try:
            write_msg_s = int(parts[1])

            if len(parts) >= 11:
                read_msg_s = int(parts[5])
            else:
                read_msg_s = int(parts[7])

            hang_detected = (write_msg_s == 0) or (read_msg_s == 0)

            if hang_detected:
                self.consecutive_zero_windows += 1
                self.logger.error(
                    f"Hang window detected: zero windows={self.consecutive_zero_windows}, "
                    f"required={self.required_zero_windows}, write={write_msg_s}, read={read_msg_s}"
                )
            else:
                if self.consecutive_zero_windows > 0:
                    self.logger.error(f"Hang window cleared: zero windows={self.consecutive_zero_windows}")
                self.consecutive_zero_windows = 0

            return self.consecutive_zero_windows >= self.required_zero_windows

        except (ValueError, IndexError) as e:
            self.logger.error(f"Failed to parse line: {line}, error: {e}")
            return False
