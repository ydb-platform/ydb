import logging
import subprocess
import time
from typing import List, Optional

from .hang_monitor import TransactionHangMonitor, TransactionHungError


class CommandExecutor:
    """Выполняет команды и логирует их с мониторингом зависших транзакций."""

    def __init__(self, hang_monitor: Optional[TransactionHangMonitor] = None):
        """
        Args:
            hang_monitor: монитор для детекции зависаний. Если None, создастся дефолтный.
        """
        self.logger = logging.getLogger(__name__)
        self.hang_monitor = hang_monitor or TransactionHangMonitor()

    def set_monitor(self, hang_timeout: int, window_interval: int) -> None:
        """Создает новый монитор с заданными параметрами.

        Args:
            hang_timeout: таймаут зависания в секундах
            window_interval: интервал между строками статистики в секундах
        """
        self.hang_monitor = TransactionHangMonitor(hang_timeout, window_interval)

    def run(self, cmd: List[str]) -> None:
        """Выполняет команду без мониторинга зависших транзакций.
        
        Используется для коротких операций (init, clean).
        
        Args:
            cmd: список аргументов команды
        """
        self.logger.debug(f"Running cmd {cmd}")
        print(f"Running cmd {cmd} at {time.time()}")
        subprocess.run(cmd, check=True, text=True)
        print(f"End at {time.time()}")
    
    def run_with_monitoring(self, cmd: List[str]) -> None:
        """Выполняет команду с мониторингом зависших транзакций.
        
        Читает stdout построчно и передает каждую строку в hang_monitor.
        Если hang_detected=True, убивает процесс и выбрасывает TransactionHungError.
        
        Args:
            cmd: список аргументов команды
        
        Raises:
            subprocess.CalledProcessError: если команда вернула non-zero exit code
            TransactionHungError: если транзакция зависла
        """
        self.logger.debug(f"Begin cmd with monitoring {cmd}")
        
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True
        )
        
        self.hang_monitor.reset()
        
        try:
            for line in process.stdout:
                self.logger.debug(f"{line}")
                
                if self.hang_monitor.check_line(line):
                    self.logger.error(
                        f"Transaction hung for {self.hang_monitor.hang_timeout} seconds"
                    )
                    process.kill()
                    process.wait()
                    raise TransactionHungError(
                        f"Transaction hung for {self.hang_monitor.hang_timeout} seconds",
                        timeout_seconds=self.hang_monitor.hang_timeout
                    )
            self.logger.debug("end of stdout")
            
            process.wait()
            
            if process.returncode != 0:
                self.logger.error(f"Command failed with exit code {process.returncode}")
                raise subprocess.CalledProcessError(
                    process.returncode,
                    cmd,
                    f"Command failed with exit code {process.returncode}"
                )
            
        except subprocess.CalledProcessError as e:
            self.logger.error(f"exception CalledProcessError: {e}")
            raise
        except Exception as e:
            self.logger.error(f"exception Exception: {e}")
            process.kill()
            process.wait()
            raise
        finally:
            self.logger.debug(f"End cmd {cmd}")
