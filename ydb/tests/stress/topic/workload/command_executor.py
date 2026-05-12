import logging
import subprocess
from typing import List, Optional

from .hang_monitor import TransactionHangMonitor, TransactionHangError


class CommandExecutor:
    # Выполняет команды и мониторит остановились операции с топиком или нет.

    def __init__(self, hang_monitor: Optional[TransactionHangMonitor] = None):
        self.logger = logging.getLogger(__name__)
        self.hang_monitor = hang_monitor or TransactionHangMonitor()

    def set_monitor(self, hang_timeout: int, window_interval: int) -> None:
        self.hang_monitor = TransactionHangMonitor(hang_timeout, window_interval)

    def run(self, cmd: List[str]) -> None:
        self.logger.error(f"Begin cmd {cmd}")
        subprocess.run(cmd, check=True, text=True)
        self.logger.error(f"End cmd {cmd}")

    def run_with_monitoring(self, cmd: List[str]) -> None:
        self.logger.error(f"Begin cmd {cmd}")

        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True
        )

        self.hang_monitor.reset()

        try:
            for line in process.stdout:
                prefix = cmd[19] if len(cmd) >= 20 else ""

                self.logger.error(f"{prefix}: {line}")

                if self.hang_monitor.check_line(line):
                    self.logger.error(
                        f"Transaction hang for {self.hang_monitor.hang_timeout} seconds"
                    )
                    process.kill()
                    process.wait()
                    raise TransactionHangError(
                        f"Transaction hang for {self.hang_monitor.hang_timeout} seconds",
                        timeout_seconds=self.hang_monitor.hang_timeout
                    )

            process.wait()

            if process.returncode != 0:
                self.logger.error(f"Command failed with exit code {process.returncode}")
                raise subprocess.CalledProcessError(
                    process.returncode,
                    cmd,
                    f"Command failed with exit code {process.returncode}"
                )

        except subprocess.CalledProcessError as e:
            self.logger.error(f"Exception 'CalledProcessError': {e}, cmd: {cmd}")
            raise
        except Exception as e:
            self.logger.error(f"Exception 'Exception': {e}, cmd: {cmd}")
            process.kill()
            process.wait()
            raise
        finally:
            self.logger.error(f"End cmd {cmd}")
