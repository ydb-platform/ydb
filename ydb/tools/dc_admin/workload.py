"""Run the YDB CLI TPC-C workload independently of admin HTTP requests."""

import json
import os
import shlex
import signal
import subprocess
import tempfile
import threading
import time
from collections import deque

_WAREHOUSES = 100
_ROOT = f"dc-admin-tpcc-{_WAREHOUSES}"
_DATA = _ROOT + "/data"
_TPCC = ("workload", "tpcc", "--path", _DATA)


class Workload:
    def __init__(self, cli):
        self._cli = list(cli)
        self._lock = threading.RLock()
        self._phase = "idle"
        self._message = "Нагрузка не запущена."
        self._log = deque(maxlen=80)
        self._started_at = None
        self._worker = None
        self._process = None
        self._closed = False

    def status(self):
        with self._lock:
            return dict(
                phase=self._phase,
                message=self._message,
                log=list(self._log),
                startedAt=self._started_at,
                warehouses=_WAREHOUSES,
            )

    def start(self):
        with self._lock:
            if self._closed or self._phase in ("preparing", "running", "stopping"):
                raise RuntimeError("Нагрузка уже выполняется или останавливается.")
            self._started_at = int(time.time())
            self._phase = "preparing"
            self._message = "Проверка данных TPC-C."
            self._log.clear()
            self._worker = threading.Thread(target=self._run, name="tpcc")
            self._worker.start()
            return self.status()

    def stop(self):
        with self._lock:
            if self._phase == "stopping":
                return self.status()
            if self._phase not in ("preparing", "running"):
                raise RuntimeError("Нет запущенной нагрузки.")
            self._phase = "stopping"
            self._message = "Остановка TPC-C."
            if self._process is not None:
                self._signal(self._process, signal.SIGTERM)
                timer = threading.Timer(10, self._signal, args=(self._process, signal.SIGKILL))
                timer.daemon = True
                timer.start()
            return self.status()

    def _signal(self, process, sig):
        with self._lock:
            if self._process is process:
                try:
                    os.killpg(process.pid, sig)
                except ProcessLookupError:
                    pass

    def close(self):
        with self._lock:
            self._closed = True
            if self._phase in ("preparing", "running"):
                self.stop()
            worker = self._worker
        if worker is not None:
            worker.join()

    def _command(self, args, message, stdout=None, running=False):
        with self._lock:
            if self._phase == "stopping":
                raise InterruptedError()
            self._message = message
            self._log.append("$ " + shlex.join([*self._cli, *args]))
            process = subprocess.Popen(
                [*self._cli, *args],
                stdin=subprocess.DEVNULL,
                stdout=stdout if stdout is not None else subprocess.PIPE,
                stderr=subprocess.PIPE if stdout is not None else subprocess.STDOUT,
                encoding="utf-8",
                errors="replace",
                start_new_session=True,
            )
            self._process = process
            if running:
                self._phase = "running"

        last_line = ""
        try:
            with process.stderr if stdout is not None else process.stdout as stream:
                for line in iter(lambda: stream.readline(1024), ""):
                    line = line.rstrip()
                    if line and line != "WM_TPCC_OK" and not line.endswith(" NOTICE: WM_TPCC_OK"):
                        last_line = line
                        with self._lock:
                            self._log.append(line)
            code = process.wait()
            if code:
                raise RuntimeError(last_line or f"ydb завершился с кодом {code}.")
        finally:
            with self._lock:
                if process.poll() is None:
                    self._signal(process, signal.SIGKILL)
                self._process = None
            process.wait()

    def _prepare(self):
        # MakeDirectory succeeds for existing directories; listing keeps ownership
        # and readiness in the same database as the benchmark across server restarts.
        self._command(["scheme", "mkdir", _ROOT], "Проверка каталога TPC-C.")
        with tempfile.TemporaryFile() as listing:
            self._command(["scheme", "ls", "--format", "json", _ROOT], "Проверка таблиц TPC-C.", stdout=listing)
            listing.seek(0)
            entries = {item["path"]: item["type"] for item in json.loads(listing.read(65536))}
        if entries and (
            entries.get("managed_v1") != "dir"
            or any(key not in ("managed_v1", "ready_v1", "data") or kind != "dir" for key, kind in entries.items())
        ):
            raise RuntimeError(f"В каталоге {_ROOT} уже есть посторонние объекты.")
        if "ready_v1" in entries:
            if "data" not in entries:
                raise RuntimeError(f"Отсутствует подготовленный набор {_DATA}.")
            return
        if "managed_v1" not in entries:
            self._command(["scheme", "mkdir", _ROOT + "/managed_v1"], "Подготовка нового набора.")
        elif "data" in entries:
            # Only clean an interrupted private import, never a published dataset.
            self._command([*_TPCC, "clean"], "Повторная подготовка незавершённого набора.")
        self._command([*_TPCC, "init", "-w", str(_WAREHOUSES)], "Создание таблиц TPC-C.")
        self._command(
            [*_TPCC, "import", "-w", str(_WAREHOUSES), "--threads", "2", "--no-tui"],
            f"Загрузка данных для {_WAREHOUSES} складов.",
        )
        self._command(["scheme", "mkdir", _ROOT + "/ready_v1"], "Сохранение готовности набора.")

    def _run(self):
        try:
            self._prepare()
            self._command(
                [
                    *_TPCC,
                    "run",
                    "-w",
                    str(_WAREHOUSES),
                    "--time",
                    "30m",
                    "--warmup",
                    "5s",
                    "--threads",
                    "2",
                    "--no-tui",
                ],
                "Выполняются транзакции TPC-C.",
                running=True,
            )
            phase, message = "finished", "Нагрузка завершена."
        except (OSError, ValueError, KeyError, TypeError, RuntimeError) as error:
            phase, message = "failed", str(error)
        with self._lock:
            if self._phase == "stopping":
                phase, message = "stopped", "Нагрузка остановлена."
            self._phase = phase
            self._message = message
