"""Persistent history of admin operations and their local cancellation handles."""

from contextlib import contextmanager
import os
import signal
import sqlite3
import subprocess
import threading
import time


class Journal:
    def __init__(self, path):
        self.lock = threading.RLock()
        self.active = {}
        self.db = sqlite3.connect(str(path), isolation_level=None, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.db.execute("""CREATE TABLE IF NOT EXISTS commands (
            id INTEGER PRIMARY KEY, command TEXT, started INTEGER, state TEXT, output TEXT, exitCode INTEGER)""")
        self.db.execute(
            """UPDATE commands SET state='interrupted', output='Сервер перезапущен. Итог операции неизвестен.'
                           WHERE state IN ('running','cancelling')"""
        )

    def history(self, before):
        with self.lock:
            rows = self.db.execute(
                "SELECT * FROM commands WHERE id < ? ORDER BY id DESC LIMIT 51", (before,)
            ).fetchall()
            return {"commands": [dict(row) for row in reversed(rows[:50])], "older": len(rows) > 50}

    def progress(self, event, output):
        with self.lock:
            ident = next(i for i, active in self.active.items() if active is event)
            self.db.execute("UPDATE commands SET output=? WHERE id=?", (output, ident))

    def reject(self, command, reason):
        with self.lock:
            self.db.execute(
                "INSERT INTO commands(command,started,state,output) VALUES(?,?,'rejected',?)",
                (command, int(time.time()), reason),
            )

    def cancel(self, ident):
        with self.lock:
            event = self.active.get(ident)
            if event is None:
                return False
            self.db.execute("UPDATE commands SET state='cancelling' WHERE id=?", (ident,))
            event.set()
            return True

    @contextmanager
    def operation(self, command):
        event = threading.Event()
        with self.lock:
            ident = self.db.execute(
                "INSERT INTO commands(command,started,state,output) VALUES(?,?,'running','')",
                (command, int(time.time())),
            ).lastrowid
            self.active[ident] = event
        result = {"command": command, "exitCode": 0, "output": ""}
        try:
            yield event, result
        except Exception as error:
            result.update(exitCode=1, output="\n".join(filter(None, [result["output"], str(error)])))
            raise
        finally:
            with self.lock:
                cancelled = event.is_set() and result["exitCode"] != 0
                state = "cancelled" if cancelled else "failed" if result["exitCode"] else "done"
                self.db.execute(
                    "UPDATE commands SET state=?, output=?, exitCode=? WHERE id=?",
                    (state, result["output"], result["exitCode"], ident),
                )
                del self.active[ident]


def run_command(argv, cancel):
    with subprocess.Popen(
        argv,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        encoding="utf-8",
        errors="replace",
        start_new_session=True,
    ) as proc:
        deadline = time.monotonic() + 25 * 60
        while True:
            try:
                output, _ = proc.communicate(timeout=0.25)
                return subprocess.CompletedProcess(argv, proc.returncode, output)
            except subprocess.TimeoutExpired:
                if not cancel.is_set() and time.monotonic() < deadline:
                    continue
                try:
                    os.killpg(proc.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                output, _ = proc.communicate()
                reason = "Ожидание отменено." if cancel.is_set() else "Истекло время ожидания."
                return subprocess.CompletedProcess(
                    argv, 1, output + "\n" + reason + " Итог операции в кластере нужно проверить."
                )
