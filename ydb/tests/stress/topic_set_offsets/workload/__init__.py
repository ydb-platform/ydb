# -*- coding: utf-8 -*-
"""Parallel write / read-commit / SetOffsets stress workload.

TODO: rewrite this test onto the Python SDK TopicClient.set_offsets (or
equivalent) as soon as the SDK exposes SetOffsets. Drop the bundled ydb CLI,
ydb_cli resource, subprocess set_offsets sessions, and CLI position parsing. Drive
earliest / latest / FROM_WRITTEN_AT from the same Python process as writers
and readers.
"""
import concurrent.futures
import datetime
import logging
import os
import random
import stat
import subprocess
import tempfile
import threading
import time
import traceback
import uuid

from library.python import resource
import ydb

logger = logging.getLogger("YdbTopicSetOffsetsWorkload")


class Workload:
    def __init__(self, endpoint, database, duration, writers=4, consumers=3, readers_per_consumer=2):
        self.endpoint = endpoint
        self.database = database
        self.duration = int(duration)
        self.writers = writers
        self.consumers = [f"consumer-{i}" for i in range(consumers)]
        self.readers_per_consumer = readers_per_consumer
        self.topic_name = f"set_offsets_{uuid.uuid1()}".replace("-", "_")
        self.driver = ydb.Driver(ydb.DriverConfig(endpoint, database))
        self.stop = threading.Event()
        self.started_at = 0.0
        self.errors = []
        self.errors_lock = threading.Lock()
        self.stats_lock = threading.Lock()
        self.stats = {
            "written": 0,
            "read_commits": 0,
            "set_ok": {"earliest": 0, "latest": 0, "timestamp": 0},
            "set_fail": {"earliest": 0, "latest": 0, "timestamp": 0},
            "reader_reconnects": 0,
        }
        self._unpack_resource("ydb_cli")

    def __enter__(self):
        self.driver.wait(timeout=60)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop.set()
        try:
            self.driver.topic_client.drop_topic(self.topic_name)
        except Exception as e:
            logger.warning("drop_topic failed: %s", e)
        self.driver.stop()
        self.tempdir.cleanup()

    def _unpack_resource(self, name):
        self.tempdir = tempfile.TemporaryDirectory(dir=os.getcwd())
        self.working_dir = os.path.join(self.tempdir.name, "topic_set_offsets_ydb_cli")
        os.makedirs(self.working_dir, exist_ok=True)
        path_to_unpack = os.path.join(self.working_dir, name)
        with open(path_to_unpack, "wb") as f:
            f.write(resource.find(name))
        st = os.stat(path_to_unpack)
        os.chmod(path_to_unpack, st.st_mode | stat.S_IEXEC)
        self.cli_path = path_to_unpack

    def _inc(self, *keys, amount=1):
        with self.stats_lock:
            cur = self.stats
            for key in keys[:-1]:
                cur = cur[key]
            cur[keys[-1]] += amount

    def _add_error(self, where, exc):
        text = f"{where}: {exc}\n{traceback.format_exc()}"
        logger.exception("%s", where)
        with self.errors_lock:
            self.errors.append(text)

    def _create_topic(self):
        self.driver.topic_client.create_topic(
            self.topic_name,
            min_active_partitions=4,
            consumers=list(self.consumers),
        )

    def _close_quietly(self, session, **close_kwargs):
        if session is None:
            return

        def close():
            try:
                session.close(**close_kwargs)
            except Exception:
                pass

        closer = threading.Thread(target=close, name="session-close", daemon=True)
        closer.start()
        closer.join(timeout=2)

    def _write_loop(self, writer_id):
        producer_id = f"producer-{writer_id}"
        seqno = 0
        while not self.stop.is_set():
            writer = None
            try:
                writer = self.driver.topic_client.writer(self.topic_name, producer_id=producer_id)
                while not self.stop.is_set():
                    seqno += 1
                    writer.write(
                        ydb.TopicWriterMessage(f"w{writer_id}-{seqno}"),
                        timeout=2,
                    )
                    self._inc("written")
            except Exception as exc:
                if self.stop.is_set():
                    return
                logger.info("writer-%s reconnect after %s", writer_id, exc)
                time.sleep(0.1)
            finally:
                self._close_quietly(writer, flush=False, timeout=1)

    def _read_loop(self, consumer, session_id):
        name = f"reader-{consumer}-{session_id}"
        while not self.stop.is_set():
            reader = None
            try:
                reader = self.driver.topic_client.reader(self.topic_name, consumer=consumer)
                while not self.stop.is_set():
                    try:
                        reader.async_wait_message().result(timeout=0.2)
                    except (TimeoutError, concurrent.futures.TimeoutError):
                        continue
                    except ydb.TopicReaderPartitionExpiredError:
                        self._inc("reader_reconnects")
                        break
                    try:
                        batch = reader.receive_batch(max_messages=32, timeout=0)
                    except TimeoutError:
                        continue
                    except ydb.TopicReaderPartitionExpiredError:
                        self._inc("reader_reconnects")
                        break
                    if batch is None:
                        continue
                    try:
                        reader.commit(batch)
                    except ydb.TopicReaderPartitionExpiredError:
                        self._inc("reader_reconnects")
                        break
                    self._inc("read_commits")
            except ydb.TopicReaderPartitionExpiredError:
                if self.stop.is_set():
                    return
                self._inc("reader_reconnects")
                time.sleep(0.1)
            except Exception as exc:
                if self.stop.is_set():
                    return
                logger.info("%s reconnect after %s", name, exc)
                self._inc("reader_reconnects")
                time.sleep(0.1)
            finally:
                self._close_quietly(reader, flush=False, timeout=1)

    def _timestamp_position(self):
        now = time.time()
        ts = random.uniform(self.started_at, now if now > self.started_at else now + 1)
        return datetime.datetime.fromtimestamp(ts, datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    def _set_once(self, kind, consumer):
        # TODO: call Python SDK set_offsets here instead of spawning ydb CLI.
        position = self._timestamp_position() if kind == "timestamp" else kind
        cmd = [
            self.cli_path,
            "--endpoint", self.endpoint,
            "--database", self.database,
            "topic", "consumer", "offset", "set",
            "--consumer", consumer,
            "--position", position,
            self.topic_name,
        ]
        proc = subprocess.Popen(cmd, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        deadline = time.time() + 30
        stdout = stderr = ""
        while True:
            remaining = deadline - time.time()
            if remaining <= 0 or self.stop.is_set():
                proc.kill()
                try:
                    proc.communicate(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
                if self.stop.is_set():
                    return
                self._inc("set_fail", kind)
                logger.info("set_offsets %s consumer=%s position=%s timed out", kind, consumer, position)
                return
            try:
                stdout, stderr = proc.communicate(timeout=min(0.2, remaining))
                break
            except subprocess.TimeoutExpired:
                continue
        if proc.returncode == 0:
            self._inc("set_ok", kind)
            return
        self._inc("set_fail", kind)
        logger.info(
            "set_offsets %s consumer=%s position=%s failed rc=%s stderr=%s",
            kind, consumer, position, proc.returncode, (stderr or stdout or "")[-500:],
        )

    def _set_loop(self, kind):
        try:
            while not self.stop.wait(timeout=random.uniform(0.3, 1.0)):
                consumer = random.choice(self.consumers)
                self._set_once(kind, consumer)
        except Exception as exc:
            if not self.stop.is_set():
                self._add_error(f"set_offsets-{kind}", exc)

    def _log_stats(self):
        with self.stats_lock:
            logger.info("stats %s", dict(self.stats))

    def loop(self):
        self._create_topic()
        self.started_at = time.time()
        threads = []
        for i in range(self.writers):
            threads.append(threading.Thread(target=self._write_loop, args=(i,), name=f"write-{i}", daemon=True))
        for consumer in self.consumers:
            for session_id in range(self.readers_per_consumer):
                threads.append(threading.Thread(
                    target=self._read_loop,
                    args=(consumer, session_id),
                    name=f"read-{consumer}-{session_id}",
                    daemon=True,
                ))
        for kind in ("earliest", "latest", "timestamp"):
            threads.append(threading.Thread(target=self._set_loop, args=(kind,), name=f"set_offsets-{kind}", daemon=True))

        for thread in threads:
            thread.start()

        deadline = time.time() + self.duration
        while time.time() < deadline and not self.stop.is_set():
            time.sleep(min(5, deadline - time.time()))
            self._log_stats()

        self.stop.set()
        for thread in threads:
            thread.join(timeout=5)
        stuck = [thread.name for thread in threads if thread.is_alive()]
        if stuck:
            logger.warning("daemon threads still running after stop: %s", stuck)
        self._log_stats()

        with self.stats_lock:
            stats = {
                "written": self.stats["written"],
                "read_commits": self.stats["read_commits"],
                "set_ok": dict(self.stats["set_ok"]),
                "set_fail": dict(self.stats["set_fail"]),
                "reader_reconnects": self.stats["reader_reconnects"],
            }

        problems = []
        if self.errors:
            problems.append("worker failures:\n" + "\n\n".join(self.errors))
        if stats["written"] == 0:
            problems.append("no messages were written")
        if stats["read_commits"] == 0:
            problems.append("no messages were read/committed")
        for kind in ("earliest", "latest", "timestamp"):
            if stats["set_ok"][kind] == 0:
                problems.append(f"no successful set_offsets to {kind}")
        if problems:
            raise AssertionError("\n".join(problems) + f"\nstats={stats}")
        logger.info("workload finished stats=%s", stats)
