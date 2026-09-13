# -*- coding: utf-8 -*-
"""ResetOffset stress workload.

1. Create a topic with N partitions and preload each with mixed-size messages
   (small / medium / large including a 10 MiB message). Writes are paced so
   write timestamps span several seconds (FROM_WRITTEN_AT truncates to seconds).
2. For `--duration` seconds (default 30) rewind the consumer in a loop:
   earliest, latest, a timestamp inside the written range, and timestamps before
   and after that range. Readers and live writers pause around each reset.
   After every successful CLI call, describe_consumer must show the expected
   committed_offset on every partition.
3. A light reader and optional live small-message writer run in parallel so
   session drops and unpersisted NewHead are exercised.

TODO: rewrite onto Python SDK TopicClient.reset_offset when it exists. Drop the
bundled ydb CLI after that.
"""
import concurrent.futures
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

logger = logging.getLogger("YdbTopicResetOffsetWorkload")

SMALL_SIZE = 256
MEDIUM_SIZE = 64 * 1024
MEGABYTE = 1024 * 1024
RESET_KINDS = ("earliest", "latest", "ts_before", "ts_in_range", "ts_after")


class Workload:
    def __init__(
        self,
        endpoint,
        database,
        duration,
        writers=1,
        consumers=2,
        readers_per_consumer=1,
        partitions=10,
        messages_per_partition=1000,
        large_message_bytes=10 * MEGABYTE,
    ):
        self.endpoint = endpoint
        self.database = database
        self.duration = int(duration)
        self.live_writers = writers
        self.consumers = [f"consumer-{i}" for i in range(consumers)]
        self.readers_per_consumer = readers_per_consumer
        self.partitions = int(partitions)
        self.messages_per_partition = int(messages_per_partition)
        self.large_message_bytes = int(large_message_bytes)
        self.topic_name = f"reset_offset_{uuid.uuid1()}".replace("-", "_")
        self.driver = ydb.Driver(ydb.DriverConfig(endpoint, database))
        self.stop = threading.Event()
        self.pause = threading.Event()
        self.quiesce_lock = threading.Lock()
        self.started_at = 0.0
        self.first_write_ts = 0.0
        self.last_write_ts = 0.0
        self.partition_first_ts = [0.0] * self.partitions
        self.partition_last_ts = [0.0] * self.partitions
        self.gaps = {partition_id: [] for partition_id in range(self.partitions)}
        self._ts_expected = {}
        self.errors = []
        self.errors_lock = threading.Lock()
        self.stats_lock = threading.Lock()
        self.stats = {
            "written": 0,
            "preloaded": 0,
            "read_commits": 0,
            "reset_ok": {kind: 0 for kind in RESET_KINDS},
            "reset_fail": {kind: 0 for kind in RESET_KINDS},
            "reader_reconnects": 0,
        }
        self._large_payload = bytes(max(self.large_message_bytes, 1))
        self._unpack_resource("ydb_cli")

    def __enter__(self):
        self.driver.wait(timeout=60)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop.set()
        self.pause.clear()
        try:
            self.driver.topic_client.drop_topic(self.topic_name)
        except Exception as e:
            logger.warning("drop_topic failed: %s", e)
        self.driver.stop()
        self.tempdir.cleanup()

    def _unpack_resource(self, name):
        self.tempdir = tempfile.TemporaryDirectory(dir=os.getcwd())
        self.working_dir = os.path.join(self.tempdir.name, "topic_reset_offset_ydb_cli")
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
        # TSchemeShard::MaxPQWriteSpeedPerPartition is 50 MiB/s.
        speed = 50 * MEGABYTE
        self.driver.topic_client.create_topic(
            self.topic_name,
            min_active_partitions=self.partitions,
            max_active_partitions=self.partitions,
            consumers=list(self.consumers),
            partition_write_speed_bytes_per_second=speed,
            partition_write_burst_bytes=max(speed, 2 * self.large_message_bytes),
        )

    def _message_size(self, index):
        if self.messages_per_partition >= 2 and index == self.messages_per_partition // 2:
            return self.large_message_bytes
        if index > 0 and index % 100 == 0:
            return min(MEGABYTE, self.large_message_bytes)
        if index % 20 == 0:
            return min(MEDIUM_SIZE, self.large_message_bytes)
        return SMALL_SIZE

    def _payload(self, size, seq):
        if size >= MEDIUM_SIZE:
            return self._large_payload[:size]
        return f"{seq}".encode().ljust(size, b"x")

    def _wait_while_paused(self):
        while self.pause.is_set() and not self.stop.is_set():
            time.sleep(0.05)

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

    def _preload_partition(self, partition_id):
        producer_id = f"preload-{partition_id}"
        writer = None
        written = 0
        first_ts = last_ts = 0.0
        try:
            writer = self.driver.topic_client.writer(
                self.topic_name,
                producer_id=producer_id,
                partition_id=partition_id,
                codec=ydb.TopicCodec.RAW,
            )
            pace = max(1, self.messages_per_partition // 5)
            for index in range(self.messages_per_partition):
                # Sleep before a small message so FROM_WRITTEN_AT lands on a
                # batched blob, not a 1 MiB/10 MiB single-message blob.
                if index > 1 and index % pace == 1:
                    gap_start = time.time()
                    time.sleep(2)
                    self.gaps[partition_id].append((index, gap_start, time.time()))
                size = self._message_size(index)
                timeout = 120 if size >= MEGABYTE else 10
                ack = writer.write_with_ack(
                    ydb.TopicWriterMessage(self._payload(size, f"{partition_id}-{index}")),
                    timeout=timeout,
                )
                last_ts = time.time()
                if first_ts == 0.0:
                    first_ts = last_ts
                offset = getattr(ack, "offset", None)
                if offset is None:
                    offset = index
                if self.gaps[partition_id] and self.gaps[partition_id][-1][0] == index:
                    # Remember the real offset of the first message after the gap.
                    gap = self.gaps[partition_id][-1]
                    self.gaps[partition_id][-1] = (offset, gap[1], gap[2])
                written += 1
                self._inc("written")
                self._inc("preloaded")
        finally:
            self._close_quietly(writer, flush=True, timeout=30)
        self.partition_first_ts[partition_id] = first_ts
        self.partition_last_ts[partition_id] = last_ts
        logger.info("preloaded partition %s messages=%s", partition_id, written)
        return written

    def _preload(self):
        self.first_write_ts = time.time()
        logger.info(
            "preload start partitions=%s messages_per_partition=%s large=%s",
            self.partitions,
            self.messages_per_partition,
            self.large_message_bytes,
        )
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.partitions) as pool:
            futures = [pool.submit(self._preload_partition, partition_id) for partition_id in range(self.partitions)]
            for future in concurrent.futures.as_completed(futures):
                future.result()
        self.last_write_ts = time.time()
        logger.info(
            "preload done written=%s first_ts=%s last_ts=%s",
            self.stats["preloaded"],
            self.first_write_ts,
            self.last_write_ts,
        )

    def _write_loop(self, writer_id):
        producer_id = f"live-{writer_id}"
        seqno = 0
        while not self.stop.is_set():
            writer = None
            try:
                writer = self.driver.topic_client.writer(
                    self.topic_name,
                    producer_id=producer_id,
                    codec=ydb.TopicCodec.RAW,
                )
                while not self.stop.is_set():
                    self._wait_while_paused()
                    if self.stop.is_set():
                        return
                    with self.quiesce_lock:
                        if self.pause.is_set() or self.stop.is_set():
                            continue
                        seqno += 1
                        writer.write_with_ack(
                            ydb.TopicWriterMessage(f"live-{writer_id}-{seqno}"),
                            timeout=5,
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
                    self._wait_while_paused()
                    if self.stop.is_set():
                        return
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
                    with self.quiesce_lock:
                        if self.pause.is_set():
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

    def _format_ts(self, epoch):
        return str(int(epoch))

    def _preload_time_range(self):
        firsts = [ts for ts in self.partition_first_ts if ts]
        lasts = [ts for ts in self.partition_last_ts if ts]
        lo = max(firsts) if firsts else self.first_write_ts
        hi = min(lasts) if lasts else self.last_write_ts
        if hi <= lo:
            hi = self.last_write_ts if self.last_write_ts > lo else time.time()
        return lo, hi

    def _pick_gap_position(self):
        self._ts_expected = {}
        sample = self.gaps.get(0, [])
        if not sample:
            return None
        # Prefer earlier gaps: they sit before the 10 MiB message and compact less.
        for offset, _gap_start, _gap_end in sample:
            starts = []
            ends = []
            expected = {}
            ok = True
            for partition_id in range(self.partitions):
                found = None
                for gap_offset, gap_start, gap_end in self.gaps[partition_id]:
                    if gap_offset == offset:
                        found = (gap_offset, gap_start, gap_end)
                        break
                if found is None:
                    ok = False
                    break
                starts.append(found[1])
                ends.append(found[2])
                expected[partition_id] = found[0]
            if not ok:
                continue
            gap_lo = max(starts)
            gap_hi = min(ends)
            t_sec = int(gap_lo) + 1
            if t_sec > gap_hi:
                continue
            self._ts_expected = expected
            logger.info("FROM_WRITTEN_AT gap offset=%s t_sec=%s", offset, t_sec)
            return str(t_sec), float(t_sec)
        return None

    def _position_for(self, kind):
        self._ts_expected = {}
        if kind == "earliest":
            return "earliest", None
        if kind == "latest":
            return "latest", None
        lo, hi = self._preload_time_range()
        if kind == "ts_before":
            epoch = lo - 3600
            return self._format_ts(epoch), epoch
        if kind == "ts_after":
            epoch = hi + 3600
            return self._format_ts(epoch), epoch
        gap = self._pick_gap_position()
        if gap is None:
            raise AssertionError("no WriteTimestamp gap with a whole interior second")
        return gap

    def _expected_committed(self, kind, partition_id, start, end, _target_epoch):
        if kind in ("earliest", "ts_before"):
            return start, start
        if kind in ("latest", "ts_after"):
            return end, end
        expected = self._ts_expected.get(partition_id)
        if expected is None:
            return start, end
        return expected, expected

    def _consumer_offsets(self, consumer):
        desc = self.driver.topic_client.describe_consumer(
            self.topic_name, consumer, include_stats=True,
        )
        states = {}
        for partition in desc.partitions:
            if partition.partition_stats is None or partition.partition_consumer_stats is None:
                continue
            states[partition.partition_id] = (
                partition.partition_stats.partition_start,
                partition.partition_stats.partition_end,
                partition.partition_consumer_stats.committed_offset,
            )
        return states

    def _offset_mismatches(self, kind, states, target_epoch):
        mismatches = []
        for partition_id in range(self.partitions):
            if partition_id not in states:
                mismatches.append(f"partition={partition_id} missing from describe")
                continue
            start, end, committed = states[partition_id]
            lo, hi = self._expected_committed(kind, partition_id, start, end, target_epoch)
            if not (lo <= committed <= hi):
                mismatches.append(
                    f"partition={partition_id} committed={committed} expected=[{lo},{hi}] "
                    f"start={start} end={end}"
                )
        return mismatches

    def _wait_committed(self, kind, consumer, target_epoch, position):
        deadline = time.time() + 15
        last_states = {}
        mismatches = ["describe not received"]
        while time.time() < deadline and not self.stop.is_set():
            last_states = self._consumer_offsets(consumer)
            mismatches = self._offset_mismatches(kind, last_states, target_epoch)
            if not mismatches:
                logger.info(
                    "reset verified kind=%s consumer=%s position=%s partitions=%s",
                    kind, consumer, position, last_states,
                )
                return True
            time.sleep(0.2)
        if self.stop.is_set():
            return False
        raise AssertionError(
            f"reset did not apply kind={kind} consumer={consumer} position={position}: "
            + "; ".join(mismatches)
            + f" states={last_states}"
        )

    def _run_reset_cli(self, kind, consumer, position):
        # TODO: call Python SDK reset_offset here instead of spawning ydb CLI.
        cmd = [
            self.cli_path,
            "--endpoint", self.endpoint,
            "--database", self.database,
            "topic", "consumer", "offset", "reset",
            "--consumer", consumer,
            "--position", position,
            self.topic_name,
        ]
        proc = subprocess.Popen(cmd, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        deadline = time.time() + 60
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
                    return False
                self._inc("reset_fail", kind)
                logger.info("reset %s consumer=%s position=%s timed out", kind, consumer, position)
                return False
            try:
                stdout, stderr = proc.communicate(timeout=min(0.2, remaining))
                break
            except subprocess.TimeoutExpired:
                continue
        if proc.returncode == 0:
            return True
        self._inc("reset_fail", kind)
        logger.info(
            "reset %s consumer=%s position=%s failed rc=%s stderr=%s",
            kind, consumer, position, proc.returncode, (stderr or stdout or "")[-500:],
        )
        return False

    def _reset_and_verify(self, kind, consumer):
        position, epoch = self._position_for(kind)
        self.pause.set()
        try:
            with self.quiesce_lock:
                if self.stop.is_set():
                    return
                if not self._run_reset_cli(kind, consumer, position):
                    return
                if not self._wait_committed(kind, consumer, epoch, position):
                    return
            self._inc("reset_ok", kind)
        except Exception as exc:
            if self.stop.is_set():
                return
            self._inc("reset_fail", kind)
            self._add_error(f"reset-{kind}-{consumer}", exc)
        finally:
            self.pause.clear()

    def _reset_loop(self):
        kinds = list(RESET_KINDS)
        i = 0
        try:
            while not self.stop.wait(timeout=random.uniform(0.2, 0.5)):
                kind = kinds[i % len(kinds)]
                i += 1
                consumer = random.choice(self.consumers)
                self._reset_and_verify(kind, consumer)
        except Exception as exc:
            if not self.stop.is_set():
                self._add_error("reset-loop", exc)

    def _log_stats(self):
        with self.stats_lock:
            logger.info("stats %s", dict(self.stats))

    def loop(self):
        self._create_topic()
        self._preload()
        self.started_at = time.time()
        threads = []
        for i in range(self.live_writers):
            threads.append(threading.Thread(target=self._write_loop, args=(i,), name=f"write-{i}", daemon=True))
        for consumer in self.consumers:
            for session_id in range(self.readers_per_consumer):
                threads.append(threading.Thread(
                    target=self._read_loop,
                    args=(consumer, session_id),
                    name=f"read-{consumer}-{session_id}",
                    daemon=True,
                ))
        threads.append(threading.Thread(target=self._reset_loop, name="reset", daemon=True))

        for thread in threads:
            thread.start()

        deadline = time.time() + self.duration
        while time.time() < deadline and not self.stop.is_set():
            time.sleep(min(5, deadline - time.time()))
            self._log_stats()

        self.stop.set()
        self.pause.clear()
        for thread in threads:
            thread.join(timeout=5)
        stuck = [thread.name for thread in threads if thread.is_alive()]
        if stuck:
            logger.warning("daemon threads still running after stop: %s", stuck)
        self._log_stats()

        with self.stats_lock:
            stats = {
                "written": self.stats["written"],
                "preloaded": self.stats["preloaded"],
                "read_commits": self.stats["read_commits"],
                "reset_ok": dict(self.stats["reset_ok"]),
                "reset_fail": dict(self.stats["reset_fail"]),
                "reader_reconnects": self.stats["reader_reconnects"],
            }

        expected_preloaded = self.partitions * self.messages_per_partition
        problems = []
        if self.errors:
            problems.append("worker failures:\n" + "\n\n".join(self.errors))
        if stats["preloaded"] < expected_preloaded:
            problems.append(f"preload incomplete: {stats['preloaded']} < {expected_preloaded}")
        if self.readers_per_consumer > 0 and stats["read_commits"] == 0:
            problems.append("no messages were read/committed after reset")
        for kind in RESET_KINDS:
            if stats["reset_ok"][kind] == 0:
                problems.append(f"no successful reset to {kind}")
        if problems:
            raise AssertionError("\n".join(problems) + f"\nstats={stats}")
        logger.info("workload finished stats=%s", stats)
