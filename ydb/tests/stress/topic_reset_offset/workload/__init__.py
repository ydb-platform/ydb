# -*- coding: utf-8 -*-
"""ResetOffset stress workload.

1. Create a topic with N partitions and preload each with mixed-size messages
   (small / medium / large). Each partition is paced on its own schedule so
   write timestamps — and therefore FROM_WRITTEN_AT offsets — differ across
   partitions. Preload writes enough data that Head compactifies (8 MiB).
2. For `--duration` seconds rewind consumers independently: each consumer has
   its own reset loop with a random start delay and pause, so rewinds are not
   aligned. Kinds: earliest, latest, a timestamp inside the written range, and
   timestamps before/after that range. Only that consumer's readers pause around
   its reset; writers pause only for latest/ts_after. After every successful CLI
   call, describe_consumer must show the expected committed_offset.
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
# PQ compactifies Head into a body blob at this size (THead / MAX_BLOB_SIZE).
MAX_PQ_BLOB_SIZE = 8 * MEGABYTE
RESET_KINDS = ("earliest", "latest", "ts_before", "ts_in_range", "ts_after")
END_OFFSET = object()


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
        self.writer_pause = threading.Event()
        self.writer_lock = threading.Lock()
        self.writer_pause_gens = 0
        self.writer_pause_mu = threading.Lock()
        self.reader_pause = {name: threading.Event() for name in self.consumers}
        self.consumer_locks = {name: threading.Lock() for name in self.consumers}
        self.started_at = 0.0
        self.first_write_ts = 0.0
        self.last_write_ts = 0.0
        self.partition_first_ts = [0.0] * self.partitions
        self.partition_last_ts = [0.0] * self.partitions
        self.writes = {partition_id: [] for partition_id in range(self.partitions)}
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
        self.writer_pause.clear()
        for pause in self.reader_pause.values():
            pause.clear()
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

    def _large_indices(self):
        n = self.messages_per_partition
        if n < 2:
            return {0} if n else set()
        if self.large_message_bytes >= 4 * MEGABYTE:
            # One 8+ MiB message already compactifies into several PQ blobs.
            return {n // 2}
        # Small "large" messages (sanitizer): several of them to exceed 8 MiB.
        return {max(0, n // 4), n // 2, min(n - 1, (3 * n) // 4)}

    def _message_size(self, index):
        if index in self._large_indices():
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

    def _wait_while_paused(self, pause):
        while pause.is_set() and not self.stop.is_set():
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
        wave_bytes = 0
        pace = max(3, self.messages_per_partition // 5)
        stride = max(1, pace // max(self.partitions, 1))
        gap_base = 1 + partition_id * stride
        try:
            # Start partitions at different wall times so one FROM_WRITTEN_AT
            # timestamp maps to different offsets.
            time.sleep(0.2 * partition_id)
            writer = self.driver.topic_client.writer(
                self.topic_name,
                producer_id=producer_id,
                partition_id=partition_id,
                codec=ydb.TopicCodec.RAW,
            )
            for index in range(self.messages_per_partition):
                size = self._message_size(index)
                # Sleep on a per-partition schedule, and after enough data that
                # Head is likely to compactify into another body blob.
                need_gap = index >= gap_base and (index - gap_base) % pace == 0
                if index > 0 and (need_gap or wave_bytes >= MAX_PQ_BLOB_SIZE):
                    time.sleep(2)
                    wave_bytes = 0
                timeout = 120 if size >= MEGABYTE else 10
                t_before = time.time()
                ack = writer.write_with_ack(
                    ydb.TopicWriterMessage(self._payload(size, f"{partition_id}-{index}")),
                    timeout=timeout,
                )
                t_after = time.time()
                if first_ts == 0.0:
                    first_ts = t_before
                last_ts = t_after
                offset = getattr(ack, "offset", None)
                if offset is None:
                    offset = index
                # Server FROM_WRITTEN_AT uses WriteTimestamp, which sits between
                # send and ack. Record both so expected committed is a range.
                self.writes[partition_id].append((offset, t_before, t_after))
                wave_bytes += size
                written += 1
                self._inc("written")
                self._inc("preloaded")
        finally:
            self._close_quietly(writer, flush=True, timeout=30)
        self.partition_first_ts[partition_id] = first_ts
        self.partition_last_ts[partition_id] = last_ts
        logger.info(
            "preloaded partition %s messages=%s first_ts=%.3f last_ts=%.3f",
            partition_id, written, first_ts, last_ts,
        )
        return written

    def _assert_preload_visible(self):
        # Do not use store_size_bytes as a blob-count proxy: UserDataSize() is 0
        # until CompactionBlobEncoder has more than one body blob (~16 MiB).
        # Sanitizer preload writes ~9 MiB, so that metric stays 0 after compactify.
        deadline = time.time() + 15
        problems = ["describe not received"]
        while time.time() < deadline:
            desc = self.driver.topic_client.describe_topic(self.topic_name, include_stats=True)
            problems = []
            for partition in desc.partitions:
                stats = partition.partition_stats
                if stats is None:
                    problems.append(f"partition={partition.partition_id} has no stats")
                    continue
                logger.info(
                    "partition %s store_size=%s start=%s end=%s",
                    partition.partition_id, stats.store_size_bytes, stats.partition_start, stats.partition_end,
                )
                if stats.partition_end - stats.partition_start < 2:
                    problems.append(
                        f"partition={partition.partition_id} has fewer than 2 messages "
                        f"start={stats.partition_start} end={stats.partition_end}"
                    )
            if not problems:
                return
            time.sleep(0.5)
        raise AssertionError(
            "preload is not visible in describe_topic: " + "; ".join(problems)
        )

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
        self._assert_preload_visible()

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
                    self._wait_while_paused(self.writer_pause)
                    if self.stop.is_set():
                        return
                    with self.writer_lock:
                        if self.writer_pause.is_set() or self.stop.is_set():
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
        pause = self.reader_pause[consumer]
        consumer_lock = self.consumer_locks[consumer]
        while not self.stop.is_set():
            reader = None
            try:
                reader = self.driver.topic_client.reader(self.topic_name, consumer=consumer)
                while not self.stop.is_set():
                    self._wait_while_paused(pause)
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
                    with consumer_lock:
                        if pause.is_set():
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
        lo = min(firsts) if firsts else self.first_write_ts
        hi = max(lasts) if lasts else self.last_write_ts
        if hi <= lo:
            hi = self.last_write_ts if self.last_write_ts > lo else time.time()
        return lo, hi

    def _is_write_gap(self, prev_after, cur_before):
        # Preload sleeps 2s between waves. Anything this long is a burst boundary
        # the tablet will not Kafka-batch across.
        return cur_before - prev_after >= 1.0

    def _expected_range_for_ts(self, partition_id, t_sec):
        # Client t_after is after ack, so slightly after WriteTimestamp.
        # Compactified PQ blobs and Kafka batches are atomic, so the tablet may
        # skip to a later blob start. Lower bound is the start of the write burst
        # that first may match; upper bound is the last preloaded offset.
        writes = self.writes[partition_id]
        lo_i = None
        for i, (_, _, t_after) in enumerate(writes):
            if t_after >= t_sec:
                lo_i = i
                break
        if lo_i is None:
            return END_OFFSET, END_OFFSET

        start_i = lo_i
        while start_i > 0 and not self._is_write_gap(writes[start_i - 1][2], writes[start_i][1]):
            start_i -= 1
        return writes[start_i][0], writes[-1][0]

    def _pick_random_timestamp(self):
        partition_id = random.randrange(self.partitions)
        lo = self.partition_first_ts[partition_id] or self.first_write_ts
        hi = (self.partition_last_ts[partition_id] or self.last_write_ts) + random.uniform(2, 8)
        if hi <= lo:
            hi = lo + 5
        writes = self.writes[partition_id]
        gap_windows = []
        for i in range(1, len(writes)):
            prev_after = writes[i - 1][2]
            cur_before = writes[i][1]
            if self._is_write_gap(prev_after, cur_before) and cur_before - prev_after > 1.2:
                gap_windows.append((prev_after + 0.2, cur_before - 0.2))
        if gap_windows and random.random() < 0.7:
            gap_lo, gap_hi = random.choice(gap_windows)
            epoch = random.uniform(gap_lo, gap_hi) if gap_hi > gap_lo else (gap_lo + gap_hi) / 2
        else:
            epoch = random.uniform(lo, hi)
        t_sec = int(epoch)
        expected = {
            pid: self._expected_range_for_ts(pid, t_sec)
            for pid in range(self.partitions)
        }

        def _fmt(value):
            lo_off, hi_off = value
            if lo_off is END_OFFSET:
                return "end"
            if hi_off is END_OFFSET:
                return f"[{lo_off},end]"
            return f"[{lo_off},{hi_off}]"

        logger.info(
            "FROM_WRITTEN_AT partition=%s range=[%.3f, %.3f] t_sec=%s expected=%s",
            partition_id, lo, hi, t_sec,
            {pid: _fmt(value) for pid, value in expected.items()},
        )
        return str(t_sec), float(t_sec), expected

    def _position_for(self, kind):
        empty = {}
        if kind == "earliest":
            return "earliest", None, empty
        if kind == "latest":
            return "latest", None, empty
        lo, hi = self._preload_time_range()
        if kind == "ts_before":
            epoch = lo - 3600
            return self._format_ts(epoch), epoch, empty
        if kind == "ts_after":
            epoch = hi + 3600
            return self._format_ts(epoch), epoch, empty
        return self._pick_random_timestamp()

    def _expected_committed(self, kind, partition_id, start, end, expected_map):
        if kind in ("earliest", "ts_before"):
            return start, start
        if kind in ("latest", "ts_after"):
            return end, end
        expected = expected_map.get(partition_id, (END_OFFSET, END_OFFSET))
        lo, hi = expected if isinstance(expected, tuple) else (expected, expected)
        if lo is END_OFFSET or lo is None:
            writes = self.writes[partition_id]
            if not writes:
                return end, end
            # T is after the last preloaded message. Live writes after preload
            # may still match, so accept anything from the first live offset to end.
            return min(writes[-1][0] + 1, end), end
        if hi is END_OFFSET or hi is None:
            hi = end
        return lo, hi

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

    def _offset_mismatches(self, kind, states, expected_map):
        mismatches = []
        for partition_id in range(self.partitions):
            if partition_id not in states:
                mismatches.append(f"partition={partition_id} missing from describe")
                continue
            start, end, committed = states[partition_id]
            lo, hi = self._expected_committed(kind, partition_id, start, end, expected_map)
            if not (lo <= committed <= hi):
                mismatches.append(
                    f"partition={partition_id} committed={committed} expected=[{lo},{hi}] "
                    f"start={start} end={end}"
                )
        return mismatches

    def _wait_committed(self, kind, consumer, target_epoch, position, expected_map):
        deadline = time.time() + 15
        last_states = {}
        mismatches = ["describe not received"]
        while time.time() < deadline and not self.stop.is_set():
            last_states = self._consumer_offsets(consumer)
            mismatches = self._offset_mismatches(kind, last_states, expected_map)
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

    def _pause_writers(self):
        with self.writer_pause_mu:
            self.writer_pause_gens += 1
            self.writer_pause.set()
        self.writer_lock.acquire()

    def _unpause_writers(self):
        self.writer_lock.release()
        with self.writer_pause_mu:
            self.writer_pause_gens -= 1
            if self.writer_pause_gens == 0:
                self.writer_pause.clear()

    def _reset_and_verify(self, kind, consumer):
        position, epoch, expected = self._position_for(kind)
        pause_writers = kind in ("latest", "ts_after", "ts_in_range")
        reader_pause = self.reader_pause[consumer]
        reader_pause.set()
        if pause_writers:
            self._pause_writers()
        try:
            with self.consumer_locks[consumer]:
                if self.stop.is_set():
                    return
                if not self._run_reset_cli(kind, consumer, position):
                    return
                if not self._wait_committed(kind, consumer, epoch, position, expected):
                    return
            self._inc("reset_ok", kind)
        except Exception as exc:
            if self.stop.is_set():
                return
            self._inc("reset_fail", kind)
            self._add_error(f"reset-{kind}-{consumer}", exc)
        finally:
            if pause_writers:
                self._unpause_writers()
            reader_pause.clear()

    def _reset_loop(self, consumer):
        kinds = list(RESET_KINDS)
        random.shuffle(kinds)
        try:
            if self.stop.wait(timeout=random.uniform(0.1, 1.5)):
                return
            i = 0
            while not self.stop.wait(timeout=random.uniform(0.3, 1.2)):
                kind = kinds[i % len(kinds)]
                i += 1
                self._reset_and_verify(kind, consumer)
        except Exception as exc:
            if not self.stop.is_set():
                self._add_error(f"reset-loop-{consumer}", exc)

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
        for consumer in self.consumers:
            threads.append(threading.Thread(
                target=self._reset_loop,
                args=(consumer,),
                name=f"reset-{consumer}",
                daemon=True,
            ))

        for thread in threads:
            thread.start()

        deadline = time.time() + self.duration
        while time.time() < deadline and not self.stop.is_set():
            time.sleep(min(5, deadline - time.time()))
            self._log_stats()

        self.stop.set()
        self.writer_pause.clear()
        for pause in self.reader_pause.values():
            pause.clear()
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
