"""Collecting and publishing YDB stress test metrics."""

import atexit
import time
import functools
import os
import sys
import traceback
import json
import threading
from typing import Any, Callable, Optional

import requests


__event_process_mode = os.getenv('YDB_STRESS_UTIL_EVENT_PROCESS_MODE', None)


def set_event_process_mode(mode):
    """Sets the event processing mode ('send', 'save', 'both' or None)."""
    global __event_process_mode
    __event_process_mode = mode


class ErrorEvent:
    """Represents a success or error event in a stress test."""
    kind: str = None
    type: str = None
    stress_util_name: str = None
    operation: str = None


class MetricsPublisher:
    """Publishes metrics to file and/or server.

    Events are buffered and flushed periodically using the timeseries field.
    Timestamps are rounded to _TS_GRID_SECONDS to match the server aggregation window.
    """

    _FLUSH_INTERVAL = 360  # seconds
    _TS_GRID_SECONDS = 15  # timestamp rounding grid (matches monium aggregation window)

    def __init__(self, mode: str = None,
                 file_path: str = "error_events.json",
                 server_url: str = "http://localhost:3124/write"):
        self.mode = mode
        self.file_path = file_path
        self.server_url = server_url
        self.save_lock = threading.Lock()

        # Buffer for accumulating events before sending to server.
        # Maps label-key -> {"labels": dict, "timeseries": {ts: count, ...}}
        # Events are aggregated at write time: each _buffer_event() call
        # increments the counter for the corresponding (label-key, ts) pair.
        self._buffer_lock = threading.Lock()
        self._buffer: dict[tuple, dict] = {}

        self._flush_timer: Optional[threading.Timer] = None
        self._stopped = False

        if self.mode in ['send', 'both']:
            self._schedule_flush()
            atexit.register(self.stop)

    def _schedule_flush(self):
        """Schedules the next periodic flush."""
        if self._stopped:
            return
        self._flush_timer = threading.Timer(self._FLUSH_INTERVAL, self._periodic_flush)
        self._flush_timer.daemon = True
        self._flush_timer.start()

    def _periodic_flush(self):
        """Called by the timer to flush buffered events and reschedule."""
        try:
            self.flush()
        except Exception:
            print(f"Error during periodic flush: {traceback.format_exc()}",
                  file=sys.stderr)
        finally:
            self._schedule_flush()

    def flush(self):
        """Flushes all buffered events to the metrics server."""
        with self._buffer_lock:
            if not self._buffer:
                return
            buffer_snapshot = self._buffer
            self._buffer = {}

        try:
            self._send_buffer_to_server(buffer_snapshot)
        except Exception:
            print(f"Error: Could not flush events: {traceback.format_exc()}",
                  file=sys.stderr)

    def stop(self):
        """Stops the flush timer and flushes remaining events."""
        self._stopped = True
        if self._flush_timer is not None:
            self._flush_timer.cancel()
            self._flush_timer = None
        self.flush()

    @staticmethod
    def _make_labels(event: ErrorEvent) -> dict:
        """Creates labels dict from an event."""
        labels = {
            "sensor": "test_metric",
            "name": "stress_util_error",
            "stress_util": event.stress_util_name,
            "type": event.type,
            "kind": event.kind,
        }

        if event.operation:
            labels['operation'] = event.operation

        return labels

    @staticmethod
    def _labels_key(labels: dict) -> tuple:
        """Creates a hashable key from labels dict for aggregation."""
        return tuple(sorted(labels.items()))

    def _buffer_event(self, event: ErrorEvent):
        """Adds an event to the buffer, aggregating by labels and rounded timestamp."""
        labels = self._make_labels(event)
        key = self._labels_key(labels)
        ts = int(time.time())
        ts = ts - (ts % self._TS_GRID_SECONDS)

        with self._buffer_lock:
            if key not in self._buffer:
                self._buffer[key] = {
                    "labels": labels,
                    "timeseries": {},
                }
            ts_map = self._buffer[key]["timeseries"]
            ts_map[ts] = ts_map.get(ts, 0) + 1

    def publish(self, event: ErrorEvent):
        """Publishes a single event (buffered for server, immediate for file)."""
        if self.mode in ['send', 'both']:
            try:
                self._buffer_event(event)
            except Exception:
                print(f"Error: Could not buffer event: {traceback.format_exc()}",
                      file=sys.stderr)

        if self.mode in ['save', 'both']:
            try:
                self._save_to_file(event)
            except Exception:
                print(f"Error: Could not save event: {traceback.format_exc()}",
                      file=sys.stderr)

    def publish_many(self, events: list[ErrorEvent]):
        """Publishes multiple events (buffered for server, immediate for file)."""
        if not events:
            return

        if self.mode in ['send', 'both']:
            try:
                for event in events:
                    self._buffer_event(event)
            except Exception:
                print(f"Error: Could not buffer events: {traceback.format_exc()}",
                      file=sys.stderr)

        if self.mode in ['save', 'both']:
            try:
                self._save_to_file_many(events)
            except Exception:
                print(f"Error: Could not save events: {traceback.format_exc()}",
                      file=sys.stderr)

    def _save_to_file(self, event: ErrorEvent):
        """Saves a single event to file."""
        event_data = {
            "timestamp": time.time(),
            "kind": event.kind,
            "type": event.type,
            "stress_util_name": event.stress_util_name,
        }

        if event.operation:
            event_data['operation'] = event.operation

        with self.save_lock:
            with open(self.file_path, "a") as f:
                json.dump(event_data, f)
                f.write('\n')

    def _send_buffer_to_server(self, buffer: dict[tuple, dict]):
        """Sends pre-aggregated buffered events to the metrics server."""
        metrics = []
        for metric_data in buffer.values():
            # timeseries is already a {ts: count} dict, aggregated at write time
            ts_map = metric_data["timeseries"]
            timeseries = [{"ts": ts, "value": count}
                          for ts, count in sorted(ts_map.items())]

            # Always use timeseries format to avoid overwriting values
            # with identical timestamps across consecutive flushes.
            # The timeseries field is mutually exclusive with ts/value.
            metrics.append({
                "labels": metric_data["labels"],
                "timeseries": timeseries,
            })

        payload = {"metrics": metrics}
        headers = {'Content-Type': 'application/json'}

        try:
            response = requests.post(self.server_url, json=payload,
                                     headers=headers, timeout=5)
            response.raise_for_status()
        except requests.exceptions.ConnectionError:
            print(f"Error: Could not connect to {self.server_url}. "
                  f"Is the server running?", file=sys.stderr)
        except requests.exceptions.Timeout:
            print(f"Error: Request to {self.server_url} timed out.",
                  file=sys.stderr)
        except requests.exceptions.HTTPError as err:
            print(f"HTTP Error: {err}", file=sys.stderr)
        except requests.exceptions.RequestException as err:
            print(f"An error occurred: {err}", file=sys.stderr)

    def _save_to_file_many(self, events: list[ErrorEvent]):
        """Saves multiple events to file."""
        with self.save_lock:
            with open(self.file_path, "a") as f:
                for event in events:
                    event_data = {
                        "timestamp": time.time(),
                        "kind": event.kind,
                        "type": event.type,
                        "stress_util_name": event.stress_util_name,
                    }

                    if event.operation:
                        event_data['operation'] = event.operation

                    json.dump(event_data, f)
                    f.write('\n')


_global_publisher = MetricsPublisher(__event_process_mode)


def get_metrics_publisher() -> MetricsPublisher:
    """Returns global metrics publisher."""
    return _global_publisher


class MetricsCollector:
    """Aggregates metrics in memory and publishes events through MetricsPublisher."""

    def __init__(self, publisher: MetricsPublisher = None):
        self.lock = threading.Lock()
        self.metrics = {
            'total_queries': 0,
            'successful_queries': 0,
            'failed_queries': 0,
            'by_operation': {},
            'by_error_type': {},
        }
        self.publisher = publisher or _global_publisher

    def wrap_call(self, method: Callable, operation_name: str, stress_util_name: str) -> Any:
        """Calls method and records success/failure metrics."""
        success = True
        error_type = None

        try:
            result = method()
            return result
        except Exception as e:
            success = False
            error_type = e.__class__.__name__
            raise
        finally:
            self.record_query(
                operation=operation_name,
                success=success,
                error_type=error_type,
                stress_util_name=stress_util_name,
            )

    def record_query(self, operation: str, success: bool, error_type: str = None,
                     stress_util_name: str = None):
        """Records a query result and publishes an event."""
        with self.lock:
            self.metrics['total_queries'] += 1

            if success:
                self.metrics['successful_queries'] += 1
            else:
                self.metrics['failed_queries'] += 1

            if operation not in self.metrics['by_operation']:
                self.metrics['by_operation'][operation] = {'success': 0, 'fail': 0}

            if success:
                self.metrics['by_operation'][operation]['success'] += 1
            else:
                self.metrics['by_operation'][operation]['fail'] += 1

            if error_type:
                self.metrics['by_error_type'][error_type] = \
                    self.metrics['by_error_type'].get(error_type, 0) + 1

        event = ErrorEvent()
        event.kind = 'query'
        event.type = 'success' if success else (error_type or 'unknown_error')
        event.stress_util_name = stress_util_name or operation
        event.operation = operation

        self.publisher.publish(event)

    def get_summary(self) -> str:
        """Returns formatted metrics summary."""
        with self.lock:
            total = self.metrics['total_queries']
            if total == 0:
                return "No queries executed"

            success_rate = (self.metrics['successful_queries'] / total) * 100

            lines = [
                f"Total queries: {total}",
                f"Success rate: {success_rate:.2f}%",
                f"Failed queries: {self.metrics['failed_queries']}",
            ]

            if self.metrics['by_operation']:
                lines.append("\nBy operation:")
                for op, stats in sorted(self.metrics['by_operation'].items()):
                    op_total = stats['success'] + stats['fail']
                    op_rate = (stats['success'] / op_total) * 100 if op_total > 0 else 0
                    lines.append(f"  {op}: {op_rate:.1f}% ({stats['success']}/{op_total})")

            if self.metrics['by_error_type']:
                lines.append("\nError types:")
                for err_type, count in sorted(
                        self.metrics['by_error_type'].items(),
                        key=lambda x: x[1], reverse=True):
                    lines.append(f"  {err_type}: {count}")

            return "\n".join(lines)


_global_metrics_collector = MetricsCollector()


def get_metrics_collector() -> MetricsCollector:
    """Returns global metrics collector."""
    return _global_metrics_collector


class report_exception(object):
    """Decorator that publishes success/error events for the wrapped function."""

    def __init__(self, func, event_kind='general', publisher: MetricsPublisher = None):
        self.kind = event_kind
        self.func = func
        self.publisher = publisher or _global_publisher
        functools.update_wrapper(self, func)

    def __set_name__(self, owner, name):
        self.full_name = f"{owner.__module__}"
        self.func_name = f"{owner.__qualname__}.{name}"

    def __call__(self, *args, **kwargs):
        try:
            res = self.func(*args, **kwargs)

            event = ErrorEvent()
            event.stress_util_name = self.full_name
            event.operation = self.func_name
            event.type = 'success'
            event.kind = self.kind
            self.publisher.publish(event)

            return res
        except Exception as e:
            event = ErrorEvent()
            event.stress_util_name = self.full_name
            event.operation = self.func_name
            event.type = e.__class__.__name__
            event.kind = self.kind
            self.publisher.publish(event)

            raise

    def __get__(self, obj, objtype):
        if obj is None:
            return self
        return functools.partial(self.__call__, obj)
