"""
Module for collecting and publishing YDB stress test metrics.

Provides:
- ErrorEvent: class for representing events (success/error)
- MetricsPublisher: class for publishing metrics (file/server)
- MetricsCollector: class for aggregating metrics in memory
- Decorators: @report_*_exception for automatic tracking
"""

import functools
import os
import sys
import time
import traceback
import json
import threading
from typing import Any, Callable

import requests


__event_process_mode = os.getenv('YDB_STRESS_UTIL_EVENT_PROCESS_MODE', None)


def set_event_process_mode(mode):
    """
    Sets the event processing mode.

    Args:
        mode: 'send', 'save', 'both' or None
    """
    global __event_process_mode
    __event_process_mode = mode


class ErrorEvent:
    """
    Represents an event (success or error) in a stress test.

    Attributes:
        kind: Event type ('query', 'init', 'work', 'teardown', 'verify')
        type: 'success' or error name ('PreconditionFailed', etc.)
        stress_util_name: Workload name (e.g., 'streaming.workload')
        operation: Operation name (optional, for query metrics)
    """
    kind: str = None
    type: str = None
    stress_util_name: str = None
    operation: str = None


class MetricsPublisher:
    """
    Class for publishing metrics to file and/or server.

    Supports three modes:
    - 'send': send metrics to server
    - 'save': save metrics to file
    - 'both': both send and save
    """

    def __init__(self, mode: str = None,
                 file_path: str = "error_events.json",
                 server_url: str = "http://localhost:3124/write"):
        """
        Initialize publisher.

        Args:
            mode: Operating mode ('send', 'save', 'both' or None)
            file_path: Path to file for saving metrics
            server_url: Server URL for sending metrics
        """
        self.mode = mode
        self.file_path = file_path
        self.server_url = server_url
        self.save_lock = threading.Lock()

    def publish(self, event: ErrorEvent):
        """
        Publishes event according to configured mode.

        Args:
            event: Event to publish
        """
        if self.mode in ['send', 'both']:
            try:
                self._send_to_server(event)
            except Exception:
                print(f"Error: Could not send event: {traceback.format_exc()}",
                      file=sys.stderr)

        if self.mode in ['save', 'both']:
            try:
                self._save_to_file(event)
            except Exception:
                print(f"Error: Could not save event: {traceback.format_exc()}",
                      file=sys.stderr)

    def _save_to_file(self, event: ErrorEvent):
        """Saves event to file."""
        event_data = {
            "timestamp": time.time(),
            "kind": event.kind,
            "type": event.type,
            "stress_util_name": event.stress_util_name,
        }

        if hasattr(event, 'operation') and event.operation:
            event_data['operation'] = event.operation

        with self.save_lock:
            with open(self.file_path, "a") as f:
                json.dump(event_data, f)
                f.write('\n')

    def _send_to_server(self, event: ErrorEvent):
        """Sends event to metrics server."""
        labels = {
            "sensor": "test_metric",
            "name": "stress_util_error",
            "stress_util": event.stress_util_name,
            "type": event.type,
            "kind": event.kind,
        }

        if hasattr(event, 'operation') and event.operation:
            labels['operation'] = event.operation

        payload = {
            "metrics": [
                {
                    "labels": labels,
                    "value": 0 if event.type == 'success' else 1
                }
            ]
        }

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


_global_publisher = MetricsPublisher(__event_process_mode)


def get_metrics_publisher() -> MetricsPublisher:
    """Returns global metrics publisher."""
    return _global_publisher


class MetricsCollector:
    """
    Collector for aggregating metrics in memory and publishing them.

    Collects statistics for all operations and can publish
    events through MetricsPublisher.
    """

    def __init__(self, publisher: MetricsPublisher = None):
        """
        Initialize collector.

        Args:
            publisher: Publisher for publishing events (optional)
        """
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
        """
        Wrapper for instrumenting call with automatic metrics collection.

        Args:
            method: Method to call
            operation_name: Operation name for metrics

        Returns:
            Method execution result
        """
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
        """
        Records query execution result.

        Args:
            operation: Operation name (e.g., 'create_table', 'insert_data')
            success: True if operation succeeded, False if error
            error_type: Error type (e.g., 'PreconditionFailed')
            stress_util_name: Workload name
        """
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
    """
    Standalone decorator class for tracking function success/errors.
    Automatically publishes events through MetricsPublisher.
    Can be used directly to decorate functions or methods.
    """

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
