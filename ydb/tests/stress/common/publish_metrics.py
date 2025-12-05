
import functools
import os
import sys
import time
import traceback
import json
import threading

import requests

__event_process_mode = os.getenv('YDB_STRESS_UTIL_EVENT_PROCESS_MODE', None)
__save_lock = threading.Lock()


def set_event_process_mode(mode):
    global __event_process_mode
    __event_process_mode = mode


class ErrorEvent:
    kind: str = None
    type: str = None
    stress_util_name: str = None
    stress_util_function_name: str = None


def process_error_event(event: ErrorEvent):
    if __event_process_mode in ['send', 'both']:
        try:
            send_error_event(event)
        except Exception:
            print(f"Error: Could not send error event: {traceback.format_exc()}", file=sys.stderr)
    if __event_process_mode in ['save', 'both']:
        try:
            save_error_event(event)
        except Exception:
            print(f"Error: Could not save error event: {traceback.format_exc()}", file=sys.stderr)


def save_error_event(event: ErrorEvent):
    event_data = {
        "timestamp": time.time(),
        "kind": event.kind,
        "type": event.type,
        "stress_util_name": event.stress_util_name,
        "stress_util_function_name": event.stress_util_function_name,
    }

    with __save_lock:
        with open("error_events.json", "a") as f:
            json.dump(event_data, f)
            f.write('\n')


def send_error_event(event: ErrorEvent):
    target_url = "http://localhost:3124/write"

    payload = {
        "metrics": [
            {
                "labels": {
                    "sensor": "test_metric",
                    "name": 'stress_util_error',
                    "stress_util": event.stress_util_name,
                    "stress_util_function": event.stress_util_function_name,
                    "type": event.type,
                    "kind": event.kind,
                },
                "value": 1 if event.kind != 'success' else 0
            }
        ]
    }
    headers = {'Content-Type': 'application/json'}
    try:
        response = requests.post(target_url, json=payload, headers=headers, timeout=5)
        response.raise_for_status()
        # print(f"Request successful. {response.status_code}")
    except requests.exceptions.ConnectionError:
        print(f"Error: Could not connect to {target_url}. Is the server running?", file=sys.stderr)
    except requests.exceptions.Timeout:
        print(f"Error: Request to {target_url} timed out.", file=sys.stderr)
    except requests.exceptions.HTTPError as err:
        print(f"HTTP Error: {err}", file=sys.stderr)
    except requests.exceptions.RequestException as err:
        print(f"An error occurred: {err}", file=sys.stderr)


class assert_exception(object):
    def __init__(self, func, event_kind='general'):
        self.kind = event_kind
        self.func = func
        functools.update_wrapper(self, func)

    def __set_name__(self, owner, name):
        self.full_name = f"{owner.__module__}"
        self.func_name = f"{owner.__qualname__}.{name}"

    def __call__(self, *args, **kwargs):
        try:
            res = self.func(*args, **kwargs)

            evnt = ErrorEvent()
            evnt.stress_util_name = self.full_name
            evnt.stress_util_function_name = self.func_name
            evnt.type = 'success'
            evnt.kind = self.kind
            process_error_event(evnt)

            return res
        except Exception as e:
            evnt = ErrorEvent()
            evnt.stress_util_name = self.full_name
            evnt.stress_util_function_name = self.func_name
            evnt.type = e.__class__.__name__
            evnt.kind = self.kind
            process_error_event(evnt)

            raise

    def __get__(self, obj, objtype):
        '''Support instance methods.'''
        if obj is None:
            return self
        return functools.partial(self.__call__, obj)


class report_init_exception(assert_exception):
    def __init__(self, func):
        super().__init__(func, 'init')

    def __call__(self, *args, **kwargs):
        return super().__call__(*args, **kwargs)


class report_work_exception(assert_exception):
    def __init__(self, func):
        super().__init__(func, 'work')

    def __call__(self, *args, **kwargs):
        return super().__call__(*args, **kwargs)


class report_teardown_exception(assert_exception):
    def __init__(self, func):
        super().__init__(func, 'teardown')

    def __call__(self, *args, **kwargs):
        return super().__call__(*args, **kwargs)


class report_verify_exception(assert_exception):
    def __init__(self, func):
        super().__init__(func, 'verify')

    def __call__(self, *args, **kwargs):
        return super().__call__(*args, **kwargs)
