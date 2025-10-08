# -*- coding: utf-8 -*-
import time
import threading
import queue
import traceback
import requests
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor

class Workload:
    def __init__(self, mon_endpoint, database, duration):
        self.mon_endpoint = mon_endpoint
        self.database = database
        self.round_size = 1000
        self.duration = duration
        self.delayed_events = queue.Queue()
        self.pool_semaphore = threading.BoundedSemaphore(value=100)
        self.worker_exception = []
        self.pool = None

    def wrapper(self, f, *args, **kwargs):
        try:
            if len(self.worker_exception) == 0:
                result = f(*args, **kwargs)
                if result.status_code != 200:
                    raise AssertionError(f"Bad response code {result.status_code}, text: {result.text}")
        except Exception:
            with self.lock:
                self.worker_exception.append(traceback.format_exc())
        finally:
            self.pool_semaphore.release()

    def call_viewer_api_post(self, url, body=None, headers=None):
        if body is None:
            body = {}
        if self.database:
            body["database"] = self.database
        return requests.post(self.mon_endpoint + url, json=body, headers=headers)

    def loop(self):
        started_at = time.time()

        while time.time() - started_at < self.duration:
            yield self.call_viewer_api_post, "/viewer/capabilities"
            yield self.call_viewer_api_post, "/viewer/whoami"
            yield self.call_viewer_api_post, "/viewer/nodelist"
            yield self.call_viewer_api_post, "/viewer/nodes?fields_required=all"
            yield self.call_viewer_api_post, "/viewer/groups?fields_required=all"
            if self.database:
                yield self.call_viewer_api_post, "/viewer/describe", {"path": self.database}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.pool:
            self.pool.shutdown(wait=True)
            self.pool = None

    def start(self):
        self.pool = ThreadPoolExecutor(max_workers=100)
        for call in self.loop():
            if len(self.worker_exception) == 0:
                self.pool_semaphore.acquire()
                func, *args = call
                self.pool.submit(self.wrapper, func, *args)
            else:
                raise AssertionError(f"Worker exceptions {self.worker_exception}")
        self.pool.shutdown(wait=True)
        self.pool = None
