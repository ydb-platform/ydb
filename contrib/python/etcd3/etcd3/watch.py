import logging
import threading

import grpc

import six
from six.moves import queue

import etcd3.etcdrpc as etcdrpc
import etcd3.events as events
import etcd3.exceptions as exceptions
import etcd3.utils as utils


_log = logging.getLogger(__name__)


class Watch(object):

    def __init__(self, watch_id, iterator=None, etcd_client=None):
        self.watch_id = watch_id
        self.etcd_client = etcd_client
        self.iterator = iterator

    def cancel(self):
        self.etcd_client.cancel_watch(self.watch_id)

    def iterator(self):
        if self.iterator is not None:
            return self.iterator

        raise ValueError('Undefined iterator')


class Watcher(object):

    def __init__(self, watchstub, timeout=None, call_credentials=None,
                 metadata=None):
        self.timeout = timeout
        self._watch_stub = watchstub
        self._credentials = call_credentials
        self._metadata = metadata

        self._lock = threading.Lock()
        self._request_queue = queue.Queue(maxsize=10)
        self._callbacks = {}
        self._callback_thread = None
        self._new_watch_cond = threading.Condition(lock=self._lock)
        self._new_watch = None

    def _create_watch_request(self, key, range_end=None, start_revision=None,
                              progress_notify=False, filters=None,
                              prev_kv=False):
        create_watch = etcdrpc.WatchCreateRequest()
        create_watch.key = utils.to_bytes(key)
        if range_end is not None:
            create_watch.range_end = utils.to_bytes(range_end)
        if start_revision is not None:
            create_watch.start_revision = start_revision
        if progress_notify:
            create_watch.progress_notify = progress_notify
        if filters is not None:
            create_watch.filters = filters
        if prev_kv:
            create_watch.prev_kv = prev_kv
        return etcdrpc.WatchRequest(create_request=create_watch)

    def add_callback(self, key, callback, range_end=None, start_revision=None,
                     progress_notify=False, filters=None, prev_kv=False):
        rq = self._create_watch_request(key, range_end=range_end,
                                        start_revision=start_revision,
                                        progress_notify=progress_notify,
                                        filters=filters, prev_kv=prev_kv)

        with self._lock:
            # Start the callback thread if it is not yet running.
            if not self._callback_thread:
                thread_name = 'etcd3_watch_%x' % (id(self),)
                self._callback_thread = threading.Thread(name=thread_name,
                                                         target=self._run)
                self._callback_thread.daemon = True
                self._callback_thread.start()

            # Only one create watch request can be pending at a time, so if
            # there one already, then wait for it to complete first.
            while self._new_watch:
                self._new_watch_cond.wait()

            # Submit a create watch request.
            new_watch = _NewWatch(callback)
            self._request_queue.put(rq)
            self._new_watch = new_watch

            try:
                # Wait for the request to be completed, or timeout.
                self._new_watch_cond.wait(timeout=self.timeout)

                # If the request not completed yet, then raise a timeout
                # exception.
                if new_watch.id is None and new_watch.err is None:
                    raise exceptions.WatchTimedOut()

                # Raise an exception if the watch request failed.
                if new_watch.err:
                    raise new_watch.err
            finally:
                # Wake up threads stuck on add_callback call if any.
                self._new_watch = None
                self._new_watch_cond.notify_all()

            return new_watch.id

    def cancel(self, watch_id):
        with self._lock:
            callback = self._callbacks.pop(watch_id, None)
            if not callback:
                return

            self._cancel_no_lock(watch_id)

    def _run(self):
        while True:
            response_iter = self._watch_stub.Watch(
                _new_request_iter(self._request_queue),
                credentials=self._credentials,
                metadata=self._metadata)
            try:
                for rs in response_iter:
                    self._handle_response(rs)

            except grpc.RpcError as err:
                with self._lock:
                    if self._new_watch:
                        self._new_watch.err = err
                        self._new_watch_cond.notify_all()

                    callbacks = self._callbacks
                    self._callbacks = {}

                    # Rotate request queue. This way we can terminate one gRPC
                    # stream and initiate another one whilst avoiding a race
                    # between them over requests in the queue.
                    self._request_queue.put(None)
                    self._request_queue = queue.Queue(maxsize=10)

                for callback in six.itervalues(callbacks):
                    _safe_callback(callback, err)

    def _handle_response(self, rs):
        with self._lock:
            if rs.created:
                # If the new watch request has already expired then cancel the
                # created watch right away.
                if not self._new_watch:
                    self._cancel_no_lock(rs.watch_id)
                    return

                if rs.compact_revision != 0:
                    self._new_watch.err = exceptions.RevisionCompactedError(
                        rs.compact_revision)
                    return

                self._callbacks[rs.watch_id] = self._new_watch.callback
                self._new_watch.id = rs.watch_id
                self._new_watch_cond.notify_all()

            callback = self._callbacks.get(rs.watch_id)

        # Ignore leftovers from canceled watches.
        if not callback:
            return

        # The watcher can be safely reused, but adding a new event
        # to indicate that the revision is already compacted
        # requires api change which would break all users of this
        # module. So, raising an exception if a watcher is still
        # alive.
        if rs.compact_revision != 0:
            err = exceptions.RevisionCompactedError(rs.compact_revision)
            _safe_callback(callback, err)
            self.cancel(rs.watch_id)
            return

        # Call the callback even when there are no events in the watch
        # response so as not to ignore progress notify responses.
        if rs.events or not (rs.created or rs.canceled):
            new_events = [events.new_event(event) for event in rs.events]
            response = WatchResponse(rs.header, new_events)
            _safe_callback(callback, response)

    def _cancel_no_lock(self, watch_id):
        cancel_watch = etcdrpc.WatchCancelRequest()
        cancel_watch.watch_id = watch_id
        rq = etcdrpc.WatchRequest(cancel_request=cancel_watch)
        self._request_queue.put(rq)


class WatchResponse(object):

    def __init__(self, header, events):
        self.header = header
        self.events = events


class _NewWatch(object):
    def __init__(self, callback):
        self.callback = callback
        self.id = None
        self.err = None


def _new_request_iter(_request_queue):
    while True:
        rq = _request_queue.get()
        if rq is None:
            return

        yield rq


def _safe_callback(callback, response_or_err):
    try:
        callback(response_or_err)

    except Exception:
        _log.exception('Watch callback failed')
