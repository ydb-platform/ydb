#  Licensed to Elasticsearch B.V. under one or more contributor
#  license agreements. See the NOTICE file distributed with
#  this work for additional information regarding copyright
#  ownership. Elasticsearch B.V. licenses this file to you under
#  the Apache License, Version 2.0 (the "License"); you may
#  not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
# 	http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

import logging
import time
from operator import methodcaller

from ..compat import Mapping, Queue, map, string_types
from ..exceptions import NotFoundError, TransportError
from .errors import BulkIndexError, ScanError

logger = logging.getLogger("elasticsearch.helpers")


def expand_action(data):
    """
    From one document or action definition passed in by the user extract the
    action/data lines needed for elasticsearch's
    :meth:`~elasticsearch.Elasticsearch.bulk` api.
    """
    # when given a string, assume user wants to index raw json
    if isinstance(data, string_types):
        return '{"index":{}}', data

    # make sure we don't alter the action
    data = data.copy()
    op_type = data.pop("_op_type", "index")
    action = {op_type: {}}

    # If '_source' is a dict use it for source
    # otherwise if op_type == 'update' then
    # '_source' should be in the metadata.
    if (
        op_type == "update"
        and "_source" in data
        and not isinstance(data["_source"], Mapping)
    ):
        action[op_type]["_source"] = data.pop("_source")

    for key in (
        "_id",
        "_index",
        "_if_seq_no",
        "_if_primary_term",
        "_parent",
        "_percolate",
        "_retry_on_conflict",
        "_routing",
        "_timestamp",
        "_type",
        "_version",
        "_version_type",
        "if_seq_no",
        "if_primary_term",
        "parent",
        "pipeline",
        "retry_on_conflict",
        "routing",
        "version",
        "version_type",
    ):
        if key in data:
            if key in {
                "_if_seq_no",
                "_if_primary_term",
                "_parent",
                "_retry_on_conflict",
                "_routing",
                "_version",
                "_version_type",
            }:
                action[op_type][key[1:]] = data.pop(key)
            else:
                action[op_type][key] = data.pop(key)

    # no data payload for delete
    if op_type == "delete":
        return action, None

    return action, data.get("_source", data)


class _ActionChunker:
    def __init__(self, chunk_size, max_chunk_bytes, serializer):
        self.chunk_size = chunk_size
        self.max_chunk_bytes = max_chunk_bytes
        self.serializer = serializer

        self.size = 0
        self.action_count = 0
        self.bulk_actions = []
        self.bulk_data = []

    def feed(self, action, data):
        ret = None
        raw_data, raw_action = data, action
        action = self.serializer.dumps(action)
        # +1 to account for the trailing new line character
        cur_size = len(action.encode("utf-8")) + 1

        if data is not None:
            data = self.serializer.dumps(data)
            cur_size += len(data.encode("utf-8")) + 1

        # full chunk, send it and start a new one
        if self.bulk_actions and (
            self.size + cur_size > self.max_chunk_bytes
            or self.action_count == self.chunk_size
        ):
            ret = (self.bulk_data, self.bulk_actions)
            self.bulk_actions, self.bulk_data = [], []
            self.size, self.action_count = 0, 0

        self.bulk_actions.append(action)
        if data is not None:
            self.bulk_actions.append(data)
            self.bulk_data.append((raw_action, raw_data))
        else:
            self.bulk_data.append((raw_action,))

        self.size += cur_size
        self.action_count += 1
        return ret

    def flush(self):
        ret = None
        if self.bulk_actions:
            ret = (self.bulk_data, self.bulk_actions)
            self.bulk_actions, self.bulk_data = [], []
        return ret


def _chunk_actions(actions, chunk_size, max_chunk_bytes, serializer):
    """
    Split actions into chunks by number or size, serialize them into strings in
    the process.
    """
    chunker = _ActionChunker(
        chunk_size=chunk_size, max_chunk_bytes=max_chunk_bytes, serializer=serializer
    )
    for action, data in actions:
        ret = chunker.feed(action, data)
        if ret:
            yield ret
    ret = chunker.flush()
    if ret:
        yield ret


def _process_bulk_chunk_success(resp, bulk_data, ignore_status, raise_on_error=True):
    # if raise on error is set, we need to collect errors per chunk before raising them
    errors = []

    # go through request-response pairs and detect failures
    for data, (op_type, item) in zip(
        bulk_data, map(methodcaller("popitem"), resp["items"])
    ):
        status_code = item.get("status", 500)

        ok = 200 <= status_code < 300
        if not ok and raise_on_error and status_code not in ignore_status:
            # include original document source
            if len(data) > 1:
                item["data"] = data[1]
            errors.append({op_type: item})

        if ok or not errors:
            # if we are not just recording all errors to be able to raise
            # them all at once, yield items individually
            yield ok, {op_type: item}

    if errors:
        raise BulkIndexError("%i document(s) failed to index." % len(errors), errors)


def _process_bulk_chunk_error(
    error, bulk_data, ignore_status, raise_on_exception=True, raise_on_error=True
):
    # default behavior - just propagate exception
    if raise_on_exception and error.status_code not in ignore_status:
        raise error

    # if we are not propagating, mark all actions in current chunk as failed
    err_message = str(error)
    exc_errors = []

    for data in bulk_data:
        # collect all the information about failed actions
        op_type, action = data[0].copy().popitem()
        info = {"error": err_message, "status": error.status_code, "exception": error}
        if op_type != "delete":
            info["data"] = data[1]
        info.update(action)
        exc_errors.append({op_type: info})

    # emulate standard behavior for failed actions
    if raise_on_error and error.status_code not in ignore_status:
        raise BulkIndexError(
            "%i document(s) failed to index." % len(exc_errors), exc_errors
        )
    else:
        for err in exc_errors:
            yield False, err


def _process_bulk_chunk(
    client,
    bulk_actions,
    bulk_data,
    raise_on_exception=True,
    raise_on_error=True,
    ignore_status=(),
    *args,
    **kwargs
):
    """
    Send a bulk request to elasticsearch and process the output.
    """
    kwargs = _add_helper_meta_to_kwargs(kwargs, "bp")

    if not isinstance(ignore_status, (list, tuple)):
        ignore_status = (ignore_status,)

    try:
        # send the actual request
        resp = client.bulk(*args, body="\n".join(bulk_actions) + "\n", **kwargs)
    except TransportError as e:
        gen = _process_bulk_chunk_error(
            error=e,
            bulk_data=bulk_data,
            ignore_status=ignore_status,
            raise_on_exception=raise_on_exception,
            raise_on_error=raise_on_error,
        )
    else:
        gen = _process_bulk_chunk_success(
            resp=resp,
            bulk_data=bulk_data,
            ignore_status=ignore_status,
            raise_on_error=raise_on_error,
        )
    for item in gen:
        yield item


def _add_helper_meta_to_kwargs(kwargs, helper_meta):
    params = (kwargs or {}).pop("params", {})
    params["__elastic_client_meta"] = (("h", helper_meta),)
    kwargs["params"] = params
    return kwargs


def streaming_bulk(
    client,
    actions,
    chunk_size=500,
    max_chunk_bytes=100 * 1024 * 1024,
    raise_on_error=True,
    expand_action_callback=expand_action,
    raise_on_exception=True,
    max_retries=0,
    initial_backoff=2,
    max_backoff=600,
    yield_ok=True,
    ignore_status=(),
    *args,
    **kwargs
):

    """
    Streaming bulk consumes actions from the iterable passed in and yields
    results per action. For non-streaming usecases use
    :func:`~elasticsearch.helpers.bulk` which is a wrapper around streaming
    bulk that returns summary information about the bulk operation once the
    entire input is consumed and sent.

    If you specify ``max_retries`` it will also retry any documents that were
    rejected with a ``429`` status code. To do this it will wait (**by calling
    time.sleep which will block**) for ``initial_backoff`` seconds and then,
    every subsequent rejection for the same chunk, for double the time every
    time up to ``max_backoff`` seconds.

    :arg client: instance of :class:`~elasticsearch.Elasticsearch` to use
    :arg actions: iterable containing the actions to be executed
    :arg chunk_size: number of docs in one chunk sent to es (default: 500)
    :arg max_chunk_bytes: the maximum size of the request in bytes (default: 100MB)
    :arg raise_on_error: raise ``BulkIndexError`` containing errors (as `.errors`)
        from the execution of the last chunk when some occur. By default we raise.
    :arg raise_on_exception: if ``False`` then don't propagate exceptions from
        call to ``bulk`` and just report the items that failed as failed.
    :arg expand_action_callback: callback executed on each action passed in,
        should return a tuple containing the action line and the data line
        (`None` if data line should be omitted).
    :arg max_retries: maximum number of times a document will be retried when
        ``429`` is received, set to 0 (default) for no retries on ``429``
    :arg initial_backoff: number of seconds we should wait before the first
        retry. Any subsequent retries will be powers of ``initial_backoff *
        2**retry_number``
    :arg max_backoff: maximum number of seconds a retry will wait
    :arg yield_ok: if set to False will skip successful documents in the output
    :arg ignore_status: list of HTTP status code that you want to ignore
    """
    actions = map(expand_action_callback, actions)

    for bulk_data, bulk_actions in _chunk_actions(
        actions, chunk_size, max_chunk_bytes, client.transport.serializer
    ):

        for attempt in range(max_retries + 1):
            to_retry, to_retry_data = [], []
            if attempt:
                time.sleep(min(max_backoff, initial_backoff * 2 ** (attempt - 1)))

            try:
                for data, (ok, info) in zip(
                    bulk_data,
                    _process_bulk_chunk(
                        client,
                        bulk_actions,
                        bulk_data,
                        raise_on_exception,
                        raise_on_error,
                        ignore_status,
                        *args,
                        **kwargs
                    ),
                ):

                    if not ok:
                        action, info = info.popitem()
                        # retry if retries enabled, we get 429, and we are not
                        # in the last attempt
                        if (
                            max_retries
                            and info["status"] == 429
                            and (attempt + 1) <= max_retries
                        ):
                            # _process_bulk_chunk expects strings so we need to
                            # re-serialize the data
                            to_retry.extend(
                                map(client.transport.serializer.dumps, data)
                            )
                            to_retry_data.append(data)
                        else:
                            yield ok, {action: info}
                    elif yield_ok:
                        yield ok, info

            except TransportError as e:
                # suppress 429 errors since we will retry them
                if attempt == max_retries or e.status_code != 429:
                    raise
            else:
                if not to_retry:
                    break
                # retry only subset of documents that didn't succeed
                bulk_actions, bulk_data = to_retry, to_retry_data


def bulk(client, actions, stats_only=False, ignore_status=(), *args, **kwargs):
    """
    Helper for the :meth:`~elasticsearch.Elasticsearch.bulk` api that provides
    a more human friendly interface - it consumes an iterator of actions and
    sends them to elasticsearch in chunks. It returns a tuple with summary
    information - number of successfully executed actions and either list of
    errors or number of errors if ``stats_only`` is set to ``True``. Note that
    by default we raise a ``BulkIndexError`` when we encounter an error so
    options like ``stats_only`` only apply when ``raise_on_error`` is set to
    ``False``.

    When errors are being collected original document data is included in the
    error dictionary which can lead to an extra high memory usage. If you need
    to process a lot of data and want to ignore/collect errors please consider
    using the :func:`~elasticsearch.helpers.streaming_bulk` helper which will
    just return the errors and not store them in memory.


    :arg client: instance of :class:`~elasticsearch.Elasticsearch` to use
    :arg actions: iterator containing the actions
    :arg stats_only: if `True` only report number of successful/failed
        operations instead of just number of successful and a list of error responses
    :arg ignore_status: list of HTTP status code that you want to ignore

    Any additional keyword arguments will be passed to
    :func:`~elasticsearch.helpers.streaming_bulk` which is used to execute
    the operation, see :func:`~elasticsearch.helpers.streaming_bulk` for more
    accepted parameters.
    """
    success, failed = 0, 0

    # list of errors to be collected is not stats_only
    errors = []

    # make streaming_bulk yield successful results so we can count them
    kwargs["yield_ok"] = True
    for ok, item in streaming_bulk(
        client, actions, ignore_status=ignore_status, *args, **kwargs
    ):
        # go through request-response pairs and detect failures
        if not ok:
            if not stats_only:
                errors.append(item)
            failed += 1
        else:
            success += 1

    return success, failed if stats_only else errors


def parallel_bulk(
    client,
    actions,
    thread_count=4,
    chunk_size=500,
    max_chunk_bytes=100 * 1024 * 1024,
    queue_size=4,
    expand_action_callback=expand_action,
    ignore_status=(),
    *args,
    **kwargs
):
    """
    Parallel version of the bulk helper run in multiple threads at once.

    :arg client: instance of :class:`~elasticsearch.Elasticsearch` to use
    :arg actions: iterator containing the actions
    :arg thread_count: size of the threadpool to use for the bulk requests
    :arg chunk_size: number of docs in one chunk sent to es (default: 500)
    :arg max_chunk_bytes: the maximum size of the request in bytes (default: 100MB)
    :arg raise_on_error: raise ``BulkIndexError`` containing errors (as `.errors`)
        from the execution of the last chunk when some occur. By default we raise.
    :arg raise_on_exception: if ``False`` then don't propagate exceptions from
        call to ``bulk`` and just report the items that failed as failed.
    :arg expand_action_callback: callback executed on each action passed in,
        should return a tuple containing the action line and the data line
        (`None` if data line should be omitted).
    :arg queue_size: size of the task queue between the main thread (producing
        chunks to send) and the processing threads.
    :arg ignore_status: list of HTTP status code that you want to ignore
    """
    # Avoid importing multiprocessing unless parallel_bulk is used
    # to avoid exceptions on restricted environments like App Engine
    from multiprocessing.pool import ThreadPool

    actions = map(expand_action_callback, actions)

    class BlockingPool(ThreadPool):
        def _setup_queues(self):
            super(BlockingPool, self)._setup_queues()  # type: ignore
            # The queue must be at least the size of the number of threads to
            # prevent hanging when inserting sentinel values during teardown.
            self._inqueue = Queue(max(queue_size, thread_count))
            self._quick_put = self._inqueue.put

    pool = BlockingPool(thread_count)

    try:
        for result in pool.imap(
            lambda bulk_chunk: list(
                _process_bulk_chunk(
                    client,
                    bulk_chunk[1],
                    bulk_chunk[0],
                    ignore_status=ignore_status,
                    *args,
                    **kwargs
                )
            ),
            _chunk_actions(
                actions, chunk_size, max_chunk_bytes, client.transport.serializer
            ),
        ):
            for item in result:
                yield item

    finally:
        pool.close()
        pool.join()


def scan(
    client,
    query=None,
    scroll="5m",
    raise_on_error=True,
    preserve_order=False,
    size=1000,
    request_timeout=None,
    clear_scroll=True,
    scroll_kwargs=None,
    **kwargs
):
    """
    Simple abstraction on top of the
    :meth:`~elasticsearch.Elasticsearch.scroll` api - a simple iterator that
    yields all hits as returned by underlining scroll requests.

    By default scan does not return results in any pre-determined order. To
    have a standard order in the returned documents (either by score or
    explicit sort definition) when scrolling, use ``preserve_order=True``. This
    may be an expensive operation and will negate the performance benefits of
    using ``scan``.

    :arg client: instance of :class:`~elasticsearch.Elasticsearch` to use
    :arg query: body for the :meth:`~elasticsearch.Elasticsearch.search` api
    :arg scroll: Specify how long a consistent view of the index should be
        maintained for scrolled search
    :arg raise_on_error: raises an exception (``ScanError``) if an error is
        encountered (some shards fail to execute). By default we raise.
    :arg preserve_order: don't set the ``search_type`` to ``scan`` - this will
        cause the scroll to paginate with preserving the order. Note that this
        can be an extremely expensive operation and can easily lead to
        unpredictable results, use with caution.
    :arg size: size (per shard) of the batch send at each iteration.
    :arg request_timeout: explicit timeout for each call to ``scan``
    :arg clear_scroll: explicitly calls delete on the scroll id via the clear
        scroll API at the end of the method on completion or error, defaults
        to true.
    :arg scroll_kwargs: additional kwargs to be passed to
        :meth:`~elasticsearch.Elasticsearch.scroll`

    Any additional keyword arguments will be passed to the initial
    :meth:`~elasticsearch.Elasticsearch.search` call::

        scan(es,
            query={"query": {"match": {"title": "python"}}},
            index="orders-*",
            doc_type="books"
        )

    """
    scroll_kwargs = scroll_kwargs.copy() if scroll_kwargs else {}
    scroll_kwargs["scroll"] = scroll
    _add_helper_meta_to_kwargs(scroll_kwargs, "s")

    if not preserve_order:
        query = query.copy() if query else {}
        query["sort"] = "_doc"

    # Grab options that should be propagated to every
    # API call within this helper instead of just 'search()'
    transport_kwargs = {}
    for key in ("headers", "api_key", "http_auth"):
        if key in kwargs:
            transport_kwargs[key] = kwargs[key]

    # If the user is using 'scroll_kwargs' we want
    # to propagate there too, but to not break backwards
    # compatibility we'll not override anything already given.
    if scroll_kwargs is not None and transport_kwargs:
        for key, val in transport_kwargs.items():
            scroll_kwargs.setdefault(key, val)

    # initial search
    search_kwargs = kwargs.copy()

    # Setting query={"from": ...} would make 'from' be used
    # as a keyword argument instead of 'from_'. We handle that here.
    if "from" in search_kwargs:
        search_kwargs["from_"] = search_kwargs.pop("from")
    if query:
        search_kwargs.update(query)
    if "from" in search_kwargs:
        search_kwargs["from_"] = search_kwargs.pop("from")

    search_kwargs["scroll"] = scroll
    search_kwargs["size"] = size
    search_kwargs["request_timeout"] = request_timeout
    _add_helper_meta_to_kwargs(search_kwargs, "s")
    resp = client.search(**search_kwargs)
    scroll_id = resp.get("_scroll_id")

    try:
        while scroll_id and resp["hits"]["hits"]:
            for hit in resp["hits"]["hits"]:
                yield hit

            # Default to 0 if the value isn't included in the response
            shards_successful = resp["_shards"].get("successful", 0)
            shards_skipped = resp["_shards"].get("skipped", 0)
            shards_total = resp["_shards"].get("total", 0)

            # check if we have any errors
            if (shards_successful + shards_skipped) < shards_total:
                shards_message = "Scroll request has only succeeded on %d (+%d skipped) shards out of %d."
                logger.warning(
                    shards_message,
                    shards_successful,
                    shards_skipped,
                    shards_total,
                )
                if raise_on_error:
                    raise ScanError(
                        scroll_id,
                        shards_message
                        % (
                            shards_successful,
                            shards_skipped,
                            shards_total,
                        ),
                    )

            scroll_kwargs["scroll_id"] = scroll_id
            resp = client.scroll(**scroll_kwargs)
            scroll_id = resp.get("_scroll_id")

    finally:
        if scroll_id and clear_scroll:
            client.clear_scroll(
                scroll_id=scroll_id,
                ignore=(404,),
                params={"__elastic_client_meta": (("h", "s"),)},
                **transport_kwargs
            )


def reindex(
    client,
    source_index,
    target_index,
    query=None,
    target_client=None,
    chunk_size=500,
    scroll="5m",
    op_type=None,
    scan_kwargs={},
    bulk_kwargs={},
):

    """
    Reindex all documents from one index that satisfy a given query
    to another, potentially (if `target_client` is specified) on a different cluster.
    If you don't specify the query you will reindex all the documents.

    Since ``2.3`` a :meth:`~elasticsearch.Elasticsearch.reindex` api is
    available as part of elasticsearch itself. It is recommended to use the api
    instead of this helper wherever possible. The helper is here mostly for
    backwards compatibility and for situations where more flexibility is
    needed.

    .. note::

        This helper doesn't transfer mappings, just the data.

    :arg client: instance of :class:`~elasticsearch.Elasticsearch` to use (for
        read if `target_client` is specified as well)
    :arg source_index: index (or list of indices) to read documents from
    :arg target_index: name of the index in the target cluster to populate
    :arg query: body for the :meth:`~elasticsearch.Elasticsearch.search` api
    :arg target_client: optional, is specified will be used for writing (thus
        enabling reindex between clusters)
    :arg chunk_size: number of docs in one chunk sent to es (default: 500)
    :arg scroll: Specify how long a consistent view of the index should be
        maintained for scrolled search
    :arg op_type: Explicit operation type. Defaults to '_index'. Data streams must
        be set to 'create'. If not specified, will auto-detect if target_index is a
        data stream.
    :arg scan_kwargs: additional kwargs to be passed to
        :func:`~elasticsearch.helpers.scan`
    :arg bulk_kwargs: additional kwargs to be passed to
        :func:`~elasticsearch.helpers.bulk`
    """
    target_client = client if target_client is None else target_client
    docs = scan(client, query=query, index=source_index, scroll=scroll, **scan_kwargs)

    def _change_doc_index(hits, index, op_type):
        for h in hits:
            h["_index"] = index
            if op_type is not None:
                h["_op_type"] = op_type
            if "fields" in h:
                h.update(h.pop("fields"))
            yield h

    kwargs = {"stats_only": True}
    kwargs.update(bulk_kwargs)

    is_data_stream = False
    try:
        # Verify if the target_index is data stream or index
        data_streams = target_client.indices.get_data_stream(
            target_index, expand_wildcards="all"
        )
        is_data_stream = any(
            data_stream["name"] == target_index
            for data_stream in data_streams["data_streams"]
        )
    except (TransportError, KeyError, NotFoundError):
        # If its not data stream, might be index
        pass

    if is_data_stream:
        if op_type not in (None, "create"):
            raise ValueError("Data streams must have 'op_type' set to 'create'")
        else:
            op_type = "create"

    return bulk(
        target_client,
        _change_doc_index(docs, target_index, op_type),
        chunk_size=chunk_size,
        **kwargs
    )
