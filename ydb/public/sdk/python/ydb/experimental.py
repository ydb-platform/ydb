from ydb.public.api.protos import ydb_experimental_pb2
from ydb.public.api.grpc.draft import ydb_experimental_v1_pb2_grpc
from . import _utilities, issues, convert


def _execute_stream_query_request_factory(yql_text, parameters=None, settings=None):
    request = ydb_experimental_pb2.ExecuteStreamQueryRequest()
    request.yql_text = yql_text
    request.profile_mode = 1
    return request


class StreamQueryResult(object):
    def __init__(self, result):
        self._result = result

    def is_progress(self):
        return self._result.WhichOneof("result") == "ping"

    @property
    def result_set(self):
        return convert.ResultSet.from_message(self._result.result_set)

    def is_result_set(self):
        return self._result.WhichOneof("result") == "result_set"


def _wrap_stream_query_response(response):
    issues._process_response(response)
    return StreamQueryResult(response.result)


class ExperimentalClient(object):
    def __init__(self, driver):
        self.driver = driver

    def async_execute_stream_query(self, yql_text, parameters=None, settings=None):
        """
        That is experimental api. Can be used only for tests, not for production.
        """
        request = _execute_stream_query_request_factory(yql_text, parameters, settings)
        stream_it = self.driver(
            request,
            ydb_experimental_v1_pb2_grpc.ExperimentalServiceStub,
            "ExecuteStreamQuery",
            settings=settings,
        )
        return _utilities.AsyncResponseIterator(stream_it, _wrap_stream_query_response)

    def execute_stream_query(self, yql_text, parameters=None, settings=None):
        """
        That is experimental api. Can be used only for tests, not for production.
        """
        request = _execute_stream_query_request_factory(yql_text, parameters, settings)
        stream_it = self.driver(
            request,
            ydb_experimental_v1_pb2_grpc.ExperimentalServiceStub,
            "ExecuteStreamQuery",
            settings=settings,
        )
        return _utilities.SyncResponseIterator(stream_it, _wrap_stream_query_response)
