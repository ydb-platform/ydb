# Workaround for good IDE and universal for runtime
# noinspection PyUnreachableCode
if False:
    from ._grpc.v4.protos import ydb_scripting_pb2
    from ._grpc.v4 import ydb_scripting_v1_pb2_grpc
else:
    from ._grpc.common.protos import ydb_scripting_pb2
    from ._grpc.common import ydb_scripting_v1_pb2_grpc


from . import issues, convert, settings


class TypedParameters(object):
    def __init__(self, parameters_types, parameters_values):
        self.parameters_types = parameters_types
        self.parameters_values = parameters_values


class ScriptingClientSettings(object):
    def __init__(self):
        self._native_date_in_result_sets = False
        self._native_datetime_in_result_sets = False

    def with_native_date_in_result_sets(self, enabled):
        self._native_date_in_result_sets = enabled
        return self

    def with_native_datetime_in_result_sets(self, enabled):
        self._native_datetime_in_result_sets = enabled
        return self


class ExplainYqlScriptSettings(settings.BaseRequestSettings):
    MODE_UNSPECIFIED = 0
    MODE_PARSE = 1
    MODE_VALIDATE = 2
    MODE_EXPLAIN = 3

    def __init__(self):
        super(ExplainYqlScriptSettings, self).__init__()
        self.mode = False

    def with_mode(self, val):
        self.mode = val
        return self


def _execute_yql_query_request_factory(script, tp=None, settings=None):
    params = (
        None
        if tp is None
        else convert.parameters_to_pb(tp.parameters_types, tp.parameters_values)
    )
    return ydb_scripting_pb2.ExecuteYqlRequest(script=script, parameters=params)


class YqlQueryResult(object):
    def __init__(self, result, scripting_client_settings=None):
        self.result_sets = convert.ResultSets(
            result.result_sets, scripting_client_settings
        )


class YqlExplainResult(object):
    def __init__(self, result):
        self.plan = result.plan


def _wrap_response(rpc_state, response, scripting_client_settings):
    issues._process_response(response.operation)
    message = ydb_scripting_pb2.ExecuteYqlResult()
    response.operation.result.Unpack(message)
    return YqlQueryResult(message)


def _wrap_explain_response(rpc_state, response):
    issues._process_response(response.operation)
    message = ydb_scripting_pb2.ExplainYqlResult()
    response.operation.result.Unpack(message)
    return YqlExplainResult(message)


class ScriptingClient(object):
    def __init__(self, driver, scripting_client_settings=None):
        self.driver = driver
        self.scripting_client_settings = (
            scripting_client_settings
            if scripting_client_settings is not None
            else ScriptingClientSettings()
        )

    def execute_yql(self, script, typed_parameters=None, settings=None):
        request = _execute_yql_query_request_factory(script, typed_parameters, settings)
        return self.driver(
            request,
            ydb_scripting_v1_pb2_grpc.ScriptingServiceStub,
            "ExecuteYql",
            _wrap_response,
            settings=settings,
            wrap_args=(self.scripting_client_settings,),
        )

    def explain_yql(self, script, settings=None):
        return self.driver(
            ydb_scripting_pb2.ExplainYqlRequest(script=script, mode=settings.mode),
            ydb_scripting_v1_pb2_grpc.ScriptingServiceStub,
            "ExplainYql",
            _wrap_explain_response,
            settings=settings,
        )
