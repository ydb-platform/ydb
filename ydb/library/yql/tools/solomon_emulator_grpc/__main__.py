import os
import logging

from concurrent import futures

from library.python.testing.recipe import declare_recipe, set_env
from library.recipes.common import find_free_ports

import grpc
from ydb.library.yql.providers.solomon.solomon_accessor.grpc.solomon_accessor_pb_pb2_grpc import \
    DataServiceServicer, add_DataServiceServicer_to_server
from ydb.library.yql.providers.solomon.solomon_accessor.grpc.solomon_accessor_pb_pb2 import ReadRequest, ReadResponse, MetricType

PID_FILENAME = "solomon_recipe.pid"

logger = logging.getLogger('solomon_emulator_grpc.recipe')
logging.basicConfig(level=logging.DEBUG)


def _dict_to_labels(request: ReadRequest):
    result = dict()

    result["from"] = str(request.from_time)
    result["to"] = str(request.to_time)
    result["program"] = f"program length {len(str(request.queries[0].value))}"
    if (request.downsampling.HasField("disabled")):
        result["downsampling.disabled"] = f"bool {True}"
    else:
        result["downsampling.aggregation"] = request.downsampling.grid_aggregation
        result["downsampling.fill"] = request.downsampling.gap_filling
        result["downsampling.gridMillis"] = f"int {request.downsampling.grid_interval}"
        result["downsampling.disabled"] = f"bool {False}"

    return result


class DataService(DataServiceServicer):
    def Read(self, request: ReadRequest, context) -> ReadResponse:
        logger.debug('ReadRequest: %s', request)

        project = request.container.project_id
        if (project == "invalid"):
            logger.debug("invalid project_id, sending error")
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(f"Project {project} does not exist")
            return ReadResponse()

        labels = _dict_to_labels(request)
        labels["project"] = project

        response = ReadResponse()

        response_query = response.response_per_query.add()
        response_query.query_name = "query"

        timeseries = response_query.timeseries_vector.values.add()
        timeseries.name = "name"
        for key, value in labels.items():
            timeseries.labels[key] = str(value)
        timeseries.type = MetricType.RATE

        timeseries.timestamp_values.values.extend([1, 2, 3])
        timeseries.double_values.values.extend([100, 200, 300])

        return response


def serve(port: int) -> None:
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
    add_DataServiceServicer_to_server(
        DataService(), server
    )
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    logger.info(f'solomon_emulator_grpc server started at {port}')

    server.wait_for_termination()
    logger.info('solomon_emulator_grpc server stopped')


def start(argv):
    logger.debug("Starting Solomon Grpc recipe")

    port = find_free_ports(1)[0]
    _update_environment(port=port)

    pid = os.fork()
    if pid == 0:
        logger.info('Starting solomon_emulator_grpc server...')
        serve(port=port)
    else:
        with open(PID_FILENAME, "w") as f:
            f.write(str(pid))


def _update_environment(port: int):
    endpoint = "localhost"
    url = "localhost"
    set_env("SOLOMON_HOST", "localhost")
    set_env("SOLOMON_PORT", str(port))
    set_env("SOLOMON_ENDPOINT", endpoint)
    set_env("SOLOMON_URL", url)


def stop(argv):
    logger.debug("Stop Solomon Grpc recipe")
    with open(PID_FILENAME, "r") as f:
        pid = int(f.read())
        os.kill(pid, 9)


if __name__ == "__main__":
    declare_recipe(start, stop)
