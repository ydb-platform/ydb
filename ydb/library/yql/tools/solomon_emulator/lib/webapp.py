from aiohttp import web
import json
import logging
from concurrent import futures

from library.python.monlib.encoder import loads
from .multi_shard import MultiShard

import grpc
from ydb.library.yql.providers.solomon.solomon_accessor.grpc.data_service_pb2_grpc import \
    DataServiceServicer, add_DataServiceServicer_to_server
from ydb.library.yql.providers.solomon.solomon_accessor.grpc.data_service_pb2 import ReadRequest, ReadResponse, MetricType

routes = web.RouteTableDef()
logger = logging.getLogger(__name__)

CONTENT_TYPE_SPACK = "application/x-solomon-spack"
CONTENT_TYPE_JSON = "application/json"


def _data_from_request(request) -> MultiShard:
    return request.app["data"]


def _config_from_request(request) -> MultiShard:
    return request.app["config"]


@routes.get("/ping")
async def get_ping(request):
    return web.Response(status=200)


@routes.post("/api/v2/push")
async def push(request):
    if request.app["features"]["response_code"] != 200:
        logging.debug("trying to push, sending response code {}".format(
            request.app["features"]["response_code"]))
        return web.Response(status=request.app["response_code"])
    logging.debug("push: {}".format(await request.read()))
    handle_auth(request)

    project = request.rel_url.query['project']
    cluster = request.rel_url.query['cluster']
    service = request.rel_url.query['service']

    shard = _data_from_request(request).get_or_create(project, cluster, service)
    content_type = request.headers['content-type']

    if content_type == CONTENT_TYPE_SPACK:
        metrics_json = json.loads(loads(await request.read()))
        logging.debug(f"spack decoded: {metrics_json}")
    elif content_type == CONTENT_TYPE_JSON:
        metrics_json = await request.json()
        logging.debug(f"json received: {metrics_json}")
    else:
        return web.HTTPBadRequest(text=f"Unknown content type {content_type}")

    return web.json_response({"sensorsProcessed": shard.add_metrics(metrics_json)})


@routes.post("/monitoring/v2/data/write")
async def write(request):
    if request.app["features"]["response_code"] != 200:
        logging.debug("trying to write, sending response code {}".format(
            request.app["features"]["response_code"]))
        return web.Response(status=request.app["response_code"])

    logging.debug("write: {}".format(await request.read()))
    handle_auth(request)

    folder_id = request.rel_url.query['folderId']
    service = request.rel_url.query['service']

    shard = _data_from_request(request).get_or_create(folder_id, folder_id, service)
    content_type = request.headers['content-type']

    if content_type != CONTENT_TYPE_JSON:
        return web.HTTPBadRequest(text=f"Unknown content type {content_type}")

    metrics_json = await request.json()
    logging.debug(f"json received: {metrics_json}")

    return web.json_response({"writtenMetricsCount": shard.add_metrics(metrics_json)})


@routes.get("/api/v2/projects/{project}/sensors/names")
async def sensors_data(request):
    return web.json_response({"names": ["test_type, test_label"]})


@routes.get("/api/v2/projects/{project}/sensors")
async def sensors_data(request):
    return web.json_response({"result": [], "page": {"pagesCount": 1, "totalCount": 5}})


@routes.get("/metrics")
async def metrics(request):
    cluster = request.rel_url.query.get('cluster', None) or request.rel_url.query['folderId']
    project = request.rel_url.query.get('project', cluster)
    service = request.rel_url.query['service']

    shard = _data_from_request(request).find(project, cluster, service)
    if shard is None:
        return web.HTTPNotFound(text=f"Unable to find shard {project}/{cluster}/{service}")
    reply = shard.as_text()
    logging.debug(f"metrics reply: {reply}")
    return web.json_response(text=reply)


@routes.post("/cleanup")
async def cleanup(request):
    cluster = request.rel_url.query.get('cluster', None) or request.rel_url.query.get('folderId', None)
    project = request.rel_url.query.get('project', cluster)
    service = request.rel_url.query.get('service')
    request.app["features"] = {"response_code": 200}

    data = _data_from_request(request)
    if project is None and cluster is None and service is None:
        data.clear()
    else:
        data.delete(project, cluster, service)
    return web.Response(status=200)


@routes.post("/config")
async def config(request):
    request.app["features"] = json.loads(await request.read())
    return web.Response(status=200)

def _dict_to_labels(request: ReadRequest):
    result = dict()

    result["from"] = str(request.from_time)
    result["to"] = str(request.to_time)
    result["program"] = f"program length {len(str(request.queries[0].value))}"
    if request.downsampling.HasField("disabled"):
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
        if project == "invalid":
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
        for key, value in labels.items():
            timeseries.labels[key] = str(value)
        timeseries.type = MetricType.RATE

        timeseries.timestamp_values.values.extend([10000, 20000, 30000])
        timeseries.double_values.values.extend([100, 200, 300])

        return response

def handle_auth(request):
    config = _config_from_request(request)
    if config.auth:
        auth_header = request.headers.get('AUTHORIZATION')
        if not config.auth == auth_header:
            logging.debug(f"Authorization header {auth_header} mismatches expected value {config.auth}")
            raise web.HTTPForbidden()


def create_web_app(config, grpc_port):
    grpc_server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
    add_DataServiceServicer_to_server(
        DataService(), grpc_server
    )
    grpc_server.add_insecure_port(f'[::]:{grpc_port}')
    
    webapp = web.Application()
    webapp.add_routes(routes)
    webapp["config"] = config
    webapp["data"] = MultiShard()
    webapp["grpc_server"] = grpc_server

    return webapp


def run_web_app(config, http_port, grpc_port):
    app = create_web_app(config, grpc_port)

    app["grpc_server"].start()
    web.run_app(app, port=http_port)
