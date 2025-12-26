from aiohttp import web
import json
import logging
import re
from concurrent import futures

from library.python.monlib.encoder import loads
from .multi_shard import MultiShard
from .shard import Shard

import grpc
from ydb.library.yql.providers.solomon.solomon_accessor.grpc.data_service_pb2_grpc import \
    DataServiceServicer, add_DataServiceServicer_to_server
from ydb.library.yql.providers.solomon.solomon_accessor.grpc.data_service_pb2 import ReadRequest, ReadResponse, MetricType

routes = web.RouteTableDef()
logger = logging.getLogger(__name__)

CONTENT_TYPE_SPACK = "application/x-solomon-spack"
CONTENT_TYPE_JSON = "application/json"


def _parse_selectors(selectors):
    result = dict()

    match = re.search(r".*{(.*)}.*", selectors)
    if (not match):
        return (result, False)

    group = match[1]
    if (len(group) == 0):
        return (result, True)

    for selector in group.split(","):
        eq_pos = selector.find("=")
        if (eq_pos == -1):
            return (result, False)

        key = selector[:eq_pos].strip()
        value = selector[eq_pos + 1:].strip('=').strip().strip('"')
        result[key] = value

    return (result, True)


class SolomonEmulator(object):
    def __init__(self, config):
        self._config = config
        self._data = MultiShard()

    def _get_shard(self, project, cluster, service):
        return self._data.get_or_create(project, cluster, service)

    def _handle_auth(self, request):
        if self._config.auth:
            auth_header = request.headers.get('AUTHORIZATION')
            if not self._config.auth == auth_header:
                logger.debug(f"Authorization header {auth_header} mismatches expected value {self._config.auth}")
                raise web.HTTPForbidden()

    async def get_ping(self, request):
        return web.Response(status=200)

    async def api_v2_push(self, request):
        if request.app["features"]["response_code"] != 200:
            logger.debug("trying to push, sending response code {}".format(
                request.app["features"]["response_code"]))
            return web.Response(status=request.app["response_code"])
        logger.debug("push: {}".format(await request.read()))
        self._handle_auth(request)

        project = request.rel_url.query['project']
        cluster = request.rel_url.query['cluster']
        service = request.rel_url.query['service']

        shard = self._get_shard(project, cluster, service)
        content_type = request.headers['content-type']

        if content_type == CONTENT_TYPE_SPACK:
            metrics_json = json.loads(loads(await request.read()))
            logger.debug(f"spack decoded: {metrics_json}")
        elif content_type == CONTENT_TYPE_JSON:
            metrics_json = await request.json()
            logger.debug(f"json received: {metrics_json}")
        else:
            return web.HTTPBadRequest(text=f"Unknown content type {content_type}")

        return web.json_response({"sensorsProcessed": shard.add_metrics(metrics_json)})

    async def data_write(self, request):
        if request.app["features"]["response_code"] != 200:
            logger.debug("trying to write, sending response code {}".format(
                request.app["features"]["response_code"]))
            return web.Response(status=request.app["response_code"])

        logger.debug("write: {}".format(await request.read()))
        self._handle_auth(request)

        folder_id = request.rel_url.query['folderId']
        service = request.rel_url.query['service']

        shard = self._get_shard(folder_id, folder_id, service)
        content_type = request.headers['content-type']

        if content_type != CONTENT_TYPE_JSON:
            return web.HTTPBadRequest(text=f"Unknown content type {content_type}")

        metrics_json = await request.json()

        return web.json_response({"writtenMetricsCount": shard.add_metrics(metrics_json)})

    async def sensor_names_post(self, request):
        json = await request.json()
        selectors, success = _parse_selectors(json["selectors"])

        if not success:
            return web.HTTPBadRequest(text="Invalid selectors")

        if "project" not in selectors or "cluster" not in selectors or "service" not in selectors:
            return web.HTTPBadRequest(text="project, cluster and service labels must be specified")

        if "projectId" in json:
            return web.HTTPBadRequest(text="Invalid query params")

        project = selectors["project"]
        cluster = selectors["cluster"]
        service = selectors["service"]

        shard = self._get_shard(project, cluster, service)
        result = shard.get_label_names(selectors)

        return web.json_response({"names": result})

    async def sensor_names_get(self, request):
        selectors, success = _parse_selectors(request.rel_url.query["selectors"])

        if not success:
            return web.HTTPBadRequest(text="Invalid selectors")

        if "project" not in selectors or "cluster" not in selectors or "service" not in selectors:
            return web.HTTPBadRequest(text="project, cluster and service labels must be specified")

        project = selectors["project"]
        cluster = selectors["cluster"]
        service = selectors["service"]

        shard = self._get_shard(project, cluster, service)
        result = shard.get_label_names(selectors)

        return web.json_response({"names": result})

    async def sensor_labels_post(self, request):
        json = await request.json()
        selectors, success = _parse_selectors(json["selectors"])

        if not success:
            return web.HTTPBadRequest(text="Invalid selectors")

        if "project" not in selectors or "cluster" not in selectors or "service" not in selectors:
            return web.HTTPBadRequest(text="project, cluster and service labels must be specified")

        if "projectId" in json:
            return web.HTTPBadRequest(text="Invalid query params")

        project = selectors["project"]
        cluster = selectors["cluster"]
        service = selectors["service"]

        shard = self._get_shard(project, cluster, service)
        labels, totalCount = shard.get_labels(selectors)

        return web.json_response({"labels": labels, "totalCount": totalCount})

    async def sensor_labels_get(self, request):
        selectors, success = _parse_selectors(request.rel_url.query["selectors"])

        if not success:
            return web.HTTPBadRequest(text="Invalid selectors")

        if "project" not in selectors or "cluster" not in selectors or "service" not in selectors:
            return web.HTTPBadRequest(text="project, cluster and service labels must be specified")

        project = selectors["project"]
        cluster = selectors["cluster"]
        service = selectors["service"]

        shard = self._get_shard(project, cluster, service)
        labels, totalCount = shard.get_labels(selectors)

        return web.json_response({"labels": labels, "totalCount": totalCount})

    async def sensors_post(self, request):
        json = await request.json()
        selectors, success = _parse_selectors(json["selectors"])

        if not success:
            return web.HTTPBadRequest(text="Invalid selectors")

        if "project" not in selectors or "cluster" not in selectors or "service" not in selectors:
            return web.HTTPBadRequest(text="project, cluster and service labels must be specified")

        if "projectId" in json or "pageSize" in json:
            return web.HTTPBadRequest(text="Invalid query params")

        project = selectors["project"]
        cluster = selectors["cluster"]
        service = selectors["service"]

        shard = self._get_shard(project, cluster, service)
        metrics, error = shard.get_metrics(selectors)

        if error is not None:
            return web.HTTPBadRequest(text=error)

        return web.json_response({"result": metrics, "page": {"pagesCount": 1, "totalCount": len(metrics)}})

    async def sensors_get(self, request):
        selectors, success = _parse_selectors(request.rel_url.query["selectors"])

        if not success:
            return web.HTTPBadRequest(text="Invalid selectors")

        if "project" not in selectors or "cluster" not in selectors or "service" not in selectors:
            return web.HTTPBadRequest(text="project, cluster and service labels must be specified")

        project = selectors["project"]
        cluster = selectors["cluster"]
        service = selectors["service"]

        shard = self._get_shard(project, cluster, service)
        metrics, error = shard.get_metrics(selectors)

        if error is not None:
            return web.HTTPBadRequest(text=error)

        return web.json_response({"result": metrics, "page": {"pagesCount": 1, "totalCount": len(metrics)}})

    async def metrics_get(self, request):
        cluster = request.rel_url.query.get('cluster', None) or request.rel_url.query['folderId']
        project = request.rel_url.query.get('project', cluster)
        service = request.rel_url.query['service']

        shard = self._get_shard(project, cluster, service)
        if shard is None:
            return web.HTTPNotFound(text=f"Unable to find shard {project}/{cluster}/{service}")
        reply = shard.as_text()
        return web.json_response(text=reply)

    async def metrics_post(self, request):
        project = request.rel_url.query['project']
        cluster = request.rel_url.query['cluster']
        service = request.rel_url.query['service']

        metrics_json = json.loads(await request.read())

        shard = self._get_shard(project, cluster, service)
        shard.add_parsed_metrics(metrics_json)

        return web.Response(status=200)

    async def cleanup(self, request):
        cluster = request.rel_url.query.get('cluster', None) or request.rel_url.query.get('folderId', None)
        project = request.rel_url.query.get('project', cluster)
        service = request.rel_url.query.get('service', None)
        request.app["features"] = {"response_code": 200}

        if project is None and cluster is None and service is None:
            self._data.clear()
        else:
            self._data.delete(project, cluster, service)
        return web.Response(status=200)

    async def config(self, request):
        request.app["features"] = json.loads(await request.read())
        return web.Response(status=200)


class DataService(DataServiceServicer):
    def __init__(self, emulator):
        self._emulator = emulator

    def Read(self, request: ReadRequest, context) -> ReadResponse:
        logger.debug('ReadRequest: %s', request)

        if request.container.HasField("project_id") and request.container.project_id in Shard.DEPRECATED_TESTS_PROJECTS:
            return self.DeprecatedTestsLogic(request, context)

        selectors, success = _parse_selectors(str(request.queries[0].value))

        if not success:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("Coulnd't parse selectors")
            return ReadResponse()

        if request.container.HasField("project_id"):
            selectors["project"] = request.container.project_id
        else:
            del selectors["folderId"]
            selectors["project"] = request.container.folder_id
            selectors["cluster"] = request.container.folder_id

        if "project" not in selectors or "cluster" not in selectors or "service" not in selectors:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("Selectors should contain ['project', 'cluster', 'service'] labels")
            return ReadResponse()

        project = selectors["project"]
        cluster = selectors["cluster"]
        service = selectors["service"]

        shard = self._emulator._get_shard(project, cluster, service)
        result, error = shard.get_data(selectors, request.from_time, request.to_time, request.downsampling)

        if len(error):
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(error)
            return ReadResponse()

        return self._build_read_response(result["labels"], result["type"], result["timestamps"], result["values"])

    def DeprecatedTestsLogic(self, request: ReadRequest, context):
        project = request.container.project_id
        if project == "invalid":
            logger.debug("invalid project_id, sending error")
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(f"Project {project} does not exist")
            return ReadResponse()

        if project == "my_project" or project == "hist":
            labels = self._dict_to_labels(request)
            labels["project"] = project
            return self._build_read_response(labels, "RATE", [10000, 20000, 30000], [100, 200, 300])

    @staticmethod
    def _map_metric_type(kind):
        if (kind == "DGAUGE"):
            return MetricType.DGAUGE
        elif (kind == "IGAUGE"):
            return MetricType.IGAUGE
        elif (kind == "COUNTER"):
            return MetricType.COUNTER
        else:
            return MetricType.RATE

    @staticmethod
    def _dict_to_labels(request):
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

    @staticmethod
    def _build_read_response(labels, type, timestamps, values):
        response = ReadResponse()

        response_query = response.response_per_query.add()
        response_query.query_name = "query"

        timeseries = response_query.timeseries_vector.values.add()
        for key, value in labels.items():
            timeseries.labels[key] = str(value)
        timeseries.type = DataService._map_metric_type(type)

        timeseries.timestamp_values.values.extend(timestamps)
        timeseries.double_values.values.extend(values)

        return response


def create_web_app(emulator):
    webapp = web.Application()
    webapp.add_routes([
        web.post("/api/v2/projects/{project}/sensors/names", emulator.sensor_names_post),
        web.get("/api/v2/projects/{project}/sensors/names", emulator.sensor_names_get),
        web.post("/api/v2/projects/{project}/sensors/labels", emulator.sensor_labels_post),
        web.get("/api/v2/projects/{project}/sensors/labels", emulator.sensor_labels_get),
        web.post("/api/v2/projects/{project}/sensors", emulator.sensors_post),
        web.get("/api/v2/projects/{project}/sensors", emulator.sensors_get),
        web.get("/metrics/get", emulator.metrics_get),
        web.get("/ping", emulator.get_ping),
        web.post("/api/v2/push", emulator.api_v2_push),
        web.post("/monitoring/v2/data/write", emulator.data_write),
        web.post("/metrics/post", emulator.metrics_post),
        web.post("/cleanup", emulator.cleanup),
        web.post("/config", emulator.config)
    ])

    return webapp


def create_grpc_server(emulator, port):
    grpc_server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
    add_DataServiceServicer_to_server(
        DataService(emulator), grpc_server
    )
    grpc_server.add_insecure_port(f'[::]:{port}')

    return grpc_server


def run_web_app(config, http_port, grpc_port):
    emulator = SolomonEmulator(config)

    app = create_web_app(emulator)
    server = create_grpc_server(emulator, grpc_port)

    server.start()
    web.run_app(app, port=http_port)
