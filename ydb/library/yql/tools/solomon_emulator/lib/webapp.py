from aiohttp import web
import json
import logging

from library.python.monlib.encoder import loads
from .multi_shard import MultiShard

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


def _dict_to_labels(body):
    """
    Possible input
    {
    "downsampling": {
         "aggregation": "MAX",
         "disabled": true,
         "fill": "PREVIOUS",
         "gridMillis": 3600000,
         "ignoreMinStepMillis": true,
         "maxPoints": 500
     },
     "forceCluster": "",
     "from": "2023-12-08T14:40:39Z",
     "program": "{execpool=User,activity=YQ_STORAGE_PROXY,sensor=ActorsAliveByActivity}",
     "to": "2023-12-08T14:45:39Z"
     })";

    :param body:
    :return:
    """
    result = dict()
    for key, value in body.items():

        if isinstance(value, dict):
            sublabels = _dict_to_labels(value)
            for subkey, subvalue in sublabels.items():
                result[f"{key}.{subkey}"] = subvalue
            continue

        label_name = key
        if key == "program":
            label_value = f"program length {str(len(value))}"
        else:
            if isinstance(value, bool):
                label_value = f"bool {value}"
            elif isinstance(value, int):
                label_value = f"int {value}"
            else:
                label_value = str(value)

        result[label_name] = label_value
    return result


@routes.post("/api/v2/projects/{project}/sensors/data")
async def sensors_data(request):
    project = request.match_info["project"]
    if project == "invalid":
        return web.HTTPNotFound(text=f"Project {project} does not exist")

    if project == "broken_json":
        return web.Response(text="{ broken json", content_type="application/json")

    labels = _dict_to_labels(await request.json())
    labels["project"] = project
    return web.json_response({"vector": [
        {
            "timeseries": {
                "kind": "MY_KIND",
                "type": "MY_TYPE",
                "labels": labels,
                "timestamps": [1, 2, 3],
                "values": [100, 200, 300],
            }
        }
    ]})


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


def handle_auth(request):
    config = _config_from_request(request)
    if config.auth:
        auth_header = request.headers.get('AUTHORIZATION')
        if not config.auth == auth_header:
            logging.debug(f"Authorization header {auth_header} mismatches expected value {config.auth}")
            raise web.HTTPForbidden()


def create_web_app(config):
    webapp = web.Application()
    webapp.add_routes(routes)
    webapp["config"] = config
    webapp["data"] = MultiShard()
    return webapp


def run_web_app(config, port):
    web.run_app(create_web_app(config), port=port)
