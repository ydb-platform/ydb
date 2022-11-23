import logging
import copy

import ydb.tests.library.common.protobuf_cms as cms_pb
from ydb.core.protos import cms_pb2

logger = logging.getLogger(__name__)


def request_increase_ratio_limit(client):
    req = cms_pb.CmsConfigRequest()
    client.send_request(req.protobuf, "CmsRequest")


def request_nodes(client, node_ids, mode, type):
    req = cms_pb.CmsPermissionRequest()
    for node_id in node_ids:
        req.add_action(str(node_id), type)
    req.set_mode(mode)

    logger.info("Sending permission request to CMS: %s", req.protobuf)
    resp = client.send_request(req.protobuf, "CmsRequest")
    logger.info("Got response from CMS: %s", resp.PermissionResponse)

    nodes = []
    for perm in resp.PermissionResponse.Permissions:
        nodes.append(int(perm.Action.Host))

    return nodes


def request_shutdown_nodes(client, node_ids, mode):
    return request_nodes(client, node_ids, mode, cms_pb2.TAction.SHUTDOWN_HOST)


def request_restart_services(client, node_ids, mode):
    return request_nodes(client, node_ids, mode, cms_pb2.TAction.RESTART_SERVICES)


def request_as_much_as_possible(client, node_ids, mode, type):
    not_allowed = copy.deepcopy(list(node_ids))
    restart_nodes = []
    allowed_nodes = request_nodes(client, node_ids, mode, type)

    while len(allowed_nodes) > 0:
        restart_nodes.extend(allowed_nodes)

        for node_id in allowed_nodes:
            not_allowed.remove(node_id)

        allowed_nodes = request_nodes(client, not_allowed, mode, type)

    return restart_nodes


def request_shutdown_as_much_as_possible(client, node_ids, mode):
    return request_as_much_as_possible(client, node_ids, mode, cms_pb2.TAction.SHUTDOWN_HOST)
