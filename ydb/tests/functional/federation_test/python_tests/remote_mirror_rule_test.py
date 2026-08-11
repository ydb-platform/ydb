# import logging
# import os
# import signal
# import time

# import ydb
# import yatest.common
# from yatest.common import process
# from library.python import port_manager
# from library.python.testing.recipe import declare_recipe, set_env
# from ydb.public.tools.federation_recipe.proto.logbroker.public.api.admin import config_manager_admin_pb2
#   from ydb.public.tools.federation_recipe.proto.logbroker.public.api.grpc import config_manager_admin_pb2_grpc
#   from ydb.public.tools.federation_recipe.proto.logbroker.public.api.common import (
#       common_pb2,
#       ydb_operation_pb2,
#       ydb_status_codes_pb2,
#   )

# _PROD_DATABASE = "/Root/logbroker-federation/prod"
# _TOPIC_PATH    = "topic"
# _CONSUMER      = "consumer"
# _TOKEN         = "root@builtin"

# def _make_driver(port, database=_PROD_DATABASE):
#       cfg = ydb.DriverConfig(endpoint="localhost:{}".format(port), database=database)
#       driver = ydb.Driver(cfg)
#       driver.wait(timeout=10)
#       return driver

# def _wait_operation(stub, operation, timeout=30):
#       deadline = time.time() + timeout
#       while not operation.ready and time.time() < deadline:
#           time.sleep(0.5)
#           req = ydb_operation_pb2.GetOperationRequest()
#           req.id = operation.id
#           resp = stub.GetOperation(req)
#           operation = resp.operation
#       return operation


# def _exec_cm(stub, actions, comment="unittest"):
#     req = config_manager_admin_pb2.ExecuteModifyCommandsRequest()
#     req.comment = comment
#     for a in actions:
#         req.actions.append(a)
#     resp = stub.ExecuteModifyCommands(req)
#     op = _wait_operation(stub, resp.operation)
#     assert op.ready, "CM operation never became ready"
#     assert op.status == ydb_status_codes_pb2.StatusIds.SUCCESS, \
#         "CM error: {}".format(op.issues)
#     result = common_pb2.ExecuteModifyCommandsResult()
#     op.result.Unpack(result)
#     return result

#  class TestRemoteMirrorRule(object):

#       def test_create_remote_mirror_rule(self):
#           cm_port = os.environ["CM_PORT"]
#           port_a = os.environ["cluster_a_port"]

#           channel = grpc.insecure_channel("localhost:{}".format(cm_port))
#           stub = config_manager_admin_pb2_grpc.ConfigurationManagerAdminServiceStub(channel)

#           action = config_manager_admin_pb2.SingleModifyRequest()
#           rmr = action.create_remote_mirror_rule

#           rmr.remote_mirror_rule.topic.path = "prod/{}".format(_TOPIC_PATH)
#           rmr.remote_mirror_rule.cluster.cluster = "cluster_b"

#           rmr.properties.src_cluster_endpoint.user_defined = "localhost:{}".format(port_a)
#           rmr.properties.src_database.user_defined = "/Root"
#           rmr.properties.src_topic.user_defined = "prod/{}".format(_TOPIC_PATH)
#           rmr.properties.src_consumer.user_defined = _CONSUMER
#           rmr.properties.credentials.oauth_token = _TOKEN

#           _exec_cm(stub, [action], comment="test: create remote mirror rule")
