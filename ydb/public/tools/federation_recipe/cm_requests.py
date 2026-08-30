# import grpc
# import logging

# from ydb.public.tools.federation_recipe.proto import config_manager_admin_pb2
# from ydb.public.tools.federation_recipe.proto import config_manager_service_pb2_grpc as config_manager_pb2_grpc
# from ydb.public.tools.federation_recipe.proto import config_manager_admin_service_pb2_grpc as config_manager_admin_pb2_grpc
# from ydb.public.tools.federation_recipe.proto import ydb_operation_pb2
# from ydb.public.tools.federation_recipe.proto import ydb_status_codes_pb2
# from ydb.public.tools.federation_recipe.proto import common_pb2

# logger = logging.getLogger(__name__)


# def request_create_cluster(name, port):
#     request = config_manager_admin_pb2.SingleModifyRequest()
#     request.create_cluster.name = name
#     request.create_cluster.properties.balancer.user_defined = 'localhost:{}'.format(port)
#     request.create_cluster.properties.write_enabled.user_defined = True
#     request.create_cluster.properties.apply_changes_enabled.user_defined = True

#     request.create_cluster.admin_properties.zk_proxy.user_defined = ''
#     request.create_cluster.admin_properties.write_speed_capacity.user_defined = 1000000000000000
#     request.create_cluster.admin_properties.read_speed_capacity.user_defined = 1000000000000000
#     request.create_cluster.admin_properties.kikimr_msgbus_in_flight.user_defined = 1024
#     request.create_cluster.admin_properties.kikimr_msgbus_max_msg_size.user_defined = 136314880
#     request.create_cluster.admin_properties.mirroring_enabled.user_defined = False
#     request.create_cluster.admin_properties.mirroring_max_delay_threshold.user_defined = 499999
#     request.create_cluster.admin_properties.mirroring_max_message_lag.user_defined = 1200
#     request.create_cluster.admin_properties.mirroring_partitions_per_fetcher.user_defined = 0
#     request.create_cluster.admin_properties.weight.user_defined = 1000
#     request.create_cluster.admin_properties.kikimr_host.user_defined = ''
#     request.create_cluster.admin_properties.kikimr_port.user_defined = 0
#     request.create_cluster.admin_properties.ydb_location.user_defined = "test"

#     return request


# def request_create_account_template():
#     request = config_manager_admin_pb2.SingleModifyRequest()
#     request.create_account_template.name = 'default'
#     request.create_account_template.properties.topic_partitions_count_max.user_defined = 10
#     request.create_account_template.properties.topic_partitions_count_sum.user_defined = 100
#     request.create_account_template.properties.topics_count.user_defined = 10
#     request.create_account_template.properties.consumers_count.user_defined = 20
#     request.create_account_template.properties.directories_count.user_defined = 10
#     request.create_account_template.properties.max_metadata_per_entry.user_defined = 10

#     request.create_account_template.admin_properties.topic_templates_count.user_defined = 1
#     request.create_account_template.admin_properties.consumer_templates_count.user_defined = 10
#     request.create_account_template.admin_properties.clusters_count.user_defined = 3
#     request.create_account_template.admin_properties.accounts_count.user_defined = 100
#     request.create_account_template.admin_properties.account_templates_count.user_defined = 1

#     return request


# def request_create_consumer_template():
#     request = config_manager_admin_pb2.SingleModifyRequest()
#     request.create_consumer_template.name = 'default'
#     request.create_consumer_template.properties.important.user_defined = False
#     request.create_consumer_template.properties.limits_mode.user_defined = 'wait'
#     request.create_consumer_template.properties.supported_codecs.user_defined = 'raw, gzip, lzop'
#     request.create_consumer_template.properties.supported_format_version.user_defined = 0
#     request.create_consumer_template.properties.responsible.user_defined = 'admin'

#     request.create_consumer_template.admin_properties.max_delay_threshold_sec.user_defined = 86400000000000
#     request.create_consumer_template.admin_properties.max_message_lags.user_defined = 100000000000000
#     request.create_consumer_template.admin_properties.max_read_rules.user_defined = 2000
#     request.create_consumer_template.admin_properties.max_partitions.user_defined = 20000

#     return request


# def request_create_topic_template():
#     request = config_manager_admin_pb2.SingleModifyRequest()
#     request.create_topic_template.name = 'default'
#     request.create_topic_template.properties.partitions_count.user_defined = 1
#     request.create_topic_template.properties.retention_period_sec.user_defined = 129600
#     request.create_topic_template.properties.allow_unauthenticated_read.user_defined = True
#     request.create_topic_template.properties.allow_unauthenticated_write.user_defined = True
#     request.create_topic_template.properties.responsible.user_defined = 'admin'
#     request.create_topic_template.properties.supported_format_version.user_defined = 0
#     request.create_topic_template.properties.supported_codecs.user_defined = 'raw, gzip, lzop'
#     request.create_topic_template.properties.auto_partitioning_strategy.user_defined = 'disabled'

#     request.create_topic_template.admin_properties.max_message_size.user_defined = 12582912
#     request.create_topic_template.admin_properties.max_disk_size.user_defined = 9223372036854775807
#     request.create_topic_template.admin_properties.partitions_per_tablet.user_defined = 2
#     request.create_topic_template.admin_properties.max_partition_write_speed.user_defined = 2097152

#     return request


# def request_create_topic(path, partitions=1):
#     request = config_manager_admin_pb2.SingleModifyRequest()
#     request.create_topic.path.path = path
#     request.create_topic.parent_template = 'default'
#     request.create_topic.properties.partitions_count.user_defined = partitions
#     return request


# def request_remove_topic(path):
#     request = config_manager_admin_pb2.SingleModifyRequest()
#     request.remove_topic.path.path = path
#     return request


# def request_make_directory(path):
#     request = config_manager_admin_pb2.SingleModifyRequest()
#     request.create_directory.path.path = path
#     return request


# def request_create_consumer(path):
#     request = config_manager_admin_pb2.SingleModifyRequest()
#     request.create_consumer.path.path = path
#     request.create_consumer.parent_template = 'default'
#     return request


# def request_create_account(account_name):
#     request = config_manager_admin_pb2.SingleModifyRequest()
#     request.create_account.name = account_name
#     request.create_account.parent_template = 'default'
#     request.create_account.properties.abc_service.user_defined = 'abc_service'
#     request.create_account.properties.abc_id.user_defined = 1
#     request.create_account.properties.responsible.user_defined = account_name
#     return request


# def request_disable_cluster(name):
#     request = config_manager_admin_pb2.SingleModifyRequest()
#     request.update_cluster.name = name
#     request.update_cluster.properties.write_enabled.user_defined = False
#     return request


# def request_enable_cluster(name):
#     request = config_manager_admin_pb2.SingleModifyRequest()
#     request.update_cluster.name = name
#     request.update_cluster.properties.write_enabled.user_defined = True
#     return request


# class CMApiHelper(object):
#     def __init__(self, endpoint):
#         self.__channel = grpc.insecure_channel(endpoint)
#         self.__admin_stub = config_manager_admin_pb2_grpc.ConfigurationManagerAdminServiceStub(self.__channel)
#         self.__user_stub = config_manager_pb2_grpc.ConfigurationManagerServiceStub(self.__channel)

#     def list_directory(self, path):
#         request = config_manager_admin_pb2.ListDirectoryRequest()
#         request.path.path = path
#         response = self.__admin_stub.ListDirectory(request)
#         logger.debug('list dir {} response: {}'.format(path, response))
#         result = config_manager_admin_pb2.ListDirectoryResult()
#         ok = self.get_final_result(response, result)
#         assert ok
#         logger.debug('list dir result: {}'.format(result))
#         return list(result.children)

#     def exec_request(self, actions):
#         request = config_manager_admin_pb2.ExecuteModifyCommandsRequest()
#         for action in actions:
#             request.actions.append(action)

#         request.comment = '###'
#         response = self.__admin_stub.ExecuteModifyCommands(request)
#         logger.debug('CM exec modify request response: {}'.format(response))
#         result = common_pb2.ExecuteModifyCommandsResult()
#         ok = self.get_final_result(response, result)
#         assert ok
#         return result

#     def get_final_result(self, response, result):
#         if response.operation.ready:
#             logger.debug('operation with response {} already ended'.format(response))
#         else:
#             retries_count = 3
#             logger.debug('start join operation with response {}'.format(response))
#             while retries_count:
#                 operation_request = ydb_operation_pb2.GetOperationRequest()
#                 operation_request.id = response.operation.id

#                 operation_response = self.__admin_stub.GetOperation(operation_request)
#                 logger.debug('operation response - {}'.format(operation_response))
#                 if operation_response.operation.ready:
#                     response = operation_response
#                     break
#                 retries_count -= 1
#             logger.debug('end join operation, final response is {}'.format(response))
#             if not retries_count:
#                 return False
#         assert response.operation.status == ydb_status_codes_pb2.StatusIds.StatusCode.SUCCESS, \
#             'issues: {}'.format(response.operation.issues)

#         response.operation.result.Unpack(result)
#         return True
