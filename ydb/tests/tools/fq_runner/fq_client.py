#!/usr/bin/env python
# -*- coding: utf-8 -*-

import grpc
import logging
import time
import uuid
import traceback

import library.python.retry as retry

from ydb.public.api.grpc.draft.fq_v1_pb2_grpc import FederatedQueryServiceStub
import ydb.public.api.protos.draft.fq_pb2 as fq

import ydb.tests.library.common.yatest_common as yatest_common

from google.protobuf.duration_pb2 import Duration
from google.protobuf.timestamp_pb2 import Timestamp

from ydb.tests.tools.fq_runner.kikimr_runner import StreamingOverKikimr

final_statuses = [fq.QueryMeta.COMPLETED, fq.QueryMeta.FAILED, fq.QueryMeta.ABORTED_BY_SYSTEM,
                  fq.QueryMeta.ABORTED_BY_USER, fq.QueryMeta.PAUSED]

CONTROL_PLANE_REQUEST_TIMEOUT = yatest_common.plain_or_under_sanitizer(30.0, 60.0)


class FederatedQueryException(Exception):
    def __init__(self, issues, result=None):
        super(FederatedQueryException, self).__init__(str(issues))
        self.result = result


class StreamingDisposition(object):
    @staticmethod
    def oldest():
        disposition = fq.StreamingDisposition()
        disposition.oldest.SetInParent()
        return disposition

    @staticmethod
    def fresh():
        disposition = fq.StreamingDisposition()
        disposition.fresh.SetInParent()
        return disposition

    @staticmethod
    def from_time(seconds):  # time()
        disposition = fq.StreamingDisposition()
        t = Timestamp()
        t.FromMilliseconds(int(seconds * 1000))
        disposition.from_time.timestamp = t
        return disposition

    @staticmethod
    def time_ago(seconds):  # time()
        disposition = fq.StreamingDisposition()
        d = Duration()
        d.FromMilliseconds(int(seconds * 1000))
        disposition.time_ago.duration = d
        return disposition

    @staticmethod
    def from_last_checkpoint(force=False):
        disposition = fq.StreamingDisposition()
        disposition.from_last_checkpoint.SetInParent()
        disposition.from_last_checkpoint.force = force
        return disposition


class AuthMethod(object):
    @staticmethod
    def no_auth():
        auth = fq.IamAuth()
        auth.none.SetInParent()
        return auth

    @staticmethod
    def current_iam():
        auth = fq.IamAuth()
        auth.current_iam.SetInParent()
        return auth

    @staticmethod
    def service_account(sa_id):
        auth = fq.IamAuth()
        auth.service_account.id = sa_id
        return auth


class FederatedQueryClient(object):
    class Response(object):
        def __init__(self, issues, result=None, check_issues=False):
            if check_issues and issues:
                raise FederatedQueryException(issues, result)
            self.issues = issues
            self.result = result

    retry_conf = retry.RetryConf(logger=retry.LOGGER).upto(seconds=60).waiting(0.2)
    _pretty_map = {}

    def __init__(self, folder_id, streaming_over_kikimr, secure_connection=False, credentials=None):
        if isinstance(streaming_over_kikimr, StreamingOverKikimr):
            endpoint = streaming_over_kikimr.endpoint()
        else:
            endpoint = str(streaming_over_kikimr)
        if secure_connection:
            self.channel = grpc.secure_channel(endpoint, credentials=grpc.ssl_channel_credentials(), options=())
        else:
            self.channel = grpc.insecure_channel(endpoint, options=())
        self.service = FederatedQueryServiceStub(self.channel)
        self.folder_id = folder_id
        self.credentials = credentials

    def _pretty_retry(self, *args):
        for arg in args:
            s = str(arg)
            if s != "":
                break
        else:
            return ""
        key = str(traceback.extract_stack(None, 2)[0])
        if self._pretty_map.get(key, "") == s:
            return "... (repeated, see above) ..."
        else:
            self._pretty_map[key] = s
            return s

    def _create_meta(self):
        scope_meta = [
            ('x-ydb-fq-project', 'yandexcloud://{folder_id}'.format(folder_id=self.folder_id))
        ]
        if self.credentials:
            auth_meta = self.credentials.auth_metadata()
        else:
            auth_meta = [
                ('x-ydb-auth-ticket', 'root@builtin'),
            ]
        return auth_meta + scope_meta

    @retry.retry_intrusive
    def create_query_impl(self, name, text, type=fq.QueryContent.QueryType.ANALYTICS, mode=fq.ExecuteMode.RUN,
                          visibility=fq.Acl.Visibility.PRIVATE, streaming_disposition=None, check_issues=True,
                          automatic=False, idempotency_key=None, pg_syntax=False, execution_ttl=0, vcpu_time_limit=0, parameters=None):
        request = fq.CreateQueryRequest()
        request.execute_mode = mode
        request.content.type = type
        request.content.name = name
        d = Duration()
        d.FromMilliseconds(execution_ttl)
        request.content.limits.execution_timeout.CopyFrom(d)
        request.content.syntax = fq.QueryContent.QuerySyntax.PG if pg_syntax else fq.QueryContent.QuerySyntax.YQL_V1
        request.content.text = text
        request.content.acl.visibility = visibility
        if streaming_disposition is not None:
            request.disposition.CopyFrom(streaming_disposition)
        if idempotency_key is not None:
            request.idempotency_key = idempotency_key
        request.content.automatic = automatic

        if vcpu_time_limit:
            request.content.limits.vcpu_time_limit = vcpu_time_limit

        if parameters is not None:
            for k, v in parameters.items():
                request.content.parameters[k].CopyFrom(v)

        logging.debug("Request: {}".format(request))

        response = self.service.CreateQuery(
            request,
            metadata=self._create_meta(),
            timeout=CONTROL_PLANE_REQUEST_TIMEOUT,
        )

        result = fq.CreateQueryResult()
        response.operation.result.Unpack(result)
        logging.debug("Result: {}".format(self._pretty_retry(result, response.operation.issues)))
        return FederatedQueryClient.Response(response.operation.issues, result, check_issues)

    def create_query(self, name, text, type=fq.QueryContent.QueryType.ANALYTICS, mode=fq.ExecuteMode.RUN,
                     visibility=fq.Acl.Visibility.PRIVATE, streaming_disposition=None, check_issues=True,
                     automatic=False, idempotency_key=None, pg_syntax=False, execution_ttl=0, vcpu_time_limit=0, parameters=None):
        idempotency_key_for_retries = idempotency_key if idempotency_key is not None else str(uuid.uuid4())

        return self.create_query_impl(name=name,
                                      text=text,
                                      type=type,
                                      mode=mode,
                                      visibility=visibility,
                                      streaming_disposition=streaming_disposition,
                                      check_issues=check_issues,
                                      automatic=automatic,
                                      idempotency_key=idempotency_key_for_retries,
                                      pg_syntax=pg_syntax, execution_ttl=execution_ttl,
                                      vcpu_time_limit=vcpu_time_limit,
                                      parameters=parameters)

    @retry.retry_intrusive
    def modify_query(self, query_id, name, text, type=fq.QueryContent.QueryType.ANALYTICS,
                     execute_mode=fq.ExecuteMode.RUN,
                     visibility=fq.Acl.Visibility.PRIVATE, state_load_mode=fq.StateLoadMode.EMPTY,
                     streaming_disposition=None, check_issues=True, parameters=None):

        request = fq.ModifyQueryRequest()
        request.query_id = query_id
        request.execute_mode = execute_mode
        request.state_load_mode = state_load_mode
        request.content.type = type
        request.content.name = name
        request.content.text = text
        request.content.acl.visibility = visibility
        if streaming_disposition is not None:
            request.disposition.CopyFrom(streaming_disposition)

        if parameters is not None:
            for k, v in parameters.items():
                request.content.parameters[k].CopyFrom(v)

        logging.debug("Request: {}".format(self._pretty_retry(request)))

        response = self.service.ModifyQuery(
            request,
            metadata=self._create_meta(),
            timeout=CONTROL_PLANE_REQUEST_TIMEOUT
        )
        result = fq.ModifyQueryResult()
        response.operation.result.Unpack(result)
        logging.debug("Result: {}".format(self._pretty_retry(result)))
        return FederatedQueryClient.Response(response.operation.issues, result, check_issues)

    @retry.retry_intrusive
    def control_query(self, query_id, action, check_issues=True):
        request = fq.ControlQueryRequest()
        request.query_id = query_id
        request.action = action

        response = self.service.ControlQuery(
            request,
            metadata=self._create_meta(),
            timeout=CONTROL_PLANE_REQUEST_TIMEOUT,
        )

        result = fq.ControlQueryResult()
        response.operation.result.Unpack(result)
        logging.debug("Result: {}".format(self._pretty_retry(result)))
        return FederatedQueryClient.Response(response.operation.issues, result, check_issues)

    def abort_query(self, query_id, check_issues=True):
        self.control_query(query_id, action=fq.QueryAction.ABORT, check_issues=check_issues)

    @retry.retry_intrusive
    def describe_query(self, query_id, check_issues=True):
        request = fq.DescribeQueryRequest()
        request.query_id = query_id

        response = self.service.DescribeQuery(
            request,
            metadata=self._create_meta(),
            timeout=CONTROL_PLANE_REQUEST_TIMEOUT,
        )

        result = fq.DescribeQueryResult()
        response.operation.result.Unpack(result)
        logging.debug("Result: {}".format(self._pretty_retry(result)))
        return FederatedQueryClient.Response(response.operation.issues, result, check_issues)

    @retry.retry_intrusive
    def get_query_status(self, query_id, check_issues=True):
        request = fq.GetQueryStatusRequest()
        request.query_id = query_id

        response = self.service.GetQueryStatus(
            request,
            metadata=self._create_meta(),
            timeout=CONTROL_PLANE_REQUEST_TIMEOUT,
        )

        result = fq.GetQueryStatusResult()
        response.operation.result.Unpack(result)
        logging.debug("Result: {}".format(self._pretty_retry(result)))
        return result.status

    # TODO: merge wait_query() and wait_query_status
    def wait_query(self, query_id, timeout=yatest_common.plain_or_under_sanitizer(40, 200), statuses=final_statuses):
        start = time.time()
        deadline = start + timeout
        while True:
            response = self.describe_query(query_id)
            status = response.result.query.meta.status
            if status in statuses:
                return response.result
            assert time.time() < deadline and status not in final_statuses, \
                "Query {} is not in expected status ({}) for already {} seconds. Query status: {}. " \
                "Issues: {}. Transient issues: {}".format(
                    query_id,
                    [fq.QueryMeta.ComputeStatus.Name(s) for s in statuses],
                    time.time() - start,
                    fq.QueryMeta.ComputeStatus.Name(status),
                    response.result.query.issue,
                    response.result.query.transient_issue
                )
            time.sleep(yatest_common.plain_or_under_sanitizer(0.5, 2))

    # Wait query status or one of statuses in list
    def wait_query_status(self, query_id, expected_status, timeout=yatest_common.plain_or_under_sanitizer(60, 150)):
        statuses = expected_status if isinstance(expected_status, list) else [expected_status]
        return self.wait_query(query_id, timeout, statuses=statuses).query.meta.status

    @retry.retry_intrusive
    def get_result_data(self, query_id, result_set_index=0, limit=10, check_issues=True):
        request = fq.GetResultDataRequest()
        request.query_id = query_id
        request.result_set_index = result_set_index
        request.limit = limit

        response = self.service.GetResultData(
            request,
            metadata=self._create_meta(),
            timeout=CONTROL_PLANE_REQUEST_TIMEOUT,
        )

        result = fq.GetResultDataResult()
        response.operation.result.Unpack(result)
        return FederatedQueryClient.Response(response.operation.issues, result, check_issues)

    def test_connection(self, request, check_issues=True):
        response = self.service.TestConnection(
            request,
            metadata=self._create_meta(),
            timeout=CONTROL_PLANE_REQUEST_TIMEOUT,
        )
        result = fq.TestConnectionResult()
        response.operation.result.Unpack(result)
        return FederatedQueryClient.Response(response.operation.issues, result, check_issues)

    @retry.retry_intrusive
    def test_ydb_connection(self, database, endpoint, auth_method=AuthMethod.no_auth(), check_issues=True):
        request = fq.TestConnectionRequest()
        ydb = request.setting.ydb_database
        ydb.database = database
        ydb.endpoint = endpoint

        ydb.auth.CopyFrom(auth_method)
        return self.test_connection(request, check_issues)

    @retry.retry_intrusive
    def test_yds_connection(self, database=None, endpoint=None, database_id=None, auth_method=AuthMethod.no_auth(),
                            check_issues=True):
        assert (database_id is not None and database is None and endpoint is None) or (
            database_id is None and database is not None and endpoint is not None)
        request = fq.TestConnectionRequest()
        yds = request.setting.data_streams
        if database_id is not None:
            yds.database_id = database_id
        else:
            yds.database = database
            yds.endpoint = endpoint

        yds.auth.CopyFrom(auth_method)
        return self.test_connection(request, check_issues)

    @retry.retry_intrusive
    def test_monitoring_connection(self, project, cluster, auth_method=AuthMethod.no_auth(), check_issues=True):
        request = fq.TestConnectionRequest()
        monitoring = request.setting.monitoring
        monitoring.project = project
        monitoring.cluster = cluster

        monitoring.auth.CopyFrom(auth_method)
        return self.test_connection(request, check_issues)

    @retry.retry_intrusive
    def test_storage_connection(self, bucket, auth_method=AuthMethod.no_auth(), check_issues=True):
        request = fq.TestConnectionRequest()
        object_storage = request.setting.object_storage
        object_storage.bucket = bucket

        object_storage.auth.CopyFrom(auth_method)
        return self.test_connection(request, check_issues)

    def create_connection(self, request, check_issues=True):
        response = self.service.CreateConnection(
            request,
            metadata=self._create_meta(),
            timeout=CONTROL_PLANE_REQUEST_TIMEOUT,
        )
        result = fq.CreateConnectionResult()
        response.operation.result.Unpack(result)
        return FederatedQueryClient.Response(response.operation.issues, result, check_issues)

    @retry.retry_intrusive
    def create_ydb_connection(self, name, database_id,
                              secure=False, visibility=fq.Acl.Visibility.PRIVATE, auth_method=AuthMethod.service_account('sa'), check_issues=True):
        request = fq.CreateConnectionRequest()
        request.content.name = name
        ydb = request.content.setting.ydb_database
        ydb.database_id = database_id
        ydb.secure = secure

        ydb.auth.CopyFrom(auth_method)
        request.content.acl.visibility = visibility
        return self.create_connection(request, check_issues)

    @retry.retry_intrusive
    def create_yds_connection(self, name, database=None, endpoint=None, database_id=None,
                              visibility=fq.Acl.Visibility.PRIVATE, auth_method=AuthMethod.no_auth(),
                              check_issues=True):
        assert (database_id is not None and database is None and endpoint is None) or (
            database_id is None and database is not None and endpoint is not None)
        request = fq.CreateConnectionRequest()
        request.content.name = name
        yds = request.content.setting.data_streams
        if database_id is not None:
            yds.database_id = database_id
        else:
            yds.database = database
            yds.endpoint = endpoint

        yds.auth.CopyFrom(auth_method)
        request.content.acl.visibility = visibility
        return self.create_connection(request, check_issues)

    @retry.retry_intrusive
    def create_clickhouse_connection(self, name, database_name, database_id, login, password,
                                     secure=False, visibility=fq.Acl.Visibility.PRIVATE, auth_method=AuthMethod.service_account('sa'), check_issues=True):
        request = fq.CreateConnectionRequest()
        request.content.name = name
        ch = request.content.setting.clickhouse_cluster
        ch.database_name = database_name
        ch.database_id = database_id
        ch.secure = secure
        ch.login = login
        ch.password = password

        ch.auth.CopyFrom(auth_method)
        request.content.acl.visibility = visibility
        return self.create_connection(request, check_issues)

    @retry.retry_intrusive
    def create_greenplum_connection(self, name, database_name, database_id, login, password,
                                     secure=False, visibility=fq.Acl.Visibility.PRIVATE, auth_method=AuthMethod.service_account('sa'), check_issues=True):
        request = fq.CreateConnectionRequest()
        request.content.name = name
        gp = request.content.setting.greenplum_cluster
        gp.database_name = database_name
        gp.database_id = database_id
        gp.login = login
        gp.password = password

        gp.auth.CopyFrom(auth_method)
        request.content.acl.visibility = visibility
        return self.create_connection(request, check_issues)

    @retry.retry_intrusive
    def create_postgresql_connection(self, name, database_name, database_id, login, password,
                                     secure=False, visibility=fq.Acl.Visibility.PRIVATE, auth_method=AuthMethod.service_account('sa'), check_issues=True):
        request = fq.CreateConnectionRequest()
        request.content.name = name
        pg = request.content.setting.postgresql_cluster
        pg.database_name = database_name
        pg.database_id = database_id
        pg.secure = secure
        pg.login = login
        pg.password = password

        pg.auth.CopyFrom(auth_method)
        request.content.acl.visibility = visibility
        return self.create_connection(request, check_issues)

    @retry.retry_intrusive
    def list_connections(self, visibility, name_substring=None, limit=100, check_issues=True, page_token=""):
        request = fq.ListConnectionsRequest()
        request.filter.visibility = visibility
        request.limit = limit
        request.page_token = page_token
        if name_substring:
            request.filter.name = name_substring
        response = self.service.ListConnections(
            request,
            metadata=self._create_meta(),
            timeout=CONTROL_PLANE_REQUEST_TIMEOUT,
        )
        result = fq.ListConnectionsResult()
        response.operation.result.Unpack(result)
        return FederatedQueryClient.Response(response.operation.issues, result, check_issues)

    @retry.retry_intrusive
    def list_bindings(self, visibility, name_substring=None, limit=100, check_issues=True, page_token=""):
        request = fq.ListBindingsRequest()
        request.filter.visibility = visibility
        request.limit = limit
        request.page_token = page_token
        if name_substring:
            request.filter.name = name_substring
        response = self.service.ListBindings(
            request,
            metadata=self._create_meta(),
            timeout=CONTROL_PLANE_REQUEST_TIMEOUT,
        )
        result = fq.ListBindingsResult()
        response.operation.result.Unpack(result)
        return FederatedQueryClient.Response(response.operation.issues, result, check_issues)

    @retry.retry_intrusive
    def describe_binding(self, binding_id, check_issues=True):
        request = fq.DescribeBindingRequest()
        request.binding_id = binding_id
        response = self.service.DescribeBinding(
            request,
            metadata=self._create_meta(),
            timeout=CONTROL_PLANE_REQUEST_TIMEOUT,
        )
        result = fq.DescribeBindingResult()
        response.operation.result.Unpack(result)
        return FederatedQueryClient.Response(response.operation.issues, result, check_issues)

    def modify_connection(self, request, check_issues=True):
        response = self.service.ModifyConnection(
            request,
            metadata=self._create_meta(),
            timeout=CONTROL_PLANE_REQUEST_TIMEOUT
        )
        result = fq.ModifyConnectionResult()
        response.operation.result.Unpack(result)
        return FederatedQueryClient.Response(response.operation.issues, result, check_issues)

    @retry.retry_intrusive
    def modify_yds_connection(self, connection_id, name, database=None, endpoint=None, database_id=None,
                              visibility=fq.Acl.Visibility.PRIVATE, auth_method=AuthMethod.no_auth(),
                              check_issues=True):
        request = fq.ModifyConnectionRequest()

        request.connection_id = connection_id
        request.content.name = name
        request.content.acl.visibility = visibility

        yds = request.content.setting.data_streams
        if database_id is not None:
            yds.database_id = database_id
        else:
            yds.database = database
            yds.endpoint = endpoint

        yds.auth.CopyFrom(auth_method)
        return self.modify_connection(request, check_issues)

    @retry.retry_intrusive
    def modify_object_storage_connection(self, connection_id, name, bucket,
                                         visibility=fq.Acl.Visibility.PRIVATE, auth_method=AuthMethod.no_auth(),
                                         check_issues=True):
        request = fq.ModifyConnectionRequest()

        request.connection_id = connection_id
        request.content.name = name
        request.content.acl.visibility = visibility

        object_storage = request.content.setting.object_storage
        object_storage.bucket = bucket
        object_storage.auth.CopyFrom(auth_method)

        return self.modify_connection(request, check_issues)

    @retry.retry_intrusive
    def create_monitoring_connection(self, name, project, cluster, visibility=fq.Acl.Visibility.PRIVATE,
                                     auth_method=AuthMethod.no_auth(), check_issues=True):
        request = fq.CreateConnectionRequest()
        request.content.name = name
        monitoring = request.content.setting.monitoring
        monitoring.project = project
        monitoring.cluster = cluster

        monitoring.auth.CopyFrom(auth_method)
        request.content.acl.visibility = visibility
        return self.create_connection(request, check_issues)

    @retry.retry_intrusive
    def create_storage_connection(self, name, bucket, visibility=fq.Acl.Visibility.PRIVATE,
                                  auth_method=AuthMethod.no_auth(), check_issues=True):
        request = fq.CreateConnectionRequest()
        request.content.name = name
        object_storage = request.content.setting.object_storage
        object_storage.bucket = bucket

        object_storage.auth.CopyFrom(auth_method)
        request.content.acl.visibility = visibility

        return self.create_connection(request, check_issues)

    @retry.retry_intrusive
    def delete_connection(self, connection_id, check_issues=True):
        request = fq.DeleteConnectionRequest()
        request.connection_id = connection_id

        response = self.service.DeleteConnection(
            request,
            metadata=self._create_meta(),
            timeout=CONTROL_PLANE_REQUEST_TIMEOUT,
        )

        result = fq.DeleteConnectionResult()
        response.operation.result.Unpack(result)
        return FederatedQueryClient.Response(response.operation.issues, result, check_issues)

    def create_binding(self, request, check_issues=True):
        response = self.service.CreateBinding(
            request,
            metadata=self._create_meta(),
            timeout=CONTROL_PLANE_REQUEST_TIMEOUT,
        )

        result = fq.CreateBindingResult()
        response.operation.result.Unpack(result)
        return FederatedQueryClient.Response(response.operation.issues, result, check_issues)

    def modify_binding(self, request, check_issues=True):
        response = self.service.ModifyBinding(
            request,
            metadata=self._create_meta(),
            timeout=CONTROL_PLANE_REQUEST_TIMEOUT,
        )

        result = fq.ModifyBindingResult()
        response.operation.result.Unpack(result)
        return FederatedQueryClient.Response(response.operation.issues, result, check_issues)

    @retry.retry_intrusive
    def create_yds_binding(self, name, stream, format, connection_id, columns, format_setting={},
                           visibility=fq.Acl.Visibility.PRIVATE,
                           check_issues=True):
        request = fq.CreateBindingRequest()
        request.content.name = name
        request.content.connection_id = connection_id
        yds = request.content.setting.data_streams
        yds.stream_name = stream
        yds.format = format
        yds.schema.column.extend(columns)
        yds.format_setting.update(format_setting)

        request.content.acl.visibility = visibility
        return self.create_binding(request, check_issues)

    @retry.retry_intrusive
    def modify_yds_binding(self, binding_id, name, stream, format, connection_id, columns, format_setting={},
                           visibility=fq.Acl.Visibility.PRIVATE,
                           check_issues=True):

        request = fq.ModifyBindingRequest()

        request.binding_id = binding_id
        request.content.name = name
        request.content.connection_id = connection_id

        yds = request.content.setting.data_streams
        yds.stream_name = stream
        yds.format = format
        yds.schema.column.extend(columns)
        yds.format_setting.update(format_setting)

        request.content.acl.visibility = visibility
        return self.modify_binding(request, check_issues)

    @retry.retry_intrusive
    def create_object_storage_binding(self, name, path, format, connection_id, columns, format_setting={},
                                      projection={}, partitioned_by=[], compression="",
                                      visibility=fq.Acl.Visibility.PRIVATE,
                                      check_issues=True):
        request = fq.CreateBindingRequest()
        request.content.name = name
        request.content.connection_id = connection_id
        subset = fq.ObjectStorageBinding.Subset()
        subset.path_pattern = path
        subset.format = format
        subset.compression = compression
        subset.format_setting.update(format_setting)
        subset.schema.column.extend(columns)
        subset.projection.update(projection)
        subset.partitioned_by.extend(partitioned_by)

        request.content.setting.object_storage.subset.append(subset)

        request.content.acl.visibility = visibility
        return self.create_binding(request, check_issues)

    @retry.retry_intrusive
    def modify_object_storage_binding(self, binding_id, name, path, format, connection_id, columns, format_setting={},
                                      projection={}, partitioned_by=[], compression="",
                                      visibility=fq.Acl.Visibility.PRIVATE,
                                      check_issues=True):

        request = fq.ModifyBindingRequest()

        request.binding_id = binding_id
        request.content.name = name
        request.content.connection_id = connection_id

        subset = fq.ObjectStorageBinding.Subset()
        subset.path_pattern = path
        subset.format = format
        subset.compression = compression
        subset.format_setting.update(format_setting)
        subset.schema.column.extend(columns)
        subset.projection.update(projection)
        subset.partitioned_by.extend(partitioned_by)

        request.content.setting.object_storage.subset.append(subset)

        request.content.acl.visibility = visibility
        return self.modify_binding(request, check_issues)

    @retry.retry_intrusive
    def delete_binding(self, binding_id, check_issues=True):
        request = fq.DeleteBindingRequest()
        request.binding_id = binding_id

        response = self.service.DeleteBinding(
            request,
            metadata=self._create_meta(),
            timeout=CONTROL_PLANE_REQUEST_TIMEOUT,
        )

        result = fq.DeleteBindingResult()
        response.operation.result.Unpack(result)
        return FederatedQueryClient.Response(response.operation.issues, result, check_issues)

    @retry.retry_intrusive
    def delete_query(self, query_id, check_issues=True):
        request = fq.DeleteQueryRequest()
        request.query_id = query_id

        response = self.service.DeleteQuery(
            request,
            metadata=self._create_meta(),
            timeout=CONTROL_PLANE_REQUEST_TIMEOUT,
        )

        result = fq.DeleteQueryResult()
        response.operation.result.Unpack(result)
        return FederatedQueryClient.Response(response.operation.issues, result, check_issues)
