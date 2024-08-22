import abc
from contextlib import contextmanager

import os
import pytest

from ydb.tests.tools.fq_runner.kikimr_runner import StreamingOverKikimr
from ydb.tests.tools.fq_runner.kikimr_runner import StreamingOverKikimrConfig
from ydb.tests.tools.fq_runner.kikimr_runner import TenantConfig
from ydb.tests.tools.fq_runner.kikimr_runner import TenantType
from ydb.tests.tools.fq_runner.kikimr_runner import YqTenant


class ExtensionPoint(abc.ABC):

    @abc.abstractmethod
    def is_applicable(self, request):
        ExtensionPoint.is_applicable.__annotations__ = {
            'request': pytest.FixtureRequest,
            'return': bool
        }
        pass

    def apply_to_kikimr_conf(self, request, configuration):
        ExtensionPoint.is_applicable.__annotations__ = {
            'request': pytest.FixtureRequest,
            'configuration': StreamingOverKikimrConfig,
            'return': None
        }
        pass

    def apply_to_kikimr(self, request, kikimr):
        ExtensionPoint.is_applicable.__annotations__ = {
            'request': pytest.FixtureRequest,
            'kikimr': StreamingOverKikimr,
            'return': None
        }
        pass


class AddInflightExtension(ExtensionPoint):
    def is_applicable(self, request):
        return (hasattr(request, 'param')
                and isinstance(request.param, dict)
                and "inflight" in request.param)

    def apply_to_kikimr(self, request, kikimr):
        kikimr.inflight = request.param["inflight"]
        kikimr.compute_plane.fq_config['gateways']['s3']['max_inflight'] = kikimr.inflight
        del request.param["inflight"]


class AddDataInflightExtension(ExtensionPoint):
    def is_applicable(self, request):
        return (hasattr(request, 'param')
                and isinstance(request.param, dict)
                and "data_inflight" in request.param)

    def apply_to_kikimr(self, request, kikimr):
        kikimr.data_inflight = request.param["data_inflight"]
        kikimr.compute_plane.fq_config['gateways']['s3']['data_inflight'] = kikimr.data_inflight
        del request.param["data_inflight"]


class AddFormatSizeLimitExtension(ExtensionPoint):
    def is_applicable(self, request):
        return (hasattr(request, 'param')
                and isinstance(request.param, dict)
                and len(request.param) != 0)

    def apply_to_kikimr(self, request, kikimr):
        s3 = {}
        s3['format_size_limit'] = []
        for name, limit in request.param.items():
            if name == "":
                s3['file_size_limit'] = limit
            else:
                s3['format_size_limit'].append(
                    {'name': name, 'file_size_limit': limit})
        kikimr.compute_plane.fq_config['gateways']['s3'] = s3  # v1
        kikimr.compute_plane.qs_config['s3'] = s3  # v2


class DefaultConfigExtension(ExtensionPoint):

    def __init__(self, s3_url):
        DefaultConfigExtension.__init__.__annotations__ = {
            's3_url': str,
            'return': None
        }
        super().__init__()
        self.s3_url = s3_url

    def is_applicable(self, request):
        return True

    def apply_to_kikimr(self, request, kikimr):
        kikimr.control_plane.fq_config['common']['object_storage_endpoint'] = self.s3_url
        if isinstance(kikimr.compute_plane, YqTenant):
            kikimr.compute_plane.fq_config['common']['object_storage_endpoint'] = self.s3_url
        kikimr.control_plane.fq_config['control_plane_storage']['retry_policy_mapping'] = [
            {
                'status_code': [2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13],
                'policy': {
                    'retry_count': 0
                }
            }
        ]
        kikimr.control_plane.config_generator.yaml_config['metering_config'] = {
            'metering_file_path': 'metering.bill'}

        solomon_endpoint = os.environ.get('SOLOMON_URL')
        if solomon_endpoint is not None:
            kikimr.compute_plane.fq_config['common']['monitoring_endpoint'] = solomon_endpoint


class YQv2Extension(ExtensionPoint):

    def __init__(self, yq_version, is_replace_if_exists=False):
        YQv2Extension.__init__.__annotations__ = {
            'yq_version': str,
            'return': None
        }
        super().__init__()
        self.yq_version = yq_version
        self.is_replace_if_exists = is_replace_if_exists

    def apply_to_kikimr_conf(self, request, configuration):
        extra_feature_flags = [
            'enable_external_data_sources',
            'enable_script_execution_operations',
            'enable_external_source_schema_inference',
        ]
        if self.is_replace_if_exists:
            extra_feature_flags.append('enable_replace_if_exists_for_external_entities')

        if isinstance(configuration.node_count, dict):
            configuration.node_count["/compute"].tenant_type = TenantType.YDB
            configuration.node_count["/compute"].extra_feature_flags = extra_feature_flags
            configuration.node_count["/compute"].extra_grpc_services = ['query_service']
        else:
            configuration.node_count = {
                "/cp": TenantConfig(node_count=1),
                "/compute": TenantConfig(node_count=1,
                                         tenant_type=TenantType.YDB,
                                         extra_feature_flags=extra_feature_flags,
                                         extra_grpc_services=['query_service']),
            }

    def is_applicable(self, request):
        return self.yq_version == 'v2'

    def apply_to_kikimr(self, request, kikimr):
        kikimr.control_plane.fq_config['control_plane_storage']['enabled'] = True
        kikimr.control_plane.fq_config['compute'] = {
            'default_compute': 'IN_PLACE',
            'compute_mapping': [
                {
                    'query_type': 'ANALYTICS',
                    'compute': 'YDB',
                    'activation': {
                        'percentage': 100
                    }
                }
            ],
            'ydb': {
                'enable': 'true',
                'control_plane': {
                    'enable': 'true',
                    'single': {
                        'connection': {
                            'endpoint': kikimr.tenants["/compute"].endpoint(),
                            'database': '/local'
                        }
                    }
                }
            },
            "supported_compute_ydb_features": {
                "replace_if_exists": self.is_replace_if_exists
            }
        }


class ComputeExtension(ExtensionPoint):
    def apply_to_kikimr_conf(self, request, configuration):
        if isinstance(configuration.node_count, dict):
            configuration.node_count["/compute"].node_count = request.param["compute"]
        else:
            configuration.node_count = {
                "/cp": TenantConfig(node_count=1),
                "/compute": TenantConfig(node_count=request.param["compute"]),
            }

    def is_applicable(self, request):
        return (hasattr(request, 'param')
                and isinstance(request.param, dict)
                and "compute" in request.param)

    def apply_to_kikimr(self, request, kikimr):
        kikimr.control_plane.fq_config['control_plane_storage']['mapping'] = {
            "common_tenant_name": ["/compute"]
        }
        del request.param["compute"]


class AuditExtension(ExtensionPoint):
    def is_applicable(self, request):
        return True

    def apply_to_kikimr(self, request, kikimr):
        ua_port = os.environ["UA_RECIPE_PORT"]
        kikimr.control_plane.fq_config['audit']['enabled'] = True
        kikimr.control_plane.fq_config['audit']['uaconfig']['uri'] = "localhost:{}".format(ua_port)


class StatsModeExtension(ExtensionPoint):

    def __init__(self, stats_mode):
        YQv2Extension.__init__.__annotations__ = {
            'stats_mode': str,
            'return': None
        }
        super().__init__()
        self.stats_mode = stats_mode

    def is_applicable(self, request):
        return self.stats_mode != ''

    def apply_to_kikimr(self, request, kikimr):
        kikimr.control_plane.fq_config['control_plane_storage']['stats_mode'] = self.stats_mode
        kikimr.control_plane.fq_config['control_plane_storage']['dump_raw_statistics'] = True


class BindingsModeExtension(ExtensionPoint):

    def __init__(self, bindings_mode, yq_version):
        YQv2Extension.__init__.__annotations__ = {
            'bindings_mode': str,
            'yq_version': str,
            'return': None
        }
        super().__init__()
        self.bindings_mode = bindings_mode
        self.yq_version = yq_version

    def is_applicable(self, request):
        return self.yq_version == 'v2' and self.bindings_mode != ''

    def apply_to_kikimr(self, request, kikimr):
        kikimr.compute_plane.config_generator.yaml_config["table_service_config"]["bindings_mode"] = self.bindings_mode


class ConnectorExtension(ExtensionPoint):

    def __init__(self, host, port, use_ssl):
        ConnectorExtension.__init__.__annotations__ = {
            'host': str,
            'port': int,
            'use_ssl': bool,
            'return': None
        }
        super().__init__()
        self.host = host
        self.port = port
        self.use_ssl = use_ssl

    def is_applicable(self, request):
        return True

    def apply_to_kikimr(self, request, kikimr):
        kikimr.control_plane.fq_config['common']['disable_ssl_for_generic_data_sources'] = True
        kikimr.control_plane.fq_config['control_plane_storage']['available_connection'].append('POSTGRESQL_CLUSTER')
        kikimr.control_plane.fq_config['control_plane_storage']['available_connection'].append('CLICKHOUSE_CLUSTER')
        kikimr.control_plane.fq_config['control_plane_storage']['available_connection'].append('YDB_DATABASE')

        generic = {
            'connector': {
                'endpoint': {
                    'host': self.host,
                    'port': self.port,
                },
                'use_ssl': self.use_ssl,
            },
        }

        kikimr.compute_plane.fq_config['gateways']['generic'] = generic  # v1
        kikimr.control_plane.fq_config['gateways']['generic'] = generic  # v1
        kikimr.compute_plane.qs_config['generic'] = generic  # v2


class MDBExtension(ExtensionPoint):

    def __init__(self, endpoint: str, use_ssl=False):
        MDBExtension.__init__.__annotations__ = {
            'endpoint': str,
            'use_ssl': bool
        }
        super().__init__()
        self.endpoint = endpoint
        self.use_ssl = use_ssl

    def is_applicable(self, request):
        return True

    def apply_to_kikimr(self, request, kikimr):
        kikimr.compute_plane.qs_config['mdb_transform_host'] = False
        kikimr.compute_plane.qs_config['generic']['mdb_gateway'] = self.endpoint

        kikimr.compute_plane.fq_config['common']['mdb_transform_host'] = False
        kikimr.compute_plane.fq_config['common']['mdb_gateway'] = self.endpoint
        kikimr.compute_plane.fq_config['gateways']['generic']['mdb_gateway'] = self.endpoint

        kikimr.control_plane.fq_config['common']['mdb_transform_host'] = False
        kikimr.control_plane.fq_config['common']['mdb_gateway'] = self.endpoint
        kikimr.control_plane.fq_config['gateways']['generic']['mdb_gateway'] = self.endpoint


class YdbMvpExtension(ExtensionPoint):

    def __init__(self, mvp_external_ydb_endpoint):
        self.mvp_external_ydb_endpoint = mvp_external_ydb_endpoint
        super().__init__()

    def is_applicable(self, request):
        return True

    def apply_to_kikimr_conf(self, request, configuration):
        configuration.mvp_external_ydb_endpoint = self.mvp_external_ydb_endpoint

    def apply_to_kikimr(self, request, kikimr):
        if 'generic' in kikimr.compute_plane.qs_config:
            kikimr.compute_plane.qs_config['generic']['ydb_mvp_endpoint'] = kikimr.control_plane.fq_config['common']['ydb_mvp_cloud_endpoint']


class TokenAccessorExtension(ExtensionPoint):

    def __init__(self, endpoint: str, hmac_secret_file: str, use_ssl=False):
        TokenAccessorExtension.__init__.__annotations__ = {
            'endpoint': str,
            'hmac_secret_file': str,
            'use_ssl': bool,
        }
        super().__init__()
        self.endpoint = endpoint
        self.hmac_secret_file = hmac_secret_file
        self.use_ssl = use_ssl

    def is_applicable(self, request):
        return True

    def apply_to_kikimr(self, request, kikimr):
        kikimr.compute_plane.auth_config['token_accessor_config'] = {
            'enabled': True,
            'endpoint': self.endpoint,
        }

        kikimr.control_plane.fq_config['token_accessor']['enabled'] = True
        kikimr.control_plane.fq_config['token_accessor']['endpoint'] = self.endpoint
        kikimr.control_plane.fq_config['token_accessor']['use_ssl'] = self.use_ssl
        kikimr.control_plane.fq_config['token_accessor']['hmac_secret_file'] = self.hmac_secret_file

        kikimr.compute_plane.fq_config['token_accessor']['enabled'] = True
        kikimr.compute_plane.fq_config['token_accessor']['endpoint'] = self.endpoint
        kikimr.compute_plane.fq_config['token_accessor']['use_ssl'] = self.use_ssl


@contextmanager
def start_kikimr(request, kikimr_extensions):
    start_kikimr.__annotations__ = {
        'request': pytest.FixtureRequest,
        'kikimr_extensions': list[ExtensionPoint],
        'return': bool
    }
    kikimr_configuration = StreamingOverKikimrConfig(cloud_mode=True)

    for extension_point in kikimr_extensions:
        if extension_point.is_applicable(request):
            extension_point.apply_to_kikimr_conf(request, kikimr_configuration)

    kikimr = StreamingOverKikimr(kikimr_configuration)

    for extension_point in kikimr_extensions:
        if extension_point.is_applicable(request):
            extension_point.apply_to_kikimr(request, kikimr)

    kikimr.start_mvp_mock_server()
    try:
        kikimr.start()
        try:
            yield kikimr
        finally:
            kikimr.stop()
    finally:
        kikimr.stop_mvp_mock_server()


YQV1_VERSION_NAME = 'v1'
YQV2_VERSION_NAME = 'v2'
YQ_STATS_FULL = 'STATS_MODE_FULL'

yq_v1 = pytest.mark.yq_version(YQV1_VERSION_NAME)
yq_v2 = pytest.mark.yq_version(YQV2_VERSION_NAME)
yq_all = pytest.mark.yq_version(YQV1_VERSION_NAME, YQV2_VERSION_NAME)
