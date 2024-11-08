import abc
from abc import abstractmethod
from .. import issues, operation, _apis


class IStorageConfigClient(abc.ABC):
    @abstractmethod
    def __init__(self, driver):
        pass

    @abstractmethod
    def replace_storage_config(self, config, settings):
        pass

    @abstractmethod
    def fetch_storage_config(self, settings):
        pass

class StorageConfig(object):
    __slots__ = ("version", "cluster", "config")

    def __init__(self, version, cluster, config, *args, **kwargs):
        self.version = version
        self.cluster = cluster
        self.config = config


class NodeLabels(object):
    __slots__ = "labels"

    def __init__(self, labels, *args, **kwargs):
        self.labels = labels


def _replace_storage_config_request_factory(config):
    request = _apis.ydb_storage_config.ReplaceStorageConfigRequest()
    request.yaml_config = config
    return request


def _fetch_storage_config_request_factory():
    request = _apis.ydb_storage_config.FetchStorageConfigRequest()
    return request


def _wrap_storage_config(config_pb, storage_config_cls=None, *args, **kwargs):
    storage_config_cls = StorageConfig if storage_config_cls is None else storage_config_cls
    return storage_config_cls(config_pb.identity.version, config_pb.identity.cluster, config_pb.config, *args, **kwargs)


def _wrap_fetch_storage_config_response(rpc_state, response):
    issues._process_response(response.operation)
    message = _apis.ydb_storage_config.FetchStorageConfigResult()
    response.operation.result.Unpack(message)
    return _wrap_storage_config(message)


class BaseBSConfigClient(IStorageConfigClient):
    __slots__ = ("_driver",)

    def __init__(self, driver):
        self._driver = driver

    def replace_storage_config(self, config, settings=None):
        return self._driver(
            _replace_storage_config_request_factory(config),
            _apis.BSConfigService.Stub,
            _apis.BSConfigService.ReplaceStorageConfig,
            operation.Operation,
            settings,
        )

    def fetch_storage_config(self, settings=None):
        return self._driver(
            _fetch_storage_config_request_factory(),
            _apis.BSConfigService.Stub,
            _apis.BSConfigService.FetchStorageConfig,
            _wrap_fetch_storage_config_response,
            settings,
        )


class BSConfigClient(BaseBSConfigClient):
    def async_replace_storage_config(self, config, settings=None):
        return self._driver.future(
            _replace_storage_config_request_factory(config),
            _apis.BSConfigService.Stub,
            _apis.BSConfigService.ReplaceStorageConfig,
            operation.Operation,
            settings,
        )

    def async_fetch_storage_config(self, settings=None):
        return self._driver.future(
            _fetch_storage_config_request_factory(),
            _apis.BSConfigService.Stub,
            _apis.BSConfigService.FetchStorageConfig,
            _wrap_fetch_storage_config_response,
            settings,
        )
