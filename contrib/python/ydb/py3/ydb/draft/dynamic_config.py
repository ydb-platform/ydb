import abc
from abc import abstractmethod
from . import _apis
from .. import issues, operation


class IDynamicConfigClient(abc.ABC):
    @abstractmethod
    def __init__(self, driver):
        pass

    @abstractmethod
    def replace_config(self, config, dry_run, allow_unknown_fields, settings):
        pass

    @abstractmethod
    def set_config(self, config, dry_run, allow_unknown_fields, settings):
        pass

    @abstractmethod
    def get_config(self, settings):
        pass

    @abstractmethod
    def get_node_labels(self, node_id, settings):
        pass


class DynamicConfig(object):
    __slots__ = ("version", "cluster", "config")

    def __init__(self, version, cluster, config, *args, **kwargs):
        self.version = version
        self.cluster = cluster
        self.config = config


class NodeLabels(object):
    __slots__ = "labels"

    def __init__(self, labels, *args, **kwargs):
        self.labels = labels


def _replace_config_request_factory(config, dry_run, allow_unknown_fields):
    request = _apis.ydb_dynamic_config.ReplaceConfigRequest()
    request.config = config
    request.dry_run = dry_run
    request.allow_unknown_fields = allow_unknown_fields
    return request


def _set_config_request_factory(config, dry_run, allow_unknown_fields):
    request = _apis.ydb_dynamic_config.SetConfigRequest()
    request.config = config
    request.dry_run = dry_run
    request.allow_unknown_fields = allow_unknown_fields
    return request


def _get_config_request_factory():
    request = _apis.ydb_dynamic_config.GetConfigRequest()
    return request


def _get_node_labels_request_factory(node_id):
    request = _apis.ydb_dynamic_config.GetNodeLabelsRequest()
    request.node_id = node_id
    return request


def _wrap_dynamic_config(config_pb, dynamic_config_cls=None, *args, **kwargs):
    dynamic_config_cls = DynamicConfig if dynamic_config_cls is None else dynamic_config_cls
    return dynamic_config_cls(config_pb.identity.version, config_pb.identity.cluster, config_pb.config, *args, **kwargs)


def _wrap_get_config_response(rpc_state, response):
    issues._process_response(response.operation)
    message = _apis.ydb_dynamic_config.GetConfigResult()
    response.operation.result.Unpack(message)
    return _wrap_dynamic_config(message)


def _wrap_node_labels(labels_pb, node_labels_cls=None, *args, **kwargs):
    node_labels_cls = NodeLabels if node_labels_cls is None else node_labels_cls
    return node_labels_cls(dict([(entry.label, entry.value) for entry in labels_pb.labels]), *args, **kwargs)


def _wrap_get_node_labels_response(rpc_state, response):
    issues._process_response(response.operation)
    message = _apis.ydb_dynamic_config.GetNodeLabelsResult()
    response.operation.result.Unpack(message)
    return _wrap_node_labels(message)


class BaseDynamicConfigClient(IDynamicConfigClient):
    __slots__ = ("_driver",)

    def __init__(self, driver):
        self._driver = driver

    def replace_config(self, config, dry_run, allow_unknown_fields, settings=None):
        return self._driver(
            _replace_config_request_factory(config, dry_run, allow_unknown_fields),
            _apis.DynamicConfigService.Stub,
            _apis.DynamicConfigService.ReplaceConfig,
            operation.Operation,
            settings,
        )

    def set_config(self, config, dry_run, allow_unknown_fields, settings=None):
        return self._driver(
            _set_config_request_factory(config, dry_run, allow_unknown_fields),
            _apis.DynamicConfigService.Stub,
            _apis.DynamicConfigService.SetConfig,
            operation.Operation,
            settings,
        )

    def get_config(self, settings=None):
        return self._driver(
            _get_config_request_factory(),
            _apis.DynamicConfigService.Stub,
            _apis.DynamicConfigService.GetConfig,
            _wrap_get_config_response,
            settings,
        )

    def get_node_labels(self, node_id, settings=None):
        return self._driver(
            _get_node_labels_request_factory(node_id),
            _apis.DynamicConfigService.Stub,
            _apis.DynamicConfigService.GetNodeLabels,
            _wrap_get_node_labels_response,
            settings,
        )


class DynamicConfigClient(BaseDynamicConfigClient):
    def async_replace_config(self, config, dry_run, allow_unknown_fields, settings=None):
        return self._driver.future(
            _replace_config_request_factory(config, dry_run, allow_unknown_fields),
            _apis.DynamicConfigService.Stub,
            _apis.DynamicConfigService.ReplaceConfig,
            operation.Operation,
            settings,
        )

    def async_set_config(self, config, dry_run, allow_unknown_fields, settings=None):
        return self._driver.future(
            _set_config_request_factory(config, dry_run, allow_unknown_fields),
            _apis.DynamicConfigService.Stub,
            _apis.DynamicConfigService.SetConfig,
            operation.Operation,
            settings,
        )

    def async_get_config(self, settings=None):
        return self._driver.future(
            _get_config_request_factory(),
            _apis.DynamicConfigService.Stub,
            _apis.DynamicConfigService.GetConfig,
            _wrap_get_config_response,
            settings,
        )

    def async_get_node_labels(self, node_id, settings=None):
        return self._driver.future(
            _get_node_labels_request_factory(node_id),
            _apis.DynamicConfigService.Stub,
            _apis.DynamicConfigService.GetNodeLabels,
            _wrap_get_node_labels_response,
            settings,
        )
