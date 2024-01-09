# -*- coding: utf-8 -*-
import abc
import enum
import six
from abc import abstractmethod
from . import issues, operation, settings as settings_impl, _apis


@enum.unique
class SchemeEntryType(enum.IntEnum):
    """
    Enumerates all available entry types.
    """

    TYPE_UNSPECIFIED = 0
    DIRECTORY = 1
    TABLE = 2
    PERS_QUEUE_GROUP = 3
    DATABASE = 4
    RTMR_VOLUME = 5
    BLOCK_STORE_VOLUME = 6
    COORDINATION_NODE = 7
    COLUMN_TABLE = 13
    SEQUENCE = 15
    REPLICATION = 16
    TOPIC = 17

    @staticmethod
    def is_table(entry):
        """
        :param entry: A scheme entry to check
        :return: True if scheme entry is a table and False otherwise
        """
        return entry == SchemeEntryType.TABLE

    @staticmethod
    def is_directory(entry):
        """
        :param entry: A scheme entry to check
        :return: True if scheme entry is a directory and False otherwise
        """
        return entry == SchemeEntryType.DIRECTORY

    @staticmethod
    def is_database(entry):
        """
        :param entry: A scheme entry to check
        :return: True if scheme entry is a database and False otherwise
        """
        return entry == SchemeEntryType.DATABASE

    @staticmethod
    def is_coordination_node(entry):
        """
        :param entry: A scheme entry to check
        :return: True if scheme entry is a coordination node and False otherwise
        """
        return entry == SchemeEntryType.COORDINATION_NODE

    @staticmethod
    def is_directory_or_database(entry):
        """
        :param entry: A scheme entry to check
        :return: True if scheme entry is a directory or database and False otherwise
        """
        return entry == SchemeEntryType.DATABASE or entry == SchemeEntryType.DIRECTORY


class SchemeEntry(object):
    __slots__ = (
        "name",
        "owner",
        "type",
        "effective_permissions",
        "permissions",
        "size_bytes",
    )

    def __init__(
        self,
        name,
        owner,
        type,
        effective_permissions,
        permissions,
        size_bytes,
        *args,
        **kwargs
    ):
        """
        Represents a scheme entry.
        :param name: A name of a scheme entry
        :param owner: A owner of a scheme entry
        :param type: A type of scheme entry
        :param effective_permissions: A list of effective permissions applied to this scheme entry
        :param permissions: A list of permissions applied to this scheme entry
        :param size_bytes: Size of entry in bytes
        """
        self.name = name
        self.owner = owner
        self.type = type
        self.effective_permissions = effective_permissions
        self.permissions = permissions
        self.size_bytes = size_bytes

    def is_directory(self):
        """
        :return: True if scheme entry is a directory and False otherwise
        """
        return SchemeEntryType.is_directory(self.type)

    def is_table(self):
        """
        :return: True if scheme entry is a table and False otherwise
        """
        return SchemeEntryType.is_table(self.type)

    def is_database(self):
        """
        :return: True if scheme entry is a database and False otherwise
        """
        return SchemeEntryType.is_database(self.type)

    def is_directory_or_database(self):
        """
        :return: True if scheme entry is a directory or a database and False otherwise
        """
        return SchemeEntryType.is_directory_or_database(self.type)

    def is_coordination_node(self):
        """
        :return: True if scheme entry is a coordination node and False otherwise
        """
        return SchemeEntryType.is_coordination_node(self.type)


class Directory(SchemeEntry):
    __slots__ = ("children",)

    def __init__(
        self,
        name,
        owner,
        type,
        effective_permissions,
        permissions,
        children,
        *args,
        **kwargs
    ):
        """
        Represents a directory scheme entry.
        :param name: A name of a scheme entry
        :param owner: A owner of a scheme entry
        :param type: A type of scheme entry
        :param effective_permissions: A list of effective permissions applied to this scheme entry
        :param permissions: A list of permissions applied to this scheme entry
        :param children: A list of children
        """
        super(Directory, self).__init__(
            name, owner, type, effective_permissions, permissions, 0
        )
        self.children = children


def _describe_path_request_factory(path):
    request = _apis.ydb_scheme.DescribePathRequest()
    request.path = path
    return request


def _list_directory_request_factory(path):
    request = _apis.ydb_scheme.ListDirectoryRequest()
    request.path = path
    return request


def _remove_directory_request_factory(path):
    request = _apis.ydb_scheme.RemoveDirectoryRequest()
    request.path = path
    return request


def _make_directory_request_factory(path):
    request = _apis.ydb_scheme.MakeDirectoryRequest()
    request.path = path
    return request


class MakeDirectorySettings(settings_impl.BaseRequestSettings):
    pass


class RemoveDirectorySettings(settings_impl.BaseRequestSettings):
    pass


class ListDirectorySettings(settings_impl.BaseRequestSettings):
    pass


class DescribePathSettings(settings_impl.BaseRequestSettings):
    pass


class ModifyPermissionsSettings(settings_impl.BaseRequestSettings):
    def __init__(self):
        super(ModifyPermissionsSettings, self).__init__()
        self._pb = _apis.ydb_scheme.ModifyPermissionsRequest()

    def grant_permissions(self, subject, permission_names):
        permission_action = self._pb.actions.add()
        permission_action.grant.MergeFrom(
            Permissions(subject, permission_names).to_pb()
        )
        return self

    def revoke_permissions(self, subject, permission_names):
        permission_action = self._pb.actions.add()
        permission_action.revoke.MergeFrom(
            Permissions(subject, permission_names).to_pb()
        )
        return self

    def set_permissions(self, subject, permission_names):
        permission_action = self._pb.actions.add()
        permission_action.set.MergeFrom(Permissions(subject, permission_names).to_pb())
        return self

    def change_owner(self, owner):
        permission_action = self._pb.actions.add()
        permission_action.change_owner = owner
        return self

    def clear_permissions(self):
        self._pb.clear_permissions = True
        return self

    def to_pb(self):
        return self._pb


class Permissions(object):
    __slots__ = ("subject", "permission_names")

    def __init__(self, subject, permission_names):
        """
        Represents permissions
        :param subject: A subject of permission names
        :param permission_names: A list of permission names
        """
        self.subject = subject
        self.permission_names = permission_names

    def to_pb(self):
        """
        :return: A protocol buffer representation of permissions
        """
        pb = _apis.ydb_scheme.Permissions()
        pb.subject = self.subject
        pb.permission_names.extend(self.permission_names)
        return pb


def _modify_permissions_request_factory(path, settings):
    """
    Constructs modify permissions request
    :param path: A path to apply permissions
    :param settings: An instance of ModifyPermissionsSettings
    :return: A constructed request
    """
    modify_permissions_request = settings.to_pb()
    modify_permissions_request.path = path
    return modify_permissions_request


def _wrap_permissions(permissions):
    """
    Wraps permissions protocol buffers into native Python objects
    :param permissions: A protocol buffer representation of permissions
    :return: A iterable of permissions
    """
    return tuple(
        Permissions(permission.subject, permission.permission_names)
        for permission in permissions
    )


def _wrap_scheme_entry(entry_pb, scheme_entry_cls=None, *args, **kwargs):
    """
    Wraps scheme entry into native Python objects.
    :param entry_pb: A protocol buffer representation of a scheme entry
    :param scheme_entry_cls: A native Python class that represents scheme entry (
    by default that is generic SchemeEntry)
    :param args: A list of optional arguments
    :param kwargs: A dictionary of with optional arguments
    :return: A native Python reprensentation of scheme entry
    """
    scheme_entry_cls = SchemeEntry if scheme_entry_cls is None else scheme_entry_cls
    return scheme_entry_cls(
        entry_pb.name,
        entry_pb.owner,
        SchemeEntryType(entry_pb.type),
        _wrap_permissions(entry_pb.effective_permissions),
        _wrap_permissions(entry_pb.permissions),
        entry_pb.size_bytes,
        *args,
        **kwargs
    )


def _wrap_list_directory_response(rpc_state, response):
    """
    Wraps list directory response
    :param response: A list directory response
    :return: A directory
    """
    issues._process_response(response.operation)
    message = _apis.ydb_scheme.ListDirectoryResult()
    response.operation.result.Unpack(message)
    children = []
    supported_items = set(i.value for i in SchemeEntryType)
    for children_item in message.children:
        if children_item.type not in supported_items:
            continue

        children.append(_wrap_scheme_entry(children_item))

    return Directory(
        message.self.name,
        message.self.owner,
        SchemeEntryType(message.self.type),
        _wrap_permissions(message.self.effective_permissions),
        _wrap_permissions(message.self.permissions),
        tuple(children),
    )


def _wrap_describe_path_response(rpc_state, response):
    issues._process_response(response.operation)
    message = _apis.ydb_scheme.DescribePathResult()
    response.operation.result.Unpack(message)
    return _wrap_scheme_entry(message.self)


@six.add_metaclass(abc.ABCMeta)
class ISchemeClient:
    @abstractmethod
    def __init__(self, driver):
        pass

    @abstractmethod
    def make_directory(self, path, settings):
        pass

    @abstractmethod
    def remove_directory(self, path, settings):
        pass

    @abstractmethod
    def list_directory(self, path, settings):
        pass

    @abstractmethod
    def describe_path(self, path, settings):
        pass

    @abstractmethod
    def modify_permissions(self, path, settings):
        """
        Modifies permissions for provided scheme entry

        :param path: A path of scheme entry
        :param settings: An instance of ModifyPermissionsSettings

        :return: An operation if success or exception on case of failure
        """
        pass


class BaseSchemeClient(ISchemeClient):
    __slots__ = ("_driver",)

    def __init__(self, driver):
        self._driver = driver

    def make_directory(self, path, settings=None):
        return self._driver(
            _make_directory_request_factory(path),
            _apis.SchemeService.Stub,
            _apis.SchemeService.MakeDirectory,
            operation.Operation,
            settings,
        )

    def remove_directory(self, path, settings=None):
        return self._driver(
            _remove_directory_request_factory(path),
            _apis.SchemeService.Stub,
            _apis.SchemeService.RemoveDirectory,
            operation.Operation,
            settings,
        )

    def list_directory(self, path, settings=None):
        return self._driver(
            _list_directory_request_factory(path),
            _apis.SchemeService.Stub,
            _apis.SchemeService.ListDirectory,
            _wrap_list_directory_response,
            settings,
        )

    def describe_path(self, path, settings=None):
        return self._driver(
            _describe_path_request_factory(path),
            _apis.SchemeService.Stub,
            _apis.SchemeService.DescribePath,
            _wrap_describe_path_response,
            settings,
        )

    def modify_permissions(self, path, settings):
        """
        Modifies permissions for provided scheme entry

        :param path: A path of scheme entry
        :param settings: An instance of ModifyPermissionsSettings

        :return: An operation if success or exception on case of failure
        """
        return self._driver(
            _modify_permissions_request_factory(path, settings),
            _apis.SchemeService.Stub,
            _apis.SchemeService.ModifyPermissions,
            operation.Operation,
            settings,
        )


class SchemeClient(BaseSchemeClient):
    def async_make_directory(self, path, settings=None):
        return self._driver.future(
            _make_directory_request_factory(path),
            _apis.SchemeService.Stub,
            _apis.SchemeService.MakeDirectory,
            operation.Operation,
            settings,
        )

    def async_remove_directory(self, path, settings=None):
        return self._driver.future(
            _remove_directory_request_factory(path),
            _apis.SchemeService.Stub,
            _apis.SchemeService.RemoveDirectory,
            operation.Operation,
            settings,
        )

    def async_list_directory(self, path, settings=None):
        return self._driver.future(
            _list_directory_request_factory(path),
            _apis.SchemeService.Stub,
            _apis.SchemeService.ListDirectory,
            _wrap_list_directory_response,
            settings,
        )

    def async_describe_path(self, path, settings=None):
        return self._driver.future(
            _describe_path_request_factory(path),
            _apis.SchemeService.Stub,
            _apis.SchemeService.DescribePath,
            _wrap_describe_path_response,
            settings,
        )

    def async_modify_permissions(self, path, settings):
        """
        Modifies permissions for provided scheme entry

        :param path: A path of scheme entry
        :param settings: An instance of ModifyPermissionsSettings

        :return: An future of computation
        """
        return self._driver.future(
            _modify_permissions_request_factory(path, settings),
            _apis.SchemeService.Stub,
            _apis.SchemeService.ModifyPermissions,
            operation.Operation,
            settings,
        )
