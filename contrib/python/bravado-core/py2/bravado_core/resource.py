# -*- coding: utf-8 -*-
import logging
from collections import defaultdict
from copy import deepcopy
from itertools import chain

import typing
from six import iteritems
from six import iterkeys
from six import PY2

from bravado_core.exception import SwaggerMappingError
from bravado_core.operation import Operation
from bravado_core.util import AliasKeyDict
from bravado_core.util import sanitize_name


if getattr(typing, 'TYPE_CHECKING', False):
    from bravado_core.spec import Spec


log = logging.getLogger(__name__)


def convert_path_to_resource(path_name):
    # type: (typing.Text) -> typing.Text
    """
    Given a path name (#/paths/{path_name}) try to convert it into a resource
    name on a best effort basis when an operation has no tags.

    Examples:
      /pet                ->  pet
      /pet/findByStatus   ->  pet
      /pet/findByTags     ->  pet
      /pet/{petId}        ->  pet

    :param path_name: #/paths/{path_name} from a swagger spec
    :return: name of the resource to which operations under the given path
        should be associated with.
    """
    tokens = path_name.lstrip('/').split('/')
    err_msg = "Could not extract resource name from path {0}"
    resource_name = tokens[0]
    if not resource_name:
        raise SwaggerMappingError(err_msg.format(path_name))
    return resource_name


def build_resources(swagger_spec):
    # type: (Spec) -> AliasKeyDict
    """Transforms the REST resources in the json-like swagger_spec into rich
    :Resource: objects that have associated :Operation:s.

    :type swagger_spec: :class:`bravado_core.spec.Spec`
    :returns: dict where (key,value) = (resource name, Resource)
    """
    # Map operations to resources using operation tags if available.
    # - If an operation has multiple tags, it will be associated with multiple
    #   resources!
    # - If an operation has no tags, its resource name will be derived from its
    #   path
    # key = tag_name   value = { operation_id : Operation }
    tag_to_ops = defaultdict(dict)  # type: typing.DefaultDict[typing.Text, typing.Dict[typing.Text, Operation]]
    deref = swagger_spec.deref
    spec_dict = deref(swagger_spec._internal_spec_dict)
    paths_spec = deref(spec_dict.get('paths', {}))
    for path_name, path_spec in iteritems(paths_spec):
        path_spec = deref(path_spec)
        for http_method, op_spec in iteritems(path_spec):
            op_spec = deref(op_spec)
            # vendor extensions and parameters that are shared across all
            # operations for a given path are also defined at this level - we
            # just need to skip over them.
            if http_method.startswith('x-') or http_method == 'parameters':
                continue

            op = Operation.from_spec(
                swagger_spec, path_name, http_method,
                op_spec,
            )
            tags = deref(op_spec.get('tags', []))

            if not tags:
                tags.append(convert_path_to_resource(path_name))

            for tag in tags:
                tag_to_ops[deref(tag)][op.operation_id] = op

    resources = AliasKeyDict()
    for tag, ops in iteritems(tag_to_ops):
        sanitized_tag = sanitize_name(tag)
        resources[sanitized_tag] = Resource(sanitized_tag, ops)
        resources.add_alias(tag, sanitized_tag)
    return resources


class Resource(object):
    """A Swagger resource is associated with multiple operations.

    :param name: resource name
    :type name: str
    :param ops: operations associated with this resource (by tag)
    :type ops: dict where (key, value) = (op_name, Operation)
    """

    def __init__(self, name, ops):
        # type: (typing.Text, typing.Mapping[typing.Text, Operation]) -> None
        log.debug(u"Building resource '%s'", name)
        self.name = name
        self.operations = ops

    def __deepcopy__(self, memo=None):
        # type: (typing.Optional[typing.Dict[int, typing.Any]]) -> 'Resource'
        if memo is None:  # pragma: no cover  # This should never happening, but better safe than sorry
            memo = {}
        return self.__class__(
            name=deepcopy(self.name, memo=memo),
            ops=deepcopy(self.operations, memo=memo),
        )

    def __getstate__(self):
        # type: () -> typing.Dict[str, typing.Any]
        return self.__dict__

    def __setstate__(self, state):
        # type: (typing.Dict[str, typing.Any]) -> None
        self.__dict__.clear()
        self.__dict__.update(state)

    def __repr__(self):
        # type: () -> str
        repr = u"{self.__class__.__name__}({self.name})".format(self=self)
        if PY2:
            return repr.encode('ascii', 'backslashreplace')
        else:
            return repr

    def __getattr__(self, item):
        # type: (typing.Text) -> Operation
        """
        :param item: name of the operation to return
        :rtype: :class:`bravado_core.operation.Operation`
        """
        op = self.operations.get(item)
        if not op:
            raise AttributeError(u"Resource '{0}' has no operation '{1}'".format(self.name, item))
        return op

    def __dir__(self):
        # type: () -> typing.Iterable[typing.Text]
        """
        :return: list of operation names
        """
        return self.operations.keys()

    def is_equal(self, other, ignore_swagger_spec=False):
        # type: (typing.Any, bool) -> bool
        """
        Compare self with `other`

        NOTE: Not implemented as __eq__ otherwise we would need to implement __hash__ to preserve
            hashability of the class and it would not necessarily be performance effective

        :param other: instance to compare self against
        :param ignore_swagger_spec: skip equality check of swagger_spec attribute.
            This is useful as equality checks do not play well with recursive definitions.

        :return: True if self and other are the same, False otherwise
        """
        if id(self) == id(other):
            return True

        if not isinstance(other, self.__class__):
            return False

        if self.name != other.name:
            return False

        for operation_id in set(chain(iterkeys(self.operations), iterkeys(other.operations))):
            operation = self.operations.get(operation_id)
            if operation is None or not operation.is_equal(
                other.operations.get(operation_id), ignore_swagger_spec=ignore_swagger_spec,
            ):
                return False

        return True
