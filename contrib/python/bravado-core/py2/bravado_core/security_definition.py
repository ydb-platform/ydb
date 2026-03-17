# -*- coding: utf-8 -*-
import logging
from copy import deepcopy

import typing


if getattr(typing, 'TYPE_CHECKING', False):
    from bravado_core._compat_typing import JSONDict
    from bravado_core.spec import Spec


log = logging.getLogger(__name__)


class SecurityDefinition(object):
    """
    Wrapper of security definition object (http://swagger.io/specification/#securityDefinitionsObject)

    :type swagger_spec: :class:`bravado_core.spec.Spec`
    :type security_definition_spec: security definition specification in dict form
    """

    def __init__(self, swagger_spec, security_definition_spec):
        # type: (Spec, JSONDict) -> None
        self.swagger_spec = swagger_spec
        self.security_definition_spec = swagger_spec.deref(security_definition_spec)

    def __deepcopy__(self, memo=None):
        # type: (typing.Optional[typing.Dict[int, typing.Any]]) -> 'SecurityDefinition'
        if memo is None:  # pragma: no cover  # This should never happening, but better safe than sorry
            memo = {}
        return self.__class__(
            swagger_spec=deepcopy(self.swagger_spec, memo=memo),
            security_definition_spec=deepcopy(self.security_definition_spec, memo=memo),
        )

    @property
    def location(self):
        # type: () -> typing.Optional[typing.Text]
        # not using 'in' as the name since it is a keyword in python
        return self.security_definition_spec.get('in')

    @property
    def type(self):
        # type: () -> typing.Text
        return self.security_definition_spec['type']

    @property
    def name(self):
        # type: () -> typing.Optional[typing.Text]
        return self.security_definition_spec.get('name')

    @property
    def flow(self):
        # type: () -> typing.Optional[typing.Text]
        return self.security_definition_spec.get('flow')

    @property
    def scopes(self):
        # type: () -> typing.Optional[typing.List[typing.Text]]
        return self.security_definition_spec.get('scopes')

    @property
    def authorizationUrl(self):
        # type: () -> typing.Optional[typing.Text]
        return self.security_definition_spec.get('authorizationUrl')

    @property
    def tokenUrl(self):
        # type: () -> typing.Optional[typing.Text]
        return self.security_definition_spec.get('tokenUrl')

    @property
    def parameter_representation_dict(self):
        # type: () -> typing.Optional[JSONDict]
        if self.type == 'apiKey':
            return {
                'required': False,
                'type': 'string',
                'description': self.security_definition_spec.get('description', ''),
                'name': self.name,
                'in': self.location,
            }
        else:
            return None
