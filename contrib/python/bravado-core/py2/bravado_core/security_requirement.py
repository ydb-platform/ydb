# -*- coding: utf-8 -*-
import logging

import six
import typing

from bravado_core.exception import SwaggerSchemaError

if getattr(typing, 'TYPE_CHECKING', False):
    from bravado_core._compat_typing import JSONDict
    from bravado_core.security_definition import SecurityDefinition
    from bravado_core.spec import Spec

    T = typing.TypeVar('T')


log = logging.getLogger(__name__)


class SecurityRequirement(object):
    """
    Wrapper of security requirement object (http://swagger.io/specification/#securityRequirementObject)

    :param swagger_spec: Spec object
    :type swagger_spec: bravado_core.spec.Spec
    :param security_requirement_spec: security requirement specification in dict form
    """

    def __init__(self, swagger_spec, security_requirement_spec):
        # type: (Spec, typing.Mapping[typing.Text, typing.Mapping[typing.Text, typing.List[typing.Text]]]) -> None
        self.swagger_spec = swagger_spec
        self.security_requirement_spec = swagger_spec.deref(security_requirement_spec)
        for security_definition in six.iterkeys(security_requirement_spec):
            if security_definition not in self.swagger_spec.security_definitions:
                raise SwaggerSchemaError(
                    '{security} not defined in {swagger_path}'.format(
                        swagger_path='/securityDefinitions',
                        security=security_definition,
                    ),
                )

    @property
    def security_definitions(self):
        # type: () -> typing.Dict[typing.Text, SecurityDefinition]
        return {
            security_name: self.swagger_spec.security_definitions[security_name]
            for security_name in six.iterkeys(self.security_requirement_spec)
        }

    @property
    def security_scopes(self):
        # type: () -> typing.Dict[typing.Text, typing.Mapping[typing.Text, typing.List[typing.Text]]]
        return {
            security_name: self.security_requirement_spec[security_name]
            for security_name in six.iterkeys(self.security_requirement_spec)
        }

    @property
    def parameters_representation_dict(self):
        # type: () -> typing.List[JSONDict]
        return [
            definition.parameter_representation_dict
            for definition in six.itervalues(self.security_definitions)
            if definition.parameter_representation_dict
        ]

    def __iter__(self):
        # type: () -> typing.Iterable[SecurityDefinition]
        return six.itervalues(self.security_definitions)
