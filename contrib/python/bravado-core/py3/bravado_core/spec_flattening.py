# -*- coding: utf-8 -*-
import copy
import functools
import os.path
import re
import warnings
from collections import defaultdict

from six import iteritems
from six import iterkeys
from six import itervalues
from six.moves.urllib.parse import ParseResult
from six.moves.urllib.parse import urldefrag
from six.moves.urllib.parse import urlparse
from six.moves.urllib.parse import urlunparse
from six.moves.urllib.request import pathname2url
from six.moves.urllib.request import url2pathname
from swagger_spec_validator.ref_validators import in_scope

from bravado_core.model import model_discovery
from bravado_core.model import MODEL_MARKER
from bravado_core.schema import is_dict_like
from bravado_core.schema import is_list_like
from bravado_core.schema import is_ref
from bravado_core.util import cached_property
from bravado_core.util import determine_object_type
from bravado_core.util import ObjectType


MARSHAL_REPLACEMENT_PATTERNS = {
    '/': '..',  # / is converted to .. (ie. api_docs/swagger.json -> api_docs..swagger.json)
    '#': '|',  # # is converted to | (ie. swagger.json#definitions -> swagger.json|definitions)
}


def _marshal_uri(target_uri, origin_uri):
    """
    Translate the URL string representation into a new string which could be used as JSON keys.
    This method is needed because many JSON parsers and reference resolvers are using '/' as
    indicator of object nesting.

    To workaround this limitation we can re-write the url representation in a way that the parsers
    will accept it, for example "#/definitions/data_type" could become "|..definitions..data_type"

    Example: Assume that you have the following JSON document
        {
            "definitions": {
                "a/possible/def": {
                    "type": "object"
                },
                "a": {
                    "possible": {
                        "def": {
                            "type": "string"
                        }
                    }
                },
                "def": {
                    "$ref": "#/definitions/a/possible/def"
                }
            }
        }

    Assuming that the JSON parser is not raising exception the dereferenced value of
    "#/definitions/def" could be {"type": "object"} or {"type": "string"} which is
    an undetermined condition which can lead to weird errors.
    Let's assume instead that the JSON parser will raise an exception in this case
    the JSON object will not be usable.

    To prevent this conditions we are removing possible '/' from the JSON keys.

    :param target_uri: URI to marshal
    :type target_uri: ParseResult
    :param origin_uri: URI of the root swagger spec file
    :type origin_uri: ParseResult

    :return: a string representation of the URL which could be used into the JSON keys
    :rtype: str
    """

    marshalled_target = urlunparse(target_uri)

    if marshalled_target and target_uri.scheme == '':  # scheme is empty for relative paths. It should NOT happen!
        target_uri = ParseResult('file', *target_uri[1:])
        marshalled_target = urlunparse(target_uri)

    if not marshalled_target or target_uri.scheme not in {'file', 'http', 'https'}:
        raise ValueError(
            'Invalid target: \'{target_uri}\''.format(target_uri=urlunparse(target_uri)),
        )

    if origin_uri and target_uri.scheme == 'file':
        scheme, netloc, path, params, query, fragment = target_uri

        # Masquerade the absolute file path on the "local" server using
        # relative paths from the root swagger spec file
        spec_dir = os.path.dirname(url2pathname(origin_uri.path))
        scheme = 'lfile'
        path = pathname2url(os.path.relpath(url2pathname(path), spec_dir))
        marshalled_target = urlunparse((scheme, netloc, path, params, query, fragment))

    for src, dst in iteritems(MARSHAL_REPLACEMENT_PATTERNS):
        marshalled_target = marshalled_target.replace(src, dst)
    return marshalled_target


class _SpecFlattener(object):
    def __init__(self, swagger_spec, marshal_uri_function):
        self.swagger_spec = swagger_spec
        self.spec_url = self.swagger_spec.origin_url

        if self.spec_url is None:
            warnings.warn(
                message='It is recommended to set origin_url to your spec before flattering it. '
                        'Doing so internal paths will be hidden, reducing the amount of exposed information.',
                category=Warning,
            )

        self.known_mappings = {
            object_type.get_root_holder(): {}
            for object_type in ObjectType
            if object_type.get_root_holder()
        }
        self.marshal_uri_function = marshal_uri_function

    @cached_property
    def marshal_uri(self):
        return functools.partial(
            self.marshal_uri_function,
            origin_uri=urlparse(self.spec_url) if self.spec_url else None,
        )

    @cached_property
    def spec_resolver(self):
        return self.swagger_spec.resolver

    @cached_property
    def resolve(self):
        return self.swagger_spec.resolver.resolve

    @cached_property
    def default_type_to_object(self):
        return self.swagger_spec.config['default_type_to_object']

    def descend(self, value):
        if is_ref(value):
            # Update spec_resolver scope to be able to dereference relative specs from a not root file
            with in_scope(self.spec_resolver, value):
                uri, deref_value = self.resolve(value['$ref'])
                object_type = determine_object_type(
                    object_dict=deref_value,
                    default_type_to_object=self.default_type_to_object,
                )

                known_mapping_key = object_type.get_root_holder()
                if known_mapping_key is None:
                    return self.descend(value=deref_value)
                else:
                    uri = urlparse(uri)
                    if uri not in self.known_mappings.get(known_mapping_key, {}):
                        # The placeholder is present to interrupt the recursion
                        # during the recursive traverse of the data model (``descend``)
                        self.known_mappings[known_mapping_key][uri] = None

                        self.known_mappings[known_mapping_key][uri] = self.descend(value=deref_value)

                    return {'$ref': '#/{}/{}'.format(known_mapping_key, self.marshal_uri(uri))}

        elif is_dict_like(value):
            return {
                key: self.descend(value=subval)
                for key, subval in iteritems(value)
            }

        elif is_list_like(value):
            return [
                self.descend(value=subval)
                for index, subval in enumerate(value)
            ]

        else:
            return value

    def warn_if_uri_clash_on_same_marshaled_representation(self, uri_schema_mappings):
        """
        Verifies that all the uris present on the definitions are represented by a different marshaled uri.
        If is not the case a warning will filed.

        In case of presence of warning please keep us informed about the issue, in the meantime you can
        workaround this calling directly ``flattened_spec(spec, marshal_uri_function)`` passing your
        marshalling function.
        """
        # Check that URIs are NOT clashing to same marshaled representation
        marshaled_uri_mapping = defaultdict(set)
        for uri in iterkeys(uri_schema_mappings):
            marshaled_uri_mapping[self.marshal_uri(uri)].add(uri)

        if len(marshaled_uri_mapping) != len(uri_schema_mappings):
            # At least two uris clashed to the same marshaled representation
            for marshaled_uri, uris in iteritems(marshaled_uri_mapping):
                if len(uris) > 1:
                    warnings.warn(
                        message='{s_uris} clashed to {marshaled}'.format(
                            s_uris=', '.join(sorted(urlunparse(uri) for uri in uris)),
                            marshaled=marshaled_uri,
                        ),
                        category=Warning,
                    )

    def rename_definition_references(self, flattened_spec_dict):
        """
        Rename definition references to more "human" names if possible.

        The used approach is to use model-name as definition key, if this does not conflict
        with an already existing key.

        :param flattened_spec_dict: swagger spec dict (pre-flattened)
        :return: swagger spec dict equivalent to flattened_spec_dict with more human references
        :rtype: dict
        """
        def _rename_references_descend(value):
            if is_ref(value):
                return {
                    '$ref': reference_renaming_mapping.get(value['$ref'], value['$ref']),
                }
            elif is_dict_like(value):
                return {
                    key: _rename_references_descend(value=subval)
                    for key, subval in iteritems(value)
                }

            elif is_list_like(value):
                return [
                    _rename_references_descend(value=subval)
                    for index, subval in enumerate(value)
                ]

            else:
                return value

        definition_key_to_model_name_mapping = {
            k: v[MODEL_MARKER]
            for k, v in iteritems(flattened_spec_dict.get('definitions', {}))
            if is_dict_like(v) and MODEL_MARKER in v
        }

        original_definition_keys = set(iterkeys(flattened_spec_dict.get('definitions', {})))
        new_definition_keys = set(itervalues(definition_key_to_model_name_mapping))

        # Ensure that the new definition keys are not overlapping with already existing ones
        # if this happens the new definition key needs be kept untouched
        reference_renaming_mapping = {
            # old-reference -> new-reference
            '#/definitions/{}'.format(k): '#/definitions/{}'.format(v)
            for k, v in iteritems(definition_key_to_model_name_mapping)
            if v in new_definition_keys and v not in original_definition_keys
        }

        for old_reference, new_reference in iteritems(reference_renaming_mapping):
            new_ref = new_reference.replace('#/definitions/', '')
            old_ref = old_reference.replace('#/definitions/', '')
            flattened_spec_dict['definitions'][new_ref] = flattened_spec_dict['definitions'][old_ref]
            del flattened_spec_dict['definitions'][old_ref]

        return _rename_references_descend(flattened_spec_dict)

    def replace_inline_models_with_refs(self, flattened_spec_dict):
        """
        Rename definition references to more "human" names if possible.

        The used approach is to use model-name as definition key, if this does not conflict
        with an already existing key.

        :param flattened_spec_dict: swagger spec dict (pre-flattened)
        :return: swagger spec dict equivalent to flattened_spec_dict with more human references
        :rtype: dict
        """
        def _set_references_to_models_descend(value, json_ref):
            if is_dict_like(value):
                if (
                    MODEL_MARKER in value and
                    not re.match('^#/definitions/[^/]+$', json_ref) and
                    value == flattened_spec_dict.get('definitions', {}).get(value[MODEL_MARKER])
                ):
                    return {
                        '$ref': '#/definitions/{model_name}'.format(model_name=value[MODEL_MARKER]),
                    }

                else:
                    return {
                        key: _set_references_to_models_descend(value=subval, json_ref='{}/{}'.format(json_ref, key))
                        for key, subval in iteritems(value)
                    }

            elif is_list_like(value):
                return [
                    _set_references_to_models_descend(value=subval, json_ref='{}/{}'.format(json_ref, index))
                    for index, subval in enumerate(value)
                ]

            else:
                return value

        return _set_references_to_models_descend(flattened_spec_dict, '#')

    def model_discovery(self):
        # local imports due to circular dependency
        from bravado_core.spec import Spec

        # Run model-discovery in order to tag the models available in known_mappings['definitions']
        # This is a required step that removes duplications of models due to the presence of models
        # in swagger.json#/definitions and the equivalent models generated by flattening
        model_discovery(
            Spec(
                spec_dict={
                    'definitions': {
                        self.marshal_uri(uri): value
                        for uri, value in iteritems(self.known_mappings['definitions'])
                    },
                },
            ),
        )

    def include_discriminated_models(self):
        """
        This function ensures that discriminated models, present on the original Spec object
        but not directly referenced by the flattened schema (because there is no direct $ref
        attribute) are included in the final flattened specs

        NOTE: The method re-run model_discovery in case additional models have been added to
              the flattened models
        """

        def unflattened_models(flattened_models):
            for m_name, m_type in iteritems(self.swagger_spec.definitions):
                if m_name not in flattened_models:
                    yield m_name, m_type

        def register_unflattened_models():
            """
            :return: True if new models have been added
            """
            initial_number_of_models = len(self.known_mappings['definitions'])
            modified = True

            flattened_models = {
                # schema objects might not have a "type" set so they won't be tagged as models
                definition.get(MODEL_MARKER)
                for definition in itervalues(self.known_mappings['definitions'])
            }

            while modified:
                modified = False
                for model_name, model_type in unflattened_models(flattened_models):
                    if any(
                        parent in flattened_models
                        for parent in model_type._inherits_from
                    ):
                        model_url = urlparse(model_type._json_reference)
                        flattened_models.add(model_name)
                        self.known_mappings['definitions'][model_url] = self.descend(
                            value=model_type._model_spec,
                        )
                        modified = True

            return len(self.known_mappings['definitions']) != initial_number_of_models

        while register_unflattened_models():
            self.model_discovery()

    def include_root_definition(self):
        self.known_mappings['definitions'].update({
            urlparse(v._json_reference): self.descend(value=v._model_spec)
            for v in itervalues(self.swagger_spec.definitions)
            # urldefrag(url)[0] returns the url without the fragment, it is guaranteed to be present
            if urldefrag(v._json_reference)[0] == self.swagger_spec.origin_url
        })

    @cached_property
    def resolved_specs(self):
        # Create internal copy of spec_dict to avoid external dict pollution
        resolved_spec = self.descend(value=copy.deepcopy(self.swagger_spec.spec_dict))

        # Perform model discovery of the newly identified definitions
        self.model_discovery()

        # Ensure that all the root definitions, even if not referenced, are not lost due to flattening.
        self.include_root_definition()

        # Add the identified models that are not available on the know_mappings definitions
        # but that are related, via polymorphism (discriminator), to flattened models
        # This could happen in case discriminated models are not directly referenced by the specs
        # but is fair to assume that they should be on the final artifact due to polymorphism
        self.include_discriminated_models()

        for mapping_key, mappings in iteritems(self.known_mappings):
            self.warn_if_uri_clash_on_same_marshaled_representation(mappings)
            if len(mappings) > 0:
                resolved_spec.update(
                    {
                        mapping_key: {
                            self.marshal_uri(uri): value
                            for uri, value in iteritems(mappings)
                        },
                    },
                )

        resolved_spec = self.rename_definition_references(resolved_spec)
        resolved_spec = self.replace_inline_models_with_refs(resolved_spec)

        return resolved_spec


def flattened_spec(swagger_spec, marshal_uri_function=_marshal_uri):
    """
    Flatten Swagger Specs description into an unique and JSON serializable document.
    The flattening injects in place the referenced [path item objects](https://swagger.io/specification/#pathItemObject)
    while it injects in '#/parameters' the [parameter objects](https://swagger.io/specification/#parameterObject),
    in '#/definitions' the [schema objects](https://swagger.io/specification/#schemaObject) and in
    '#/responses' the [response objects](https://swagger.io/specification/#responseObject).

    Note: the object names in '#/definitions', '#/parameters' and '#/responses' are evaluated by
    ``marshal_uri_function``, the default method takes care of creating unique names for all the used references.
    Since name clashing are still possible take care that a warning could be filed.
    If it happen please report to us the specific warning text and the specs that generated it.
    We can work to improve it and in the mean time you can "plug" a custom marshalling function.

    Note: https://swagger.io/specification/ has been update to track the latest version of the Swagger/OpenAPI specs.
    Please refer to https://github.com/OAI/OpenAPI-Specification/blob/3.0.0/versions/2.0.md#responseObject for the
    most recent Swagger 2.0 specifications.

    WARNING: In the future releases all the parameters except swagger_spec and marshal_uri_function will be removed.
    Please make sure to use only those two parameters.
    Until the deprecation is not effective you can still pass all the parameters but it's strongly discouraged.

    :param swagger_spec: bravado-core Spec object
    :type swagger_spec: bravado_core.spec.Spec
    :param marshal_uri_function: function used to marshal uris in string suitable to be keys in Swagger Specs.
    :type marshal_uri_function: Callable with the same signature of ``_marshal_uri``

    :return: Flattened representation of the Swagger Specs
    :rtype: dict
    """
    return _SpecFlattener(swagger_spec, marshal_uri_function).resolved_specs
