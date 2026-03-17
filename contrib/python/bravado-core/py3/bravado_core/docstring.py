# -*- coding: utf-8 -*-
import logging

from six import iteritems


log = logging.getLogger(__name__)


def operation_docstring_wrapper(operation):
    """
    Workaround for docstrings to work for operations in a sane way.

    Docstrings can only be specified for modules, classes, and functions. Hence,
    the docstring for an Operation is static as defined by the Operation class.
    To make the docstring for an Operation invocation work correctly, it has
    to be masqueraded as a function. The docstring will then be attached to the
    function which works quite nicely. Example in REPL:

    >> petstore = SwaggerClient.from_url('http://url_to_petstore/')
    >> pet = petstore.pet
    >> help(pet.getPetById)

    Help on function getPetById in module bravado_core.resource:

    getPetById(**kwargs)
        [GET] Find pet by ID

        Returns a pet when ID < 10.  ID > 10 or non-integers will simulate API error conditions  # noqa

        :param petId: ID of pet that needs to be fetched
        :type petId: integer
        :returns: 200: successful operation
        :rtype: #/definitions/Pet
        :returns: 400: Invalid ID supplied
        :returns: 404: Pet not found

    :param operation: :class:`Operation`
    :return: callable that executes the operation
    """
    def wrapper(**kwargs):
        return operation(**kwargs)

    # TODO: make docstring lazy by using 'docstring_property'
    wrapper.__doc__ = create_operation_docstring(operation)
    wrapper.__name__ = str(operation.operation_id)
    return wrapper


def create_operation_docstring(op):
    """Builds a comprehensive docstring for an Operation.

    :param op: :class:`bravado_core.operation.Operation`
    :rtype: str or unicode

    Example: ::

        client.pet.findPetsByStatus?

    Outputs: ::

        [GET] Finds Pets by status

        Multiple status values can be provided with comma seperated strings

        :param status: Statuses to be considered for filter
        :type status: str
        :param from_date: Start date filter
        :type from_date: str
        :rtype: list
    """
    # TODO: remove once lazy docstrings implemented
    log.debug('creating op docstring for %s', op.operation_id)
    s = ''
    op_spec = op.op_spec
    is_deprecated = op_spec.get('deprecated', False)
    if is_deprecated:
        s += '** DEPRECATED **\n'

    summary = op_spec.get('summary')
    if summary:
        s += u'[{0}] {1}\n\n'.format(op.http_method.upper(), summary)

    desc = op_spec.get('description')
    if desc:
        s += u'{0}\n\n'.format(desc)

    # TODO: add shared parameters
    for param_spec in op_spec.get('parameters', []):
        s += create_param_docstring(param_spec)

    # TODO: add 'examples' if available
    # TODO: good idea to identify the default response?
    responses = op_spec.get('responses')
    for http_status_code, response_spec in iter(sorted(iteritems(responses))):
        response_desc = response_spec.get('description')
        s += u':returns: {0}: {1}\n'.format(http_status_code, response_desc)
        schema_spec = response_spec.get('schema')
        if schema_spec:
            s += u':rtype: {0}\n'.format(formatted_type(schema_spec))
    return s


def create_param_docstring(param_spec):
    """Builds the docstring for a parameter from its specification.

    :param param_spec: parameter spec in json-line dict form
    :rtype: str or unicode

    Example: ::
        :param status: Status to be considered for filter
        :type status: string
    """
    name = param_spec.get('name')
    desc = param_spec.get('description', 'Document your spec, yo!')
    desc = desc if desc else 'Document your spec, yo!'
    default_value = param_spec.get('default')
    location = param_spec.get('in')
    required = param_spec.get('required', False)

    s = u':param {0}: {1}'.format(name, desc)
    if default_value is not None:
        s += u' (Default: {0})'.format(default_value)
    if not required:
        s += ' (optional)'
    s += '\n'

    if location == 'body':
        param_type = formatted_type(param_spec.get('schema'))
    else:
        param_type = param_spec.get('type')
    s += u':type {0}: {1}\n'.format(name, param_type)

    # TODO: Lot more stuff can go in here - see "Parameter Object" in 2.0 Spec.
    return s


def formatted_type(spec):
    """Returns the swagger type from a spec in a colon separated format.

    Example:

    .. code-block:: python

        {
            ...
            "type": "array",
            "items": {
                 "type": "integer",
                 "format": "int64"
                 }
            ...
        }

    Returns:

    .. code-block:: python

        "array:integer:int64"


    :param spec: object spec in dict form
    :rtype: str
    """
    obj_type = spec.get('type')
    obj_format = spec.get('format')
    ref = spec.get('$ref')
    if obj_format and obj_type:
        return "{0}:{1}".format(obj_type, obj_format)
    elif obj_type == 'array':
        return "{0}:{1}".format(obj_type, formatted_type(spec["items"]))
    elif ref:
        return ref
    elif obj_type:
        return obj_type
    else:  # not obj_type
        # if 'default_type_to_object' config is True, then this is defaulted to object type
        return 'notype'
