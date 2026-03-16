from __future__ import unicode_literals


TYPE_MESSAGES = {
    'unknown': 'Unknown type: {0}',
    'invalid': "Got value `{0}` of type `{1}`. Value must be of type(s): `{2}`",
    'invalid_header_type': (
        "Invalid type for header: `{0}`. Must be one of 'string', 'number', "
        "'integer', 'boolean', or 'array'."
    ),
    'invalid_type_for_minimum': '`minimum` can only be used for json number types',
    'invalid_type_for_maximum': '`maximum` can only be used for json number types',
    'invalid_type_for_multiple_of': '`multipleOf` can only be used for json number types',
    'invalid_type_for_min_length': '`minLength` can only be used for string types',
    'invalid_type_for_max_length': '`maxLength` can only be used for string types',
    'invalid_type_for_min_items': '`minItems` can only be used for array types',
    'invalid_type_for_max_items': '`maxItems` can only be used for array types',
    'invalid_type_for_unique_items': '`uniqueItems` can only be used for array types',
    'invalid_type_for_min_properties': 'minProperties can only be used for `object` types',
    'invalid_type_for_max_properties': 'maxProperties can only be used for `object` types',
    'non_body_parameters_must_declare_a_type': (
        "A Parameter who's `in` value is not 'body' must declare a type."
    ),
}

FORMAT_MESSAGES = {
    'invalid': "Value {0} does not conform to the format {1}",
    'invalid_uuid': "{0} is not a valid uuid",
    'invalid_date': "{0} is not a valid RFC3339 full-date",
    'invalid_datetime': "{0} is not a valid RFC3339 date-time",
    'invalid_int': (
        "Integer {0} does not conform to the format int{1}. Must be no more than {1} bits"
    ),
    'invalid_uri': "The value `{0}` is not valid according to RFC3987.",
    'invalid_email': "The email address `{0}` is invalid according to RFC5322.",
}

REQUIRED_MESSAGES = {
    'required': "This value is required",
    'path_parameters_must_be_required': (
        "A Parameter who's `in` value is 'path' must be declared as required."
    ),
}

MULTIPLE_OF_MESSAGES = {
    'invalid': "Value must be a multiple of {0}. Got {1} which is not.",
}

MINIMUM_AND_MAXIMUM_MESSAGES = {
    'invalid': "{0} must be {1} than {2}",
    'must_be_greater_than_minimum': (
        "The value of `maximum` must be greater than or equal to the value of `minimum`"
    ),
    'exclusive_minimum_required_minimum': (
        "When `exclusiveMinimum` is set, `minimum` is required"
    ),
    'exclusive_maximum_required_maximum': (
        "When `exclusiveMaximum` is set, `maximum` is required"
    ),
}


MIN_LENGTH_MESSAGES = {
    "invalid": "value must be no less than {0} characters in length.",
}


MAX_LENGTH_MESSAGES = {
    "invalid": "value must be no greater than {0} characters in length.",
    'must_be_greater_than_min_length': (
        'The value of `maxLength` must be greater than or equal to the `minLength` value'
    ),
}


MIN_ITEMS_MESSAGES = {
    'invalid': "Array must have at least {0} items. It had only had {1} items.",
}


MAX_ITEMS_MESSAGES = {
    'must_be_greater_than_min_items': (
        "The value of `maxItems` must be greater than or equal to the value of `minItems`"
    ),
    'invalid': "Array must have no more than {0} items. It had {1} items.",
}


UNIQUE_ITEMS_MESSAGES = {
    'invalid': "Array items must be unique. The following items appeard more than once: {0}",
}


ENUM_MESSAGES = {
    'invalid': "Invalid value. {0} is not one of the available options ({1})",
}


PATTERN_MESSAGES = {
    'invalid_regex': "{0} is not a valid regular expression",
    'invalid': "{0} did not match the pattern `{1}`.",
}


MIN_PROPERTIES_MESSAGES = {
    'invalid': "Object must have more than {0} properties. It had {1}",
}


MAX_PROPERTIES_MESSAGES = {
    'invalid': "Object must have less than {0} properties. It had {1}",
    'must_be_greater_than_min_properties': (
        "The value of `maxProperties` must be greater than or equal to `minProperties`."
    ),
}


ADDITIONAL_PROPERTIES_MESSAGES = {
    'extra_properties': (
        "When `additionalProperties` is False, no unspecified properties are "
        "allowed. The following unspecified properties were found:\n\t`{0}`"
    ),
}


ITEMS_MESSAGES = {
    'invalid_type': '`items` must be a reference, a schema, or an array of schemas.',
    'items_required_for_type_array': (
        "For type \"array\", the items is required."
    )
}


DEFAULT_MESSAGES = {
    'invalid_type': (
        "The value of `default` must be of one of the declared types for the "
        "schema. `{0}` is not one of `{1}`"
    ),
}


REQUEST_MESSAGES = {
    'invalid_method': (
        'Request was not one of the allowed request methods. Got '
        '`{0}`: Expected one of: `{1}`'
    ),
}


RESPONSE_MESSAGES = {
    'invalid_status_code': (
        "Request status code was not found in the known response codes. Got "
        "`{0}`: Expected one of: `{1}`"
    )
}


PATH_MESSAGES = {
    'no_matching_paths_found': 'No paths found for {0}',
    'multiple_paths_found': (
        'Unable to determine path for {0}. Found multiple matches: `{1}`'
    ),
    'unknown_path': 'Request path did not match any of the known api paths.',
    'missing_parameter': (
        "The parameter named `{0}` is declared to be a PATH parameter but does "
        "not appear in the api path `{1}`. All path parameters must exist as a "
        "parameter in the api path"
    ),
    'must_start_with_slash': "Path must start with a '/'",
    'invalid': "Invalid Path: {0}",
}


CONTENT_TYPE_MESSAGES = {
    'invalid': 'Invalid content type `{0}`. Must be one of `{1}`.',
    'not_specified': 'Content type not specified. Must be be one of `{0}`',
}


HOST_MESSAGES = {
    'invalid': (
        "Invalid host: {0}. This MUST be the host only and does not include the "
        "scheme nor sub-paths. It MAY include a port. If the host is not "
        "included, the host serving the documentation is to be used (including "
        "the port)"
    ),
    'may_not_include_path': (
        "Invalid host: {0}. Includes the path component. The host value "
        "should be the host only, without path or scheme."
    ),
    'may_not_include_scheme': (
        "Invalid host: {0}. Includes the scheme component. The host value "
        "should be the host only, without path or scheme."
    ),
}

SCHEMES_MESSAGES = {
    'invalid': "Invalid scheme: {0}. Must be one of (http, https, ws, wss).",
}


MIMETYPE_MESSAGES = {
    'invalid': "Invalid mimetype: {0}.",
}


REFERENCE_MESSAGES = {
    'unsupported': (
        "Unsupported Reference: `{0}` - $ref validation does not currently "
        "support references that are anything more than a url fragment."
    ),
    'security': "Unknown SecurityScheme reference `{0}`",
    'parameter': "Unknown Parameter reference `{0}`",
    'undefined': "The $ref `{0}` was not found in the schema",
    'no_definitions': "No definitions found in context",
}


SCHEMA_MESSAGES = {
    'body_parameters_must_include_a_schema': (
        "A Parameter who's `in` value is 'body' must declare a schema."
    ),
}

COLLECTION_FORMAT_MESSAGES = {
    'invalid_based_on_in_value': (
        "The collectionFormat 'multi' is only valid for `in` values of "
        "\"query\" or \"formData\"."
    ),
}


MESSAGES = {
    'type': TYPE_MESSAGES,
    'format': FORMAT_MESSAGES,
    'required': REQUIRED_MESSAGES,
    'multiple_of': MULTIPLE_OF_MESSAGES,
    'minimum': MINIMUM_AND_MAXIMUM_MESSAGES,
    'maximum': MINIMUM_AND_MAXIMUM_MESSAGES,
    'max_length': MAX_LENGTH_MESSAGES,
    'min_length': MIN_LENGTH_MESSAGES,
    'min_items': MIN_ITEMS_MESSAGES,
    'max_items': MAX_ITEMS_MESSAGES,
    'min_properties': MIN_PROPERTIES_MESSAGES,
    'max_properties': MAX_PROPERTIES_MESSAGES,
    'additional_properties': ADDITIONAL_PROPERTIES_MESSAGES,
    'unique_items': UNIQUE_ITEMS_MESSAGES,
    'enum': ENUM_MESSAGES,
    'pattern': PATTERN_MESSAGES,
    'items': ITEMS_MESSAGES,
    'request': REQUEST_MESSAGES,
    'response': RESPONSE_MESSAGES,
    'path': PATH_MESSAGES,
    'content_type': CONTENT_TYPE_MESSAGES,
    'host': HOST_MESSAGES,
    'schemes': SCHEMES_MESSAGES,
    'mimetype': MIMETYPE_MESSAGES,
    'default': DEFAULT_MESSAGES,
    'reference': REFERENCE_MESSAGES,
    'schema': SCHEMA_MESSAGES,
    'collection_format': COLLECTION_FORMAT_MESSAGES,
}
