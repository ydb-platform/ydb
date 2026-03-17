"""
Functionality for converting from Swagger/OpenAPI 2.0 to OpenAPI 3.0.0.

The functions use https://mermade.org.uk/ APIs for conversion.
"""

__author__ = "Jens Finkhaeuser"
__copyright__ = "Copyright (c) 2018 Jens Finkhaeuser"
__license__ = "MIT"
__all__ = ()


class ConversionError(ValueError):
    pass  # pragma: nocover


def convert_str(spec_str, filename=None, **kwargs):
    """
    Convert the serialized spec.

    We parse the spec first to ensure there is no parse error, then
    send it off to the API for conversion.

    :param str spec_str: The specifications as string.
    :param str filename: [optional] Filename to determine the format from.
    :param str content_type: [optional] Content type to determine the format
        from.
    :return: The converted spec and content type.
    :rtype: tuple
    :raises ParseError: when parsing fails.
    :raises ConversionError: when conversion fails.
    """
    # Parse, but discard the results. The function raises parse error.
    from .util.formats import parse_spec_details

    spec, content_type, extension = parse_spec_details(spec_str, filename, **kwargs)

    # Ok, parsing went fine, so let's convert.
    data = {
        "source": spec_str,
    }
    if filename is not None:
        data["filename"] = filename
    else:
        data["filename"] = f"openapi{extension}"

    headers = {"accept": f"{content_type}; charset=utf-8"}

    # Convert via API
    import requests

    r = requests.post(
        "https://mermade.org.uk/api/v1/convert", data=data, headers=headers
    )
    if not r.ok:  # pragma: nocover
        raise ConversionError(
            "Could not convert spec: %d %s" % (r.status_code, r.reason)
        )

    return r.text, "{}; {}".format(r.headers["content-type"], r.apparent_encoding)


def convert_url(url, cache={}):
    """
    Fetch a URL, and try to convert it to OpenAPI 3.x.y.

    :param str url: The URL to fetch.
    :return: The converted spec and content type.
    :rtype: tuple
    :raises ParseError: when parsing fails.
    :raises ConversionError: when conversion fails.
    """
    # Fetch URL contents
    from .util.url import fetch_url_text

    content, content_type = fetch_url_text(url, cache)

    # Try converting
    return convert_str(content, None, content_type=content_type)


def convert_spec(parser_or_spec, parser_klass=None, *args, **kwargs):
    """
    Convert an already parsed spec to OpenAPI 3.x.y.

    Returns a new parser instance with the parsed specs, if possible.

    The first parameter is either a parsed OpenAPI 2.0 spec, or a parser
    instance, i.e. something derived from prance.BaseParser. If a parser,
    the returned parser's options are taken from this source parser.

    If the first parameter is a parsed spec, you must specify the class
    of parser to instantiate. You can specify other options as key word
    arguments. See the parser klass for details.

    Any key word arguments specified here also override options from a
    source parser.

    This parametrization may seem a little convoluted. What it does, though,
    is allow maximum flexibility. You can create parsed (but unvalidated)
    OpenAPI 3.0 specs even if you only have backends that support version 2.0.
    You can pass the source parser, and the lazy flag, and that's it. If your
    version 2.0 specs were valid, there's a good chance your converted 3.0
    specs are also valid.

    :param mixed parser_or_spec: A dict (spec) or an instance of BaseParser
    :param type parser_klass: [optional] A parser class to instantiate for
      the result.
    :return: A parser instance.
    :rtype: BaseParser or derived.
    """
    # Figure out exact configuration to use
    klass = None
    options = None
    spec = None

    from . import BaseParser

    if isinstance(parser_or_spec, BaseParser):
        # We have a parser instance
        klass = parser_klass or type(parser_or_spec)
        options = parser_or_spec.options.copy()
        options.update(kwargs)
        spec = parser_or_spec.specification
    else:
        # We must assume a specification
        klass = parser_klass or BaseParser
        options = kwargs.copy()
        spec = parser_or_spec

    # print('Class', klass)
    # print('Options', options)
    # print('Spec', spec)

    # We have to serialize the specs in order to convert them. Let's use
    # YAML.
    from .util import formats

    serialized = formats.serialize_spec(spec, content_type="text/yaml")

    # Convert serialized
    converted, ctype = convert_str(serialized, content_type="text/yaml")

    # Create parser with options
    result = klass(spec_string=converted, **options)
    return result
