import mimeparse

from tastypie.exceptions import BadRequest


def determine_format(request, serializer, default_format='application/json'):
    """
    Tries to "smartly" determine which output format is desired.

    First attempts to find a ``format`` override from the request and supplies
    that if found.

    If no request format was demanded, it falls back to ``mimeparse`` and the
    ``Accepts`` header, allowing specification that way.

    If still no format is found, returns the ``default_format`` (which defaults
    to ``application/json`` if not provided).

    NOTE: callers *must* be prepared to handle BadRequest exceptions due to
          malformed HTTP request headers!
    """
    # First, check if they forced the format.
    format = request.GET.get('format')
    if format:
        if format in serializer.formats:
            return serializer.get_mime_for_format(format)

    # If callback parameter is present, use JSONP.
    if 'callback' in request.GET and 'jsonp' in serializer.formats:
        return serializer.get_mime_for_format('jsonp')

    # Try to fallback on the Accepts header.
    accept = request.META.get('HTTP_ACCEPT', '*/*')
    if accept != '*/*':
        try:
            best_format = mimeparse.best_match(serializer.supported_formats_reversed, accept)
        except ValueError:
            raise BadRequest('Invalid Accept header')

        if best_format:
            return best_format

    # No valid 'Accept' header/formats. Sane default.
    return default_format


def build_content_type(format, encoding='utf-8'):
    """
    Appends character encoding to the provided format if not already present.
    """
    if 'charset' in format:
        return format

    if format in ('application/json', 'text/javascript'):
        return format

    return "%s; charset=%s" % (format, encoding)
