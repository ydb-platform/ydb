from io import BytesIO

import falcon

from .parser import Parser


class MultipartMiddleware(object):

    def __init__(self, parser=None):
        self.parser = parser or Parser

    def parse(self, stream, environ):
        return self.parser(fp=stream, environ=environ)

    def parse_field(self, field):
        if isinstance(field, list):
            return [self.parse_field(subfield) for subfield in field]

        # When file name isn't ascii FieldStorage will not consider it.
        encoded = field.disposition_options.get('filename*')
        if encoded:
            # http://stackoverflow.com/a/93688
            encoding, filename = encoded.split("''")
            field.filename = filename
            # FieldStorage will decode the file content by itself when
            # file name is encoded, but we need to keep a consistent
            # API, so let's go back to bytes.
            # WARNING we assume file encoding will be same as filename
            # but there is no guaranty.
            field.file = BytesIO(field.file.read().encode(encoding))
        if getattr(field, 'filename', False):
            return field
        # This is not a file, thus get flat value (not
        # FieldStorage instance).
        return field.value

    def process_request(self, req, resp, **kwargs):

        if 'multipart/form-data' not in (req.content_type or ''):
            return

        # This must be done to avoid a bug in cgi.FieldStorage.
        req.env.setdefault('QUERY_STRING', '')

        # To avoid all stream consumption problem which occurs in falcon 1.0.0
        # or above.
        stream = (req.stream.stream if hasattr(req.stream, 'stream') else
                  req.stream)
        try:
            form = self.parse(stream=stream, environ=req.env)
        except ValueError as e:  # Invalid boundary?
            raise falcon.HTTPBadRequest('Error parsing file', str(e))

        for key in form:
            # TODO: put files in req.files instead when #418 get merged.
            req._params[key] = self.parse_field(form[key])
