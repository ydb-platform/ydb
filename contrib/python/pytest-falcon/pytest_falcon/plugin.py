# -*- coding:utf-8 -*-

from io import BytesIO
import json
import mimetypes
from uuid import uuid4

import pytest
from falcon.testing.helpers import create_environ
from falcon.testing.srmock import StartResponseMock

try:
    from urllib.parse import urlparse, urlencode
except ImportError:
    from urlparse import urlparse
    from urllib import urlencode


def encode_multipart(data, charset='utf-8'):
    # Ported from Werkzeug testing.
    boundary = '---------------Boundary%s' % uuid4().hex
    body = BytesIO()

    def write(string):
        body.write(string.encode(charset))

    if isinstance(data, dict):
        data = data.items()

    for key, values in data:
        if not isinstance(values, (list, tuple)):
            values = [values]
        for value in values:
            write('--%s\r\nContent-Disposition: form-data; name="%s"' %
                  (boundary, key))
            reader = getattr(value, 'read', None)
            if reader is not None:
                filename = getattr(value, 'filename',
                                   getattr(value, 'name', None))
                content_type = getattr(value, 'content_type', None)
                if content_type is None:
                    content_type = filename and \
                        mimetypes.guess_type(filename)[0] or \
                        'application/octet-stream'
                if filename is not None:
                    write('; filename="%s"\r\n' % filename)
                else:
                    write('\r\n')
                write('Content-Type: %s\r\n\r\n' % content_type)
                while 1:
                    chunk = reader(16384)
                    if not chunk:
                        break
                    body.write(chunk)
            else:
                if not isinstance(value, str):
                    value = str(value)
                else:
                    value = value.encode(charset)
                write('\r\n\r\n')
                body.write(value)
            write('\r\n')
    write('--%s--\r\n' % boundary)

    body.seek(0)
    content_type = 'multipart/form-data; boundary=%s' % boundary
    return body.read(), content_type


class Client(object):

    content_type = 'application/x-www-form-urlencoded'

    def __init__(self, app, **kwargs):
        self.app = app
        self._before = kwargs.get('before')
        self._after = kwargs.get('after')

    def __call__(self, app=None, **kwargs):
        return Client(app or self.app, **kwargs)

    def encode_body(self, kwargs):
        body = kwargs.get('body')
        headers = kwargs.get('headers')
        content_type = headers.get('Content-Type')
        if not body:
            kwargs['body'] = ''  # Workaround None or empty dict values.
        if not body or isinstance(body, str):
            return
        if not content_type:
            if self.content_type:
                headers['Content-Type'] = content_type = self.content_type
            else:
                return
        if 'application/x-www-form-urlencoded' in content_type:
            kwargs['body'] = urlencode(body)
        elif 'multipart/form-data' in content_type:
            kwargs['body'], headers['Content-Type'] = encode_multipart(body)
        elif 'application/json' in headers['Content-Type']:
            kwargs['body'] = json.dumps(body)

    def handle_files(self, kwargs):
        files = kwargs.pop('files', None)
        if files:
            kwargs.setdefault('body', {})
            kwargs['headers']['Content-Type'] = 'multipart/form-data'
            if isinstance(files, dict):
                files = files.items()
            for key, els in files:
                if not els:
                    continue
                if not isinstance(els, (list, tuple)):
                    # Allow passing a file instance.
                    els = [els]
                file_ = els[0]
                if isinstance(file_, str):
                    file_ = file_.encode()
                if isinstance(file_, bytes):
                    file_ = BytesIO(file_)
                if len(els) > 1:
                    file_.name = els[1]
                if len(els) > 2:
                    file_.charset = els[2]
                kwargs['body'][key] = file_

    def fake_request(self, path, **kwargs):
        kwargs.setdefault('headers', {})
        content_type = kwargs.pop('content_type', None)
        if content_type:
            kwargs['headers']['Content-Type'] = content_type
        if self._before:
            self._before(kwargs)
        parsed = urlparse(path)
        path = parsed.path
        if parsed.query:
            kwargs['query_string'] = parsed.query
        if isinstance(kwargs.get('query_string', None), dict):
            kwargs['query_string'] = urlencode(kwargs['query_string'])
        self.encode_body(kwargs)
        resp = StartResponseMock()
        environ = create_environ(path, **kwargs)
        resp.environ = environ
        body = self.app(environ, resp)
        resp.headers = resp.headers_dict
        resp.status_code = int(resp.status.split(' ')[0])
        resp.body = b''.join(list(body)) if body else b''
        try:
            # …to be smart and provide the response as str if it let iself
            # decode.
            resp.body = resp.body.decode()
        except UnicodeDecodeError:
            # But do not insist.
            pass
        if 'application/json' in resp.headers.get('Content-Type', ''):
            resp.json = json.loads(resp.body) if resp.body else {}
        if self._after:
            self._after(resp)
        return resp

    def get(self, path, **kwargs):
        return self.fake_request(path, method='GET', **kwargs)

    def post(self, path, data=None, **kwargs):
        kwargs.setdefault('headers', {})
        kwargs.setdefault('body', data or {})
        self.handle_files(kwargs)
        return self.fake_request(path, method='POST', **kwargs)

    def put(self, path, body, **kwargs):
        return self.fake_request(path, method='PUT', body=body, **kwargs)

    def patch(self, path, body, **kwargs):
        return self.fake_request(path, method='PATCH', body=body, **kwargs)

    def delete(self, path, **kwargs):
        return self.fake_request(path, method='DELETE', **kwargs)

    def options(self, path, **kwargs):
        return self.fake_request(path, method='OPTIONS', **kwargs)

    def before(self, func):
        self._before = func

    def after(self, func):
        self._after = func


@pytest.fixture
def client(app):
    return Client(app)
