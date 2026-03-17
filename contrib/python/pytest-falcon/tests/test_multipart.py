from io import BytesIO

import falcon
import pytest
from pytest_falcon.plugin import Client

try:
    from falcon_multipart.middleware import MultipartMiddleware
    application = falcon.API(middleware=MultipartMiddleware())
except ImportError:
    MultipartMiddleware = None
    application = None

pytestmark = pytest.mark.skipif(MultipartMiddleware is None,
                                reason='falcon_multipart not available')


@pytest.fixture
def app():
    return application


@pytest.fixture
def client(app):
    return Client(app)


@pytest.mark.parametrize('params', [
    ('filecontent', 'afile.txt'),
    (b'filecontent', 'afile.txt'),
    (BytesIO(b'filecontent'), 'afile.txt'),
])
def test_should_handle_files_kwarg_on_post(client, params):

    class Resource:

        def on_post(self, req, resp, **kwargs):
            assert req.get_param('afile').file.read() == b'filecontent'
            assert req.get_param('afile').filename == 'afile.txt'
            resp.body = 'parsed'
            resp.content_type = 'text/plain'

    application.add_route('/route', Resource())

    resp = client.post('/route', files={'afile': params})
    assert resp.status == falcon.HTTP_OK
    assert resp.body == 'parsed'


def test_should_accept_file_as_files_value(client):
    filepath = 'image.jpg'
    image = open(filepath, 'rb')

    class Resource:

        def on_post(self, req, resp, **kwargs):
            resp.data = req.get_param('afile').file.read()
            resp.content_type = 'image/jpg'

    application.add_route('/route', Resource())

    resp = client.post('/route', files={'afile': image})
    assert resp.status == falcon.HTTP_OK
    image.seek(0)
    assert resp.body == image.read()


def test_merge_files_and_data_into_body(client):

    class Resource:

        def on_post(self, req, resp, **kwargs):
            assert req.get_param('simple') == 'ok'
            assert req.get_param('afile').file.read() == b'filecontent'
            assert req.get_param('afile').filename == 'afile.txt'
            resp.body = 'parsed'
            resp.content_type = 'text/plain'

    application.add_route('/route', Resource())

    resp = client.post('/route', data={'simple': 'ok'},
                       files={'afile': ('filecontent', 'afile.txt')})
    assert resp.status == falcon.HTTP_OK
    assert resp.body == 'parsed'
