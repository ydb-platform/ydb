import os
import json

import falcon
import pytest
from pytest_falcon.plugin import Client

application = falcon.API()
application.req_options.auto_parse_form_urlencoded = True


@pytest.fixture
def app():
    return application


@pytest.fixture
def client(app):
    return Client(app)


def test_get(client):

    class Resource:

        def on_get(self, req, resp, **kwargs):
            resp.body = '{"foo": "bar"}'

    application.add_route('/route', Resource())

    resp = client.get('/route')
    assert resp.status == falcon.HTTP_OK
    assert resp.json['foo'] == 'bar'


def test_stream_response(client):
    filepath = 'image.jpg'

    class Resource:
        def on_get(self, req, resp, **kwargs):
            image = open(filepath, 'rb')
            resp.set_stream(image, os.path.getsize(filepath))
            resp.content_type = 'image/jpeg'

    application.add_route('/route', Resource())

    resp = client.get('/route')
    assert resp.status == falcon.HTTP_OK
    assert resp.headers['content-length'] == '25919'
    img = open(filepath, 'rb')
    assert img.read() == resp.body


def test_empty_response_with_json_content_type_should_not_fail(client):

    class Resource:

        def on_get(self, req, resp, **kwargs):
            resp.set_header('Content-Type', 'application/json')

    application.add_route('/route', Resource())

    resp = client.get('/route')
    assert resp.status == falcon.HTTP_OK
    assert resp.json == {}


def test_post(client):

    class Resource:

        def on_post(self, req, resp, **kwargs):
            resp.body = json.dumps(req.params)

    application.add_route('/route', Resource())

    resp = client.post('/route', {'myparam': 'myvalue'},
                       content_type='application/x-www-form-urlencoded')
    assert resp.status == falcon.HTTP_OK
    assert resp.json['myparam'] == 'myvalue'


def test_post_with_content_type_shoud_not_reencode(client):

    class Resource:

        def on_post(self, req, resp, **kwargs):
            resp.data = req.stream.read()

    application.add_route('/route', Resource())

    headers = {'Content-Type': 'application/json'}
    resp = client.post('/route', '{"myparam": "myvalue"}', headers=headers)
    assert resp.status == falcon.HTTP_OK
    assert resp.json['myparam'] == 'myvalue'


def test_post_with_json_content_type_shoud_reencode_dict(client):

    class Resource:

        def on_post(self, req, resp, **kwargs):
            resp.data = req.stream.read()

    application.add_route('/route', Resource())

    headers = {'Content-Type': 'application/json'}
    resp = client.post('/route', {"myparam": "myvalue"}, headers=headers)
    assert resp.status == falcon.HTTP_OK
    assert resp.json['myparam'] == 'myvalue'


def test_empty_post_data_should_not_fail(client):

    class Resource:

        def on_post(self, req, resp, **kwargs):
            resp.body = json.dumps(req.params)

    application.add_route('/route', Resource())

    resp = client.post('/route', {})
    assert resp.status == falcon.HTTP_OK
    assert resp.json == {}

    resp = client.post('/route', None)
    assert resp.status == falcon.HTTP_OK
    assert resp.json == {}

    resp = client.post('/route')
    assert resp.status == falcon.HTTP_OK
    assert resp.json == {}


def test_put(client):

    class Resource:

        def on_put(self, req, resp, **kwargs):
            resp.body = req.stream.read().decode()

    application.add_route('/route', Resource())

    resp = client.put('/route', '{"myparam": "myvalue"}')
    assert resp.status == falcon.HTTP_OK
    assert resp.json['myparam'] == 'myvalue'


def test_put_with_json_content_type_shoud_reencode_dict(client):

    class Resource:

        def on_put(self, req, resp, **kwargs):
            resp.data = req.stream.read()

    application.add_route('/route', Resource())

    headers = {'Content-Type': 'application/json'}
    resp = client.put('/route', {"myparam": "myvalue"}, headers=headers)
    assert resp.status == falcon.HTTP_OK
    assert resp.json['myparam'] == 'myvalue'


def test_patch(client):

    class Resource:

        def on_patch(self, req, resp, **kwargs):
            resp.body = req.stream.read().decode()

    application.add_route('/route', Resource())

    resp = client.patch('/route', '{"myparam": "myvalue"}')
    assert resp.status == falcon.HTTP_OK
    assert resp.json['myparam'] == 'myvalue'


def test_delete(client):

    class Resource:

        def on_delete(self, req, resp, **kwargs):
            resp.status = falcon.HTTP_204

    application.add_route('/route', Resource())

    resp = client.delete('/route')
    assert resp.status == falcon.HTTP_204


def test_custom_header(client):

    class Resource:

        def on_get(self, req, resp, **kwargs):
            resp.set_headers(req.headers)

    application.add_route('/route', Resource())
    resp = client.get('/route', headers={'X-Foo': 'Bar'})
    assert resp.headers['X-Foo'] == 'Bar'


def test_non_json_content_type(client):

    class Resource:

        def on_get(self, req, resp, **kwargs):
            resp.set_header('Content-Type', 'text/html')
            resp.body = '<html></html>'

    application.add_route('/route', Resource())
    resp = client.get('/route')
    assert resp.body == '<html></html>'


def test_content_type_can_be_set_at_client_level(client):

    class Resource:

        def on_post(self, req, resp, **kwargs):
            resp.body = json.dumps(req.params)

    application.add_route('/route', Resource())

    client.content_type = 'application/x-www-form-urlencoded'
    resp = client.post('/route', {'myparam': 'myvalue'})
    assert resp.status == falcon.HTTP_OK
    assert resp.json['myparam'] == 'myvalue'


def test_before_as_parameter(client):

    class Resource:

        def on_get(self, req, resp, **kwargs):
            resp.set_headers(req.headers)

    def patch_kwargs(kwargs):
        kwargs['headers']['X-Patched'] = 'Yep'

    application.add_route('/route', Resource())
    client = client(before=patch_kwargs)
    resp = client.get('/route')
    assert resp.headers['X-Patched'] == 'Yep'


def test_before_as_decorator(client):

    class Resource:

        def on_get(self, req, resp, **kwargs):
            resp.set_headers(req.headers)

    @client.before
    def patch_kwargs(kwargs):
        kwargs['headers']['X-Patched'] = 'Yep'

    application.add_route('/route', Resource())
    resp = client.get('/route')
    assert resp.headers['X-Patched'] == 'Yep'


def test_after_as_parameter(client):

    class Resource:

        def on_get(self, req, resp, **kwargs):
            resp.body = '{"foo": "bar"}'

    def patch_response(resp):
        resp.my_prop = 'ok'

    application.add_route('/route', Resource())
    client = client(after=patch_response)
    resp = client.get('/route')
    assert resp.my_prop == 'ok'


def test_after_as_decorator(client):

    class Resource:

        def on_get(self, req, resp, **kwargs):
            resp.body = '{"foo": "bar"}'

    @client.after
    def patch_response(resp):
        resp.my_prop = 'ok'

    application.add_route('/route', Resource())
    resp = client.get('/route')
    assert resp.my_prop == 'ok'


def test_query_string_can_be_passed_as_string(client):

    class Resource:

        def on_get(self, req, resp, **kwargs):
            resp.body = json.dumps(req.params)

    application.add_route('/route', Resource())
    resp = client.get('/route', query_string='myparam=myvalue')
    assert resp.json['myparam'] == 'myvalue'


def test_query_string_can_be_passed_as_dict(client):

    class Resource:

        def on_get(self, req, resp, **kwargs):
            resp.body = json.dumps(req.params)

    application.add_route('/route', Resource())
    resp = client.get('/route', query_string={'myparam': 'myvalue'})
    assert resp.json['myparam'] == 'myvalue'


def test_query_string_can_be_passed_in_the_url(client):

    class Resource:

        def on_get(self, req, resp, **kwargs):
            resp.body = json.dumps(req.params)

    application.add_route('/route', Resource())
    resp = client.get('/route?myparam=myvalue')
    assert resp.json['myparam'] == 'myvalue'
