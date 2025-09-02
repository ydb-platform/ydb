import requests

from pytest_localserver import http, plugin


# define test fixture here again in order to run tests without having to
# install the plugin anew every single time
httpserver = plugin.httpserver


def test_httpserver_funcarg(httpserver):
    assert isinstance(httpserver, http.ContentServer)
    assert httpserver.is_alive()
    assert httpserver.server_address


def test_server_does_not_serve_file_at_startup(httpserver):
    assert httpserver.code == 204
    assert httpserver.content == ''


def test_some_content_retrieval(httpserver):
    httpserver.serve_content('TEST!')
    resp = requests.get(httpserver.url)
    assert resp.text == 'TEST!'
    assert resp.status_code == 200


def test_request_is_stored(httpserver):
    httpserver.serve_content('TEST!')
    assert len(httpserver.requests) == 0
    resp = requests.get(httpserver.url)
    assert len(httpserver.requests) == 1


def test_GET_request(httpserver):
    httpserver.serve_content('TEST!', headers={'Content-type': 'text/plain'})
    resp = requests.get(httpserver.url, headers={'User-Agent': 'Test method'})
    assert resp.text == 'TEST!'
    assert resp.status_code == 200
    assert 'text/plain' in resp.headers['Content-type']


# FIXME get compression working!
# def test_gzipped_GET_request(httpserver):
#     httpserver.serve_content('TEST!', headers={'Content-type': 'text/plain'})
#     httpserver.compress = 'gzip'
#     resp = requests.get(httpserver.url, headers={
#         'User-Agent': 'Test method',
#         'Accept-encoding': 'gzip'
#     })
#     assert resp.text == 'TEST!'
#     assert resp.status_code == 200
#     assert resp.content_encoding == 'gzip'
#     assert resp.headers['Content-type'] == 'text/plain'
#     assert resp.headers['content-encoding'] == 'gzip'


def test_HEAD_request(httpserver):
    httpserver.serve_content('TEST!', headers={'Content-type': 'text/plain'})
    resp = requests.head(httpserver.url)
    assert resp.status_code == 200
    assert resp.headers['Content-type'] == 'text/plain'


# def test_POST_request(httpserver):
#     headers = {'Content-type': 'application/x-www-form-urlencoded',
#                'set-cookie': 'some _cookie_content'}
#
#     httpserver.serve_content('TEST!', headers=headers)
#     resp = requests.post(httpserver.url, data={'data': 'value'}, headers=headers)
#     assert resp.text == 'TEST!'
#     assert resp.status_code == 200
#
#     httpserver.serve_content('TEST!', headers=headers, show_post_vars=True)
#     resp = requests.post(httpserver.url, data={'data': 'value'}, headers=headers)
#     assert resp.json() == {'data': 'value'}
#     assert resp.status_code == 200
