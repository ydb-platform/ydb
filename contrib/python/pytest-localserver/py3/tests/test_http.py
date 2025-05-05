import itertools
import subprocess
import sys
import textwrap

import pytest
import requests
from werkzeug.exceptions import ClientDisconnected

from pytest_localserver import http
from pytest_localserver import plugin


# define test fixture here again in order to run tests without having to
# install the plugin anew every single time
httpserver = plugin.httpserver


transfer_encoded = pytest.mark.parametrize(
    "transfer_encoding_header", ["Transfer-encoding", "Transfer-Encoding", "transfer-encoding", "TRANSFER-ENCODING"]
)


def test_httpserver_funcarg(httpserver):
    assert isinstance(httpserver, http.ContentServer)
    assert httpserver.is_alive()
    assert httpserver.server_address


def test_server_does_not_serve_file_at_startup(httpserver):
    assert httpserver.code == 204
    assert httpserver.content == ""


def test_some_content_retrieval(httpserver):
    httpserver.serve_content("TEST!")
    resp = requests.get(httpserver.url)
    assert resp.text == "TEST!"
    assert resp.status_code == 200


def test_request_is_stored(httpserver):
    httpserver.serve_content("TEST!")
    assert len(httpserver.requests) == 0
    requests.get(httpserver.url)
    assert len(httpserver.requests) == 1


def test_GET_request(httpserver):
    httpserver.serve_content("TEST!", headers={"Content-type": "text/plain"})
    resp = requests.get(httpserver.url, headers={"User-Agent": "Test method"})
    assert resp.text == "TEST!"
    assert resp.status_code == 200
    assert "text/plain" in resp.headers["Content-type"]


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
    httpserver.serve_content("TEST!", headers={"Content-type": "text/plain"})
    resp = requests.head(httpserver.url)
    assert resp.status_code == 200
    assert resp.headers["Content-type"] == "text/plain"


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


def test_POST_request_no_store_data(httpserver):
    headers = {"Content-type": "text/plain"}
    httpserver.serve_content("TEST!", store_request_data=False)
    requests.post(httpserver.url, data=b"testdata", headers=headers)

    request = httpserver.requests[-1]
    request.input_stream.close()

    with pytest.raises(ClientDisconnected):
        request.data


def test_POST_request_store_data(httpserver):
    headers = {"Content-type": "text/plain"}
    httpserver.serve_content("TEST!", store_request_data=True)
    requests.post(httpserver.url, data=b"testdata", headers=headers)

    request = httpserver.requests[-1]
    request.input_stream.close()

    assert httpserver.requests[-1].data == b"testdata"


@pytest.mark.parametrize("chunked_flag", [http.Chunked.YES, http.Chunked.AUTO, http.Chunked.NO])
def test_chunked_attribute_without_header(httpserver, chunked_flag):
    """
    Test that passing the chunked attribute to serve_content() properly sets
    the chunked property of the server.
    """
    httpserver.serve_content(("TEST!", "test"), headers={"Content-type": "text/plain"}, chunked=chunked_flag)
    assert httpserver.chunked == chunked_flag


@pytest.mark.parametrize("chunked_flag", [http.Chunked.YES, http.Chunked.AUTO, http.Chunked.NO])
def test_chunked_attribute_with_header(httpserver, chunked_flag):
    """
    Test that passing the chunked attribute to serve_content() properly sets
    the chunked property of the server even when the transfer-encoding header is
    also set.
    """
    httpserver.serve_content(
        ("TEST!", "test"), headers={"Content-type": "text/plain", "Transfer-encoding": "chunked"}, chunked=chunked_flag
    )
    assert httpserver.chunked == chunked_flag


@transfer_encoded
@pytest.mark.parametrize("chunked_flag", [http.Chunked.YES, http.Chunked.AUTO])
def test_GET_request_chunked_parameter(httpserver, transfer_encoding_header, chunked_flag):
    """
    Test that passing YES or AUTO as the chunked parameter to serve_content()
    causes the response to be sent using chunking when the Transfer-encoding
    header is also set.
    """
    httpserver.serve_content(
        ("TEST!", "test"),
        headers={"Content-type": "text/plain", transfer_encoding_header: "chunked"},
        chunked=chunked_flag,
    )
    resp = requests.get(httpserver.url, headers={"User-Agent": "Test method"})
    assert resp.text == "TEST!test"
    assert resp.status_code == 200
    assert "text/plain" in resp.headers["Content-type"]
    assert "chunked" in resp.headers["Transfer-encoding"]


@transfer_encoded
@pytest.mark.parametrize("chunked_flag", [http.Chunked.YES, http.Chunked.AUTO])
def test_GET_request_chunked_attribute(httpserver, transfer_encoding_header, chunked_flag):
    """
    Test that setting the chunked attribute of httpserver to YES or AUTO
    causes the response to be sent using chunking when the Transfer-encoding
    header is also set.
    """
    httpserver.serve_content(
        ("TEST!", "test"), headers={"Content-type": "text/plain", transfer_encoding_header: "chunked"}
    )
    httpserver.chunked = chunked_flag
    resp = requests.get(httpserver.url, headers={"User-Agent": "Test method"})
    assert resp.text == "TEST!test"
    assert resp.status_code == 200
    assert "text/plain" in resp.headers["Content-type"]
    assert "chunked" in resp.headers["Transfer-encoding"]


@transfer_encoded
def test_GET_request_not_chunked(httpserver, transfer_encoding_header):
    """
    Test that setting the chunked attribute of httpserver to NO causes
    the response not to be sent using chunking even if the Transfer-encoding
    header is set.
    """
    httpserver.serve_content(
        ("TEST!", "test"),
        headers={"Content-type": "text/plain", transfer_encoding_header: "chunked"},
        chunked=http.Chunked.NO,
    )
    with pytest.raises(requests.exceptions.ChunkedEncodingError):
        requests.get(httpserver.url, headers={"User-Agent": "Test method"})


@pytest.mark.parametrize("chunked_flag", [http.Chunked.NO, http.Chunked.AUTO])
def test_GET_request_chunked_parameter_no_header(httpserver, chunked_flag):
    """
    Test that passing NO or AUTO as the chunked parameter to serve_content()
    causes the response not to be sent using chunking when the Transfer-encoding
    header is not set.
    """
    httpserver.serve_content(("TEST!", "test"), headers={"Content-type": "text/plain"}, chunked=chunked_flag)
    resp = requests.get(httpserver.url, headers={"User-Agent": "Test method"})
    assert resp.text == "TEST!test"
    assert resp.status_code == 200
    assert "text/plain" in resp.headers["Content-type"]
    assert "Transfer-encoding" not in resp.headers


@pytest.mark.parametrize("chunked_flag", [http.Chunked.NO, http.Chunked.AUTO])
def test_GET_request_chunked_attribute_no_header(httpserver, chunked_flag):
    """
    Test that setting the chunked attribute of httpserver to NO or AUTO
    causes the response not to be sent using chunking when the Transfer-encoding
    header is not set.
    """
    httpserver.serve_content(("TEST!", "test"), headers={"Content-type": "text/plain"})
    httpserver.chunked = chunked_flag
    resp = requests.get(httpserver.url, headers={"User-Agent": "Test method"})
    assert resp.text == "TEST!test"
    assert resp.status_code == 200
    assert "text/plain" in resp.headers["Content-type"]
    assert "Transfer-encoding" not in resp.headers


def test_GET_request_chunked_no_header(httpserver):
    """
    Test that setting the chunked attribute of httpserver to YES causes
    the response to be sent using chunking even if the Transfer-encoding
    header is not set.
    """
    httpserver.serve_content(("TEST!", "test"), headers={"Content-type": "text/plain"}, chunked=http.Chunked.YES)
    resp = requests.get(httpserver.url, headers={"User-Agent": "Test method"})
    # Without the Transfer-encoding header set, requests does not undo the chunk
    # encoding so it comes through as "raw" chunks
    assert resp.text == "5\r\nTEST!\r\n4\r\ntest\r\n0\r\n\r\n"


def _format_chunk(chunk):
    r = repr(chunk)
    if len(r) <= 40:
        return r
    else:
        return r[:13] + "..." + r[-14:] + " (length {})".format(len(chunk))


def _compare_chunks(expected, actual):
    __tracebackhide__ = True
    if expected != actual:
        message = [_format_chunk(expected) + " != " + _format_chunk(actual)]
        if type(expected) is type(actual):
            for i, (e, a) in enumerate(itertools.zip_longest(expected, actual, fillvalue="<end>")):
                if e != a:
                    message += [
                        "  Chunks differ at index {}:".format(i),
                        "    Expected: " + (repr(expected[i : i + 5]) + "..." if e != "<end>" else "<end>"),
                        "    Found:    " + (repr(actual[i : i + 5]) + "..." if a != "<end>" else "<end>"),
                    ]
                    break
        pytest.fail("\n".join(message))


@pytest.mark.parametrize("chunk_size", [400, 499, 500, 512, 750, 1024, 4096, 8192])
def test_GET_request_large_chunks(httpserver, chunk_size):
    """
    Test that a response with large chunks comes through correctly
    """
    body = b"0123456789abcdef" * 1024  # 16 kb total
    # Split body into fixed-size chunks, from https://stackoverflow.com/a/18854817/56541
    chunks = [body[0 + i : chunk_size + i] for i in range(0, len(body), chunk_size)]
    httpserver.serve_content(
        chunks, headers={"Content-type": "text/plain", "Transfer-encoding": "chunked"}, chunked=http.Chunked.YES
    )
    resp = requests.get(httpserver.url, headers={"User-Agent": "Test method"}, stream=True)
    assert resp.status_code == 200
    text = b""
    for original_chunk, received_chunk in itertools.zip_longest(chunks, resp.iter_content(chunk_size=None)):
        _compare_chunks(original_chunk, received_chunk)
        text += received_chunk
    assert text == body
    assert "chunked" in resp.headers["Transfer-encoding"]


@pytest.mark.parametrize("chunked_flag", [http.Chunked.YES, http.Chunked.AUTO])
def test_GET_request_chunked_no_content_length(httpserver, chunked_flag):
    """
    Test that a chunked response does not include a Content-length header
    """
    httpserver.serve_content(
        ("TEST!", "test"), headers={"Content-type": "text/plain", "Transfer-encoding": "chunked"}, chunked=chunked_flag
    )
    resp = requests.get(httpserver.url, headers={"User-Agent": "Test method"})
    assert resp.status_code == 200
    assert "Transfer-encoding" in resp.headers
    assert "Content-length" not in resp.headers


def test_httpserver_init_failure_no_stderr_during_cleanup(tmp_path):
    """
    Test that, when the server encounters an error during __init__, its cleanup
    does not raise an AttributeError in its __del__ method, which would emit a
    warning onto stderr.
    """

    script_path = tmp_path.joinpath("script.py")

    script_path.write_text(
        textwrap.dedent(
            """
        from pytest_localserver import http
        from unittest.mock import patch

        with patch("pytest_localserver.http.make_server", side_effect=RuntimeError("init failure")):
            server = http.ContentServer()
    """
        )
    )

    result = subprocess.run([sys.executable, str(script_path)], stderr=subprocess.PIPE)

    # We ensure that no warning about an AttributeError is printed on stderr
    # due to an error in the server's __del__ method. This AttributeError is
    # raised during cleanup and doesn't affect the exit code of the script, so
    # we can't use the exit code to tell whether the script did its job.
    assert b"AttributeError" not in result.stderr
