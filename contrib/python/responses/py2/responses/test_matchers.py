from __future__ import absolute_import, print_function, division, unicode_literals

import six
from sys import version_info

import pytest
import requests
import responses
from requests.exceptions import ConnectionError
from responses import matchers


def assert_response(resp, body=None, content_type="text/plain"):
    assert resp.status_code == 200
    assert resp.reason == "OK"
    assert resp.headers["Content-Type"] == content_type
    assert resp.text == body


def assert_reset():
    assert len(responses._default_mock.registered()) == 0
    assert len(responses.calls) == 0


def test_query_string_matcher():
    @responses.activate
    def run():
        url = "http://example.com?test=1&foo=bar"
        responses.add(
            responses.GET,
            url,
            body=b"test",
            match=[matchers.query_string_matcher("test=1&foo=bar")],
        )
        resp = requests.get("http://example.com?test=1&foo=bar")
        assert_response(resp, "test")
        resp = requests.get("http://example.com?foo=bar&test=1")
        assert_response(resp, "test")
        resp = requests.get("http://example.com/?foo=bar&test=1")
        assert_response(resp, "test")

    run()
    assert_reset()


def test_request_matches_post_params():
    @responses.activate
    def run(deprecated):
        if deprecated:
            json_params_matcher = getattr(responses, "json_params_matcher")
            urlencoded_params_matcher = getattr(responses, "urlencoded_params_matcher")
        else:
            json_params_matcher = matchers.json_params_matcher
            urlencoded_params_matcher = matchers.urlencoded_params_matcher

        responses.add(
            method=responses.POST,
            url="http://example.com/",
            body="one",
            match=[json_params_matcher({"page": {"name": "first", "type": "json"}})],
        )
        responses.add(
            method=responses.POST,
            url="http://example.com/",
            body="two",
            match=[urlencoded_params_matcher({"page": "second", "type": "urlencoded"})],
        )

        resp = requests.request(
            "POST",
            "http://example.com/",
            headers={"Content-Type": "x-www-form-urlencoded"},
            data={"page": "second", "type": "urlencoded"},
        )
        assert_response(resp, "two")

        resp = requests.request(
            "POST",
            "http://example.com/",
            headers={"Content-Type": "application/json"},
            json={"page": {"name": "first", "type": "json"}},
        )
        assert_response(resp, "one")

    with pytest.deprecated_call():
        run(deprecated=True)
        assert_reset()

    run(deprecated=False)
    assert_reset()


def test_request_matches_empty_body():
    def run():
        with responses.RequestsMock(assert_all_requests_are_fired=True) as rsps:
            # test that both json and urlencoded body are empty in matcher and in request
            rsps.add(
                method=responses.POST,
                url="http://example.com/",
                body="one",
                match=[matchers.json_params_matcher(None)],
            )

            rsps.add(
                method=responses.POST,
                url="http://example.com/",
                body="two",
                match=[matchers.urlencoded_params_matcher(None)],
            )

            resp = requests.request("POST", "http://example.com/")
            assert_response(resp, "one")

            resp = requests.request(
                "POST",
                "http://example.com/",
                headers={"Content-Type": "x-www-form-urlencoded"},
            )
            assert_response(resp, "two")

        with responses.RequestsMock(assert_all_requests_are_fired=False) as rsps:
            # test exception raise if matcher body is None but request data is not None
            rsps.add(
                method=responses.POST,
                url="http://example.com/",
                body="one",
                match=[matchers.json_params_matcher(None)],
            )

            with pytest.raises(ConnectionError) as excinfo:
                resp = requests.request(
                    "POST",
                    "http://example.com/",
                    json={"my": "data"},
                    headers={"Content-Type": "application/json"},
                )

            msg = str(excinfo.value)
            assert "request.body doesn't match: {my: data} doesn't match {}" in msg

        with responses.RequestsMock(assert_all_requests_are_fired=False) as rsps:
            rsps.add(
                method=responses.POST,
                url="http://example.com/",
                body="two",
                match=[matchers.urlencoded_params_matcher(None)],
            )
            with pytest.raises(ConnectionError) as excinfo:
                resp = requests.request(
                    "POST",
                    "http://example.com/",
                    headers={"Content-Type": "x-www-form-urlencoded"},
                    data={"page": "second", "type": "urlencoded"},
                )
            msg = str(excinfo.value)
            assert (
                "request.body doesn't match: {page: second, type: urlencoded} doesn't match {}"
                in msg
            )

    run()
    assert_reset()


def test_request_matches_params():
    @responses.activate
    def run():
        url = "http://example.com/test"
        params = {"hello": "world", "I am": "a big test"}
        responses.add(
            method=responses.GET,
            url=url,
            body="test",
            match=[matchers.query_param_matcher(params)],
            match_querystring=False,
        )

        # exchange parameter places for the test
        params = {
            "I am": "a big test",
            "hello": "world",
        }
        resp = requests.get(url, params=params)

        if six.PY3 and version_info[1] >= 7:
            # only after py 3.7 dictionaries are ordered, so we can check URL
            constructed_url = r"http://example.com/test?I+am=a+big+test&hello=world"
            assert resp.url == constructed_url
            assert resp.request.url == constructed_url

        resp_params = getattr(resp.request, "params")
        assert resp_params == params

    run()
    assert_reset()


def test_fail_matchers_error():
    """
    Validate that Exception is raised if request does not match responses.matchers
        validate matchers.urlencoded_params_matcher
        validate matchers.json_params_matcher
        validate matchers.query_param_matcher
        validate matchers.request_kwargs_matcher
    :return: None
    """

    def run():
        with responses.RequestsMock(assert_all_requests_are_fired=False) as rsps:
            rsps.add(
                "POST",
                "http://example.com",
                match=[matchers.urlencoded_params_matcher({"foo": "bar"})],
            )
            rsps.add(
                "POST",
                "http://example.com",
                match=[matchers.json_params_matcher({"fail": "json"})],
            )

            with pytest.raises(ConnectionError) as excinfo:
                requests.post("http://example.com", data={"id": "bad"})

            msg = str(excinfo.value)
            assert (
                "request.body doesn't match: {id: bad} doesn't match {foo: bar}" in msg
            )

            assert (
                "request.body doesn't match: JSONDecodeError: Cannot parse request.body"
                in msg
            )

        with responses.RequestsMock(assert_all_requests_are_fired=False) as rsps:
            rsps.add(
                "GET",
                "http://111.com",
                match=[matchers.query_param_matcher({"my": "params"})],
            )

            rsps.add(
                method=responses.GET,
                url="http://111.com/",
                body="two",
                match=[matchers.json_params_matcher({"page": "one"})],
            )

            with pytest.raises(ConnectionError) as excinfo:
                requests.get(
                    "http://111.com", params={"id": "bad"}, json={"page": "two"}
                )

            msg = str(excinfo.value)
            assert (
                "Parameters do not match. {id: bad} doesn't match {my: params}" in msg
            )
            assert (
                "request.body doesn't match: {page: two} doesn't match {page: one}"
                in msg
            )

        with responses.RequestsMock(assert_all_requests_are_fired=False) as rsps:
            req_kwargs = {
                "stream": True,
                "verify": False,
            }
            rsps.add(
                "GET",
                "http://111.com",
                match=[matchers.request_kwargs_matcher(req_kwargs)],
            )

            with pytest.raises(ConnectionError) as excinfo:
                requests.get("http://111.com", stream=True)

            msg = str(excinfo.value)
            assert (
                "Arguments don't match: "
                "{stream: True, verify: True} doesn't match {stream: True, verify: False}"
            ) in msg

    run()
    assert_reset()


@pytest.mark.parametrize(
    "req_file,match_file",
    [
        (b"Old World!", "Old World!"),
        ("Old World!", b"Old World!"),
        (b"Old World!", b"Old World!"),
        ("Old World!", "Old World!"),
        (b"\xacHello World!", b"\xacHello World!"),
    ],
)
def test_multipart_matcher(req_file, match_file):
    @responses.activate
    def run():
        req_data = {"some": "other", "data": "fields"}
        responses.add(
            responses.POST,
            url="http://httpbin.org/post",
            match=[
                matchers.multipart_matcher(
                    files={"file_name": match_file}, data=req_data
                )
            ],
        )
        resp = requests.post(
            "http://httpbin.org/post", data=req_data, files={"file_name": req_file}
        )
        assert resp.status_code == 200

        with pytest.raises(TypeError):
            responses.add(
                responses.POST,
                url="http://httpbin.org/post",
                match=[matchers.multipart_matcher(files={})],
            )

    run()
    assert_reset()


def test_multipart_matcher_fail():
    """
    Validate that Exception is raised if request does not match responses.matchers
        validate matchers.multipart_matcher
    :return: None
    """

    def run():
        # different file contents
        with responses.RequestsMock(assert_all_requests_are_fired=False) as rsps:
            req_data = {"some": "other", "data": "fields"}
            req_files = {"file_name": b"Old World!"}
            rsps.add(
                responses.POST,
                url="http://httpbin.org/post",
                match=[matchers.multipart_matcher(req_files, data=req_data)],
            )

            with pytest.raises(ConnectionError) as excinfo:
                requests.post(
                    "http://httpbin.org/post",
                    data=req_data,
                    files={"file_name": b"New World!"},
                )

            msg = str(excinfo.value)
            assert "multipart/form-data doesn't match. Request body differs." in msg
            if six.PY2:
                assert (
                    '\r\nContent-Disposition: form-data; name="file_name"; '
                    'filename="file_name"\r\n\r\nOld World!\r\n'
                ) in msg
                assert (
                    '\r\nContent-Disposition: form-data; name="file_name"; '
                    'filename="file_name"\r\n\r\nNew World!\r\n'
                ) in msg
            else:
                assert (
                    r'\r\nContent-Disposition: form-data; name="file_name"; '
                    r'filename="file_name"\r\n\r\nOld World!\r\n'
                ) in msg
                assert (
                    r'\r\nContent-Disposition: form-data; name="file_name"; '
                    r'filename="file_name"\r\n\r\nNew World!\r\n'
                ) in msg

        # x-www-form-urlencoded request
        with responses.RequestsMock(assert_all_requests_are_fired=False) as rsps:
            req_data = {"some": "other", "data": "fields"}
            req_files = {"file_name": b"Old World!"}
            rsps.add(
                responses.POST,
                url="http://httpbin.org/post",
                match=[matchers.multipart_matcher(req_files, data=req_data)],
            )

            with pytest.raises(ConnectionError) as excinfo:
                requests.post("http://httpbin.org/post", data=req_data)

            msg = str(excinfo.value)
            assert (
                "multipart/form-data doesn't match. Request headers['Content-Type'] is different."
                in msg
            )
            assert (
                "application/x-www-form-urlencoded isn't equal to multipart/form-data; boundary="
                in msg
            )

        # empty body request
        with responses.RequestsMock(assert_all_requests_are_fired=False) as rsps:
            req_files = {"file_name": b"Old World!"}
            rsps.add(
                responses.POST,
                url="http://httpbin.org/post",
                match=[matchers.multipart_matcher(req_files)],
            )

            with pytest.raises(ConnectionError) as excinfo:
                requests.post("http://httpbin.org/post")

            msg = str(excinfo.value)
            assert "Request is missing the 'Content-Type' header" in msg

    run()
    assert_reset()


def test_query_string_matcher_raises():
    """
    Validate that Exception is raised if request does not match responses.matchers
        validate matchers.query_string_matcher
            :return: None
    """

    def run():
        with responses.RequestsMock(assert_all_requests_are_fired=False) as rsps:
            rsps.add(
                "GET",
                "http://111.com",
                match=[matchers.query_string_matcher("didi=pro")],
            )

            with pytest.raises(ConnectionError) as excinfo:
                requests.get("http://111.com", params={"test": "1", "didi": "pro"})

            msg = str(excinfo.value)
            assert (
                "Query string doesn't match. {didi: pro, test: 1} doesn't match {didi: pro}"
                in msg
            )

    run()
    assert_reset()


def test_request_matches_headers():
    @responses.activate
    def run():
        url = "http://example.com/"
        responses.add(
            method=responses.GET,
            url=url,
            json={"success": True},
            match=[matchers.header_matcher({"Accept": "application/json"})],
        )

        responses.add(
            method=responses.GET,
            url=url,
            body="success",
            match=[matchers.header_matcher({"Accept": "text/plain"})],
        )

        # the actual request can contain extra headers (requests always adds some itself anyway)
        resp = requests.get(
            url, headers={"Accept": "application/json", "Accept-Charset": "utf-8"}
        )
        assert_response(resp, body='{"success": true}', content_type="application/json")

        resp = requests.get(url, headers={"Accept": "text/plain"})
        assert_response(resp, body="success", content_type="text/plain")

    run()
    assert_reset()


def test_request_matches_headers_no_match():
    @responses.activate
    def run():
        url = "http://example.com/"
        responses.add(
            method=responses.GET,
            url=url,
            json={"success": True},
            match=[matchers.header_matcher({"Accept": "application/json"})],
        )

        with pytest.raises(ConnectionError) as excinfo:
            requests.get(url, headers={"Accept": "application/xml"})

        msg = str(excinfo.value)
        assert (
            "Headers do not match: {Accept: application/xml} doesn't match "
            "{Accept: application/json}"
        ) in msg

    run()
    assert_reset()


def test_request_matches_headers_strict_match():
    @responses.activate
    def run():
        url = "http://example.com/"
        responses.add(
            method=responses.GET,
            url=url,
            body="success",
            match=[
                matchers.header_matcher({"Accept": "text/plain"}, strict_match=True)
            ],
        )

        # requests will add some extra headers of its own, so we have to use prepared requests
        session = requests.Session()

        # make sure we send *just* the header we're expectin
        prepped = session.prepare_request(
            requests.Request(
                method="GET",
                url=url,
            )
        )
        prepped.headers.clear()
        prepped.headers["Accept"] = "text/plain"

        resp = session.send(prepped)
        assert_response(resp, body="success", content_type="text/plain")

        # include the "Accept-Charset" header, which will fail to match
        prepped = session.prepare_request(
            requests.Request(
                method="GET",
                url=url,
            )
        )
        prepped.headers.clear()
        prepped.headers["Accept"] = "text/plain"
        prepped.headers["Accept-Charset"] = "utf-8"

        with pytest.raises(ConnectionError) as excinfo:
            session.send(prepped)

        msg = str(excinfo.value)
        assert (
            "Headers do not match: {Accept: text/plain, Accept-Charset: utf-8} "
            "doesn't match {Accept: text/plain}"
        ) in msg

    run()
    assert_reset()


def test_fragment_identifier_matcher():
    @responses.activate
    def run():
        responses.add(
            responses.GET,
            "http://example.com",
            match=[matchers.fragment_identifier_matcher("test=1&foo=bar")],
            body=b"test",
        )

        resp = requests.get("http://example.com#test=1&foo=bar")
        assert_response(resp, "test")

    run()
    assert_reset()


def test_fragment_identifier_matcher_error():
    @responses.activate
    def run():
        responses.add(
            responses.GET,
            "http://example.com/",
            match=[matchers.fragment_identifier_matcher("test=1")],
        )
        responses.add(
            responses.GET,
            "http://example.com/",
            match=[matchers.fragment_identifier_matcher(None)],
        )

        with pytest.raises(ConnectionError) as excinfo:
            requests.get("http://example.com/#test=2")

        msg = str(excinfo.value)
        assert (
            "URL fragment identifier is different: test=1 doesn't match test=2"
        ) in msg
        assert (
            "URL fragment identifier is different: None doesn't match test=2"
        ) in msg

    run()
    assert_reset()


def test_fragment_identifier_matcher_and_match_querystring():
    @responses.activate
    def run():
        url = "http://example.com?ab=xy&zed=qwe#test=1&foo=bar"
        responses.add(
            responses.GET,
            url,
            match_querystring=True,
            match=[matchers.fragment_identifier_matcher("test=1&foo=bar")],
            body=b"test",
        )

        # two requests to check reversed order of fragment identifier
        resp = requests.get("http://example.com?ab=xy&zed=qwe#test=1&foo=bar")
        assert_response(resp, "test")
        resp = requests.get("http://example.com?zed=qwe&ab=xy#foo=bar&test=1")
        assert_response(resp, "test")

    run()
    assert_reset()


def test_matchers_create_key_val_str():
    """
    Test that matchers._create_key_val_str does recursive conversion
    """
    data = {
        "my_list": [
            1,
            2,
            "a",
            {"key1": "val1", "key2": 2, 3: "test"},
            "!",
            [["list", "nested"], {"nested": "dict"}],
        ],
        1: 4,
        "test": "val",
        "high": {"nested": "nested_dict"},
    }
    conv_str = matchers._create_key_val_str(data)
    reference = (
        "{1: 4, high: {nested: nested_dict}, my_list: [!, 1, 2, [[list, nested], {nested: dict}], "
        "a, {3: test, key1: val1, key2: 2}], test: val}"
    )
    assert conv_str == reference
