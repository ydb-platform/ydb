import os
from six.moves import urllib_parse as urlparse
import requests
import responses
import json
import tempfile
from library.python import resource

from flex.exceptions import ValidationError
from flex.core import load, validate_api_call, validate_api_request, validate_api_response
from flex.error_messages import MESSAGES

import pytest
import tests
from tests.utils import (
    assert_message_in_errors
)

BASE_DIR = os.path.dirname(tests.__file__)


def test_validate_api_request(httpserver):
    httpserver.serve_content('{}', code=200, headers={'content-type': 'application/json'})
    with tempfile.NamedTemporaryFile() as f:
        f.write(resource.find('schemas/httpbin.yaml'))
        f.flush()
        schema = load(f.name)
        response = requests.get(urlparse.urljoin(httpserver.url, '/get'))
        validate_api_request(schema, raw_request=response.request)


def test_validate_api_response(httpserver):
    httpserver.serve_content('{}', code=200, headers={'content-type': 'application/json'})
    with tempfile.NamedTemporaryFile() as f:
        f.write(resource.find('schemas/httpbin.yaml'))
        f.flush()
        schema = load(f.name)
        response = requests.get(urlparse.urljoin(httpserver.url, '/get'))
        validate_api_response(schema, raw_response=response)


def test_validate_api_call(httpserver):
    httpserver.serve_content('{}', code=200, headers={'content-type': 'application/json'})
    with tempfile.NamedTemporaryFile() as f:
        f.write(resource.find('schemas/httpbin.yaml'))
        f.flush()
        schema = load(f.name)
        response = requests.get(urlparse.urljoin(httpserver.url, '/get'))
        validate_api_call(schema, raw_request=response.request, raw_response=response)


@responses.activate
def test_invalid_api_call_with_polymorphism():
    request_payload = """{
        "events": [
            {
                "eventType": "Impression",
                "timestamp": 12312312
            }
        ]
    }"""
    responses.add(responses.POST, "http://test.com/poly/report",
                  body="{}", status=200, content_type="application/json")

    response = requests.post(
        "http://test.com/poly/report",
        json=json.loads(request_payload),
        headers={'content-type': 'application/json'}
    )

    with tempfile.NamedTemporaryFile() as f:
        f.write(resource.find('schemas/polymorphism.yaml'))
        f.flush()
        schema = load(f.name)

    with pytest.raises(ValidationError) as err:
        validate_api_call(schema, raw_request=response.request, raw_response=response)

    assert_message_in_errors(
        MESSAGES['required']['required'],
        err.value.detail,
    )


@responses.activate
def test_valid_api_call_with_polymorphism():
    request_payload = """{
        "events": [
            {
                "eventType": "Impression",
                "advertisementId" : "ad7",
                "timestamp": 12312312
            }
        ]
    }"""
    responses.add(responses.POST, "http://test.com/poly/report",
                  body="{}", status=200, content_type="application/json")
    response = requests.post("http://test.com/poly/report",
                             json=json.loads(request_payload))
    with tempfile.NamedTemporaryFile() as f:
        f.write(resource.find('schemas/polymorphism.yaml'))
        f.flush()
        schema = load(f.name)
    validate_api_call(schema, raw_request=response.request, raw_response=response)
