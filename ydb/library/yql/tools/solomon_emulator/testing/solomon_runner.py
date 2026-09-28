import os
import pytest
import requests


class SolomonWrapper(object):
    def __init__(self, url, http_endpoint, grpc_endpoint):
        self._url = url
        self._endpoint = http_endpoint
        self._grpc_endpoint = grpc_endpoint
        self.table_prefix = ""

    def is_valid(self):
        return self._url is not None

    def cleanup(self):
        res = requests.post(self._url + "/cleanup")
        res.raise_for_status()

    def get_metrics(self):
        res = requests.get(self._url + "/metrics/get?project=my_project&cluster=my_cluster&service=my_service")
        res.raise_for_status()
        return res.text

    def prepare_program(self, program, program_file, res_dir, lang='sql'):
        return program, program_file

    @property
    def url(self):
        return self._url

    @property
    def endpoint(self):
        return self._endpoint

    @property
    def grpc_endpoint(self):
        return self._grpc_endpoint


@pytest.fixture(scope='module')
def solomon_emulator(request):
    solomon_url = os.environ.get("SOLOMON_HTTP_URL")
    solomon_endpoint = os.environ.get("SOLOMON_HTTP_ENDPOINT")
    solomon_grpc_endpoint = os.environ.get("SOLOMON_GRPC_ENDPOINT")
    return SolomonWrapper(solomon_url, solomon_endpoint, solomon_grpc_endpoint)
