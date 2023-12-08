import logging
import requests

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

MAX_RETRY_FOR_SESSION = 4
BACK_OFF_FACTOR = 0.3
TIME_BETWEEN_RETRIES = 1000
ERROR_CODES = (500, 502, 504)


def requests_retry_session(retries=MAX_RETRY_FOR_SESSION,
                           back_off_factor=BACK_OFF_FACTOR,
                           status_force_list=ERROR_CODES,
                           session=None):
    retry = Retry(total=retries, read=retries, connect=retries,
                  backoff_factor=back_off_factor,
                  status_forcelist=status_force_list,
                  method_whitelist=frozenset(['GET', 'POST']))
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session


class YandexQueryHttpClient(object):
    def __init__(self, endpoint, token=None, project=None, db=None):
        self.endpoint = endpoint.rstrip("/")
        self.db = db
        self.project = project
        self.token = token
        self.session = requests_retry_session(session=requests.Session())

    def _build_headers(self, idempotency_key=None, request_id=None):
        headers = {
            "Authorization": self.token
        }
        if idempotency_key is not None:
            headers["Idempotency-Key"] = idempotency_key

        if request_id is not None:
            headers["x-request-id"] = request_id

        return headers

    def _build_params(self):
        params = {}
        if self.project is not None:
            params["project"] = self.project

        if self.db is not None:
            params["db"] = self.db

        return params

    def _compose_url(self, path):
        return self.endpoint + path

    def _validate_http_error(self, response, expected_code=200):
        logging.info(f"Response: {response.status_code}, {response.text}")
        if response.status_code != expected_code:
            raise Exception(f"Error occurred: {response.status_code}, {response.text}")

    def create_query(self,
                     query_text=None,
                     type=None,
                     name=None,
                     description=None,
                     idempotency_key=None,
                     request_id=None,
                     expected_code=200):
        body = dict()
        if query_text is not None:
            body["text"] = query_text

        if type is not None:
            body["type"] = type

        if name is not None:
            body["name"] = name

        if description is not None:
            body["description"] = description

        response = self.session.post(self._compose_url("/api/fq/v1/queries"),
                                     headers=self._build_headers(idempotency_key=idempotency_key,
                                                                 request_id=request_id),
                                     params=self._build_params(),
                                     json=body)

        self._validate_http_error(response, expected_code=expected_code)
        return response.json()

    def get_query_status(self, query_id, request_id=None, expected_code=200):
        response = self.session.get(self._compose_url(f"/api/fq/v1/queries/{query_id}/status"),
                                    headers=self._build_headers(request_id=request_id),
                                    params=self._build_params()
                                    )

        self._validate_http_error(response, expected_code=expected_code)
        return response.json()

    def get_query(self, query_id, request_id=None, expected_code=200):
        response = self.session.get(self._compose_url(f"/api/fq/v1/queries/{query_id}"),
                                    headers=self._build_headers(request_id=request_id),
                                    params=self._build_params())

        self._validate_http_error(response, expected_code=expected_code)
        return response.json()

    def stop_query(self, query_id, idempotency_key=None, request_id=None, expected_code=204):
        response = self.session.post(self._compose_url(f"/api/fq/v1/queries/{query_id}/stop"),
                                     headers=self._build_headers(idempotency_key=idempotency_key,
                                                                 request_id=request_id),
                                     params=self._build_params())
        self._validate_http_error(response, expected_code=expected_code)
        return response

    def get_query_results(self, query_id, result_set_index, offset=None, limit=None, request_id=None,
                          expected_code=200):
        params = self._build_params()
        if offset is not None:
            params["offset"] = offset

        if limit is not None:
            params["limit"] = limit

        response = self.session.get(self._compose_url(f"/api/fq/v1/queries/{query_id}/results/{result_set_index}"),
                                    headers=self._build_headers(request_id=request_id),
                                    params=params)

        self._validate_http_error(response, expected_code=expected_code)
        return response.json()

    def get_openapi_spec(self):
        response = self.session.get(self._compose_url("/resources/v1/openapi.yaml"))
        self._validate_http_error(response)
        return response.text
