import functools
import logging
from typing import List, Dict, Any, Union, Tuple

import requests
from requests.adapters import Retry, HTTPAdapter
from urllib3.exceptions import InsecureRequestWarning

from toloka.client import TolokaClient
from toloka.client.primitives.retry import TolokaRetry
from toloka.client.exceptions import raise_on_api_error

TST_YANDEX_COM = 'https://autotest-external-back.admin.toloka.tst.yandex.com'

requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

logger = logging.getLogger(__name__)


def _make_user_info(toloka_user: Dict[str, Any]) -> Dict[str, Any]:
    return {
        'type': 'INDIVIDUAL',
        'firstName': 'Testing',
        'lastName': 'Requester',
        'displayName': {'EN': toloka_user['displayName']},
        # 'displayName': toloka_user['displayName'],
        'registrationAddress': {'country': 'BY', 'cityId': 157, 'street': 'street', 'house': 'house', 'index': 'index'},
        'postAddress': {'country': 'BY', 'cityId': 157, 'street': 'street', 'house': 'house', 'index': 'index'},
        'contactPhone': '+375199999999',
        'userLang': 'RU',
        "rsaAccepted": True,
        "subscriptionOnNewsAccepted": False,
        'defaultEmail': toloka_user['login'] + '@yandex.ru'
        # f'{}requester_emaiil@example.org',
    }


def _make_worker_info(toloka_user: Dict[str, Any]) -> Dict[str, Any]:
    # ./autotest/xspec/src/main/groovy/code/worker/WorkerForm.groovy
    return {
        'uid': toloka_user['uid'],
        'login': toloka_user['login'],
        'defaultEmail': toloka_user['login'] + '@yandex.ru',
        'displayName': {'EN': toloka_user['displayName']},
        'citizenship': 'RU',
        'education': 'MIDDLE',
        'userLang': 'RU',
        'fullName': 'John Doe',
        'firstName': 'John',
        'lastName': 'Doe',
        'gender': 'MALE',
        'birthDay': '1990-01-01',
        'country': 'BY',
        'cityId': 157,
        'languages': ['RU', 'BE', 'EN'],
        'adultAllowed': True,
    }


class NDARegisterClient:
    def __init__(self):
        self._session = requests.Session()

        retries = Retry(
            total=20,
            backoff_factor=2,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=frozenset({'DELETE', 'GET', 'HEAD', 'OPTIONS', 'PUT', 'TRACE', 'POST', 'PATCH'}),
        )
        self._session.mount('https://', HTTPAdapter(max_retries=retries))

    def _create_user(self, user_type: str, url: str = TST_YANDEX_COM) -> Dict[str, Any]:
        response = self._session.post(
            f'{url}/public/testing/env/register', json={'loginSuffix': user_type}, verify=False
        )
        response.raise_for_status()
        logger.debug(f'Created user with oauth {response.json()["oauth"]}')
        return response.json()

    def create_admin(self, url: str = TST_YANDEX_COM) -> Dict[str, Any]:
        toloka_user = self._create_user('adm', url=url)
        # Grant admin privileges
        response = self._session.post(
            f'{url}/api/dmz/dev/admin', json={'uid': toloka_user['uid'], 'allowedToSpy': True}, verify=False
        )
        response.raise_for_status()
        return toloka_user

    def create_user(self, user_type: str = 'worker', url: str = TST_YANDEX_COM) -> Dict[str, Any]:
        registration_type = {
            'worker': 'wrk',
            'requester': 'req',
        }[user_type]
        user = self._create_user(registration_type)
        headers = {'Authorization': f'OAuth {user["oauth"]}'}

        # Making an attempt to set data to get an error with a conformation key
        user_info = {'worker': _make_worker_info, 'requester': _make_user_info}[user_type](user)

        if user_type == 'worker':
            user_info['displayName'] = user_info['displayName']['EN']
        response = self._session.post(
            f'{url}/api/users/current/{user_type}', headers=headers, json=user_info, verify=False
        )
        assert response.status_code == 409, (response.status_code, response.json())
        assert response.json()['code'] == 'NEED_PHONE_CONFIRMATION', response.json()
        key = response.json()['payload']['key']

        # Confirming data as if by SMS
        code = self._session.get(f'{url}/public/testing/env/sms?trackId={key}', verify=False).json()
        response = self._session.post(
            f'{url}/api/passport/sms/validate', headers=headers, verify=False, json={'key': key, 'code': code}
        )
        response.raise_for_status()

        # Actually setting user data
        response = self._session.post(
            f'{url}/api/users/current/{user_type}', headers=headers, json=user_info, verify=False
        )
        response.raise_for_status()
        return {**response.json(), **user}


class NDATolokaClient(TolokaClient):
    def __init__(self, token: str):
        retries = 60
        retry_quotas = TolokaRetry.Unit.MIN

        def retryer_factory(
            retries: int,
            retry_quotas: Union[List[str], str, None],
            status_list: Tuple[int] = (408, 409, 429, 500, 503, 504),
        ):
            return TolokaRetry(
                retry_quotas=retry_quotas,
                total=retries,
                status_forcelist=list(status_list),
                allowed_methods=['HEAD', 'GET', 'PUT', 'DELETE', 'OPTIONS', 'TRACE', 'POST', 'PATCH'],
                backoff_factor=2,  # summary retry time more than 10 seconds
            )

        super().__init__(
            token,
            url=TST_YANDEX_COM,
            retryer_factory=functools.partial(retryer_factory, retries, retry_quotas),
            timeout=(60.0, 240.0),
        )

    def _raw_request(self, method: str, path: str, **kwargs: Any) -> requests.Response:
        # return super()._raw_request(method, path, verify=False, **kwargs)

        if kwargs.get('params'):
            params = kwargs['params']
            for key, value in params.items():
                if isinstance(value, bool):
                    params[key] = 'true' if value else 'false'
        if self.default_timeout is not None and 'timeout' not in kwargs:
            kwargs['timeout'] = self.default_timeout

        response = self._session.request(method, f'{self.url}/api{path}', verify=False, **kwargs)
        raise_on_api_error(response)
        return response

    # For worker
    # AssignmentService.groovy
    def request_assignment(self, pool_id: str) -> Dict[str, Any]:
        return self._request('post', '/i-v2/assignment-executions', json={'poolId': pool_id})

    def skip_assignment(self, assignment_id: str) -> Dict[str, Any]:
        return self._request('post', f'/i-v2/assignment-executions/{assignment_id}/skip')

    def expire_assignment(self, assignment_id: str) -> Dict[str, Any]:
        return self._request('post', f'/i-v2/assignment-executions/{assignment_id}/expire')

    def submit_assignment(self, assignment_id: str, solutions: List[Dict[str, Any]]) -> Dict[str, Any]:
        solutions = {'solutions': [{'output_values': solution} for solution in solutions]}
        return self._request('post', f'/i-v2/assignment-executions/{assignment_id}/submit', json=solutions)

    # Requires admin privileges
    def change_credit(self, user_id: str, credit_diff: int) -> Dict[str, Any]:
        return self._request('post', f'/admin/requesters/{user_id}/change-credit', json={'creditDiff': credit_diff})

    def resolve_id(self, uid: str) -> Dict[str, Any]:
        return self._request('get', f'/dmz/dev/resolve-worker-id/{uid}')


class CallRecorderClient(NDATolokaClient):
    def __init__(self, *args, **kwargs):
        self.calls: List[Tuple[str, str, Tuple[Any], Dict[str, Any], Dict[str, Any]]] = []
        super().__init__(*args, **kwargs)

    def _raw_request(self, method: str, path: str, *args, **kwargs):
        result = super()._raw_request(method, path, *args, **kwargs)
        self.calls.append((method, path, args, kwargs, result.json()))
        return result
