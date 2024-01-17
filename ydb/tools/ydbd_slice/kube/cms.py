# this is evil
# it should be exterminated...
# after KIKIMR-19357

import os
import copy
import time
import grpc
import logging
import tenacity as tc
from urllib.parse import urlparse
from google.protobuf import text_format
from contextlib import contextmanager

from ydb.core.protos import grpc_pb2_grpc
from ydb.core.protos import msgbus_pb2
from ydb.core.protos import console_config_pb2
from ydb.public.api.protos import ydb_status_codes_pb2


logger = logging.getLogger(__name__)


DEBUG = True
LEGACY_CMS_CONFIG_ITEMS_DIR = 'legacy-cms-config-items'


GRPC_MAX_MESSAGE_LENGTH = 130 * 1024**2
GRPC_DEFAULT_OPTIONS = {
    'grpc.max_send_message_length': GRPC_MAX_MESSAGE_LENGTH,
    'grpc.max_receive_message_length': GRPC_MAX_MESSAGE_LENGTH,
    'grpc.max_concurrent_streams': 200,
}

RETRYABLE_STATUS_CODES = [
    ydb_status_codes_pb2.StatusIds.StatusCode.INTERNAL_ERROR,
    ydb_status_codes_pb2.StatusIds.StatusCode.ABORTED,
    ydb_status_codes_pb2.StatusIds.StatusCode.UNAVAILABLE,
    ydb_status_codes_pb2.StatusIds.StatusCode.OVERLOADED,
    ydb_status_codes_pb2.StatusIds.StatusCode.TIMEOUT,
    ydb_status_codes_pb2.StatusIds.StatusCode.CANCELLED,
    ydb_status_codes_pb2.StatusIds.StatusCode.SESSION_BUSY,
]


def get_file_or_none(path, *args, **kwargs):
    if path is None:
        return None
    with open(path, *args, **kwargs) as file:
        return file.read()


class RequestFailed(Exception):
    def __init__(self, method, request, response):
        self.method = method
        self.request = request
        self.response = response

    def __str__(self):
        return f'{self.method} request: {self.request} failed with response {self.response}'


def listify(data):
    if isinstance(data, (list, str, set)):
        return list(data)
    elif isinstance(data, dict):
        raise ValueError('cannot listify dict')
    else:
        return [data]


@contextmanager
def _connect(url, grpc_cacert_file=None, grpc_options=None, grpc_timeout=60, result=None):
    logger.debug(f'creating grpc channel using url: {url}')

    if result is None:
        result = {
            'changed': False,
        }

    grpc_options_defaults = copy.deepcopy(GRPC_DEFAULT_OPTIONS)
    if grpc_options is not None:
        grpc_options_defaults.update(grpc_options)
    grpc_options = list(grpc_options_defaults.items())

    url_data = urlparse(url)

    if url_data.scheme == 'grpc':
        grpc_channel = grpc.insecure_channel(url_data.netloc, options=grpc_options)

    elif url_data.scheme == 'grpcs':
        grpc_cacert = get_file_or_none(grpc_cacert_file, 'rb')
        if grpc_cacert is None:
            result['msg'] = f'Cannot load Root CA file {grpc_cacert_file}.'
            raise RuntimeError(result)

        grpc_credentials = grpc.ssl_channel_credentials(
            root_certificates=grpc_cacert, private_key=None, certificate_chain=None
        )
        grpc_channel = grpc.secure_channel(url_data.netloc, grpc_credentials, options=grpc_options)

    else:
        raise RuntimeError(f'invalid scheme: {url_data.scheme}')

    ready_future = grpc.channel_ready_future(grpc_channel)
    ready_future.result(timeout=grpc_timeout)

    grpc_server_stub = grpc_pb2_grpc.TGRpcServerStub(grpc_channel)

    yield grpc_server_stub

    grpc_channel.close()


@contextmanager
def connect(url_list, grpc_cacert_file=None, grpc_options=None, grpc_timeout=60, result=None):
    if isinstance(url_list, str):
        url_list = [url_list]
    last_error = None
    for url in url_list:
        try:
            with _connect(url, grpc_cacert_file, grpc_options, grpc_timeout, result) as stub:
                yield stub
            return
        except grpc.FutureTimeoutError as e:
            last_error = e
            time.sleep(1)
            continue
    raise last_error


@tc.retry(
    retry=tc.retry_if_exception_type(RequestFailed),
    wait=tc.wait_exponential(max=60),
    stop=tc.stop_after_attempt(10),
    reraise=True,
)
def _send_retry_request(grpc_server_stub, method, request):
    if request.HasField('GetConfigItemsRequest'):
        logger.debug('sending GetConfigItemsRequest')
    elif request.HasField('ConfigureRequest'):
        logger.debug('sending ConfigureRequest')
    func = getattr(grpc_server_stub, method)
    response = func(request)
    if response.Status.Code in RETRYABLE_STATUS_CODES:
        raise RequestFailed(method, request, response)
    if response.HasField('GetConfigItemsResponse'):
        logger.debug(f'receiving GetConfigItemsResponse with code: {response.GetConfigItemsResponse.Status.Code}')
        if response.GetConfigItemsResponse.Status.Code in RETRYABLE_STATUS_CODES:
            raise RequestFailed(method, request, response)
    elif response.HasField('ConfigureResponse'):
        logger.debug(f'receiving ConfigureResponse with code: {response.ConfigureResponse.Status.Code}')
        if response.ConfigureResponse.Status.Code in RETRYABLE_STATUS_CODES:
            raise RequestFailed(method, request, response)
    return response


def send_request(grpc_server_stub, method, request):
    response = _send_retry_request(grpc_server_stub, method, request)
    if response.Status.Code != ydb_status_codes_pb2.StatusIds.StatusCode.SUCCESS:
        raise RequestFailed(method, request, response)
    if response.HasField('GetConfigItemsResponse'):
        if response.GetConfigItemsResponse.Status.Code != ydb_status_codes_pb2.StatusIds.StatusCode.SUCCESS:
            raise RequestFailed(method, request, response)
    elif response.HasField('ConfigureResponse'):
        if response.ConfigureResponse.Status.Code != ydb_status_codes_pb2.StatusIds.StatusCode.SUCCESS:
            raise RequestFailed(method, request, response)
    return response


def get_from_files(project_path):
    config_items = dict()
    cms_configs_dir = os.path.abspath(os.path.join(project_path, LEGACY_CMS_CONFIG_ITEMS_DIR))
    if not os.path.exists(cms_configs_dir):
        return None
    for item in os.listdir(cms_configs_dir):
        filename = os.path.join(cms_configs_dir, item)
        if not filename.endswith('.txt'):
            logger.warning(f'skipping file: {filename}, not txt file extension')
            continue
        cookie = item
        with open(filename) as file:
            config_item_text = file.read()
        config_item = console_config_pb2.TConfigItem()
        try:
            text_format.Parse(config_item_text, config_item)
        except Exception as e:
            logger.warning(f'skipping file: {filename}, parse error: {str(e)}')
        config_item.Cookie = cookie
        config_items[cookie] = config_item
    return config_items


def get(grpc_server_stub, cookie=None):
    request = msgbus_pb2.TConsoleRequest()
    request.GetConfigItemsRequest.CopyFrom(console_config_pb2.TGetConfigItemsRequest())

    if cookie is not None:
        logger.debug(f'getting config items, filter by cookie: {cookie}')
        request.GetConfigItemsRequest.CookieFilter.Cookies.append(cookie)
    else:
        logger.debug('getting all config items')

    response = send_request(grpc_server_stub, 'ConsoleRequest', request)

    config_items = dict()
    for item in response.GetConfigItemsResponse.ConfigItems:
        if item.Cookie == '':
            item_id = str(item.Id).replace('\n', ' ')
            logger.info(f'got config item with empty cookie and id {item_id}, possibly managed from UI, skipping')
            continue
        if item.Cookie in config_items:
            raise RuntimeError('found more than one config item with same cookie, dont know which one to process')
        config_items[item.Cookie] = item

    return config_items


def remove(grpc_server_stub, old_config_item):
    logger.debug(f'removing config item {old_config_item.Cookie}')
    request = msgbus_pb2.TConsoleRequest()
    configure_action = request.ConfigureRequest.Actions.add()
    configure_action.RemoveConfigItem.ConfigItemId.CopyFrom(old_config_item.Id)
    send_request(grpc_server_stub, 'ConsoleRequest', request)


def compare(old_config_item, new_config_item):
    for attr in ['Kind', 'Config', 'UsageScope', 'Order', 'MergeStrategy']:
        old_value = getattr(old_config_item, attr)
        new_value = getattr(new_config_item, attr)
        if old_value != new_value:
            return False
    return True


def modify(grpc_server_stub, old_config_item, new_config_item):
    if compare(old_config_item, new_config_item):
        logger.debug(f'skipping config item {new_config_item.Cookie}, not changed')
        return
    logger.debug(f'modifying config item {new_config_item.Cookie}')
    request = msgbus_pb2.TConsoleRequest()
    configure_action = request.ConfigureRequest.Actions.add()
    config_item = console_config_pb2.TConfigItem()
    config_item.CopyFrom(new_config_item)
    config_item.Id.CopyFrom(old_config_item.Id)
    configure_action.ModifyConfigItem.ConfigItem.CopyFrom(config_item)
    send_request(grpc_server_stub, 'ConsoleRequest', request)


def create(grpc_server_stub, new_config_item):
    logger.debug(f'creating config item {new_config_item.Cookie}')
    request = msgbus_pb2.TConsoleRequest()
    configure_action = request.ConfigureRequest.Actions.add()
    configure_action.AddConfigItem.ConfigItem.CopyFrom(new_config_item)
    send_request(grpc_server_stub, 'ConsoleRequest', request)


def apply_legacy_cms_config_items(new_config_items, url, grpc_cacert_file=None, grpc_options=None, grpc_timeout=60):
    logger.debug(f'applying legacy cms config items from files: {list(new_config_items)}')
    with connect(
        url, grpc_cacert_file=grpc_cacert_file, grpc_options=grpc_options, grpc_timeout=grpc_timeout
    ) as grpc_server_stub:
        old_config_items = get(grpc_server_stub, cookie=None)

        old_keys = set(old_config_items)
        new_keys = set(new_config_items)
        rem_keys = old_keys.difference(new_keys)
        mod_keys = new_keys.intersection(old_keys)
        cre_keys = new_keys.difference(old_keys)

        for cookie in rem_keys:
            old_config_item = old_config_items[cookie]
            try:
                remove(grpc_server_stub, old_config_item)
            except RequestFailed as e:
                logger.error(f'failed to remove config item with cookie: {cookie}, '
                             f'method: {e.method}, request: {e.request}, response: {e.response}')

        for cookie in mod_keys:
            old_config_item = old_config_items[cookie]
            new_config_item = new_config_items[cookie]
            try:
                modify(grpc_server_stub, old_config_item, new_config_item)
            except RequestFailed as e:
                logger.error(f'failed to modify config item with cookie: {cookie}, '
                             f'method: {e.method}, request: {e.request}, response: {e.response}')

        for cookie in cre_keys:
            new_config_item = new_config_items[cookie]
            try:
                create(grpc_server_stub, new_config_item)
            except RequestFailed as e:
                logger.error(f'failed to create config item with cookie: {cookie}, '
                             f'method: {e.method}, request: {e.request}, response: {e.response}')
