import json
import pkgutil

from botocore.exceptions import ClientError

from aws_xray_sdk.core import xray_recorder
from aws_xray_sdk.core.models import http
from aws_xray_sdk.core.exceptions.exceptions import SegmentNotFoundException

from aws_xray_sdk.ext.util import inject_trace_header, to_snake_case

# `.decode('utf-8')` needed for Python 3.4, 3.5
whitelist = json.loads(pkgutil.get_data(__name__, 'resources/aws_para_whitelist.json').decode('utf-8'))


def inject_header(wrapped, instance, args, kwargs):
    # skip tracing for SDK built-in centralized sampling pollers
    url = args[0].url
    if 'GetCentralizedSamplingRules' in url or 'SamplingTargets' in url:
        return wrapped(*args, **kwargs)

    headers = args[0].headers
    # skip if the recorder is unable to open the subsegment
    # for the outgoing request
    subsegment = None
    try:
        subsegment = xray_recorder.current_subsegment()
    except SegmentNotFoundException:
        pass
    if subsegment:
        inject_trace_header(headers, subsegment)
    return wrapped(*args, **kwargs)


def aws_meta_processor(wrapped, instance, args, kwargs,
                       return_value, exception, subsegment, stack):
    region = instance.meta.region_name

    if 'operation_name' in kwargs:
        operation_name = kwargs['operation_name']
    else:
        operation_name = args[0]

    aws_meta = {
        'operation': operation_name,
        'region': region,
    }

    if return_value:
        resp_meta = return_value.get('ResponseMetadata')
        if resp_meta:
            aws_meta['request_id'] = resp_meta.get('RequestId')
            subsegment.put_http_meta(http.STATUS,
                                     resp_meta.get('HTTPStatusCode'))
            # for service like S3 that returns special request id in response headers
            if 'HTTPHeaders' in resp_meta and resp_meta['HTTPHeaders'].get('x-amz-id-2'):
                aws_meta['id_2'] = resp_meta['HTTPHeaders']['x-amz-id-2']

    elif exception:
        _aws_error_handler(exception, stack, subsegment, aws_meta)

    _extract_whitelisted_params(subsegment.name, operation_name,
                                aws_meta, args, kwargs, return_value)

    subsegment.set_aws(aws_meta)


def _aws_error_handler(exception, stack, subsegment, aws_meta):

    if not exception or not isinstance(exception, ClientError):
        return

    response_metadata = exception.response.get('ResponseMetadata')

    if not response_metadata:
        return

    aws_meta['request_id'] = response_metadata.get('RequestId')

    status_code = response_metadata.get('HTTPStatusCode')

    subsegment.put_http_meta(http.STATUS, status_code)
    subsegment.add_exception(exception, stack, True)


def _extract_whitelisted_params(service, operation,
                                aws_meta, args, kwargs, response):

    # check if service is whitelisted
    if service not in whitelist['services']:
        return
    operations = whitelist['services'][service]['operations']

    # check if operation is whitelisted
    if operation not in operations:
        return
    params = operations[operation]

    # record whitelisted request/response parameters
    if 'request_parameters' in params:
        _record_params(params['request_parameters'], args[1], aws_meta)

    if 'request_descriptors' in params:
        _record_special_params(params['request_descriptors'],
                               args[1], aws_meta)

    if 'response_parameters' in params and response:
        _record_params(params['response_parameters'], response, aws_meta)

    if 'response_descriptors' in params and response:
        _record_special_params(params['response_descriptors'],
                               response, aws_meta)


def _record_params(whitelisted, actual, aws_meta):

    for key in whitelisted:
        if key in actual:
            snake_key = to_snake_case(key)
            aws_meta[snake_key] = actual[key]


def _record_special_params(whitelisted, actual, aws_meta):

    for key in whitelisted:
        if key in actual:
            _process_descriptor(whitelisted[key], actual[key], aws_meta)


def _process_descriptor(descriptor, value, aws_meta):

    # "get_count" = true
    if 'get_count' in descriptor and descriptor['get_count']:
        value = len(value)

    # "get_keys" = true
    if 'get_keys' in descriptor and descriptor['get_keys']:
        value = value.keys()

    aws_meta[descriptor['rename_to']] = value
