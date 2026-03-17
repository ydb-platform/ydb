import json
import wrapt
import pynamodb

from aws_xray_sdk.core import xray_recorder
from aws_xray_sdk.core.models import http
from aws_xray_sdk.ext.boto_utils import _extract_whitelisted_params

PYNAMODB4 = int(pynamodb.__version__.split('.')[0]) >= 4

if PYNAMODB4:
    import botocore.httpsession
else:
    import botocore.vendored.requests.sessions


def patch():
    """Patch PynamoDB so it generates subsegements when calling DynamoDB."""

    if PYNAMODB4:
        if hasattr(botocore.httpsession, '_xray_enabled'):
            return
        setattr(botocore.httpsession, '_xray_enabled', True)

        module = 'botocore.httpsession'
        name = 'URLLib3Session.send'
    else:
        if hasattr(botocore.vendored.requests.sessions, '_xray_enabled'):
            return
        setattr(botocore.vendored.requests.sessions, '_xray_enabled', True)

        module = 'botocore.vendored.requests.sessions'
        name = 'Session.send'

    wrapt.wrap_function_wrapper(
        module, name, _xray_traced_pynamodb,
    )


def _xray_traced_pynamodb(wrapped, instance, args, kwargs):

    # Check if it's a request to DynamoDB and return otherwise.
    try:
        service = args[0].headers['X-Amz-Target'].decode('utf-8').split('_')[0]
    except KeyError:
        return wrapped(*args, **kwargs)
    if service.lower() != 'dynamodb':
        return wrapped(*args, **kwargs)

    return xray_recorder.record_subsegment(
        wrapped, instance, args, kwargs,
        name='dynamodb',
        namespace='aws',
        meta_processor=pynamodb_meta_processor,
    )


def pynamodb_meta_processor(wrapped, instance, args, kwargs, return_value,
                            exception, subsegment, stack):
    operation_name = args[0].headers['X-Amz-Target'].decode('utf-8').split('.')[1]
    region = args[0].url.split('.')[1]

    aws_meta = {
        'operation': operation_name,
        'region': region
    }

    # in case of client timeout the return value will be empty
    if return_value is not None:
        aws_meta['request_id'] = return_value.headers.get('x-amzn-RequestId')
        subsegment.put_http_meta(http.STATUS, return_value.status_code)

    if exception:
        subsegment.add_error_flag()
        subsegment.add_exception(exception, stack, True)

    if PYNAMODB4:
        resp = json.loads(return_value.text) if return_value else None
    else:
        resp = return_value.json() if return_value else None
    _extract_whitelisted_params(subsegment.name, operation_name, aws_meta,
                                [None, json.loads(args[0].body.decode('utf-8'))],
                                None, resp)

    subsegment.set_aws(aws_meta)
