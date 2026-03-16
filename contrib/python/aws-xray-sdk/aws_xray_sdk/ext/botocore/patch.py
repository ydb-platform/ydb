import wrapt
import botocore.client

from aws_xray_sdk.core import xray_recorder
from aws_xray_sdk.ext.boto_utils import inject_header, aws_meta_processor


def patch():
    """
    Patch botocore client so it generates subsegments
    when calling AWS services.
    """
    if hasattr(botocore.client, '_xray_enabled'):
        return
    setattr(botocore.client, '_xray_enabled', True)

    wrapt.wrap_function_wrapper(
        'botocore.client',
        'BaseClient._make_api_call',
        _xray_traced_botocore,
    )

    wrapt.wrap_function_wrapper(
        'botocore.endpoint',
        'Endpoint.prepare_request',
        inject_header,
    )


def _xray_traced_botocore(wrapped, instance, args, kwargs):
    service = instance._service_model.metadata["endpointPrefix"]
    if service == 'xray':
        # skip tracing for SDK built-in sampling pollers
        if ('GetSamplingRules' in args or
            'GetSamplingTargets' in args or
                'PutTraceSegments' in args):
            return wrapped(*args, **kwargs)
    return xray_recorder.record_subsegment(
        wrapped, instance, args, kwargs,
        name=service,
        namespace='aws',
        meta_processor=aws_meta_processor,
    )
