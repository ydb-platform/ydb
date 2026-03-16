import aiobotocore.client
import wrapt

from aws_xray_sdk.core import xray_recorder
from aws_xray_sdk.ext.boto_utils import inject_header, aws_meta_processor


def patch():
    """
    Patch aiobotocore client so it generates subsegments
    when calling AWS services.
    """
    if hasattr(aiobotocore.client, '_xray_enabled'):
        return
    setattr(aiobotocore.client, '_xray_enabled', True)

    wrapt.wrap_function_wrapper(
        'aiobotocore.client',
        'AioBaseClient._make_api_call',
        _xray_traced_aiobotocore,
    )

    wrapt.wrap_function_wrapper(
        'aiobotocore.endpoint',
        'AioEndpoint.prepare_request',
        inject_header,
    )


async def _xray_traced_aiobotocore(wrapped, instance, args, kwargs):
    service = instance._service_model.metadata["endpointPrefix"]
    result = await xray_recorder.record_subsegment_async(
        wrapped, instance, args, kwargs,
        name=service,
        namespace='aws',
        meta_processor=aws_meta_processor,
    )

    return result
