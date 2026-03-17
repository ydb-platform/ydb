import asyncio
import hashlib

from nose.tools import eq_ as eq
from nose.plugins.attrib import attr
from aiobotocore.session import get_session
from botocore.client import Config
from botocore.exceptions import ClientError

from s3tests_boto3.functional import (
    config,
    get_client,
    get_new_bucket,
    )

from s3tests_boto3.functional.utils import (
    _get_status,
    _get_body,
    assert_raises,
    )


MB = 1024*1024

@attr(resource='object')
@attr(method='patch')
@attr(operation='Test simple object patch')
@attr(assertion='patched range has updated data')
def test_patch_simple():
    bucket, key = get_new_bucket(), 'test'

    client = get_client()
    client.put_object(Bucket=bucket, Key=key, Body='a' * 100)

    expected_result = 'a' * 50 + 'b' * 150
    expected_etag = '"%s"' % hashlib.md5(expected_result.encode()).hexdigest()

    resp = client.patch_object(Bucket=bucket, Key=key, ContentRange=_patch_range(50, 200), Body='b' * 150)
    eq(resp['Object']['ETag'], expected_etag)

    resp = client.get_object(Bucket=bucket, Key=key)
    eq(_get_body(resp), expected_result)
    eq(_num_parts(resp), 0)

@attr(resource='object')
@attr(method='patch')
@attr(operation='Test multipart object patch - replace')
@attr(assertion='patched range has updated data')
def test_patch_multipart_replace():
    bucket, key = get_new_bucket(), 'test'
    client = get_client()

    _mpu_from_parts(client, bucket, key, ['a' * 10*MB for _ in range(5)])
    client.patch_object(Bucket=bucket, Key=key, ContentRange=_patch_range(5*MB, 25*MB), Body='b' * 20*MB)
    client.patch_object(Bucket=bucket, Key=key, ContentRange=_patch_range(25*MB, 30*MB), Body='c' * 5*MB)
    client.patch_object(Bucket=bucket, Key=key, ContentRange=_patch_range(35*MB, 40*MB), Body='d' * 5*MB)

    resp = client.get_object(Bucket=bucket, Key=key)
    eq(_get_body(resp), 'a' * 5*MB + 'b' * 20*MB + 'c' * 5*MB + 'a' * 5*MB + 'd' * 5*MB + 'a' * 10*MB)
    eq(_num_parts(resp), 5)

@attr(resource='object')
@attr(method='patch')
@attr(operation='Test multipart object patch - append')
@attr(assertion='patch data has been appended to the tail part')
def test_patch_multipart_append():
    bucket, key = get_new_bucket(), 'test'

    client = get_client()
    resp = _mpu_from_parts(client, bucket, key, ['a' * 10*MB for _ in range(2)])

    expected_result = 'a' * 15*MB + 'b' * 10*MB + 'c' * 5*MB

    expected_etag = _etag_md5(resp['ETag'])
    expected_etag = _patch_md5(_patch_md5(expected_etag, 'b' * 10*MB), 'c' * 5*MB)
    expected_etag = '"%s-%d"' % (expected_etag, 2)

    client.patch_object(Bucket=bucket, Key=key, ContentRange=_patch_range(15*MB, 25*MB), Body='b' * 10*MB)
    resp = client.patch_object(Bucket=bucket, Key=key, ContentRange=_patch_range(25*MB, 30*MB), Body='c' * 5*MB)
    eq(resp['Object']['ETag'], expected_etag)

    resp = client.get_object(Bucket=bucket, Key=key)
    eq(_get_body(resp), expected_result)
    eq(_num_parts(resp), 2)

@attr(resource='object')
@attr(method='patch')
@attr(operation='Test multipart object patch - append with custom part size')
@attr(assertion='patch data has been appended as a new part')
def test_patch_multipart_custom_part_size():
    bucket, key = get_new_bucket(), 'test'
    client = get_client()

    _mpu_from_parts(client, bucket, key, ['a' * 10*MB for _ in range(2)])
    client.patch_object(Bucket=bucket, Key=key, ContentRange=_patch_range(15*MB, 25*MB), Body='b' * 10*MB, PatchAppendPartSize=10*MB)
    client.patch_object(Bucket=bucket, Key=key, ContentRange=_patch_range(25*MB, 30*MB), Body='c' * 5*MB, PatchAppendPartSize=10*MB)

    resp = client.get_object(Bucket=bucket, Key=key)
    eq(_get_body(resp), 'a' * 15*MB + 'b' * 10*MB + 'c' * 5*MB)
    eq(_num_parts(resp), 3)

@attr(resource='object')
@attr(method='patch')
@attr(operation='Test simple object patch concurrent')
@attr(assertion='all patched ranges have corresponding updated data')
def test_patch_simple_concurrent():
    bucket, key = get_new_bucket(), 'test'
    client = get_client()

    client.put_object(Bucket=bucket, Key=key, Body='a' * 100)

    patches = [
        {'Bucket': bucket, 'Key': key, 'ContentRange': _patch_range(10, 50), 'Body': 'b' * 40},
        {'Bucket': bucket, 'Key': key, 'ContentRange': _patch_range(50, 90), 'Body': 'c' * 40},
    ]

    loop = asyncio.get_event_loop()
    sess = get_session()
    loop.run_until_complete(apply_concurrent_patches(sess, patches))

    resp = client.get_object(Bucket=bucket, Key=key)
    eq(_get_body(resp), 'a' * 10 + 'b' * 40 + 'c' * 40 + 'a' * 10)
    eq(_num_parts(resp), 0)

@attr(resource='object')
@attr(method='patch')
@attr(operation='Test multipart object patch concurrent')
@attr(assertion='all patched ranges have corresponding updated data')
def test_patch_multipart_concurrent():
    bucket, key = get_new_bucket(), 'test'
    client = get_client()

    _mpu_from_parts(client, bucket, key, ['a' * 5*MB for _ in range(4)])

    patches = [
        {'Bucket': bucket, 'Key': key, 'ContentRange': _patch_range(2*MB, 6*MB), 'Body': 'b' * 4*MB},
        {'Bucket': bucket, 'Key': key, 'ContentRange': _patch_range(6*MB, 10*MB), 'Body': 'c' * 4*MB},
        {'Bucket': bucket, 'Key': key, 'ContentRange': _patch_range(10*MB, 14*MB), 'Body': 'd' * 4*MB},
        {'Bucket': bucket, 'Key': key, 'ContentRange': _patch_range(14*MB, 18*MB), 'Body': 'e' * 4*MB},
    ]

    loop = asyncio.get_event_loop()
    sess = get_session()
    loop.run_until_complete(apply_concurrent_patches(sess, patches))

    resp = client.get_object(Bucket=bucket, Key=key)
    eq(_get_body(resp), 'a' * 2*MB + 'b' * 4*MB + 'c' * 4*MB + 'd' * 4*MB + 'e' * 4*MB + 'a' * 2*MB)
    eq(_num_parts(resp), 4)

async def apply_concurrent_patches(sess, patches):
    params = {
        'service_name': 's3',
        'aws_access_key_id': config.main_access_key,
        'aws_secret_access_key': config.main_secret_key,
        'endpoint_url': config.default_endpoint,
        'use_ssl': config.default_is_secure,
        'config': Config(signature_version='s3v4'),
    }
    async with sess.create_client(**params) as client:
        await asyncio.gather(*[client.patch_object(**patch) for patch in patches])

@attr(resource='object')
@attr(method='patch')
@attr(operation='Test patch with preconditions')
@attr(assertion='preconditions are checked')
def test_patch_preconditions():
    bucket, key = get_new_bucket(), 'test'
    client = get_client()

    client.put_object(Bucket=bucket, Key=key, Body='abcd')
    resp = client.head_object(Bucket=bucket, Key=key)
    etag, last_modified = resp['ETag'], resp['LastModified']

    params = {
        'Bucket': bucket, 'Key': key, 'Body': '1234', 'ContentRange': _patch_range(0, 4), 
    }

    resp = client.patch_object(**params, IfMatch=etag)
    eq(_get_status(resp), 200)

    resp = client.patch_object(**params, IfUnmodifiedSince=last_modified)
    eq(_get_status(resp), 200)

@attr(resource='object')
@attr(method='patch')
@attr(operation='Test patch versioned object')
@attr(assertion='patch fails for versioned objects')
def test_patch_versioned_object():
    client = get_client()

    bucket, key = get_new_bucket(), 'test'
    client.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={'Status': 'Enabled'})

    client.put_object(Bucket=bucket, Key=key, Body='abcd')
    e = assert_raises(ClientError, client.patch_object, Bucket=bucket, Key=key, Body='1234', ContentRange=_patch_range(0, 4))
    eq(_get_status(e.response), 405)

@attr(resource='object')
@attr(method='patch')
@attr(operation='Test invalid patch range')
@attr(assertion='patch fails if range is invalid')
def test_patch_invalid_range():
    bucket, key = get_new_bucket(), 'test'
    client = get_client()

    client.put_object(Bucket=bucket, Key=key, Body='abcd')
    e = assert_raises(ClientError, client.patch_object, Bucket=bucket, Key=key, Body='1234', ContentRange=_patch_range(100, 104))
    eq(_get_status(e.response), 416)

def _mpu_from_parts(client, bucket, key, parts_data):
    response = client.create_multipart_upload(Bucket=bucket, Key=key)

    upload_id = response['UploadId']
    parts = []

    for i, data in enumerate(parts_data):
        response = client.upload_part(UploadId=upload_id, Bucket=bucket, Key=key, PartNumber=i+1, Body=data)
        parts.append({'ETag': response['ETag'].strip('"'), 'PartNumber': i+1})

    return client.complete_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id, MultipartUpload={'Parts': parts})

def _patch_range(l, r):
    return 'bytes {}-{}/*'.format(l, r-1)

def _num_parts(resp):
    etag = resp['ETag'].strip('"')
    if not '-' in etag:
        return 0
    _, num_parts = etag.split('-')
    return int(num_parts)

def _etag_md5(etag):
    etag = etag.strip('"')
    if '-' in etag:
        return etag.split('-')[0]
    return etag

def _patch_md5(orig_etag, data):
    h1, h2 = orig_etag.encode(), hashlib.md5(data.encode()).hexdigest().encode()
    return hashlib.md5(h1 + h2).hexdigest()
