from nose.tools import eq_ as eq
from nose.plugins.attrib import attr

from s3tests_boto3.functional import (
    get_client,
    get_new_bucket,
    )

def _get_keys(response):
    """
    return lists of strings that are the keys from a client.list_objects() response
    """
    keys = []
    if 'Contents' in response:
        objects_list = response['Contents']
        keys = [obj['Key'] for obj in objects_list]
    return keys

def _get_prefixes(response):
    """
    return lists of strings that are prefixes from a client.list_objects() response
    """
    prefixes = []
    if 'CommonPrefixes' in response:
        prefix_list = response['CommonPrefixes']
        prefixes = [prefix['Prefix'] for prefix in prefix_list]
    return prefixes

@attr(resource='object.metadata')
@attr(method='put')
@attr(operation='metadata write/extended listing')
@attr(assertion='read metadata back in extended listing')
def test_object_set_list_meta():
    bucket_name = get_new_bucket()
    client = get_client()
    client.put_object(Bucket=bucket_name, Key='foo', Body='bar', Metadata={'META1': 'HeLloworld'})

    response = client.list_objects_v1_ext(Bucket=bucket_name, MaxKeys=1000)
    keys = _get_keys(response)
    eq(len(keys), 1)
    eq(keys, ['foo'])
    eq(response['IsTruncated'], False)

    eq(response['Contents'][0]['Metadata'], {'meta1': 'HeLloworld'})

@attr(resource='bucket')
@attr(method='get')
@attr(operation='list all keys')
@attr(assertion='pagination w/max_keys=2, no marker')
@attr('list-objects-v1ext')
def test_bucket_listv1ext_many():
    bucket_name = get_new_bucket()
    client = get_client()
    client.put_object(Bucket=bucket_name, Key='bar', Body='huu', Metadata={'META1': 'Value1'})
    client.put_object(Bucket=bucket_name, Key='baz', Body='ska', Metadata={'META2': 'Value2'})
    client.put_object(Bucket=bucket_name, Key='foo', Body='bar', Metadata={'META3': 'Value3'})
    client.put_object(Bucket=bucket_name, Key='foo/xyz', Body='xyz', Metadata={'META4': 'Value4'})
    client.put_object(Bucket=bucket_name, Key='foo/xyz/buck+1', Body='buck', Metadata={'META5': 'Value5'})

    # Paging by using StartAfter, also KeyCount and Metadata
    response = client.list_objects_v1_ext(Bucket=bucket_name, MaxKeys=2)
    keys = _get_keys(response)
    eq(keys, ['bar', 'baz'])
    eq(response['KeyCount'], 2)
    eq(response['IsTruncated'], True)
    eq(response['Contents'][0]['Metadata'], {'meta1': 'Value1'})
    eq(response['Contents'][1]['Metadata'], {'meta2': 'Value2'})

    response = client.list_objects_v1_ext(Bucket=bucket_name, StartAfter='baz', MaxKeys = 4)
    keys = _get_keys(response)
    eq(response['KeyCount'], 3)
    eq(response['IsTruncated'], False)
    eq(keys, ['foo', 'foo/xyz', 'foo/xyz/buck+1'])
    eq(response['Contents'][0]['Metadata'], {'meta3': 'Value3'})
    eq(response['Contents'][1]['Metadata'], {'meta4': 'Value4'})
    eq(response['Contents'][2]['Metadata'], {'meta5': 'Value5'})

    # Paging by using ContinuationToken
    response = client.list_objects_v1_ext(Bucket=bucket_name, MaxKeys=2)
    keys = _get_keys(response)
    eq(keys, ['bar', 'baz'])
    eq(response['IsTruncated'], True)
    eq(response['Contents'][0]['Metadata'], {'meta1': 'Value1'})
    eq(response['Contents'][1]['Metadata'], {'meta2': 'Value2'})

    response = client.list_objects_v1_ext(Bucket=bucket_name, ContinuationToken=response['NextContinuationToken'])
    keys = _get_keys(response)
    eq(keys, ['foo', 'foo/xyz', 'foo/xyz/buck+1'])
    eq(response['Contents'][0]['Metadata'], {'meta3': 'Value3'})
    eq(response['Contents'][1]['Metadata'], {'meta4': 'Value4'})
    eq(response['Contents'][2]['Metadata'], {'meta5': 'Value5'})

    # Prefix
    response = client.list_objects_v1_ext(Bucket=bucket_name, Delimiter='/', Prefix='foo/')
    eq(response['Prefix'], 'foo/')
    keys = _get_keys(response)
    prefixes = _get_prefixes(response)
    eq(keys, ['foo/xyz'])
    eq(prefixes, ['foo/xyz/'])

    # Delimiter
    response = client.list_objects_v1_ext(Bucket=bucket_name, Delimiter='/')
    eq(response['Delimiter'], '/')
    keys = _get_keys(response)
    eq(keys, ['bar', 'baz', 'foo'])

    prefixes = _get_prefixes(response)
    eq(prefixes, ['foo/'])
    eq(response['KeyCount'], len(prefixes) + len(keys))
