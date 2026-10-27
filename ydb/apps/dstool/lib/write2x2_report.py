"""Versioned blobsan scan reports and histogram aggregation."""

import re


GC_STATES = {'hard_barrier', 'do_not_keep', 'soft_without_keep', 'soft_with_keep', 'no_covering_barrier'}
METRICS = ('blobs', 'logical_bytes', 'observed_replica_bytes', 'missing_metadata_blobs')
ANNOTATIONS = ('phantom_like', 'inv1_candidate', 'inv2_candidate')


def local3(local):
    """Order observed realm counts descending, with unknown realms last."""
    return '+'.join(sorted(local.split('+'), key=lambda part: -1 if part == '?' else int(part), reverse=True))


def local2(local):
    """Keep the first two realms of Local3, dropping the smallest or unknown."""
    return local3(local).rsplit('+', 1)[0]


def bucket_key(bucket):
    return tuple(bucket[key] for key in ('local', 'ingress', 'gc')) + tuple(bucket.get(key) for key in ANNOTATIONS)


def totals(buckets):
    return {key: sum(bucket[key] for bucket in buckets) for key in METRICS}


def validate(report, gid, completed):
    """Reject incompatible, mismatched or internally inconsistent analyzer output."""
    if not isinstance(report, dict) or type(report.get('version')) is not int or report['version'] != 1 or report.get('group_id') != gid:
        raise ValueError('Invalid scan report version/group')
    coverage = report.get('completed_vdisks')
    if not isinstance(coverage, list) or any(type(index) is not int for index in coverage) or coverage != sorted(completed):
        raise ValueError('Scan report coverage does not match captured VDisks')
    buckets = report.get('buckets')
    if not isinstance(buckets, list):
        raise ValueError('Invalid scan report buckets')
    seen = set()
    for bucket in buckets:
        if not isinstance(bucket, dict) or not isinstance(bucket.get('local'), str) or not re.fullmatch(
                r'[0-3?]\+[0-3?]\+[0-3?]', bucket['local']):
            raise ValueError('Invalid local layout in scan report')
        if not isinstance(bucket.get('ingress'), str) or not re.fullmatch(r'[0-3]\+[0-3]\+[0-3]', bucket['ingress']):
            raise ValueError('Invalid ingress layout in scan report')
        if not isinstance(bucket.get('gc'), str) or bucket['gc'] not in GC_STATES:
            raise ValueError('Invalid GC state in scan report')
        for realm, count in enumerate(bucket['local'].split('+')):
            known = all(realm * 3 + domain in completed for domain in range(3))
            if known == (count == '?'):
                raise ValueError('Local layout contradicts scan coverage')
        if any(type(bucket.get(key)) is not int or bucket[key] < 0 for key in METRICS):
            raise ValueError('Invalid scan counters')
        if not bucket['blobs'] or bucket['missing_metadata_blobs'] > bucket['blobs']:
            raise ValueError('Invalid blob counts')
        if any(bucket.get(key) is not None and type(bucket[key]) is not bool for key in ANNOTATIONS):
            raise ValueError('Invalid scan annotation')
        if bucket.get('phantom_like') is not None and bucket['missing_metadata_blobs'] != (
                bucket['blobs'] if bucket['phantom_like'] else 0):
            raise ValueError('Phantom annotation contradicts missing metadata')
        if any(bucket.get(key) is True for key in ('inv1_candidate', 'inv2_candidate')) and (
                bucket.get('phantom_like') is not False or
                bucket['gc'] not in ('soft_with_keep', 'no_covering_barrier')):
            raise ValueError('Invalid restore candidate annotation')
        if bucket.get('inv1_candidate') is True and bucket.get('inv2_candidate') is not True:
            raise ValueError('inv1_candidate must be a subset of inv2_candidate')
        key = bucket_key(bucket)
        if key in seen:
            raise ValueError('Duplicate scan bucket')
        seen.add(key)
    if report.get('totals') != totals(buckets):
        raise ValueError('Scan report totals do not match histogram')
    return report


def merge(histogram, buckets):
    for bucket in buckets:
        key = bucket_key(bucket)
        target = histogram.setdefault(key, dict(zip(('local', 'ingress', 'gc') + ANNOTATIONS, key),
                                                **dict.fromkeys(METRICS, 0)))
        for metric in METRICS:
            target[metric] += bucket[metric]


def histogram_rows(histogram):
    return [histogram[key] for key in sorted(histogram, key=lambda k: k[:3] + tuple(
        -1 if value is None else int(value) for value in k[3:]))]
