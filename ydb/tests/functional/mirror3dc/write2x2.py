"""Restore mirror-3-dc layouts using dstool snapshots and blobsan candidates.

The caller excludes the DC before running recovery. Both tools remain separate
executables; in ya tests their paths are resolved through yatest.common.
"""

from concurrent.futures import ThreadPoolExecutor
import json
import logging
import random
import re
import subprocess
import time
from urllib.parse import urlencode
from urllib.request import urlopen


logger = logging.getLogger(__name__)
CANDIDATE = re.compile(r'GroupId#\s+(\d+)\s+BlobId#\s+(\[[^\]]+\])')


def _get_json(viewer_url, path, *, timeout, params=None):
    url = viewer_url.rstrip('/') + '/' + path
    if params:
        url += '?' + urlencode(params)
    with urlopen(url, timeout=timeout) as response:
        return json.load(response)


def scan_write2x2(endpoint, *, group_ids=None, dstool=None, blobsan=None, timeout=120, workers=16):
    """Take fresh snapshots; return candidates and local layouts keyed by (GroupId, BlobId).

    All selected groups must be mirror-3-dc. Failed tool invocations fail the
    scan; old reports are never reused to conclude that recovery succeeded.
    """
    if dstool is None or blobsan is None:
        import yatest.common
        dstool = dstool or yatest.common.binary_path('ydb/apps/dstool/ydb-dstool')
        blobsan = blobsan or yatest.common.binary_path('ydb/tools/blobsan/blobsan')
    command = [str(dstool), '-e', endpoint]
    if group_ids is None:
        result = subprocess.run(
            command + ['group', 'list', '--no-header', '--format=tsv', '--columns', 'GroupId'],
            check=True, stdout=subprocess.PIPE, timeout=timeout, text=True,
        )
        group_ids = {0, *(int(value) for value in result.stdout.split())}
    else:
        group_ids = set(map(int, group_ids))

    if not group_ids:
        raise ValueError('No groups selected for recovery')

    def process_group(group_id):
        snapshot = subprocess.run(
            command + [
                'group', 'take-snapshot', '--group-ids', str(group_id),
                '--output', '/dev/stdout',
            ],
            stdout=subprocess.PIPE, check=True, timeout=timeout,
        ).stdout
        if not snapshot:
            raise RuntimeError(f'Empty snapshot for group {group_id}')
        result = subprocess.run(
            [str(blobsan), '--write2x2-candidates', 'mirror-3-dc'],
            input=snapshot, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            check=True, timeout=timeout,
        )
        layouts = parse_snapshot_layouts(result.stdout.decode())
        report = result.stderr.decode()
        pairs = {(int(match[1]), match[2]) for match in CANDIDATE.finditer(report)}
        logger.debug('blobsan group %s report:\n%s', group_id, report)
        logger.info(
            'Snapshot group %s: %d total blobs, %d recovery candidates',
            group_id, len(layouts), len(pairs),
        )
        return pairs, layouts

    groups = list(group_ids)
    random.shuffle(groups)
    candidates = set()
    layouts = {}
    with ThreadPoolExecutor(max_workers=workers) as executor:
        for pairs, group_layouts in executor.map(process_group, groups):
            candidates.update(pairs)
            layouts.update(group_layouts)
    logger.info('Scan: %d total blobs, %d recovery candidates', len(layouts), len(candidates))
    return candidates, layouts


def recover_write2x2(viewer_url, candidates, *, node_ids, timeout=120, workers=64):
    """Perform one recovery pass; return failed candidates and replies in memory.

    An OK reply with an empty list is allowed for a blob collected meanwhile.
    Successful replies still require a fresh scan to validate the layout.
    """
    candidates = set(candidates)
    node_ids = tuple(node_ids)
    if not node_ids:
        raise ValueError('No recovery nodes provided')
    pairs = list(candidates)
    random.shuffle(pairs)

    def restore(pair):
        group_id, blob_id = pair
        node_id = random.choice(node_ids)
        try:
            response = _get_json(
                viewer_url, f'node/{node_id}/blob_range', timeout=timeout,
                params={
                    'groupId': group_id, 'from': blob_id, 'to': blob_id,
                    'mustRestoreFirst': 1, 'tabletId': blob_id[1:-1].split(':')[0], 'json': 1,
                },
            )
            success = response.get('Status') == 'OK' and response.get('Blobs') in ([], [blob_id])
        except (OSError, ValueError) as error:
            response = {'error': str(error)}
            success = False
        return pair, success, response

    failed = set()
    replies = {}
    with ThreadPoolExecutor(max_workers=workers) as executor:
        for pair, success, response in executor.map(restore, pairs):
            replies[pair] = response
            logger.debug('Recovery reply for %s: success=%s, response=%s', pair, success, response)
            if not success:
                failed.add(pair)
    return failed, replies


def restore_write2x2(
    endpoint, viewer_url, *, node_ids, group_ids=None,
    dstool=None, blobsan=None, timeout=120,
    scan_workers=16, recovery_workers=64, attempts=10, retry_interval=60,
):
    """Restore layouts and return local layouts from a fresh clean scan.

    Call after excluding a DC. endpoint is the dstool endpoint; viewer_url includes
    the HTTP scheme and monitoring port. group_ids selects mirror-3-dc groups.
    node_ids explicitly supplies recovery nodes from the cluster setup.
    Tool paths default to ya build outputs.
    Raise on tool failures or if candidates remain after the allowed passes.
    """
    if attempts < 1:
        raise ValueError('attempts must be positive')
    group_ids = None if group_ids is None else tuple(group_ids)
    node_ids = tuple(node_ids)
    if not node_ids:
        raise ValueError('No recovery nodes provided')
    for attempt in range(attempts + 1):
        candidates, layouts = scan_write2x2(
            endpoint, group_ids=group_ids, dstool=dstool, blobsan=blobsan,
            timeout=timeout, workers=scan_workers,
        )
        if not candidates:
            return layouts
        if attempt == attempts:
            raise RuntimeError(f'{len(candidates)} candidates remain after {attempts} passes: {sorted(candidates)}')
        failed, replies = recover_write2x2(
            viewer_url, candidates, node_ids=node_ids,
            timeout=timeout, workers=recovery_workers,
        )
        logger.info('Recovery pass %d: %d failed requests', attempt + 1, len(failed))
        if failed:
            logger.warning('Failed recovery replies: %s', {pair: replies[pair] for pair in failed})
        time.sleep(retry_interval)


def parse_snapshot_layouts(dump):
    """Return local (subgroup realm, domain) copies for each (GroupId, BlobId).

    Merge repeated ingress records across index tables, without inferring copies
    from remote ingress bits. Empty sets retain metadata-only blob records.
    """
    record = re.compile(
        r'^(\d+)\s+\d+\s+A\s+.*?Key#\s+(\[[^\]]+\])\s+Ingress#\s+'
        r'\{nodeId:\s+(\d+).*?local:\s+([01])\s+([01])\s+([01])'
    )
    layouts = {}
    for line in dump.splitlines():
        match = record.search(line)
        if match:
            group_id, blob_id, position, *parts = match.groups()
            position = int(position)
            layout = layouts.setdefault((int(group_id), blob_id), set())
            if parts[position % 3] == '1':
                layout.add((position % 3, position // 3))
            if any(part == '1' for i, part in enumerate(parts) if i != position % 3):
                raise AssertionError(f'Misplaced local part: {line.rstrip()}')
    return layouts
