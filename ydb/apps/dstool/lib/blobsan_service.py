"""Coordinator and worker runtime for blobsan-service (standard library)."""

import argparse
import concurrent.futures
import hashlib
import hmac
from http import server as http_server
import json
import logging
import math
import os
from pathlib import Path

import re
import random
import shlex
import shutil
import socket
import socketserver
import sqlite3
import ssl
import struct
import subprocess
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

from ydb.apps.dstool.lib import common, write2x2_report
from ydb.apps.dstool.protos import restore_part_pb2
from google.protobuf import text_format


LOG = logging.getLogger('blobsan-service')
STOP = threading.Event()
MAX_BODY = 128 * 1024 * 1024
CANDIDATE = re.compile(r'^GroupId#\s+(\d+)\s+BlobId#\s+(\[(?:\d+:){6}0\])(?:\s+(.*))?$')
CATEGORIES = ('inv1_candidate', 'inv2_candidate')


def encode(value):
    return json.dumps(value, separators=(',', ':')).encode()


def has_two_complete_realms(indexes):
    return len(complete_realms(indexes)) >= 2


def pool_identity(layout):
    # Older persisted inventory has only names, which need not be unique across boxes.
    return layout.get('pool_key') or ('static' if layout['id'] < 0x80000000 else 'unknown:%d' % layout['id'])


def atomic_json(path, value):
    tmp = path.with_suffix('.tmp')
    with tmp.open('wb') as out:
        out.write(encode(value))
        out.flush()
        os.fsync(out.fileno())
    tmp.replace(path)
    fd = os.open(str(path.parent), os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def secret(path):
    return Path(path).expanduser().read_text().strip() if path else ''


def token_headers():
    params = common.connection_params
    if not params.token:
        return {}
    value = (params.token_type + ' ' if params.token_type else '') + params.token
    return {'Authorization': value}


def http(url, headers=None, data=None, timeout=30, raw=False, context=None):
    req = urllib.request.Request(url, data=None if data is None else encode(data), headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=timeout, context=context) as response:
            body = response.read(MAX_BODY + 1)
    except urllib.error.HTTPError as exc:
        # urlopen raises before entering the context manager. HTTPError owns
        # the response body, which can otherwise keep a socket alive until GC.
        exc.close()
        raise
    if len(body) > MAX_BODY:
        raise ValueError('HTTP response exceeds 128 MiB')
    return body if raw else json.loads(body)


def log_failure(message, exc, *args):
    reason = exc.reason if isinstance(exc, urllib.error.URLError) and not isinstance(exc, urllib.error.HTTPError) else exc
    LOG.error(message + ': %s', *args, ' '.join(str(reason).split()), exc_info=LOG.isEnabledFor(logging.DEBUG))


def viewer_url(args):
    if args.viewer:
        return args.viewer.rstrip('/')
    params = common.connection_params
    endpoint = params.make_endpoint_info(args.endpoint[0])
    protocol = endpoint.protocol if endpoint.protocol in ('http', 'https') else params.mon_protocol
    return protocol + '://' + endpoint.host_with_mon_port


def view(args, path, **params):
    return ydb_http(args, viewer_url(args) + '/' + path + '?' + urllib.parse.urlencode(params))


def ydb_http(args, url, raw=False):
    return http(url, token_headers(), timeout=args.request_timeout, raw=raw, context=ydb_ssl_context())


def ydb_ssl_context():
    params = common.connection_params
    context = ssl.create_default_context(cafile=params.cafile)
    if params.insecure:
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
    return context


def blob_size(blob):
    return int(blob[1:-1].split(':')[5])


def restore_blob(args, layout, blob):
    """Issue one MustRestoreFirst read, without retrying an uncertain outcome."""
    params = dict(groupId=layout['id'], blob=blob, mustRestoreFirst=1, binary=1)
    url = viewer_url(args) + '/get_blob?' + urllib.parse.urlencode(params)
    request = urllib.request.Request(url, headers=token_headers())
    expected = blob_size(blob)
    try:
        with urllib.request.urlopen(request, timeout=args.request_timeout, context=ydb_ssl_context()) as response:
            if response.status == 204:
                return 'absent'
            if response.status != 200 or response.headers.get_content_type() != 'application/octet-stream':
                raise ValueError('Unexpected get_blob response: HTTP %d' % response.status)
            received = 0
            while True:
                chunk = response.read(min(65536, expected + 1 - received))
                if not chunk:
                    break
                received += len(chunk)
                if received > expected:
                    break
            if received != expected:
                raise ValueError('get_blob returned %d bytes; expected %d' % (received, expected))
    except urllib.error.HTTPError as exc:
        exc.close()
        raise
    return 'restored'


def complete_realms(coverage):
    indexes = set(coverage)
    return [realm for realm in range(3) if all(realm * 3 + domain in indexes for domain in range(3))]


def restore_part_request(args, body):
    """Retain non-2xx bodies: the batch can contain successful item responses."""
    headers = dict(token_headers(), **{'Content-Type': 'application/x-www-form-urlencoded'})
    request = urllib.request.Request(viewer_url(args) + '/restore_part', data=body, headers=headers)
    try:
        response = urllib.request.urlopen(request, timeout=max(args.request_timeout, args.restore_timeout + 10),
                                          context=ydb_ssl_context())
    except urllib.error.HTTPError as exc:
        response = exc
    with response:
        return response.status, response.read(MAX_BODY + 1)


def parse_restore_part_response(body, expected):
    """Keep individually valid protobuf-text rows even in an incomplete response."""
    if len(body) > MAX_BODY:
        return {}, 'restore_part response exceeds 128 MiB; saved response is truncated'
    try:
        lines = body.decode('utf-8').splitlines()
    except UnicodeError as exc:
        return {}, 'Invalid restore_part response encoding: ' + str(exc)
    results, errors, duplicates = {}, [], set()
    for line in lines:
        fields = line.split(maxsplit=2)
        if len(fields) != 3 or (fields[0], fields[1]) not in expected:
            errors.append('Unexpected restore_part response row: ' + line[:200])
            continue
        key = tuple(fields[:2])
        if key in results or key in duplicates:
            results.pop(key, None)
            duplicates.add(key)
            errors.append('Duplicate restore_part response row: ' + ' '.join(key))
            continue
        try:
            record = restore_part_pb2.TRestorePartResult()
            text_format.Parse(fields[2], record)
            if not record.HasField('Status'):
                raise ValueError('response has no Status')
            descriptor = record.DESCRIPTOR.fields_by_name
            status = descriptor['Status'].enum_type.values_by_number[record.Status].name
            origin = descriptor['ResponseOrigin'].enum_type.values_by_number[record.ResponseOrigin].name if (
                'ResponseOrigin' in descriptor and record.HasField('ResponseOrigin')) else None
        except Exception as exc:
            errors.append('Invalid restore_part response row: %s: %s' % (' '.join(key), exc))
        else:
            results[key] = dict(status=status, origin=origin, error=record.ErrorReason, raw=line)
    if results.keys() != expected:
        errors.append('restore_part response omitted %d valid request rows' % len(expected - results.keys()))
    return results, '; '.join(errors)[:4000] or None


def topology(args):
    """BSC inventory for dynamic groups, whiteboard for static groups."""
    data = common.fetch_base_config_and_storage_pools(cache=False)
    base = data['BaseConfig']
    pools = {(p.BoxId, p.StoragePoolId): p.Name for p in data['StoragePools']}
    slots = {(s.VSlotId.NodeId, s.VSlotId.PDiskId, s.VSlotId.VSlotId): s for s in base.VSlot}
    groups = {}
    for row in base.Group:
        if row.ErasureSpecies != 'mirror-3-dc' or row.HasField('VirtualGroupInfo'):
            continue
        group = dict(id=row.GroupId, generation=row.GroupGeneration,
                     pool=pools.get((row.BoxId, row.StoragePoolId)),
                     pool_key='%d:%d' % (row.BoxId, row.StoragePoolId), slots=[])
        for id_ in row.VSlotId:
            slot = slots[id_.NodeId, id_.PDiskId, id_.VSlotId]
            if slot.GroupId != row.GroupId or slot.GroupGeneration != row.GroupGeneration:
                raise ValueError('Inconsistent BSC layout')
            if slot.VDiskIdx != 0 or slot.FailRealmIdx not in range(3) or slot.FailDomainIdx not in range(3):
                raise ValueError('Only 3x3 mirror-3-dc geometry is supported')
            group['slots'].append(dict(index=slot.FailRealmIdx * 3 + slot.FailDomainIdx,
                                       node=id_.NodeId, pdisk=id_.PDiskId, slot=id_.VSlotId))
        groups[row.GroupId] = group
    infos = view(args, 'viewer/json/bsgroupinfo').get('BSGroupStateInfo', [])
    static = [g for g in infos if int(g['GroupID']) < 0x80000000 and g.get('ErasureSpecies') == 'mirror-3-dc']
    static.sort(key=lambda g: int(g['GroupGeneration']), reverse=True)
    if static:
        rows = view(args, 'viewer/json/vdiskinfo').get('VDiskStateInfo', [])
        for info in static:
            gid, generation = int(info['GroupID']), int(info['GroupGeneration'])
            if gid in groups:
                continue
            group = dict(id=gid, generation=generation, pool='static', pool_key='static', slots=[])
            for row in rows:
                v = row.get('VDiskId', {})
                if int(v.get('GroupID', -1)) != gid or int(v.get('GroupGeneration', -1)) != generation or row.get('DonorMode'):
                    continue
                if int(v.get('VDisk', 0)) != 0:
                    raise ValueError('Only 3x3 geometry is supported')
                group['slots'].append(dict(index=int(v.get('Ring', 0)) * 3 + int(v.get('Domain', 0)),
                                           node=int(row['NodeId']), pdisk=int(row['PDiskId']),
                                           slot=int(row['VDiskSlotId'])))
            groups[gid] = group
    for group in groups.values():
        group['slots'].sort(key=lambda s: s['index'])
        indexes = [s['index'] for s in group['slots']]
        minimum = 1 if group['pool_key'] == 'static' else 9
        if len(indexes) < minimum or len(set(indexes)) != len(indexes) or any(i not in range(9) for i in indexes):
            raise ValueError('Incomplete/ambiguous layout for group %s' % group['id'])
        group['fingerprint'] = hashlib.sha256(encode(group)).hexdigest()
    if not groups:
        raise ValueError('No mirror-3-dc groups found')
    return list(groups.values())


class Master:
    def __init__(self, args, directory):
        self.args = args
        self.lock = threading.RLock()
        self.refresh = threading.Event()
        self.db = sqlite3.connect(str(directory / 'state.sqlite3'), check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.db.executescript('''
            PRAGMA journal_mode=WAL;
            PRAGMA synchronous=FULL;
            CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY, value TEXT);
            CREATE TABLE IF NOT EXISTS groups(
                id INTEGER PRIMARY KEY, layout TEXT, seen REAL, due REAL DEFAULT 0,
                token TEXT, owner TEXT, expires REAL DEFAULT 0, last_token TEXT,
                last_scan REAL, last_count INTEGER, last_result TEXT, error TEXT,
                scans INTEGER DEFAULT 0, new_total INTEGER DEFAULT 0);
            CREATE TABLE IF NOT EXISTS workers(id TEXT PRIMARY KEY, seen REAL, info TEXT);
            CREATE TABLE IF NOT EXISTS findings(
                gid INTEGER, blob TEXT, first_seen REAL, last_seen REAL, active INTEGER,
                status TEXT, coverage TEXT NOT NULL, PRIMARY KEY(gid,blob));
            CREATE TABLE IF NOT EXISTS restore_candidates(
                gid INTEGER, category TEXT, blob TEXT, first_seen REAL, last_seen REAL, active INTEGER,
                coverage TEXT NOT NULL, generation INTEGER, fingerprint TEXT,
                PRIMARY KEY(gid,category,blob));
            CREATE INDEX IF NOT EXISTS restore_candidates_active
                ON restore_candidates(active,category,gid,blob);
            CREATE TABLE IF NOT EXISTS scan_reports(
                gid INTEGER PRIMARY KEY, generation INTEGER, fingerprint TEXT,
                started REAL, finished REAL, result TEXT);
            CREATE TABLE IF NOT EXISTS restore_runs(id TEXT PRIMARY KEY, result TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS restore_batches(
                id TEXT PRIMARY KEY, started REAL NOT NULL, finished REAL, state TEXT NOT NULL,
                request BLOB NOT NULL, http_status INTEGER, response BLOB, error TEXT,
                blob_count INTEGER NOT NULL, request_count INTEGER NOT NULL,
                successful_blobs INTEGER NOT NULL DEFAULT 0, successful_bytes INTEGER NOT NULL DEFAULT 0);
            CREATE INDEX IF NOT EXISTS restore_batches_finished ON restore_batches(finished);
            CREATE INDEX IF NOT EXISTS restore_batches_state ON restore_batches(state,started);
            CREATE TABLE IF NOT EXISTS restore_attempts(
                id INTEGER PRIMARY KEY AUTOINCREMENT, batch_id TEXT NOT NULL, gid INTEGER NOT NULL,
                blob TEXT NOT NULL, category TEXT NOT NULL, generation INTEGER, fingerprint TEXT,
                scan_started REAL NOT NULL, started REAL NOT NULL, finished REAL, state TEXT NOT NULL,
                logical_bytes INTEGER NOT NULL, error TEXT);
            CREATE INDEX IF NOT EXISTS restore_attempts_batch ON restore_attempts(batch_id);
            CREATE INDEX IF NOT EXISTS restore_attempts_finished ON restore_attempts(finished,category);
            CREATE TABLE IF NOT EXISTS restore_requests(
                attempt_id INTEGER NOT NULL, vdisk TEXT NOT NULL, status TEXT, origin TEXT, error TEXT, raw TEXT,
                PRIMARY KEY(attempt_id,vdisk));
            CREATE TABLE IF NOT EXISTS auto_restore_work(
                gid INTEGER, blob TEXT, attempt_id INTEGER NOT NULL, PRIMARY KEY(gid,blob));
        ''')
        self.db.execute('BEGIN')
        group_columns = {row['name'] for row in self.db.execute('PRAGMA table_info(groups)')}
        if 'claimed_at' not in group_columns:
            self.db.execute('ALTER TABLE groups ADD COLUMN claimed_at REAL NOT NULL DEFAULT 0')
        columns = {row['name'] for row in self.db.execute('PRAGMA table_info(restore_candidates)')}
        if 'scan_started' not in columns:
            self.db.execute('ALTER TABLE restore_candidates ADD COLUMN scan_started REAL NOT NULL DEFAULT 0')
        if 'realm_mask' not in columns:
            self.db.execute('ALTER TABLE restore_candidates ADD COLUMN realm_mask INTEGER NOT NULL DEFAULT 0')
            for row in self.db.execute('SELECT gid,category,blob,coverage FROM restore_candidates'):
                mask = sum(1 << realm for realm in complete_realms(json.loads(row['coverage'])))
                self.db.execute('UPDATE restore_candidates SET realm_mask=? WHERE gid=? AND category=? AND blob=?',
                                (mask, row['gid'], row['category'], row['blob']))
        now = time.time()
        interrupted = 'Master restarted before outcome was recorded; a fresh scan is required before retrying'
        self.db.execute("UPDATE restore_batches SET state='unknown',finished=?,error=? WHERE state='inflight'",
                        (now, interrupted))
        self.db.execute("""UPDATE restore_requests SET status='UNKNOWN',error=? WHERE attempt_id IN
            (SELECT id FROM restore_attempts WHERE state='inflight') AND status IS NULL""", (interrupted,))
        self.db.execute("UPDATE restore_attempts SET state='unknown',finished=?,error=? WHERE state='inflight'",
                        (now, interrupted))
        for key, value in [('paused', 0), ('interval', args.interval), ('retry', args.retry)]:
            self.db.execute('INSERT OR IGNORE INTO settings VALUES(?,?)', (key, str(value)))
        self.db.commit()
        self._restore_thread = None
        previous = self.restore_status()
        if previous and previous['state'] == 'running':
            previous.update(state='interrupted', finished_at=time.time(),
                            error='Master restarted; the last request outcome may be unknown. Refresh candidates before repeating.')
            with self.db:
                self.save_restore_status(previous)

    def setting(self, key):
        return float(self.db.execute('SELECT value FROM settings WHERE key=?', (key,)).fetchone()[0])

    def discover(self):
        while not STOP.is_set():
            self.refresh.clear()
            try:
                layouts = topology(self.args)
                now = time.time()
                with self.lock, self.db:
                    for layout in layouts:
                        gid = layout['id']
                        old = self.db.execute('SELECT layout FROM groups WHERE id=?', (gid,)).fetchone()
                        if old and json.loads(old[0])['fingerprint'] != layout['fingerprint']:
                            self.db.execute('UPDATE groups SET token=NULL,expires=0,due=0 WHERE id=?', (gid,))
                        self.db.execute('''INSERT INTO groups(id,layout,seen) VALUES(?,?,?)
                            ON CONFLICT(id) DO UPDATE SET layout=excluded.layout,seen=excluded.seen''',
                                        (gid, json.dumps(layout), now))
                    self.db.execute('INSERT OR REPLACE INTO settings VALUES(?,?)', ('discovery_error', ''))
                    self.db.execute('INSERT OR REPLACE INTO settings VALUES(?,?)', ('discovered_at', str(now)))
                LOG.info('Discovered %d groups', len(layouts))
            except Exception as exc:
                LOG.exception('Discovery failed')
                with self.lock, self.db:
                    self.db.execute('INSERT OR REPLACE INTO settings VALUES(?,?)', ('discovery_error', str(exc)))
            self.refresh.wait(self.args.refresh)

    def rpc(self, action, body):
        now = time.time()
        worker = str(body['worker'])
        if len(worker) > 200:
            raise ValueError('Invalid worker ID')
        with self.lock, self.db:
            if action not in ('/claim', '/heartbeat', '/result'):
                raise ValueError('Unknown operation')
            self.db.execute('INSERT OR REPLACE INTO workers VALUES(?,?,?)', (worker, now, str(body.get('info', ''))[:500]))
            if action == '/claim':
                # A lost claim response must not allocate a second group to the worker.
                owned = self.db.execute('SELECT * FROM groups WHERE owner=? AND token IS NOT NULL AND expires>?',
                                        (worker, now)).fetchone()
                if owned:
                    return {'job': dict(layout=json.loads(owned['layout']), token=owned['token'], lease_seconds=owned['expires'] - now)}
                if self.setting('paused'):
                    return {'job': None}
                busy = set()
                for row in self.db.execute('SELECT layout FROM groups WHERE token IS NOT NULL AND expires>?', (now,)):
                    busy.update(s['node'] for s in json.loads(row[0])['slots'])
                rows = self.db.execute('''SELECT * FROM groups WHERE due<=? AND
                    (token IS NULL OR expires<=?) AND seen>? ORDER BY due,id''',
                                       (now, now, now - self.args.refresh * 2)).fetchall()
                for row in rows:
                    layout = json.loads(row['layout'])
                    if busy.intersection(s['node'] for s in layout['slots']):
                        continue
                    lease = uuid.uuid4().hex
                    self.db.execute('UPDATE groups SET token=?,owner=?,expires=?,claimed_at=? WHERE id=?',
                                    (lease, worker, now + self.args.lease, now, row['id']))
                    return {'job': dict(layout=layout, token=lease, lease_seconds=self.args.lease)}
                return {'job': None}
            gid, lease = int(body['group']), body['token']
            row = self.db.execute('SELECT * FROM groups WHERE id=?', (gid,)).fetchone()
            if action == '/result' and row and row['last_token'] == lease:
                return {'accepted': True}
            if not row or row['token'] != lease or row['owner'] != worker or row['expires'] <= now:
                return {'accepted': False}
            if action == '/heartbeat':
                self.db.execute('UPDATE groups SET expires=? WHERE id=?', (now + self.args.lease, gid))
                return {'accepted': True}
            if action != '/result':
                raise ValueError('Unknown operation')
            result = body['result']
            # A persisted result from an older worker must be acknowledged so
            # pending.json can drain, but its old category cannot be relabeled.
            if 'candidates' not in result and 'findings' in result and not result.get('error'):
                result = dict(error='Legacy candidate result; rescan with --restore-canidates')
            error = result.get('error')
            if not error:
                candidates = result['candidates']
                if not isinstance(candidates, dict) or set(candidates) != set(CATEGORIES):
                    raise ValueError('Invalid candidate categories')
                for blobs in candidates.values():
                    if not isinstance(blobs, list) or any(
                            not isinstance(blob, str) or not re.fullmatch(r'\[(?:\d+:){6}0\]', blob) for blob in blobs
                    ) or len(set(blobs)) != len(blobs):
                        raise ValueError('Invalid candidate list')
                if not set(candidates['inv1_candidate']).issubset(candidates['inv2_candidate']):
                    raise ValueError('inv1_candidate must be a subset of inv2_candidate')
                completed = result['completed_vdisks']
                if not isinstance(completed, list) or any(type(i) is not int or i not in range(9) for i in completed) or len(set(completed)) != len(completed):
                    raise ValueError('Invalid coverage')
                report = result.get('scan_report')
                if report is not None:
                    write2x2_report.validate(report, gid, completed)
                    started, finished = result['started_at'], result['finished_at']
                    if any(type(t) not in (int, float) or not math.isfinite(t) for t in (started, finished)) or started > finished:
                        raise ValueError('Invalid scan timestamps')
                candidates_complete = result['candidates_complete']
                if type(candidates_complete) is not bool or (not candidates_complete and (
                        report is None or any(candidates.values()))):
                    raise ValueError('Invalid candidate scan status')
                if not completed or (candidates_complete and not has_two_complete_realms(completed)):
                    raise ValueError('Incomplete scan')
                previous = {(r['category'], r['blob']): r['coverage'] for r in self.db.execute(
                    'SELECT category,blob,coverage FROM restore_candidates WHERE gid=? AND active=1', (gid,))}
                current = {(category, blob) for category, blobs in candidates.items() for blob in blobs}
                resolved = {key for key, coverage in previous.items() if candidates_complete and key not in current and
                            (len(completed) == 9 or set(completed) == set(json.loads(coverage)))}
                self.db.executemany('UPDATE restore_candidates SET active=0 WHERE gid=? AND category=? AND blob=?',
                                    ((gid, category, blob) for category, blob in resolved))
                coverage = json.dumps(sorted(completed))
                # Compare master-owned lease times; worker clocks can differ.
                scan_started = row['claimed_at']
                realm_mask = sum(1 << realm for realm in complete_realms(completed))
                layout = json.loads(row['layout'])
                self.db.executemany(
                    '''INSERT INTO restore_candidates
                    (gid,category,blob,first_seen,last_seen,active,coverage,generation,fingerprint,scan_started,realm_mask)
                    VALUES(?,?,?,?,?,1,?,?,?,?,?) ON CONFLICT(gid,category,blob) DO UPDATE SET
                    last_seen=excluded.last_seen,active=1,coverage=excluded.coverage,
                    generation=excluded.generation,fingerprint=excluded.fingerprint,
                    scan_started=excluded.scan_started,realm_mask=excluded.realm_mask''',
                    ((gid, category, blob, now, now, coverage, layout['generation'], layout['fingerprint'], scan_started, realm_mask)
                     for category, blob in current))
                previous_blobs = {blob for _, blob in previous}
                current_blobs = {blob for _, blob in current}
                unresolved_blobs = {blob for _, blob in (previous.keys() - resolved) | current}
                new = len(current_blobs - previous_blobs)
                result = {k: v for k, v in result.items() if k != 'candidates'}
                result.update(candidates=len(current_blobs) if candidates_complete else None,
                              candidate_counts={category: len(candidates[category]) for category in CATEGORIES}
                              if candidates_complete else None, new=new,
                              disappeared=len(previous_blobs - unresolved_blobs), unresolved=len(unresolved_blobs))
                self.db.execute('''UPDATE groups SET last_scan=?,last_count=?,scans=scans+1,
                    new_total=new_total+? WHERE id=?''', (now, result['candidates'], new, gid))
                if report is not None:
                    self.db.execute('''INSERT OR REPLACE INTO scan_reports
                        VALUES(?,?,?,?,?,?)''', (gid, layout['generation'], layout['fingerprint'],
                                                 started, finished, json.dumps(result)))
                delay = self.setting('retry' if unresolved_blobs or len(completed) < 9 else 'interval')
                if new:
                    LOG.warning('Group %s: %d new/reappeared candidates, %d total', gid, new, len(current_blobs))
            else:
                delay = self.setting('retry')
                LOG.warning('Group %s failed: %s', gid, error)
            self.db.execute('''UPDATE groups SET token=NULL,expires=0,last_token=?,due=?,
                last_result=?,error=? WHERE id=?''', (lease, now + delay, json.dumps(result), error, gid))
            return {'accepted': True}

    def layouts(self, gid, now):
        row = self.db.execute('''SELECT g.layout,g.seen,g.error,s.* FROM groups g
            LEFT JOIN scan_reports s ON s.gid=g.id WHERE g.id=?''', (gid,)).fetchone()
        if row is None:
            raise ValueError('Unknown group')
        layout = json.loads(row['layout'])
        result = dict(group_id=gid, generation=layout['generation'], pool=layout.get('pool'),
                      pool_key=pool_identity(layout), latest_error=row['error'],
                      inventory_stale=row['seen'] < now - self.args.refresh * 2)
        if row['result'] is None:
            return dict(result, scan_report=None)
        result.update(json.loads(row['result']))
        result.update(report_generation=row['generation'],
                      outdated=row['fingerprint'] != layout['fingerprint'] or row['generation'] != layout['generation'],
                      scan_age_seconds=max(0, round(now - row['finished'])))
        return result

    def pools(self, selector, now):
        pools = {}
        rows = self.db.execute('''SELECT g.id,g.layout,g.seen,g.error,s.generation,s.fingerprint,
            s.finished,s.result FROM groups g LEFT JOIN scan_reports s ON s.gid=g.id''')
        for row in rows:
            layout = json.loads(row['layout'])
            key = pool_identity(layout)
            name = layout.get('pool')
            if selector is not None and selector not in (key, name):
                continue
            pool = pools.setdefault(key, dict(pool_key=key, pool=name, groups=0, reported_groups=0,
                                              full_groups=0, partial_groups=0, never_scanned_groups=0, outdated_groups=0,
                                              stale_inventory_groups=0, failed_groups=0, oldest_scan=None, newest_scan=None, histogram={}))
            pool['groups'] += 1
            pool['failed_groups'] += bool(row['error'])
            if row['seen'] < now - self.args.refresh * 2:
                pool['stale_inventory_groups'] += 1
                continue
            if row['result'] is None:
                pool['never_scanned_groups'] += 1
                continue
            if row['fingerprint'] != layout['fingerprint'] or row['generation'] != layout['generation']:
                pool['outdated_groups'] += 1
                continue
            report = json.loads(row['result'])['scan_report']
            pool['reported_groups'] += 1
            pool['full_groups' if len(report['completed_vdisks']) == 9 else 'partial_groups'] += 1
            for field, func in (('oldest_scan', min), ('newest_scan', max)):
                pool[field] = row['finished'] if pool[field] is None else func(pool[field], row['finished'])
            write2x2_report.merge(pool['histogram'], report['buckets'])
        results = []
        for key in sorted(pools, key=lambda key: str(key)):
            pool = pools[key]
            pool['buckets'] = write2x2_report.histogram_rows(pool.pop('histogram'))
            pool['totals'] = write2x2_report.totals(pool['buckets'])
            pool['oldest_scan_age_seconds'] = None if pool['oldest_scan'] is None else max(0, round(now - pool['oldest_scan']))
            results.append(pool)
        return results

    def candidates(self, words):
        category = words[0] if words else 'all'
        if category not in (*CATEGORIES, 'all') or len(words) > 4:
            raise ValueError('Use candidates CATEGORY [GROUP|all] [OFFSET] [LIMIT]')
        gid = None if len(words) < 2 or words[1] == 'all' else int(words[1])
        offset = int(words[2]) if len(words) > 2 else 0
        limit = int(words[3]) if len(words) > 3 else 1000
        if offset < 0 or not 1 <= limit <= 1000:
            raise ValueError('Offset must be nonnegative and limit must be in [1, 1000]')
        conditions, values = ['c.active=1'], []
        if category != 'all':
            conditions.append('c.category=?')
            values.append(category)
        if gid is not None:
            conditions.append('c.gid=?')
            values.append(gid)
        where = ' AND '.join(conditions)
        total = self.db.execute('SELECT count(*) FROM restore_candidates c WHERE ' + where, values).fetchone()[0]
        query = '''SELECT c.gid,c.category,c.blob,c.first_seen,c.last_seen,c.coverage,
            c.generation,c.fingerprint,g.layout,g.seen FROM restore_candidates c
            JOIN groups g ON g.id=c.gid WHERE ''' + where + ' ORDER BY c.gid,c.category,c.blob LIMIT ? OFFSET ?'
        rows = self.db.execute(query, (*values, limit, offset))
        items = []
        for row in rows:
            item = dict(row)
            layout = json.loads(item.pop('layout'))
            item['coverage'] = json.loads(item['coverage'])
            item['outdated'] = item.pop('fingerprint') != layout['fingerprint'] or item['generation'] != layout['generation']
            item['inventory_stale'] = item.pop('seen') < time.time() - self.args.refresh * 2
            items.append(item)
        return dict(category=category, group=gid, offset=offset, limit=limit, total=total, rows=items)

    def automatic_queue(self, now):
        # A retained inv1 observation can have obsolete layout/coverage while the
        # same blob has a fresh inv2 observation. Apply the shared attempt gate
        # before choosing one observation, then prefer inv1 at equal readiness.
        # Rank only keys/state in SQLite; do not materialize all layouts in Python.
        return '''WITH observations AS (
            SELECT c.gid,c.blob,c.category,CASE
                WHEN a.state='inflight' THEN 'inflight'
                WHEN c.generation IS NULL OR c.fingerprint IS NULL OR g.seen<:fresh
                    OR c.generation!=json_extract(g.layout,'$.generation')
                    OR c.fingerprint!=json_extract(g.layout,'$.fingerprint')
                    OR c.realm_mask NOT IN (3,5,6,7) THEN 'excluded'
                WHEN a.id IS NULL THEN 'pending'
                WHEN c.scan_started<=a.finished THEN CASE WHEN a.state='awaiting_rescan'
                    THEN 'awaiting_rescan' WHEN a.state='unknown' THEN 'unknown' ELSE 'retry_wait' END
                WHEN a.finished+:retry>:now THEN 'retry_wait'
                ELSE 'pending' END queue_state
            FROM restore_candidates c JOIN groups g ON g.id=c.gid
            LEFT JOIN auto_restore_work w ON w.gid=c.gid AND w.blob=c.blob
            LEFT JOIN restore_attempts a ON a.id=w.attempt_id
            WHERE c.active=1
        ), ranked AS (
            SELECT *,row_number() OVER (PARTITION BY gid,blob ORDER BY
                CASE queue_state WHEN 'pending' THEN 0 WHEN 'inflight' THEN 0
                    WHEN 'retry_wait' THEN 1 WHEN 'excluded' THEN 3 ELSE 2 END,
                CASE category WHEN 'inv1_candidate' THEN 0 ELSE 1 END) preference
            FROM observations
        ), queue AS (
            SELECT c.*,g.layout,g.seen,r.queue_state
            FROM ranked r JOIN restore_candidates c ON c.gid=r.gid AND c.blob=r.blob AND c.category=r.category
            JOIN groups g ON g.id=c.gid WHERE r.preference=1
        ) ''', dict(now=now, fresh=now - self.args.refresh * 2, retry=self.setting('retry'))

    def automatic_status(self):
        now = time.time()
        prefix, params = self.automatic_queue(now)
        rows = self.db.execute(prefix + 'SELECT queue_state,category,count(*) n FROM queue GROUP BY queue_state,category', params)
        result = dict(enabled=bool(getattr(self.args, 'restore', False)), state='disabled', pending_inv1=0,
                      pending_inv2_only=0, inflight_blobs=0, awaiting_rescan=0, retry_wait=0, unknown_outcomes=0,
                      excluded_blobs=0, active_unique_blobs=0, completed_blobs=0, completed_bytes=0,
                      blobs_per_second=0.0, bytes_per_second=0.0, eta_seconds=None,
                      last_batch_finished_at=None, status_counts={}, errors=[], current_batch=None, phase_rates={})
        names = dict(inflight='inflight_blobs', awaiting_rescan='awaiting_rescan', retry_wait='retry_wait',
                     unknown='unknown_outcomes', excluded='excluded_blobs')
        for row in rows:
            result['active_unique_blobs'] += row['n']
            key = ('pending_inv1' if row['category'] == 'inv1_candidate' else 'pending_inv2_only') if (
                row['queue_state'] == 'pending') else names[row['queue_state']]
            result[key] += row['n']
        total = self.db.execute('''SELECT coalesce(sum(successful_blobs),0),coalesce(sum(successful_bytes),0),
            max(finished) FROM restore_batches''').fetchone()
        result.update(completed_blobs=total[0], completed_bytes=total[1], last_batch_finished_at=total[2])
        recent = self.db.execute('''SELECT min(started),sum(successful_blobs),sum(successful_bytes)
            FROM restore_batches WHERE finished>=?''', (now - 300,)).fetchone()
        if recent[0] is not None:
            elapsed = max(1.0, now - max(now - 300, recent[0]))
            result.update(blobs_per_second=(recent[1] or 0) / elapsed, bytes_per_second=(recent[2] or 0) / elapsed)
        pending = result['pending_inv1'] + result['pending_inv2_only']
        for category, phase, key in (('inv1_candidate', 'inv1', 'pending_inv1'),
                                     ('inv2_candidate', 'inv2_only', 'pending_inv2_only')):
            recent_phase = self.db.execute('''SELECT min(started),count(*),sum(logical_bytes)
                FROM restore_attempts WHERE finished>=? AND category=? AND state='awaiting_rescan' ''',
                                           (now - 300, category)).fetchone()
            elapsed = max(1.0, now - max(now - 300, recent_phase[0])) if recent_phase[0] is not None else 1.0
            rate = recent_phase[1] / elapsed
            result['phase_rates'][phase] = dict(
                pending=result[key], blobs_per_second=rate,
                bytes_per_second=(recent_phase[2] or 0) / elapsed,
                eta_seconds=result[key] / rate if rate else None if result[key] else 0)
        estimates = [phase['eta_seconds'] for phase in result['phase_rates'].values()]
        if all(value is not None for value in estimates) and (pending or result['blobs_per_second']):
            result['eta_seconds'] = sum(estimates)
        current = self.db.execute('''SELECT id,blob_count blobs,request_count requests,started started_at
            FROM restore_batches WHERE state='inflight' ORDER BY started DESC LIMIT 1''').fetchone()
        result['current_batch'] = dict(current) if current else None
        result['status_counts'] = dict(self.db.execute('''SELECT coalesce(r.status,'UNKNOWN'),count(*)
            FROM restore_requests r JOIN restore_attempts a ON a.id=r.attempt_id
            JOIN restore_batches b ON b.id=a.batch_id WHERE b.finished>=? GROUP BY r.status''', (now - 300,)))
        errors = self.db.execute('''SELECT a.gid,a.blob,r.vdisk,r.status,r.origin,r.error
            FROM restore_requests r JOIN restore_attempts a ON a.id=r.attempt_id
            WHERE a.id IN (SELECT id FROM restore_attempts ORDER BY id DESC LIMIT 1000)
                AND r.error IS NOT NULL AND r.error!=''
            ORDER BY a.id DESC LIMIT 20''')
        result['errors'] = [dict(row) for row in errors]
        if result['enabled']:
            result['state'] = (
                'paused' if self.setting('paused') else
                'running' if current else
                'ready' if pending else 'waiting_for_scan')
        return result

    def prepare_automatic_batch(self):
        now = time.time()
        with self.lock, self.db:
            if STOP.is_set() or self.setting('paused') or not getattr(self.args, 'restore', False):
                return None
            if self.db.execute("SELECT 1 FROM restore_batches WHERE state='inflight'").fetchone():
                return None
            prefix, params = self.automatic_queue(now)
            rows = self.db.execute(prefix + '''SELECT * FROM queue WHERE queue_state='pending'
                ORDER BY CASE category WHEN 'inv1_candidate' THEN 0 ELSE 1 END,random() LIMIT 500''', params).fetchall()
            if not rows:
                return None
            batch_id = uuid.uuid4().hex
            items, lines = [], []
            for row in rows:
                layout = json.loads(row['layout'])
                slots = {slot['index'] for slot in layout['slots']}
                count = 1 if row['category'] == 'inv1_candidate' else 2
                targets = []
                for realm in range(3):
                    if row['realm_mask'] & (1 << realm):
                        indexes = [realm * 3 + domain for domain in range(3)]
                        if not set(indexes).issubset(slots):
                            raise ValueError('Incomplete group layout for restoration')
                        targets.extend(random.sample(indexes, count))
                random.shuffle(targets)
                requests = ['[%x:%d:%d:%d:0]' % (row['gid'], row['generation'], index // 3, index % 3)
                            for index in targets]
                items.append((row, requests))
                lines.extend(vdisk + ' ' + row['blob'] for vdisk in requests)
            random.shuffle(lines)
            body = urllib.parse.urlencode(dict(
                requests='\n'.join(lines), inflight=self.args.restore_inflight,
                timeout=self.args.restore_timeout)).encode()
            self.db.execute('''INSERT INTO restore_batches
                (id,started,state,request,blob_count,request_count) VALUES(?,?,'inflight',?,?,?)''',
                            (batch_id, now, body, len(items), len(lines)))
            for row, requests in items:
                attempt = self.db.execute(
                    '''INSERT INTO restore_attempts
                    (batch_id,gid,blob,category,generation,fingerprint,scan_started,started,state,logical_bytes)
                    VALUES(?,?,?,?,?,?,?,?,'inflight',?)''',
                    (batch_id, row['gid'], row['blob'], row['category'],
                     row['generation'], row['fingerprint'], row['scan_started'], now, blob_size(row['blob']))).lastrowid
                self.db.execute('''INSERT INTO auto_restore_work VALUES(?,?,?) ON CONFLICT(gid,blob)
                    DO UPDATE SET attempt_id=excluded.attempt_id''', (row['gid'], row['blob'], attempt))
                self.db.executemany('INSERT INTO restore_requests(attempt_id,vdisk) VALUES(?,?)',
                                    ((attempt, vdisk) for vdisk in requests))
            return batch_id, body

    def finish_automatic_batch(self, batch_id, status=None, body=None, error=None, sent=True):
        finished = time.time()
        # Save the exact HTTP response before parsing. A parser failure must not
        # erase evidence of partial success, and a crash must retain the response.
        with self.lock, self.db:
            self.db.execute('UPDATE restore_batches SET http_status=?,response=?,error=? WHERE id=?',
                            (status, body, error, batch_id))
            requests = self.db.execute(
                '''SELECT a.id,a.gid,a.blob,a.logical_bytes,r.vdisk
                FROM restore_attempts a JOIN restore_requests r ON r.attempt_id=a.id WHERE a.batch_id=?''',
                (batch_id,)).fetchall()
        parsed = {}
        if body is not None:
            try:
                parsed, parse_error = parse_restore_part_response(body, {(r['vdisk'], r['blob']) for r in requests})
                error = parse_error or error
            except Exception as exc:
                error = 'Cannot parse restore_part response: ' + str(exc)
        success, successful_bytes = 0, 0
        with self.lock, self.db:
            outcomes = {}
            for row in requests:
                value = parsed.get((row['vdisk'], row['blob']))
                if value is None:
                    value = dict(status='UNKNOWN' if sent else 'NOT_SENT', origin=None,
                                 error=error or 'No response received', raw=None)
                self.db.execute('''UPDATE restore_requests SET status=?,origin=?,error=?,raw=?
                    WHERE attempt_id=? AND vdisk=?''',
                                (value['status'], value['origin'], value['error'], value['raw'], row['id'], row['vdisk']))
                outcomes.setdefault(row['id'], (row, []))[1].append(value['status'])
            for attempt, (row, statuses) in outcomes.items():
                good = all(value in ('OK', 'ALREADY') for value in statuses)
                state = 'awaiting_rescan' if good else 'unknown' if 'UNKNOWN' in statuses else 'retry_wait'
                self.db.execute('UPDATE restore_attempts SET finished=?,state=?,error=? WHERE id=?',
                                (finished, state, error, attempt))
                success += good
                successful_bytes += row['logical_bytes'] if good else 0
            self.db.execute('''UPDATE restore_batches SET finished=?,state=?,error=?,successful_blobs=?,successful_bytes=?
                WHERE id=?''', (finished, 'completed' if parsed and not error else 'partial' if parsed else 'unknown' if sent else 'cancelled',
                                error, success, successful_bytes, batch_id))
            # Preserve the age of an already queued rescan so frequent restores
            # in smaller group IDs cannot starve groups that have waited longer.
            self.db.executemany('UPDATE groups SET due=min(due,?) WHERE id=?',
                                ((finished, gid) for gid in {r['gid'] for r in requests}))
        LOG.info('Automatic restore batch %s: %d/%d blobs successful; HTTP %s',
                 batch_id, success, len(outcomes), status)

    def run_automatic_restore(self):
        while not STOP.is_set():
            try:
                batch = self.prepare_automatic_batch()
                if batch is None:
                    STOP.wait(1)
                    continue
                batch_id, body = batch
                if STOP.is_set():
                    self.finish_automatic_batch(batch_id, error='Master stopped before sending', sent=False)
                    break
                try:
                    status, response = restore_part_request(self.args, body)
                except Exception as exc:
                    self.finish_automatic_batch(batch_id, error=str(exc))
                    log_failure('Automatic restore batch %s failed', exc, batch_id)
                else:
                    self.finish_automatic_batch(batch_id, status, response)
            except Exception:
                LOG.exception('Automatic restore scheduler failed')
                with self.lock:
                    retry = self.setting('retry')
                STOP.wait(retry)

    def restore_status(self):
        row = self.db.execute('SELECT result FROM restore_runs ORDER BY rowid DESC LIMIT 1').fetchone()
        return json.loads(row[0]) if row else None

    def save_restore_status(self, run):
        self.db.execute('INSERT INTO restore_runs VALUES(?,?) ON CONFLICT(id) DO UPDATE SET result=excluded.result',
                        (run['id'], json.dumps(run)))

    def restore_plan(self, words, collect=False):
        if len(words) not in (1, 2) or words[0] not in CATEGORIES:
            raise ValueError('Use restore-preview CATEGORY [GROUP|all]; CATEGORY is inv1_candidate or inv2_candidate')
        category = words[0]
        gid = None if len(words) == 1 or words[1] == 'all' else int(words[1])
        query = '''SELECT c.gid,c.blob,c.generation,c.fingerprint,g.layout,g.seen FROM restore_candidates c
            JOIN groups g ON g.id=c.gid WHERE c.active=1 AND c.category=?'''
        values = (category,)
        if gid is not None:
            query += ' AND c.gid=?'
            values += (gid,)
        rows = self.db.execute(query + ' ORDER BY c.gid,c.blob', values)
        plan, groups = [], set()
        preview = dict(category=category, group=gid, blobs=0, logical_bytes=0, excluded_blobs=0, excluded_logical_bytes=0)
        digest = hashlib.sha256(encode([category, gid]))
        fresh = time.time() - self.args.refresh * 2
        layout = None
        for row in rows:
            if layout is None or layout['id'] != row['gid']:
                layout = json.loads(row['layout'])
            length = blob_size(row['blob'])
            if row['seen'] < fresh or row['fingerprint'] != layout['fingerprint'] or row['generation'] != layout['generation']:
                preview['excluded_blobs'] += 1
                preview['excluded_logical_bytes'] += length
                continue
            preview['blobs'] += 1
            preview['logical_bytes'] += length
            groups.add(row['gid'])
            digest.update(encode([row['gid'], row['blob'], row['generation'], row['fingerprint']]))
            if collect:
                plan.append((layout, row['blob']))
        preview.update(groups=len(groups), token=digest.hexdigest())
        return preview, plan

    def start_restore(self, words):
        if getattr(self.args, 'restore', False):
            raise ValueError('Automatic restoration is enabled; manual restore is unavailable')
        if len(words) != 3:
            raise ValueError('Use restore CATEGORY GROUP|all PREVIEW_TOKEN')
        previous = self.restore_status()
        if previous and previous['state'] == 'running':
            raise ValueError('A restore is already running')
        preview, plan = self.restore_plan(words[:2], collect=True)
        if preview['token'] != words[2]:
            raise ValueError('Restore candidates changed; refresh the preview before restoring')
        if not plan:
            raise ValueError('No current restore candidates in this selection')
        run = dict(preview, id=uuid.uuid4().hex, state='running', started_at=time.time(), finished_at=None,
                   restored=0, restored_bytes=0, absent=0, failed=0, skipped=0, errors=[])
        self.save_restore_status(run)
        # Publish the job before returning to the client; HTTP runs outside the DB lock.
        self._restore_thread = threading.Thread(target=self.run_restore, args=(run, plan), daemon=True)
        self._restore_thread.start()
        return run

    def run_restore(self, run, plan):
        checked, group_error = None, None
        try:
            for layout, blob in plan:
                if STOP.is_set():
                    run.update(state='interrupted', error='Master stopped; remaining requests were not issued')
                    break
                gid = layout['id']
                try:
                    if checked != gid:
                        checked, group_error = gid, None
                        try:
                            validate_layout(self.args, dict(layout=layout))
                        except Exception as exc:
                            group_error = str(exc)
                    with self.lock:
                        row = self.db.execute('''SELECT g.layout,g.seen,c.active,c.generation,c.fingerprint
                            FROM groups g JOIN restore_candidates c
                            ON c.gid=g.id WHERE g.id=? AND c.category=? AND c.blob=?''',
                                              (gid, run['category'], blob)).fetchone()
                        current = json.loads(row['layout']) if row else None
                        eligible = row and row['active'] and row['seen'] >= time.time() - self.args.refresh * 2 and (
                            current['fingerprint'] == row['fingerprint'] == layout['fingerprint']
                            and current['generation'] == row['generation'] == layout['generation'])
                    if not eligible:
                        run['skipped'] += 1
                        continue
                    if STOP.is_set():
                        run.update(state='interrupted', error='Master stopped; remaining requests were not issued')
                        break
                    if group_error:
                        raise ValueError(group_error)
                    status = restore_blob(self.args, layout, blob)
                    run[status] += 1
                    if status == 'restored':
                        run['restored_bytes'] += blob_size(blob)
                except Exception as exc:
                    run['failed'] += 1
                    if len(run['errors']) < 20:
                        run['errors'].append(dict(gid=gid, blob=blob, error=str(exc)[:500]))
                    log_failure('Restore group %s blob %s failed', exc, gid, blob)
                finally:
                    with self.lock, self.db:
                        self.save_restore_status(run)
            else:
                run['state'] = 'completed'
        except Exception as exc:
            run.update(state='interrupted', error=str(exc)[:500])
            log_failure('Restore job failed', exc)
        finally:
            run['finished_at'] = time.time()
            with self.lock, self.db:
                self.save_restore_status(run)
                self.db.executemany('UPDATE groups SET due=0 WHERE id=?', ((gid,) for gid in {g['id'] for g, _ in plan}))

    def console(self, command):
        words = shlex.split(command)
        if not words:
            words = ['stats']
        op = words[0]
        with self.lock, self.db:
            if op in ('pause', 'resume'):
                self.db.execute('UPDATE settings SET value=? WHERE key="paused"', (str(int(op == 'pause')),))
            elif op in ('interval', 'retry'):
                value = positive(words[1])
                self.db.execute('UPDATE settings SET value=? WHERE key=?', (str(value), op))
            elif op == 'scan':
                if len(words) == 1 or words[1] == 'all':
                    self.db.execute('UPDATE groups SET due=0')
                else:
                    self.db.execute('UPDATE groups SET due=0 WHERE id=?', (int(words[1]),))
            elif op == 'refresh':
                self.refresh.set()
            elif op == 'candidates':
                return json.dumps(self.candidates(words[1:]))
            elif op == 'restore-preview':
                preview, _ = self.restore_plan(words[1:])
                return json.dumps(dict(preview, run=self.restore_status(), automatic=self.automatic_status()))
            elif op == 'restore-status':
                return json.dumps(self.restore_status())
            elif op == 'restore':
                return json.dumps(self.start_restore(words[1:]))
            elif op == 'layouts':
                return json.dumps(self.layouts(int(words[1]), time.time()), indent=2)
            elif op == 'pools':
                return json.dumps(self.pools(words[1] if len(words) > 1 else None, time.time()), indent=2)
            elif op == 'stats':
                now = time.time()
                stats = dict(self.db.execute('SELECT key,value FROM settings'))
                stats.update(dict(self.db.execute('''SELECT count(*) groups,
                    sum(token IS NOT NULL AND expires>?) busy, sum(last_scan IS NULL) never_scanned,
                    sum(error IS NOT NULL) failed, sum(last_count) candidates_at_last_scans,
                    min(last_scan) oldest_scan, sum(scans) scans, sum(new_total) new_total
                    FROM groups''', (now,)).fetchone()))
                stats['workers_alive'] = self.db.execute(
                    'SELECT count(*) FROM workers WHERE seen>?', (now - self.args.lease,)).fetchone()[0]
                stats['stale_inventory_groups'] = self.db.execute(
                    'SELECT count(*) FROM groups WHERE seen<?', (now - self.args.refresh * 2,)).fetchone()[0]
                stats['unresolved_candidates'] = self.db.execute(
                    'SELECT count(DISTINCT gid || ":" || blob) FROM restore_candidates WHERE active=1').fetchone()[0]
                for category, key in zip(CATEGORIES, ('inv1_candidates', 'inv2_candidates')):
                    stats[key] = self.db.execute(
                        'SELECT count(*) FROM restore_candidates WHERE active=1 AND category=?', (category,)).fetchone()[0]
                stats['scans_with_missing_vdisks'] = sum(
                    bool(json.loads(r[0]).get('missing_vdisks'))
                    for r in self.db.execute('SELECT last_result FROM groups WHERE last_result IS NOT NULL'))
                if stats['oldest_scan'] is not None:
                    stats['oldest_scan_age_seconds'] = round(now - stats.pop('oldest_scan'))
                stats['automatic'] = self.automatic_status()
                return json.dumps(stats, indent=2)
            elif op in ('groups', 'workers', 'findings'):
                if op == 'groups':
                    if words[1:] == ['brief']:
                        rows = self.db.execute('''SELECT id,layout,owner,expires,due,last_scan,last_count,error
                            FROM groups ORDER BY id''')
                        result = []
                        for row in rows:
                            item = dict(row)
                            layout = json.loads(item.pop('layout'))
                            item.update(generation=layout['generation'], pool=layout.get('pool'),
                                        pool_key=pool_identity(layout))
                            result.append(json.dumps(item))
                        return '\n'.join(result) or '(empty)'
                    query, values = ('SELECT id,owner,expires,due,last_scan,last_count,error,last_result FROM groups ORDER BY id', ())
                elif op == 'workers':
                    query, values = ('SELECT * FROM workers ORDER BY seen DESC', ())
                elif len(words) > 1:
                    query, values = ('SELECT * FROM findings WHERE active=1 AND gid=? LIMIT 1000', (int(words[1]),))
                else:
                    query, values = ('SELECT * FROM findings WHERE active=1 LIMIT 1000', ())
                return '\n'.join(json.dumps(dict(r)) for r in self.db.execute(query, values)) or '(empty)'
            elif op == 'help':
                return ('stats | groups [brief] | layouts GROUP | pools [POOL] | workers | '
                        'candidates CATEGORY [GROUP|all] [OFFSET] [LIMIT] (max 1000/page) | '
                        'restore-preview CATEGORY [GROUP|all] | restore CATEGORY GROUP|all PREVIEW_TOKEN | restore-status | '
                        'pause | resume | scan [GROUP|all] | interval SEC | retry SEC | refresh | quit')
            else:
                raise ValueError('Unknown command; use help')
        return 'OK'


class API(http_server.BaseHTTPRequestHandler):
    def do_POST(self):
        try:
            self.connection.settimeout(60)
            expected = self.server.api_secret
            if expected and not hmac.compare_digest(self.headers.get('Authorization', ''), 'Bearer ' + expected):
                self.send_error(403)
                return
            size = int(self.headers.get('Content-Length', '0'))
            if not 0 < size <= MAX_BODY:
                self.send_error(413)
                return
            result = encode(self.server.master.rpc(self.path, json.loads(self.rfile.read(size))))
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(result)))
            self.end_headers()
            self.wfile.write(result)
        except Exception as exc:
            LOG.exception('API request failed')
            self.send_error(400, str(exc)[:200])

    def log_message(self, *_):
        pass


class Console(socketserver.StreamRequestHandler):
    def handle(self):
        def send(text):
            self.wfile.write((text.replace('\n', '\r\n') + '\r\n> ').encode())
        try:
            send('blobsan-service console. help for commands.\n' + self.server.master.console('stats'))
            while not STOP.is_set():
                line = self.rfile.readline(4096)
                if not line or line.strip() in (b'quit', b'exit'):
                    return
                # Ignore initial telnet option negotiation (IAC verb option).
                line = re.sub(b'\xff[\xfb-\xfe].', b'', line)
                try:
                    send(self.server.master.console(line.decode().strip()))
                except Exception as exc:
                    send('ERROR: ' + str(exc))
        except (OSError, UnicodeError):
            pass


class UnixServer(socketserver.ThreadingUnixStreamServer):
    daemon_threads = True


def serve(args, directory):
    master = Master(args, directory)
    server = http_server.ThreadingHTTPServer((args.bind, args.listen), API)
    server.master, server.api_secret = master, secret(args.secret_file)
    path = Path(args.console_sock).expanduser()
    if path.exists():
        # Never unlink another live console, or an unrelated file.
        if not path.is_socket():
            raise ValueError('Console path exists and is not a socket')
        with socket.socket(socket.AF_UNIX) as probe:
            try:
                probe.connect(str(path))
            except ConnectionRefusedError:
                path.unlink()
            else:
                raise ValueError('Console socket is already in use')
    console = UnixServer(str(path), Console)
    console.master = master
    path.chmod(0o777)
    for target in (server.serve_forever, console.serve_forever, master.discover):
        threading.Thread(target=target, daemon=True).start()
    automatic_thread = None
    if args.restore:
        automatic_thread = threading.Thread(target=master.run_automatic_restore, daemon=True)
        automatic_thread.start()
    LOG.info('Master listening on %s:%d; console %s', args.bind, args.listen, path)
    try:
        STOP.wait()
    finally:
        master.refresh.set()
        server.shutdown()
        console.shutdown()
        server.server_close()
        console.server_close()
        path.unlink(missing_ok=True)
        if automatic_thread is not None:
            automatic_thread.join(max(args.request_timeout, args.restore_timeout + 10) + 5)


def check_running(cancel):
    if STOP.is_set() or cancel.is_set() or time.monotonic() >= getattr(cancel, 'deadline', float('inf')):
        raise RuntimeError('Stopped or lost master lease')


class ScanError(RuntimeError):
    def __init__(self, message, coverage):
        super().__init__(message)
        self.coverage = coverage


def scan(args, job, directory, cancel):
    layout, base = job['layout'], viewer_url(args)
    started = time.time()
    deadline = time.monotonic() + args.scan_timeout

    def fetch(slot):
        index = slot['index']
        params = dict(pdiskId=slot['pdisk'], vdiskSlotId=slot['slot'], sessionId=uuid.uuid4().hex)
        url = base + '/node/%d/vdisk_stream?' % slot['node'] + urllib.parse.urlencode(params)
        with (directory / ('slot-%d' % index)).open('wb') as out:
            while True:
                check_running(cancel)
                if time.monotonic() >= deadline:
                    raise TimeoutError('snapshot timeout')
                # No retries: a lost response has already advanced the server stream.
                data = ydb_http(args, url, raw=True)
                if not data:
                    return index
                if data == b'ERROR':
                    raise ValueError('VDisk stream returned ERROR')
                out.write(struct.pack('<III', layout['id'], index, len(data)))
                out.write(data)

    good, errors = [], {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=9) as pool:
        futures = {pool.submit(fetch, slot): slot['index'] for slot in layout['slots']}
        for future in concurrent.futures.as_completed(futures):
            try:
                good.append(future.result())
            except Exception as exc:
                errors[futures[future]] = str(exc)
    check_running(cancel)
    coverage = dict(completed_vdisks=sorted(good), missing_vdisks=sorted(set(range(9)) - set(good)),
                    stream_errors=errors, started_at=started)
    if not good:
        raise ScanError('No complete VDisk streams', coverage)
    # Reproduce dstool take-snapshot framing without interpreting VDisk metadata.
    snapshot = directory / 'snapshot'
    with snapshot.open('wb') as out:
        for index in sorted(good):
            check_running(cancel)
            with (directory / ('slot-%d' % index)).open('rb') as inp:
                shutil.copyfileobj(inp, out)
    atomic_json(directory / 'manifest.json', dict(group_id=layout['id'], completed_vdisks=sorted(good)))
    try:
        candidates, report = run_blobsan(args, layout['id'], directory, cancel)
    except Exception as exc:
        raise ScanError(str(exc), coverage) from exc
    coverage.update(scan_report=report, finished_at=time.time())
    return candidates, coverage


def run_blobsan(args, gid, directory, cancel):
    """The supplied executable alone classifies restoration candidates."""
    check_running(cancel)
    report = directory / 'blobsan.err'
    manifest_path = (directory / 'manifest.json').resolve()
    manifest = json.loads(manifest_path.read_text())
    statistics_path = (directory / 'report.json').resolve()
    statistics_path.unlink(missing_ok=True)
    command = [args.blobsan, '--restore-canidates', '--scan-manifest', str(manifest_path),
               '--scan-report', str(statistics_path)]
    command.append('mirror-3-dc')
    with (directory / 'snapshot').open('rb') as inp, report.open('wb') as err:
        process = subprocess.Popen(command,
                                   stdin=inp, stdout=subprocess.DEVNULL, stderr=err)
        try:
            deadline = time.monotonic() + args.scan_timeout
            while process.poll() is None:
                check_running(cancel)
                if time.monotonic() >= deadline:
                    raise TimeoutError('blobsan timeout')
                cancel.wait(0.2)
            if process.returncode:
                raise RuntimeError('blobsan exited %d; see %s' % (process.returncode, report))
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
    check_running(cancel)
    statistics = write2x2_report.validate(json.loads(statistics_path.read_text()), gid, manifest['completed_vdisks'])
    candidates = {category: set() for category in CATEGORIES}
    with report.open(errors='replace') as inp:
        for line in inp:
            if 'GroupId#' not in line:
                continue
            match = CANDIDATE.fullmatch(line.strip())
            flags = set(match[3].split()) if match and match[3] else set()
            categories = flags.intersection(CATEGORIES)
            if not match or int(match[1]) != gid or not categories or flags - {*CATEGORIES, 'FULL'} or (
                    'inv1_candidate' in categories and 'inv2_candidate' not in categories):
                raise ValueError('Unrecognized blobsan candidate: ' + line[:200])
            for category in categories:
                candidates[category].add(match[2])
    return candidates, statistics


def validate_layout(args, job):
    layout = job['layout']
    nodes = sorted({s['node'] for s in layout['slots']})
    if not nodes:
        raise ValueError('No group nodes to verify generation')
    # Query only this group's storage nodes on every check. Filter by ID, not
    # generation: a newer generation must remain visible in the response.
    groups = view(args, 'viewer/json/bsgroupinfo', node_id=','.join(map(str, nodes)),
                  filter='GroupID=%d' % layout['id']).get('BSGroupStateInfo', [])
    groups = [g for g in groups if int(g['GroupID']) == layout['id']]
    group = max(groups, key=lambda g: int(g['GroupGeneration'])) if groups else None
    if group is None or int(group['GroupGeneration']) != layout['generation']:
        raise ValueError('Group generation changed or cannot be verified')
    if group.get('ErasureSpecies') != 'mirror-3-dc':
        raise ValueError('Group erasure changed')


def work(args, directory):
    state = directory / 'worker.json'
    if not state.exists():
        atomic_json(state, {'id': socket.gethostname() + '-' + uuid.uuid4().hex[:12]})
    worker = json.loads(state.read_text())['id']
    pending = directory / 'pending.json'
    base = args.master if '://' in args.master else 'http://' + args.master
    auth = secret(args.secret_file)

    def rpc(action, **body):
        return http(base.rstrip('/') + '/' + action, {'Authorization': 'Bearer ' + auth},
                    dict(worker=worker, info=socket.gethostname(), **body), timeout=args.request_timeout)

    while not STOP.is_set():
        try:
            if pending.exists():
                body = json.loads(pending.read_text())
                response = rpc('result', **body)
                LOG.info('Pending result group %s: %s', body['group'], response)
                pending.unlink()
                continue
            claim_started = time.monotonic()
            job = rpc('claim')['job']
            if not job:
                STOP.wait(args.poll)
                continue
            gid, lease = job['layout']['id'], job['token']
            cancel, finished = threading.Event(), threading.Event()
            lease_seconds = job['lease_seconds']
            cancel.deadline = claim_started + lease_seconds

            def heartbeat():
                while not finished.wait(min(args.heartbeat, lease_seconds / 3)):
                    try:
                        start = time.monotonic()
                        if not rpc('heartbeat', group=gid, token=lease)['accepted']:
                            cancel.set()
                            return
                        cancel.deadline = start + lease_seconds
                    except Exception:
                        # Stop scanning while ownership cannot be confirmed.
                        cancel.set()
                        return

            thread = threading.Thread(target=heartbeat, daemon=True)
            thread.start()
            started = time.monotonic()
            scratch = directory / 'scan'
            LOG.info('Scanning group %s', gid)
            try:
                shutil.rmtree(scratch, ignore_errors=True)
                scratch.mkdir()
                check_running(cancel)
                validate_layout(args, job)
                candidates, coverage = scan(args, job, scratch, cancel)
                check_running(cancel)
                validate_layout(args, job)
                candidates_complete = has_two_complete_realms(coverage['completed_vdisks'])
                result = dict(candidates={category: sorted(candidates[category]) if candidates_complete else []
                                          for category in CATEGORIES},
                              candidates_complete=candidates_complete, **coverage)
                LOG.info('Group %s: %d observed blobs, candidate counts %s', gid,
                         coverage['scan_report']['totals']['blobs'],
                         {category: len(blobs) for category, blobs in result['candidates'].items()}
                         if candidates_complete else 'not evaluated')
            except Exception as exc:
                log_failure('Group %s failed', exc, gid)
                result = dict(getattr(exc, 'coverage', {}), error=str(exc))
            finally:
                finished.set()
                thread.join()
            result['seconds'] = round(time.monotonic() - started, 2)
            atomic_json(pending, dict(group=gid, token=lease, result=result))
        except Exception as exc:
            log_failure('Worker operation failed; retrying in %ss', exc, args.poll)
            STOP.wait(args.poll)


def positive(value):
    value = int(value)
    if value <= 0:
        raise argparse.ArgumentTypeError('Must be positive')
    return value
