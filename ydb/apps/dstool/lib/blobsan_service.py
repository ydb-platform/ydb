"""Coordinator and worker runtime for blobsan-service (standard library)."""

import argparse
import collections
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


LOG = logging.getLogger('blobsan-service')
STOP = threading.Event()
MAX_BODY = 128 * 1024 * 1024
CANDIDATE = re.compile(r'GroupId#\s+(\d+)\s+BlobId#\s+(\[(?:\d+:){6}\d+\])')


def encode(value):
    return json.dumps(value, separators=(',', ':')).encode()


def has_two_complete_realms(indexes):
    return sum(all(r * 3 + d in indexes for d in range(3)) for r in range(3)) >= 2


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
    params = common.connection_params
    context = ssl.create_default_context(cafile=params.cafile)
    if params.insecure:
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
    return http(url, token_headers(), timeout=args.request_timeout, raw=raw, context=context)


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
            CREATE TABLE IF NOT EXISTS scan_reports(
                gid INTEGER PRIMARY KEY, generation INTEGER, fingerprint TEXT,
                started REAL, finished REAL, result TEXT);
        ''')
        for key, value in [('paused', 0), ('interval', args.interval), ('retry', args.retry)]:
            self.db.execute('INSERT OR IGNORE INTO settings VALUES(?,?)', (key, str(value)))
        self.db.commit()

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
                    self.db.execute('UPDATE groups SET token=?,owner=?,expires=? WHERE id=?',
                                    (lease, worker, now + self.args.lease, row['id']))
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
            error = result.get('error')
            if not error:
                findings = result['findings']
                if not isinstance(findings, dict) or any(
                        not re.fullmatch(r'\[(?:\d+:){6}0\]', b) or not isinstance(s, str) for b, s in findings.items()):
                    raise ValueError('Invalid findings')
                completed = result['completed_vdisks']
                if not isinstance(completed, list) or any(type(i) is not int or i not in range(9) for i in completed) or len(set(completed)) != len(completed):
                    raise ValueError('Invalid coverage')
                report = result.get('scan_report')
                if report is not None:
                    write2x2_report.validate(report, gid, completed)
                    started, finished = result['started_at'], result['finished_at']
                    if any(type(t) not in (int, float) or not math.isfinite(t) for t in (started, finished)) or started > finished:
                        raise ValueError('Invalid scan timestamps')
                findings_complete = result.get('findings_complete', True)
                if type(findings_complete) is not bool or (not findings_complete and (report is None or findings)):
                    raise ValueError('Invalid candidate scan status')
                if not completed or (findings_complete and not has_two_complete_realms(completed)):
                    raise ValueError('Incomplete scan')
                previous = {r['blob']: r['coverage'] for r in self.db.execute(
                    'SELECT blob,coverage FROM findings WHERE gid=? AND active=1', (gid,))}
                resolved = {blob for blob, coverage in previous.items() if findings_complete and blob not in findings and
                            (len(completed) == 9 or set(completed) == set(json.loads(coverage)))}
                self.db.executemany('UPDATE findings SET active=0 WHERE gid=? AND blob=?',
                                    ((gid, blob) for blob in resolved))
                coverage = json.dumps(sorted(completed))
                new = 0
                for blob, status in findings.items():
                    new += blob not in previous
                    self.db.execute('''INSERT INTO findings(gid,blob,first_seen,last_seen,active,status,coverage)
                        VALUES(?,?,?,?,1,?,?) ON CONFLICT(gid,blob) DO UPDATE SET
                        last_seen=excluded.last_seen,active=1,status=excluded.status,coverage=excluded.coverage''',
                                    (gid, blob, now, now, status, coverage))
                unresolved = len((previous.keys() - resolved) | findings.keys())
                result = {k: v for k, v in result.items() if k != 'findings'}
                result.update(candidates=len(findings) if findings_complete else None, new=new,
                              disappeared=len(resolved), unresolved=unresolved)
                self.db.execute('''UPDATE groups SET last_scan=?,last_count=?,scans=scans+1,
                    new_total=new_total+? WHERE id=?''', (now, result['candidates'], new, gid))
                if report is not None:
                    layout = json.loads(row['layout'])
                    self.db.execute('''INSERT OR REPLACE INTO scan_reports
                        VALUES(?,?,?,?,?,?)''', (gid, layout['generation'], layout['fingerprint'],
                                                 started, finished, json.dumps(result)))
                delay = self.setting('retry' if unresolved or len(completed) < 9 else 'interval')
                if new:
                    LOG.warning('Group %s: %d new/reappeared candidates, %d total', gid, new, len(findings))
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
                stats['unresolved_findings'] = self.db.execute('SELECT count(*) FROM findings WHERE active=1').fetchone()[0]
                stats['scans_with_missing_vdisks'] = sum(
                    bool(json.loads(r[0]).get('missing_vdisks'))
                    for r in self.db.execute('SELECT last_result FROM groups WHERE last_result IS NOT NULL'))
                if stats['oldest_scan'] is not None:
                    stats['oldest_scan_age_seconds'] = round(now - stats.pop('oldest_scan'))
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
                return 'stats | groups [brief] | layouts GROUP | pools [POOL] | workers | findings [GROUP] (max 1000) | pause | resume | scan [GROUP|all] | interval SEC | retry SEC | refresh | quit'
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
    """The supplied executable alone decides which blobs need repair."""
    check_running(cancel)
    report = directory / 'blobsan.err'
    manifest_path = (directory / 'manifest.json').resolve()
    manifest = json.loads(manifest_path.read_text())
    statistics_path = (directory / 'report.json').resolve()
    statistics_path.unlink(missing_ok=True)
    command = [args.blobsan, '--write2x2-candidates', '--scan-manifest', str(manifest_path),
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
    candidates = set()
    with report.open(errors='replace') as inp:
        for line in inp:
            if 'GroupId#' not in line:
                continue
            match = CANDIDATE.search(line)
            if not match or int(match[1]) != gid or match[2].split(':')[-1] != '0]':
                raise ValueError('Unrecognized blobsan candidate: ' + line[:200])
            candidates.add(match[2])
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


def repair(args, job, candidates, cancel):
    layout, base = job['layout'], viewer_url(args)
    if not candidates:
        return {}
    tenants = ydb_http(args, base + '/viewer/json/tenantinfo').get('TenantInfo', [])
    systems = ydb_http(args, base + '/viewer/json/sysinfo').get('SystemStateInfo', [])
    pool = layout.get('pool') or 'static'
    tenant = pool.rpartition(':')[0] if ':' in pool else None
    nodes = {int(n) for t in tenants if t['Name'] == tenant for n in t.get('NodeIds', [])}
    nodes.update(int(s['NodeId']) for s in systems if tenant and tenant in s.get('Tenants', []))
    if not nodes:
        nodes = {int(s['NodeId']) for s in systems if not s.get('Tenants')}
    if not nodes:
        raise ValueError('No proxy nodes available for group')
    nodes = sorted(nodes)
    results = {}
    for i, blob in enumerate(sorted(candidates)):
        check_running(cancel)
        params = dict(groupId=layout['id'], tabletId=blob[1:].split(':')[0],
                      mustRestoreFirst=1, json=1, **{'from': blob, 'to': blob})
        try:
            response = ydb_http(args, base + '/node/%d/blob_range?' % nodes[i % len(nodes)] + urllib.parse.urlencode(params))
            if response.get('Status') != 'OK' or response.get('Blobs') not in ([], [blob]):
                raise ValueError('Unexpected response: ' + str(response)[:300])
            results[blob] = 'request_ok' if response['Blobs'] else 'absent'
        except Exception as exc:
            results[blob] = 'failed: ' + str(exc)[:300]
        if args.repair_delay:
            cancel.wait(args.repair_delay)
    return results


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
                        # No new writes while ownership cannot be confirmed.
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
                validate_layout(args, job)
                findings_complete = not args.scan_only and has_two_complete_realms(coverage['completed_vdisks'])
                try:
                    findings = repair(args, job, candidates, cancel) if findings_complete else {}
                except Exception as exc:
                    check_running(cancel)
                    findings = {blob: 'failed: ' + str(exc)[:300] for blob in candidates}
                check_running(cancel)
                validate_layout(args, job)
                result = dict(findings=findings, findings_complete=findings_complete, **coverage)
                result['repairs'] = dict(collections.Counter(s.split(':')[0] for s in findings.values()))
                LOG.info('Group %s: %d observed blobs, candidates %s, repairs %s', gid,
                         coverage['scan_report']['totals']['blobs'], len(candidates) if findings_complete else 'not evaluated',
                         result['repairs'])
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
