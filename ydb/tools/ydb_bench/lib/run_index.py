"""Rebuildable SQLite read index; manifests and artifacts remain authoritative."""

import json
import sqlite3
import threading
import uuid
from contextlib import closing, contextmanager
from datetime import datetime
from pathlib import Path

from ydb.tools.ydb_bench.lib.common import BenchmarkError
from ydb.tools.ydb_bench.lib.results import load_manifest

FIELDS = (
    'id',
    'status',
    'state',
    'source',
    'queued_at',
    'started_at',
    'finished_at',
    'duration_seconds',
    'profiles',
    'repetitions',
    'perf',
    'config_path',
    'output_directory',
    'benchmarks',
    'profile_names',
    'deployment',
)


def order_value(record, order):
    if order == 'longest':
        return -float(record.get('duration_seconds') or 0)
    try:
        value = datetime.fromisoformat(
            (record.get('started_at') or record.get('queued_at') or '').replace('Z', '+00:00')
        ).timestamp()
    except (ValueError, OverflowError):
        value = 0
    return value if order == 'oldest' else -value


class _InvalidIndex(Exception):
    pass


class RunIndex:
    def __init__(self, root, project, interval=5):
        self.root = Path(root).resolve()
        self.path = self.root / '.run-index.sqlite3'
        self.project = project
        self.lock = threading.RLock()
        self.pending_lock = threading.Lock()
        self.pending = set()
        self.stop = threading.Event()
        self.error = None
        self.interval = interval
        if self.path.is_symlink():
            raise BenchmarkError('run index must not be a symbolic link')
        try:
            self._initialize()
        except (sqlite3.DatabaseError, _InvalidIndex) as error:
            if not isinstance(error, _InvalidIndex) and getattr(error, 'sqlite_errorcode', None) not in (
                sqlite3.SQLITE_CORRUPT,
                sqlite3.SQLITE_NOTADB,
            ):
                raise
            suffix = '.invalid-' + uuid.uuid4().hex
            for ending in ('', '-wal', '-shm'):
                source = Path(str(self.path) + ending)
                if source.exists():
                    source.rename(str(source) + suffix)
            self._initialize()
        self.reconcile()
        self.thread = threading.Thread(target=self._watch, name='benchmark-run-index', daemon=True)
        self.thread.start()

    def _connect(self):
        return sqlite3.connect(self.path, timeout=10)

    @contextmanager
    def _database(self):
        with closing(self._connect()) as db:
            with db:
                yield db

    def _initialize(self):
        with self._database() as db:
            if db.execute('PRAGMA quick_check').fetchone()[0] != 'ok':
                raise _InvalidIndex('run index integrity check failed')
            db.execute('PRAGMA journal_mode=WAL')
            db.execute(
                'CREATE TABLE IF NOT EXISTS runs (id TEXT PRIMARY KEY, signature TEXT NOT NULL, '
                'status TEXT, source TEXT, started TEXT, newest REAL, oldest REAL, longest REAL, record TEXT NOT NULL)'
            )
            db.execute('CREATE TABLE IF NOT EXISTS names (id TEXT, kind TEXT, name TEXT, PRIMARY KEY(id,kind,name))')
            db.execute('CREATE INDEX IF NOT EXISTS names_lookup ON names(kind,name,id)')
            for column in ('newest', 'oldest', 'longest', 'status', 'source', 'started'):
                db.execute('CREATE INDEX IF NOT EXISTS runs_' + column + ' ON runs(' + column + ',id)')

    def _watch(self):
        while not self.stop.wait(self.interval):
            try:
                self.reconcile()
                self.refresh_pending()
                self.error = None
            except (OSError, sqlite3.Error, BenchmarkError) as error:
                self.error = str(error)

    def close(self):
        self.stop.set()
        self.thread.join()

    def reconcile(self):
        # Directory traversal is outside the query lock. Only changed manifests are parsed.
        paths = list(self.root.rglob('run.json'))
        self.refresh(paths, prune=True)

    def mark_dirty(self, path):
        # Called after a successful manifest write; never perform SQLite I/O in the writer.
        with self.pending_lock:
            self.pending.add(path)

    def refresh_pending(self):
        with self.pending_lock:
            paths, self.pending = self.pending, set()
        try:
            self.refresh(paths)
        except Exception:
            with self.pending_lock:
                self.pending.update(paths)
            raise

    def refresh(self, paths, prune=False):
        if not paths and not prune:
            return
        with self.lock, self._database() as db:
            seen = set()
            for path in paths:
                path = Path(path)
                run_id = str(path.parent.relative_to(self.root))
                seen.add(run_id)
                try:
                    stat = path.stat()
                    signature = json.dumps(
                        [
                            stat.st_ino,
                            stat.st_mtime_ns,
                            stat.st_size,
                            (path.parent / '.imported').exists(),
                            (path.parent / 'config.yaml').exists(),
                        ]
                    )
                    old = db.execute('SELECT signature FROM runs WHERE id=?', (run_id,)).fetchone()
                    if old and old[0] == signature:
                        continue
                    manifest = load_manifest(path)
                    if 'topology' not in manifest and 'steps' not in manifest:
                        raise BenchmarkError('not a run manifest')
                    value = self.project(run_id, manifest, self.root)
                    record = {key: value.get(key) for key in FIELDS}
                except (OSError, BenchmarkError):
                    db.execute('DELETE FROM runs WHERE id=?', (run_id,))
                    db.execute('DELETE FROM names WHERE id=?', (run_id,))
                    continue
                db.execute(
                    'INSERT OR REPLACE INTO runs VALUES (?,?,?,?,?,?,?,?,?)',
                    (
                        run_id,
                        signature,
                        record['status'],
                        record['source'],
                        record['started_at'] or '',
                        order_value(record, 'newest'),
                        order_value(record, 'oldest'),
                        order_value(record, 'longest'),
                        json.dumps(record, allow_nan=False),
                    ),
                )
                db.execute('DELETE FROM names WHERE id=?', (run_id,))
                db.executemany(
                    'INSERT INTO names VALUES (?,?,?)',
                    [(run_id, kind, name) for kind in ('benchmarks', 'profile_names') for name in record[kind]],
                )
            if prune:
                for (run_id,) in db.execute('SELECT id FROM runs').fetchall():
                    if run_id not in seen:
                        db.execute('DELETE FROM runs WHERE id=?', (run_id,))
                        db.execute('DELETE FROM names WHERE id=?', (run_id,))

    def query(self, filters, order='newest', limit=None, after=None, host_id=''):
        if order not in ('newest', 'oldest', 'longest'):
            raise BenchmarkError('invalid run order')
        clauses, args = [], []
        if 'selected' in filters:
            ids = [value[len(host_id) + 1 :] for value in filters['selected'] if value.startswith(host_id + ':')]
            if not ids:
                return []
            clauses.append('id IN (' + ','.join('?' for _ in ids) + ')')
            args.extend(ids)
        for key in ('status', 'source'):
            if filters.get(key):
                clauses.append(key + '=?')
                args.append(filters[key])
        for key, kind in (('benchmark', 'benchmarks'), ('profile', 'profile_names')):
            if filters.get(key):
                clauses.append('id IN (SELECT id FROM names WHERE kind=? AND name=?)')
                args.extend((kind, filters[key]))
        for key, comparison in (('since', '>='), ('until', '<=')):
            if filters.get(key):
                clauses.append('substr(started,1,10)' + comparison + '?')
                args.append(filters[key])
        if filters.get('query'):
            clauses.append(
                '(instr(lower(id),lower(?))>0 OR id IN ' '(SELECT id FROM names WHERE instr(lower(name),lower(?))>0))'
            )
            args.extend((filters['query'], filters['query']))
        if after is not None:
            clauses.append('(' + order + ',?,id)>(?,?,?)')
            args.extend((host_id, *after))
        sql = 'SELECT record FROM runs' + (' WHERE ' + ' AND '.join(clauses) if clauses else '')
        sql += ' ORDER BY ' + order + ',id'
        if limit is not None:
            sql += ' LIMIT ?'
            args.append(limit)
        with closing(self._connect()) as db:
            return [json.loads(row[0]) for row in db.execute(sql, args)]

    def facets(self):
        with closing(self._connect()) as db:
            return {
                kind: [
                    row[0] for row in db.execute('SELECT DISTINCT name FROM names WHERE kind=? ORDER BY name', (kind,))
                ]
                for kind in ('benchmarks',)
            }
