from __future__ import absolute_import
import glob
import os
import logging
import sys
import time
import six

import app
import core.yarg
import core.common_opts
import core.config as cc
import build.ya_make as ym
from exts import fs
from core.common_opts import CustomBuildRootOptions
from build.build_opts import LocalCacheOptions, DistCacheSetupOptions, parse_size_arg, parse_timespan_arg
from exts.windows import on_win
from yalibrary.runner import result_store
import yalibrary.toolscache as tc

if six.PY3:
    long = int

logger = logging.getLogger(__name__)


def _to_size_in_gb(size):
    try:
        return int(float(size) * 1024 * 1024 * 1024)
    except ValueError:
        pass

    return parse_size_arg(size)


def _to_size_in_mb(size):
    try:
        return int(float(size) * 1024 * 1024)
    except ValueError:
        pass

    return parse_size_arg(size)


def _to_timespan_in_hours(timespan):
    try:
        return int(float(timespan) * 60 * 60)
    except ValueError:
        pass

    return parse_timespan_arg(timespan)


class CollectCacheOptions(LocalCacheOptions):
    def __init__(self):
        super(CollectCacheOptions, self).__init__()
        self.object_size_limit = None
        self.age_limit = None
        self.symlinks_ttl = 0

    def consumer(self):
        return super(CollectCacheOptions, self).consumer() + [
            core.yarg.ArgConsumer(
                ['--size-limit'],
                help='Strip build cache to size (in GiB if not set explicitly)',
                hook=core.yarg.SetValueHook(
                    'cache_size',
                    transform=_to_size_in_gb,
                    default_value=lambda x: str(_to_size_in_gb(x) * 1.0 / 1024 / 1024 / 1024),
                ),
                group=core.yarg.BULLET_PROOF_OPT_GROUP,
                visible=False,
            ),
            core.yarg.ArgConsumer(
                ['--object-size-limit'],
                help='Strip build cache from large objects (in MiB if not set explicitly)',
                hook=core.yarg.SetValueHook('object_size_limit', transform=_to_size_in_mb),
                group=core.yarg.BULLET_PROOF_OPT_GROUP,
            ),
            core.yarg.ArgConsumer(
                ['--age-limit'],
                help='Strip build cache from old objects (in hours if not set explicitly)',
                hook=core.yarg.SetValueHook('age_limit', transform=_to_timespan_in_hours),
                group=core.yarg.BULLET_PROOF_OPT_GROUP,
            ),
        ]


class GarbageCollectionYaHandler(core.yarg.CompositeHandler):
    def __init__(self):
        core.yarg.CompositeHandler.__init__(self, description='Collect garbage')
        self['cache'] = core.yarg.OptsHandler(
            action=app.execute(action=do_cache, respawn=app.RespawnType.OPTIONAL),
            description='Strip build cache and old build directories',
            opts=[core.common_opts.ShowHelpOptions(), CollectCacheOptions(), CustomBuildRootOptions()],
            visible=True,
        )
        self['dist_cache'] = core.yarg.OptsHandler(
            action=app.execute(action=do_strip_yt_cache, respawn=app.RespawnType.NONE),
            description='Strip distributed (YT) cache',
            opts=[core.common_opts.ShowHelpOptions(), DistCacheSetupOptions()],
            visible=False,
        )


class FilterBySize(object):
    def __init__(self, size_limit):
        self.size_limit = size_limit
        self.total_size = 0
        self._items = {}

    def __call__(self, item):
        if item.uid in self._items:
            return False
        size = item.size
        self.total_size += size
        self._items[item.uid] = item
        return self.total_size < self.size_limit


class FilterByObjectSize(object):
    def __init__(self, size_limit):
        self.size_limit = size_limit

    def __call__(self, item):
        return item.size < self.size_limit


class FilterByAge(object):
    def __init__(self, age_limit):
        self.age_limit = age_limit
        self.now = time.time()

    def __call__(self, item):
        return item.timestamp > self.now - self.age_limit


def _clean_dir_selectively(logs, file_dir, file_name):
    try:
        for i in os.listdir(logs):
            date_dir = os.path.join(logs, i)
            if date_dir != file_dir:
                fs.remove_tree_safe(date_dir)

        if file_dir:
            for i in os.listdir(file_dir):
                if i != file_name:
                    fs.remove_tree_safe(os.path.join(file_dir, i))
    except OSError as e:
        logger.debug('Cleaning %s root failed %s', logs, e)


def _clean_logs():
    logger.debug('Cleaning logs root')

    try:
        import app_ctx

        file_dir, file_name = os.path.split(app_ctx.file_log)
    except Exception as e:
        file_dir, file_name = ('', '')
        logger.debug('Log name was not obtained %s', e)

    logs = os.path.join(cc.misc_root(), 'logs')
    _clean_dir_selectively(logs, file_dir, file_name)


def _clean_evlogs():
    logger.debug('Cleaning evlogs root')

    try:
        import app_ctx

        file_dir, file_name = os.path.split(app_ctx.evlog._fileobj.name)
    except Exception as e:
        file_dir, file_name = ('', '')
        logger.debug('Log name was not obtained %s', e)

    evlogs = os.path.join(cc.misc_root(), 'evlogs')
    _clean_dir_selectively(evlogs, file_dir, file_name)


def _clean_tools():
    running_ya_bin_dir = os.path.dirname(sys.argv[0])
    base_running_ya_bin_dir = os.path.basename(running_ya_bin_dir).replace('_d', '')
    errors = 0
    for i in os.listdir(os.path.dirname(cc.tool_root())):
        full_path = os.path.join(os.path.dirname(cc.tool_root()), i)
        if i == 'v4':
            if len(glob.glob(os.path.join(full_path, '*.corrupted'))) == 0:
                tc.unlock_all()
                continue
            else:
                errors += 1
        elif full_path == running_ya_bin_dir:
            continue
        elif os.path.isfile(full_path):
            try:
                if os.path.getsize(full_path) < 1024 * 100:
                    with open(full_path, 'r') as f:
                        if base_running_ya_bin_dir in f.read():
                            continue
            except Exception:
                errors += 1
        logger.debug('Cleaning tool %s', full_path)
        fs.remove_tree_safe(full_path)
    return errors


def do_cache(opts):
    build_root = opts.custom_build_directory or cc.build_root()

    cache = None
    try:
        cache = ym.make_cache(opts, build_root)
    except Exception:
        logger.exception("While initializing cache")

    lock = ym.make_lock(opts, build_root, write_lock=True)

    with lock:
        if opts.cache_stat:
            import app_ctx

            if not cache:
                app_ctx.display.emit_message("Cache not initialized, can't show stats")
            else:
                cache.analyze(app_ctx.display)
            errors = 0
        else:
            errors = _do_collect_cache(cache, build_root, opts)

        if cache:
            cache.flush()

        logger.debug(
            "ya gc stats %s",
            {
                'cache_size': opts.cache_size,
                'object_size_limit': opts.object_size_limit,
                'age_limit': opts.age_limit,
                'symlinks_ttl': opts.symlinks_ttl,
                'errors': errors,
            },
        )


def _do_collect_cache(cache, build_root, opts):
    logger.debug('Cleaning tmp root')
    fs.remove_tree_safe(cc.tmp_path())

    logger.debug('Cleaning snowden root')
    fs.remove_tree_safe(os.path.join(cc.misc_root(), 'snowden'))

    logger.debug('Cleaning build root')
    fs.remove_tree_safe(os.path.join(build_root, 'build_root'))

    logger.debug('Cleaning conf root')
    fs.remove_tree_safe(os.path.join(build_root, 'conf'))

    errors = _clean_tools()

    logger.debug('Cleaning tmp root')
    fs.remove_tree_safe(os.path.join(cc.misc_root(), 'tmp'))

    _clean_logs()
    _clean_evlogs()

    if not on_win():
        logger.debug('Cleaning symres')
        symres_dir = os.path.join(build_root, 'symres')
        src_dir = cc.find_root(fail_on_error=False) or ""
        if os.path.isdir(symres_dir):
            logger.debug('Cleaning symres %s for %s, ttl=%s', symres_dir, src_dir, opts.symlinks_ttl)
            result_store.SymlinkResultStore(symres_dir, src_dir).sieve(
                state=None, ttl=opts.symlinks_ttl, cleanup=opts.symlinks_ttl == 0
            )

    if hasattr(cache, '_store_path'):
        for dir in os.listdir(os.path.join(build_root, 'cache')):
            full_path = os.path.join(build_root, 'cache', dir)
            if cache._store_path == full_path:
                if len(glob.glob(os.path.join(full_path, '*.corrupted'))) == 0:
                    continue
                else:
                    errors += 1
            logger.debug('Cleaning cache directory %s', full_path)
            fs.remove_tree_safe(full_path)

    if opts.cache_size is not None and opts.object_size_limit is None and opts.age_limit is None:
        logger.debug('Cleaning for total size %s', opts.cache_size)
        if hasattr(cache, 'strip'):
            cache.strip(FilterBySize(opts.cache_size))
        elif hasattr(cache, 'strip_total_size'):
            cache.strip_total_size(opts.cache_size)

        from yalibrary.toolscache import tc_force_gc

        tc_force_gc(opts.cache_size)

    if opts.object_size_limit is not None:
        logger.debug('Cleaning for object size %s', opts.object_size_limit)
        if hasattr(cache, 'strip'):
            cache.strip(FilterByObjectSize(opts.object_size_limit))
        elif hasattr(cache, 'strip_max_object_size'):
            cache.strip_max_object_size(opts.object_size_limit)

    if opts.age_limit is not None:
        logger.debug('Cleaning for age %s', opts.age_limit)
        if hasattr(cache, 'strip'):
            cache.strip(FilterByAge(opts.age_limit))
        elif hasattr(cache, 'strip_max_age'):
            cache.strip_max_age(opts.age_limit)

    return errors


def do_strip_yt_cache(opts):
    try:
        from yalibrary.store.yt_store import yt_store
    except ImportError as e:
        logger.warn("YT store is not available: %s", e)

    token = opts.yt_token or opts.oauth_token
    cache = yt_store.YtStore(
        opts.yt_proxy,
        opts.yt_dir,
        None,
        token=token,
        readonly=opts.yt_readonly,
        max_cache_size=opts.yt_max_cache_size,
        ttl=opts.yt_store_ttl,
    )

    counters = cache.strip()
    if counters:
        logger.info(
            'Deleted: meta rows:%d, data rows:%d, net data size:%d',
            counters['meta_rows'],
            counters['data_rows'],
            counters['data_size'],
        )
