import logging
import os

from library.python import resource

from . import warning_filter, write_file


_RESOURCE_KEY_PREFIX = 'resfs/file/'

log = logging.getLogger(__name__)
log.addFilter(warning_filter)


def entry_point_prefix(entry_point):
    return package_prefix(entry_point.value)


def package_prefix(package_name):
    p = package_name.replace('.', '/') + '/'
    log.debug('resource_prefix for %s is %s', package_name, p)
    return p


def load_content(*key):
    key = _parts(*key)
    return resource.find(key)


def find_keys_by_prefix(*prefix):
    prefix = resource_key(*prefix) + '/'
    log.debug('get_keys for %s', prefix)
    return list([k for k in resource.iterkeys() if k.startswith(prefix)])


def resource_key(*parts):
    if len(parts) == 1:
        p = parts[0]
        return _RESOURCE_KEY_PREFIX + p if not p.startswith(_RESOURCE_KEY_PREFIX) else p
    else:
        return _parts(_RESOURCE_KEY_PREFIX, *parts) if parts[0] != _RESOURCE_KEY_PREFIX else _parts(parts)


def unpack_resource_files(dest_dir, keep_layout=True, *prefix):
    log.debug('unpack resource files by prefix %s to dir %s with source layout(%s)', prefix, dest_dir, keep_layout)
    prefix = resource_key(*prefix)
    files, errors = [], []
    for key in find_keys_by_prefix(prefix):
        rel_filename = key.replace(prefix, '').lstrip('/') if keep_layout else key.split('/')[-1]

        if unpack_resource_file(os.path.join(dest_dir, rel_filename), key):
            files.append(rel_filename)
        else:
            errors.append(rel_filename)

    return files, errors


def unpack_resource_file(dest, *key):
    log.debug('unpack resource file by key %s to %s', key, dest)
    content = load_content(*key)
    if content is None:
        log.warn('Resource file with key %s required, but its content is None', key)
        return None

    write_file(content, dest)
    return dest


def _parts(*parts):
    return parts[0] if len(parts) == 1 else '/'.join(p.strip('/') for p in parts)
