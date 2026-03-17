"""Tools for specifying BSON codec options."""

import collections
from distutils import version  # pylint: disable=no-name-in-module

try:
    from bson import codec_options
    import pymongo
    _PYMONGO_VERSION = version.LooseVersion(pymongo.version)
except ImportError:
    codec_options = None
    _PYMONGO_VERSION = version.LooseVersion('0.0')


class TypeRegistry(object):
    pass


_FIELDS = (
    'document_class', 'tz_aware', 'uuid_representation', 'unicode_decode_error_handler', 'tzinfo',
)

if _PYMONGO_VERSION >= version.LooseVersion('3.8'):
    _DEFAULT_TYPE_REGISTRY = codec_options.TypeRegistry()
    _FIELDS = _FIELDS + ('type_registry',)
else:
    _DEFAULT_TYPE_REGISTRY = TypeRegistry()


class CodecOptions(collections.namedtuple('CodecOptions', _FIELDS)):

    def __new__(cls, document_class=dict,
                tz_aware=False,
                uuid_representation=None,
                unicode_decode_error_handler='strict',
                tzinfo=None, type_registry=None):

        if document_class != dict:
            raise NotImplementedError(
                'Mongomock does not implement custom document_class yet: %r' % document_class)

        if not isinstance(tz_aware, bool):
            raise TypeError('tz_aware must be True or False')

        if uuid_representation is None:
            uuid_representation = 3
        if uuid_representation != 3:
            raise NotImplementedError('Mongomock does not handle custom uuid_representation yet')

        if unicode_decode_error_handler not in ('strict', None):
            raise NotImplementedError(
                'Mongomock does not handle custom unicode_decode_error_handler yet')

        if tzinfo:
            raise NotImplementedError('Mongomock does not handle custom tzinfo yet')

        values = (
            document_class, tz_aware, uuid_representation, unicode_decode_error_handler, tzinfo)

        if 'type_registry' in _FIELDS:
            if not type_registry:
                type_registry = _DEFAULT_TYPE_REGISTRY
            elif not type_registry == _DEFAULT_TYPE_REGISTRY:
                raise NotImplementedError(
                    'Mongomock does not handle custom type_registry yet %r' % type_registry)
            values = values + (type_registry,)

        return tuple.__new__(cls, values)

    def with_options(self, **kwargs):
        opts = self._asdict()
        opts.update(kwargs)
        return CodecOptions(**opts)


def is_supported(custom_codec_options):

    if not custom_codec_options:
        return None

    return CodecOptions(**custom_codec_options._asdict())
