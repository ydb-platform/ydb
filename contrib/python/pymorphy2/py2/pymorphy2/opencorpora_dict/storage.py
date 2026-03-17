# -*- coding: utf-8 -*-
"""
:mod:`pymorphy2.opencorpora_dict.storage` is a
module for saving and loading pymorphy2 dictionaries.
"""
from __future__ import absolute_import, unicode_literals
import datetime
import os
import logging
import collections
import io
import itertools
import array
import struct
import sys

try:
    izip = itertools.izip
except AttributeError:
    izip = zip

import pymorphy2
from pymorphy2 import tagset
from pymorphy2 import dawg
from pymorphy2.utils import json_write, json_read

from library.python.resource import resfs_read

logger = logging.getLogger(__name__)

CURRENT_FORMAT_VERSION = '2.4'

LoadedDictionary = collections.namedtuple('LoadedDictionary', [
    'meta',
    'gramtab',
    'suffixes',
    'paradigms',
    'words',
    'prediction_suffixes_dawgs',
    'Tag',
    'paradigm_prefixes',
])


def load_dict(path, gramtab_format='opencorpora-int'):
    """
    Load pymorphy2 dictionary.
    ``path`` is a folder name with dictionary data.
    """

    _f = lambda p: os.path.join(path, p)

    meta = load_meta(_f('meta.json'))
    _assert_format_is_compatible(meta, path)

    Tag = _load_tag_class(gramtab_format, _f('grammemes.json'))

    str_gramtab = _load_gramtab(meta, gramtab_format, path)
    gramtab = [Tag(tag_str) for tag_str in str_gramtab]

    suffixes = json_read(_f('suffixes.json'))
    paradigms = _load_paradigms(_f('paradigms.array'))
    words = dawg.WordsDawg().load(_f('words.dawg'))

    try:
        paradigm_prefixes = meta["compile_options"]["paradigm_prefixes"]
    except KeyError:
        # support dicts v2.4
        paradigm_prefixes = json_read(_f('paradigm-prefixes.json'))

    prediction_suffixes_dawgs = []
    for prefix_id in range(len(paradigm_prefixes)):
        fn = _f('prediction-suffixes-%s.dawg' % prefix_id)
        #assert os.path.exists(fn)
        prediction_suffixes_dawgs.append(dawg.PredictionSuffixesDAWG().load(fn))

    return LoadedDictionary(
        meta=meta,
        gramtab=gramtab,
        suffixes=suffixes,
        paradigms=paradigms,
        words=words,
        prediction_suffixes_dawgs=prediction_suffixes_dawgs,
        Tag=Tag,
        paradigm_prefixes=paradigm_prefixes,
    )


def save_compiled_dict(compiled_dict, out_path, source_name, language_code):
    """
    Save a compiled_dict to ``out_path``
    ``out_path`` should be a name of folder where to put dictionaries.
    """
    logger.info("Saving...")
    _f = lambda path: os.path.join(out_path, path)

    json_write(_f('grammemes.json'), compiled_dict.parsed_dict.grammemes)

    gramtab_formats = {}
    for format, Tag in tagset.registry.items():
        Tag._init_grammemes(compiled_dict.parsed_dict.grammemes)
        new_gramtab = [Tag._from_internal_tag(tag) for tag in compiled_dict.gramtab]

        gramtab_name = "gramtab-%s.json" % format
        gramtab_formats[format] = gramtab_name

        json_write(_f(gramtab_name), new_gramtab)

    with open(_f('paradigms.array'), 'wb') as f:
        f.write(struct.pack(str("<H"), len(compiled_dict.paradigms)))
        for para in compiled_dict.paradigms:
            f.write(struct.pack(str("<H"), len(para)))
            para.tofile(f)

    json_write(_f('suffixes.json'), compiled_dict.suffixes)
    compiled_dict.words_dawg.save(_f('words.dawg'))

    for prefix_id, prediction_suffixes_dawg in enumerate(compiled_dict.prediction_suffixes_dawgs):
        prediction_suffixes_dawg.save(_f('prediction-suffixes-%s.dawg' % prefix_id))

    logger.debug("computing metadata..")

    def _dawg_len(dawg):
        return sum(1 for k in dawg.iterkeys())

    logger.debug('  words_dawg_len')
    words_dawg_len = _dawg_len(compiled_dict.words_dawg)
    logger.debug('  prediction_suffixes_dawgs_len')

    prediction_suffixes_dawg_lenghts = []
    for prediction_suffixes_dawg in compiled_dict.prediction_suffixes_dawgs:
        prediction_suffixes_dawg_lenghts.append(_dawg_len(prediction_suffixes_dawg))

    write_meta(_f('meta.json'), [
        ['language_code', language_code],
        ['format_version', CURRENT_FORMAT_VERSION],
        ['pymorphy2_version', pymorphy2.__version__],
        ['compiled_at', datetime.datetime.utcnow().isoformat()],

        ['source', source_name],
        ['source_version', compiled_dict.parsed_dict.version],
        ['source_revision', compiled_dict.parsed_dict.revision],
        ['source_lexemes_count', len(compiled_dict.parsed_dict.lexemes)],
        ['source_links_count', len(compiled_dict.parsed_dict.links)],

        ['gramtab_length', len(compiled_dict.gramtab)],
        ['gramtab_formats', gramtab_formats],
        ['paradigms_length', len(compiled_dict.paradigms)],
        ['suffixes_length', len(compiled_dict.suffixes)],

        ['words_dawg_length', words_dawg_len],
        ['compile_options', compiled_dict.compile_options],
        ['prediction_suffixes_dawg_lengths', prediction_suffixes_dawg_lenghts],
    ])


def load_meta(filename):
    """ Load metadata. """
    meta = json_read(filename, parse_float=str)
    if hasattr(collections, 'OrderedDict'):
        return collections.OrderedDict(meta)
    return dict(meta)


def write_meta(filename, meta):
    """ Save metadata to a file. """
    if isinstance(meta, dict):
        meta = list(meta.items())
    json_write(filename, meta)


def update_meta(filename, extra):
    """ Update meta with extra fields """
    meta = load_meta(filename)
    meta.update(extra)
    write_meta(filename, meta)


def _load_tag_class(gramtab_format, grammemes_filename):
    """ Load and initialize Tag class (according to ``gramtab_format``). """
    if gramtab_format not in tagset.registry:
        raise ValueError("This gramtab format ('%s') is unsupported." % gramtab_format)

    # FIXME: clone the class
    Tag = tagset.registry[gramtab_format] #._clone_class()

    grammemes = json_read(grammemes_filename)
    Tag._init_grammemes(grammemes)

    return Tag


def _load_gramtab(meta, gramtab_format, path):
    """ Load gramtab (a list of tags) """
    gramtab_formats = meta.get('gramtab_formats', {})
    if gramtab_format not in gramtab_formats:
        raise ValueError("This gramtab format (%s) is unavailable; available formats: %s" % (gramtab_format, gramtab_formats.keys()))

    gramtab_filename = os.path.join(path, gramtab_formats[gramtab_format])
    return json_read(gramtab_filename)


def _load_paradigms(filename):
    """ Load paradigms data """
    paradigms = []
    data = resfs_read(filename)
    if data:
        f = io.BytesIO(data)
    else:
        f = open(filename, 'rb')

    paradigms_count = struct.unpack(str("<H"), f.read(2))[0]

    for x in range(paradigms_count):
        paradigm_len = struct.unpack(str("<H"), f.read(2))[0]

        para = array.array(str("H"))
        if sys.version_info > (3, 8):
            para.frombytes(f.read(paradigm_len * 2))
        else:
            para.fromstring(f.read(paradigm_len * 2))

        paradigms.append(para)

    return paradigms


def _assert_format_is_compatible(meta, path):
    """ Raise an exception if dictionary format is not compatible """
    format_version = str(meta.get('format_version', '0.0'))

    if '.' not in format_version:
        raise ValueError('Invalid format_version: %s' % format_version)

    major, minor = format_version.split('.')
    curr_major, curr_minor = CURRENT_FORMAT_VERSION.split('.')

    if major != curr_major:
        msg = ("Error loading dictionaries from %s: "
               "the format ('%s') is not supported; "
               "required format is '%s.x'.") % (path, format_version, curr_major)
        raise ValueError(msg)

