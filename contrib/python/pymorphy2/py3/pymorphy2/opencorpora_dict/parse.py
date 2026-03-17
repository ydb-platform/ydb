# -*- coding: utf-8 -*-
"""
:mod:`pymorphy2.opencorpora_dict.parse` is a
module for OpenCorpora XML dictionaries parsing.
"""
from __future__ import absolute_import, unicode_literals, division

import logging
import collections

try:
    from lxml.etree import iterparse

    def xml_clear_elem(elem):
        elem.clear()
        while elem.getprevious() is not None:
            del elem.getparent()[0]

except ImportError:
    try:
        from xml.etree.cElementTree import iterparse
    except ImportError:
        from xml.etree.ElementTree import iterparse

    def xml_clear_elem(elem):
        elem.clear()

from pymorphy2.utils import with_progress


logger = logging.getLogger(__name__)

ParsedDictionary = collections.namedtuple('ParsedDictionary', 'lexemes links grammemes version revision')


def get_dictionary_info(filename, elem_limit=1000):
    """ Return dictionary version and revision """
    for idx, (ev, elem) in enumerate(iterparse(filename, events=(str('start'),))):
        if elem.tag == 'dictionary':
            version = elem.get('version')
            revision = elem.get('revision')
            return version, revision
        if idx > elem_limit:
            return None, None
    return None, None


def parse_opencorpora_xml(filename):
    """
    Parse OpenCorpora dict XML and return a ``ParsedDictionary`` namedtuple.
    """

    links = []
    lexemes = {}
    grammemes = []

    version, revision = get_dictionary_info(filename)
    logger.info("dictionary v%s, rev%s", version, revision)
    interesting_tags = set(['grammeme', 'lemma', 'link'])

    def _parse(filename):
        for ev, elem in iterparse(filename):
            if elem.tag not in interesting_tags:
                continue
            yield ev, elem

    logger.info("parsing XML dictionary")

    for ev, elem in with_progress(_parse(filename), "XML parsing"):
        if elem.tag == 'grammeme':
            name = elem.find('name').text
            parent = elem.get('parent')
            alias = elem.find('alias').text
            description = elem.find('description').text

            grammeme = (name, parent, alias, description)
            grammemes.append(grammeme)
            xml_clear_elem(elem)

        if elem.tag == 'lemma':
            lex_id, word_forms = _word_forms_from_xml_elem(elem)
            lexemes[lex_id] = word_forms
            xml_clear_elem(elem)

        elif elem.tag == 'link':
            link_tuple = (
                elem.get('from'),
                elem.get('to'),
                elem.get('type'),
            )
            links.append(link_tuple)
            xml_clear_elem(elem)

    return ParsedDictionary(
        lexemes=lexemes,
        links=links,
        grammemes=grammemes,
        version=version,
        revision=revision
    )


def _grammemes_from_elem(elem):
    return ",".join([g.get('v') for g in elem.iter('g')])


def _word_forms_from_xml_elem(elem):
    """
    Return a list of (word, tag) pairs given "lemma" XML element.
    """
    lexeme = []
    lex_id = elem.get('id')

    if len(elem) == 0:  # deleted lexeme?
        return lex_id, lexeme

    base_info = list(elem.iter('l'))

    assert len(base_info) == 1
    base_grammemes = _grammemes_from_elem(base_info[0])

    for form_elem in elem.iter('f'):
        grammemes = _grammemes_from_elem(form_elem)
        form = form_elem.get('t').lower()
        if not (base_grammemes + grammemes):
            logger.warning("no information provided for word %s, dropping the whole lexeme" % form)
            return lex_id, []
        if isinstance(form, bytes):  # Python 2.x
            form = form.decode('ascii')
        lexeme.append(
            (form, (base_grammemes + " " + grammemes).strip())
        )

    return lex_id, lexeme
