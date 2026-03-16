# -*- coding: utf-8 -*-
"""
Webstruct supports WebAnnotator_ and GATE_ annotation formats out of box;
WebAnnotator_ is recommended.

Both GATE and WebAnnotator embed annotations into HTML using special tags:
GATE uses custom tags like ``<ORG>`` while WebAnnotator uses tags like
``<span wa-type="ORG">``.

:mod:`webstruct.loaders` classes convert GATE and WebAnnotator tags into
``__START_TAGNAME__`` and ``__END_TAGNAME__`` tokens, clean the HTML
and return the result as a tree parsed by lxml::

    >>> from webstruct import WebAnnotatorLoader  # doctest: +SKIP
    >>> loader = WebAnnotatorLoader()  # doctest: +SKIP
    >>> loader.load('0.html')  # doctest: +SKIP
    <Element html at ...>

Such trees can be processed with utilities from
:mod:`webstruct.feature_extraction`.

.. _WebAnnotator: https://github.com/xtannier/WebAnnotator
.. _GATE: http://gate.ac.uk/
"""
from __future__ import absolute_import
import re
import glob
from collections import defaultdict
import six

import lxml.html
import lxml_html_clean
from lxml.etree import ProcessingInstruction

from webstruct.utils import human_sorted, html_document_fromstring
from webstruct import webannotator


class HtmlLoader(object):
    """
    Class for loading unannotated HTML files.
    """
    def __init__(self, encoding=None, cleaner=None):
        self.encoding = encoding
        self.cleaner = cleaner or _get_default_cleaner()

    def load(self, filename):
        with open(filename, 'rb') as f:
            return self.loadbytes(f.read())

    def loadbytes(self, data):
        tree = html_document_fromstring(data, self.encoding)
        return self.cleaner.clean_html(tree)


class WebAnnotatorLoader(HtmlLoader):
    """
    Class for loading HTML annotated using
    `WebAnnotator <https://github.com/xtannier/WebAnnotator>`_.

    .. note::

        Use WebAnnotator's "save format", not "export format".

    """
    def __init__(self, encoding=None, cleaner=None, known_entities=None):
        self.known_entities = known_entities
        super(WebAnnotatorLoader, self).__init__(encoding, cleaner)

    def loadbytes(self, data):
        # defer cleaning the tree to prevent custom cleaners from cleaning
        # WebAnnotator markup
        tree = html_document_fromstring(data, encoding=self.encoding)
        webannotator.apply_wa_title(tree)
        if self.known_entities:
            self._prune_tags(tree)
        entities = self._get_entities(tree)
        self._process_entities(entities)
        return self._cleanup_tree(tree)

    def _prune_tags(self, tree):
        """remove the element with wa-type not in ``known_entities``"""
        for el in tree.xpath('//span[@wa-type]'):
            if el.attrib['wa-type'] not in self.known_entities:
                el.drop_tag()

    def _get_entities(self, tree):
        entities = defaultdict(list)
        for el in tree.xpath('//span[@wa-id]'):
            entities[el.attrib['wa-id']].append(el)
        return dict(entities)

    def _process_entities(self, entities):
        for _id, elems in entities.items():
            tp = elems[0].attrib['wa-type']
            elems[0].text = ' __START_%s__ %s' % (tp, elems[0].text or '')
            elems[-1].text = '%s __END_%s__ ' % (elems[-1].text or '', tp)
            for el in elems:
                el.drop_tag()

    def _cleanup_tree(self, tree):
        for el in tree.xpath('//wa-color'):
            el.drop_tree()
        return self.cleaner.clean_html(tree)


class GateLoader(HtmlLoader):
    """
    Class for loading HTML annotated using `GATE <http://gate.ac.uk/>`_

    >>> import lxml.html
    >>> from webstruct import GateLoader

    >>> loader = GateLoader(known_entities={'ORG', 'CITY'})
    >>> html = b"<html><body><p><ORG>Scrapinghub</ORG> has an <b>office</b> in <CITY>Montevideo</CITY></p></body></html>"
    >>> tree = loader.loadbytes(html)
    >>> lxml.html.tostring(tree).decode()
    '<html><body><p> __START_ORG__ Scrapinghub __END_ORG__  has an <b>office</b> in  __START_CITY__ Montevideo __END_CITY__ </p></body></html>'

    Note that you must specify known_entities when creating GateLoader.
    It should contain all entities which are present in data, even if
    you want to use only a subset of them for training. Use arguments of
    :class:`~.HtmlLoader` to train a tagger which uses a subset of labels.
    """

    def __init__(self, encoding=None, cleaner=None, known_entities=None):
        if known_entities is None:
            raise ValueError("Please pass `known_entities` argument with a "
                             "list of all possible entities")
        self.known_entities = known_entities
        super(GateLoader, self).__init__(encoding, cleaner)

    def loadbytes(self, data):
        # tags are replaced before parsing data as HTML because
        # GATE's html is invalid
        data = self._replace_entities(data)
        return super(GateLoader, self).loadbytes(data)

    def _replace_entities(self, html_bytes):
        # replace requested entities with unified tokens
        open_re, close_re = self._entity_patterns(self.known_entities)
        html_bytes = re.sub(open_re, br' __START_\1__ ', html_bytes)
        html_bytes = re.sub(close_re, br' __END_\1__ ', html_bytes)
        return html_bytes

    def _entity_patterns(self, entities):
        entities_pattern = '|'.join(list(entities))
        open_re = re.compile(six.b('<(%s)>' % entities_pattern), re.I)
        close_re = re.compile(six.b('</(%s)>' % entities_pattern), re.I)
        return open_re, close_re


def load_trees(pattern, loader, verbose=False):
    """
    Load HTML data using loader ``loader`` from all files matched by
    ``pattern`` glob pattern.

    Example:

    >>> trees = load_trees('path/*.html', HtmlLoader())  # doctest: +SKIP

    """
    for path in human_sorted(glob.glob(pattern)):
        if verbose:
            print(path)
        yield loader.load(path)


def _get_default_cleaner():
    return lxml_html_clean.Cleaner(
        scripts=False,     # non-default: preserve scripts
        javascript=False,  # non-default: keep external stylesheets
                           # (javascript=True removes them)

        # Non-default: keep comments because they may contain <base> tag.
        # Just comments=False doesn't work; we need to disable processing
        # instructions as well (and enable them again, via kill_tags).
        comments=False,
        processing_instructions=False,  # required to keep comments
        kill_tags=[ProcessingInstruction],

        style=False,  # non-default: keep stylesheets
        links=False,  # non-default: keep external stylesheets
        meta=False,   # non-default
        page_structure=False,  # non-default
        embedded=True,
        frames=True,
        forms=False,  # non-default
        annoying_tags=False,  # non-default
        remove_unknown_tags=False,  # non-default
        safe_attrs_only=False,  # non-default
    )
