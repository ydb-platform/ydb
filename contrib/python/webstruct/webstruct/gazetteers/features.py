# -*- coding: utf-8 -*-
from __future__ import absolute_import
from webstruct.gazetteers.geonames import GAZETTEER_FORMAT
from webstruct.features.global_features import LongestMatchGlobalFeature


class MarisaGeonamesGlobalFeature(LongestMatchGlobalFeature):
    """
    Global feature that matches longest entities from a lexicon
    extracted from geonames.org and stored in a MARISA Trie.
    """
    def __init__(self, filename, featname, format=None):
        import marisa_trie

        self.filename = filename
        self.data = marisa_trie.RecordTrie(format or GAZETTEER_FORMAT)
        self.data.load(filename)

        super(MarisaGeonamesGlobalFeature, self).__init__(self.data, featname)


# TODO: add features that'd allow to check entities for compatibility.
# For example, that detected entites are from the same US state.
