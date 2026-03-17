# -*- coding: utf-8 -*-
from __future__ import absolute_import

from webstruct.utils import LongestMatch


class LongestMatchGlobalFeature(object):
    def __init__(self, lookup_data, featname):
        """
        Create a global feature function that adds 3 types of features:

        1) B-featname - if current token starts an entity from
           the ``lookup_data``;
        2) I-featname - if current token is inside an entity from
           the ``lookup_data``;
        3) featname - if current token belongs to an entity from the
           ``lookup_data``.

        """
        if hasattr(lookup_data, 'find_ranges'):
            self.lm = lookup_data
        else:
            self.lm = LongestMatch(lookup_data)
        self.b_featname = 'B-' + featname
        self.i_featname = 'I-' + featname
        self.featname = featname

    def __call__(self, doc):
        token_strings = [tok.token for tok, feat in doc]
        for start, end, matched_text in self.lm.find_ranges(token_strings):
            self.process_range(doc, start, end, matched_text)

    def process_range(self, doc, start, end, matched_text):
        doc[start][1][self.b_featname] = True
        doc[start][1][self.featname] = True

        for idx in range(start+1, end):
            doc[idx][1][self.i_featname] = True
            doc[idx][1][self.featname] = True


class DAWGGlobalFeature(LongestMatchGlobalFeature):
    """
    Global feature that matches longest entities from a lexicon
    stored either in a ``dawg.CompletionDAWG`` (if ``format`` is None)
    or in a ``dawg.RecordDAWG`` (if ``format`` is not None).
    """
    def __init__(self, filename, featname, format=None):
        import dawg

        if format is None:
            self.data = dawg.CompletionDAWG()
        else:
            self.data = dawg.RecordDAWG(format)
        self.data.load(filename)

        self.filename = filename
        super(DAWGGlobalFeature, self).__init__(self.data, featname)


class Pattern(object):
    """
    Global feature that combines local features.
    """
    def __init__(self, *lookups, **kwargs):
        self.separator = kwargs.get('separator', '/')
        self.out_value = kwargs.get('out_value', '?')
        self.missing_value = kwargs.get('missing_value', '_NA_')
        self.lookups = lookups
        # TODO: add an option to use index values on HTML element level

    def __call__(self, doc):
        _add_pattern_features(
            feature_dicts = [feat for html_token, feat in doc],
            pattern = self.lookups,
            out_value = self.out_value,
            missing_value = self.missing_value,
            separator = self.separator
        )


def _add_pattern_features(feature_dicts, pattern, out_value, missing_value, separator):
    for pos, featdict in enumerate(feature_dicts):
        keys = []
        values = []
        for offset, key in pattern:
            if offset == 0:
                keys.append(key)
            elif offset < 0:
                keys.append('%s[%s]' % (key, offset))
            else:
                keys.append('%s[+%s]' % (key, offset))

            index = pos + offset
            if 0 <= index < len(feature_dicts):
                values.append(feature_dicts[index].get(key, missing_value))
            else:
                values.append(out_value)

        # FIXME: there should be a cleaner/faster way
        if not all(v == out_value for v in values):
            featdict[separator.join(keys)] = separator.join(values)
