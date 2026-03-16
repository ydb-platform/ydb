# -*- coding: utf-8 -*-
"""
:mod:`webstruct.feature_extraction` contains classes that help
with:

- converting HTML pages into lists of feature dicts and
- extracting annotations.

Usually, the approach is the following:

1. Convert a web page to a list of :class:`~.HtmlToken` instances
   and a list of annotation tags (if present). :class:`~.HtmlTokenizer`
   is used for that.

2. Run a number of "token feature functions" that return bits of information
   about each token: token text, token shape (uppercased/lowercased/...),
   whether token is in ``<a>`` HTML element, etc. For each token information
   is combined into a single feature dictionary.

   Use :class:`HtmlFeatureExtractor` at this stage. There is a number of
   predefined token feature functions in :mod:`webstruct.features`.

3. Run a number of "global feature functions" that can modify token feature
   dicts inplace (insert new features, change, remove them) using "global"
   information - information about all other tokens in a document and their
   existing token-level feature dicts. Global feature functions are applied
   sequentially: subsequent global feature functions get feature dicts updated
   by previous feature functions.

   This is also done by :class:`HtmlFeatureExtractor`.

   :class:`~webstruct.features.utils.LongestMatchGlobalFeature` can be used
   to create features that capture multi-token patterns. Some predefined
   global feature functions can be found in :mod:`webstruct.gazetteers`.

"""
from __future__ import absolute_import, print_function
from itertools import chain
from collections import Counter
from six.moves import zip

from sklearn.base import BaseEstimator, TransformerMixin
from webstruct.utils import merge_dicts


class HtmlFeatureExtractor(BaseEstimator, TransformerMixin):
    """
    This class extracts features from lists of :class:`~.HtmlToken` instances
    (:class:`~.HtmlTokenizer` can be used to create such lists).

    :meth:`fit` / :meth:`transform` / :meth:`fit_transform` interface
    may look familiar to you if you ever used scikit-learn_:
    :class:`HtmlFeatureExtractor` implements sklearn's
    Transformer interface. But there is one twist: usually for sequence
    labelling tasks the whole sequences are considered observations.
    So in our case a single observation is a tokenized document
    (a list of tokens), not an individual token:
    :meth:`fit` / :meth:`transform` / :meth:`fit_transform` methods accept
    lists of documents (lists of lists of tokens), and return lists
    of documents' feature dicts (lists of lists of feature dicts).

    .. _scikit-learn: http://scikit-learn.org

    Parameters
    ----------

    token_features : list of callables
        List of "token" feature functions. Each function accepts
        a single ``html_token`` parameter and returns a dictionary
        wich maps feature names to feature values. Dicts from all
        token feature functions are merged by HtmlFeatureExtractor.
        Example token feature (it just returns token text)::

            >>> def current_token(html_token):
            ...     return {'tok': html_token.token}

        :mod:`webstruct.features` module provides some predefined feature
        functions, e.g. :func:`parent_tag <webstruct.features.block_features.parent_tag>`
        which returns token's parent tag.

        Example::

            >>> from webstruct import GateLoader, HtmlTokenizer, HtmlFeatureExtractor
            >>> from webstruct.features import parent_tag

            >>> loader = GateLoader(known_entities={'PER'})
            >>> html_tokenizer = HtmlTokenizer()
            >>> feature_extractor = HtmlFeatureExtractor(token_features=[parent_tag])

            >>> tree = loader.loadbytes(b"<p>hello, <PER>John <b>Doe</b></PER> <br> <PER>Mary</PER> said</p>")
            >>> html_tokens, tags = html_tokenizer.tokenize_single(tree)
            >>> feature_dicts = feature_extractor.transform_single(html_tokens)
            >>> for token, tag, feat in zip(html_tokens, tags, feature_dicts):
            ...     print("%s %s %s" % (token.token, tag, feat))
            hello O {'parent_tag': 'p'}
            John B-PER {'parent_tag': 'p'}
            Doe I-PER {'parent_tag': 'b'}
            Mary B-PER {'parent_tag': 'p'}
            said O {'parent_tag': 'p'}

    global_features : list of callables, optional
        List of "global" feature functions. Each "global" feature function
        should accept a single argument - a list
        of ``(html_token, feature_dict)`` tuples.
        This list contains all tokens from the document and
        features extracted by previous feature functions.

        "Global" feature functions are applied after "token" feature
        functions in the order they are passed.

        They should change feature dicts ``feature_dict`` inplace.

    min_df : integer or Mapping, optional
        Feature values that have a document frequency strictly
        lower than the given threshold are removed.
        If ``min_df`` is integer, its value is used as threshold.

        TODO: if ``min_df`` is a dictionary, it should map feature names
        to thresholds.

    """
    def __init__(self, token_features, global_features=None, min_df=1):
        self.token_features = token_features
        self.global_features = global_features or []
        self.min_df = min_df

    def fit(self, html_token_lists, y=None):
        self.fit_transform(html_token_lists)
        return self

    def fit_transform(self, html_token_lists, y=None, **fit_params):
        X = [self.transform_single(html_tokens) for html_tokens in html_token_lists]
        return self._pruned(X, low=self.min_df)

    def transform(self, html_token_lists):
        return [self.transform_single(html_tokens) for html_tokens in html_token_lists]

    def transform_single(self, html_tokens):
        feature_func = _CombinedFeatures(*self.token_features)
        token_data = list(zip(html_tokens, map(feature_func, html_tokens)))

        for feat in self.global_features:
            feat(token_data)

        return [featdict for tok, featdict in token_data]

    def _pruned(self, X, low=None):
        if low is None or low <= 1:
            return X
        cnt = self._document_frequency(X)
        keep = {k for (k, v) in cnt.items() if v >= low}
        del cnt
        return [
            [{k: v for k, v in fd.items() if (k, v) in keep} for fd in doc]
            for doc in X
        ]

    def _document_frequency(self, X):
        cnt = Counter()
        for doc in X:
            seen_features = set(chain.from_iterable(fd.items() for fd in doc))
            cnt.update(seen_features)
        return cnt


class _CombinedFeatures(object):
    """
    Utility for combining several feature functions::

        >>> from pprint import pprint
        >>> def f1(tok): return {'upper': tok.isupper()}
        >>> def f2(tok): return {'len': len(tok)}
        >>> features = _CombinedFeatures(f1, f2)
        >>> pprint(features('foo'))
        {'len': 3, 'upper': False}

    """
    def __init__(self, *feature_funcs):
        self.feature_funcs = list(feature_funcs)

    def __call__(self, *args, **kwargs):
        features = [f(*args, **kwargs) for f in self.feature_funcs]
        return merge_dicts(*features)
