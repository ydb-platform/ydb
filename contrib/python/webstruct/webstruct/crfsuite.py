# -*- coding: utf-8 -*-
"""
CRFsuite_ backend for webstruct based on python-crfsuite_ and sklearn-crfsuite_.

.. _CRFsuite: http://www.chokkan.org/software/crfsuite/
.. _python-crfsuite: https://github.com/tpeng/python-crfsuite
.. _sklearn-crfsuite: https://github.com/TeamHG-Memex/sklearn-crfsuite

"""
from __future__ import absolute_import
from sklearn.pipeline import Pipeline

from webstruct import HtmlFeatureExtractor


class CRFsuitePipeline(Pipeline):
    """
    A pipeline for HTML tagging using CRFsuite. It combines
    a feature extractor and a CRF; they are available
    as :attr:`fe` and :attr:`crf` attributes for easier access.

    In addition to that, this class adds support for X_dev/y_dev arguments
    for :meth:`fit` and :meth:`fit_transform` methods - they work as expected,
    being transformed using feature extractor.
    """
    def __init__(self, fe, crf):
        self.fe = fe
        self.crf = crf
        super(CRFsuitePipeline, self).__init__([
            ('vec', self.fe),
            ('clf', self.crf),
        ])

    def fit(self, X, y=None, **fit_params):
        X_dev = fit_params.pop('X_dev', None)
        if X_dev is not None:
            fit_params['clf__X_dev'] = self.fe.transform(X_dev)
            fit_params['clf__y_dev'] = fit_params.pop('y_dev', None)
        return super(CRFsuitePipeline, self).fit(X, y, **fit_params)

    def fit_transform(self, X, y=None, **fit_params):
        X_dev = fit_params.pop('X_dev', None)
        if X_dev is not None:
            fit_params['clf__X_dev'] = self.fe.transform(X_dev)
            fit_params['clf__y_dev'] = fit_params.pop('y_dev', None)
        return super(CRFsuitePipeline, self).fit_transform(X, y, **fit_params)


def create_crfsuite_pipeline(token_features=None,
                             global_features=None,
                             min_df=1,
                             **crf_kwargs):
    """
    Create :class:`CRFsuitePipeline` for HTML tagging using CRFsuite.
    This pipeline expects data produced by
    :class:`~.HtmlTokenizer` as an input and produces
    sequences of IOB2 tags as output.

    Example::

        import webstruct
        from webstruct.features import EXAMPLE_TOKEN_FEATURES

        # load train data
        html_tokenizer = webstruct.HtmlTokenizer()
        train_trees = webstruct.load_trees(
            "train/*.html",
            webstruct.WebAnnotatorLoader()
        )
        X_train, y_train = html_tokenizer.tokenize(train_trees)

        # train
        model = webstruct.create_crfsuite_pipeline(
            token_features = EXAMPLE_TOKEN_FEATURES,
        )
        model.fit(X_train, y_train)

        # load test data
        test_trees = webstruct.load_trees(
            "test/*.html",
            webstruct.WebAnnotatorLoader()
        )
        X_test, y_test = html_tokenizer.tokenize(test_trees)

        # do a prediction
        y_pred = model.predict(X_test)

    """
    from sklearn_crfsuite import CRF

    if token_features is None:
        token_features = []

    fe = HtmlFeatureExtractor(token_features, global_features, min_df=min_df)
    crf = CRF(**crf_kwargs)

    return CRFsuitePipeline(fe, crf)
