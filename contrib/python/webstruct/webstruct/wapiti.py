# -*- coding: utf-8 -*-
"""
:mod:`webstruct.wapiti` module provides utilities for easier creation
of Wapiti_ models, templates and data files.

.. _Wapiti: http://wapiti.limsi.fr/

"""

from __future__ import absolute_import
import os
import re
import six
import shlex
import tempfile
import copy
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.pipeline import Pipeline
from webstruct import HtmlFeatureExtractor
from webstruct.base import BaseSequenceClassifier
from webstruct.utils import get_combined_keys, run_command
from webstruct._fileresource import FileResource
from webstruct.sequence_encoding import IobEncoder


def create_wapiti_pipeline(model_filename=None,
                           token_features=None,
                           global_features=None,
                           min_df=1,
                           **crf_kwargs):
    """
    Create a scikit-learn Pipeline for HTML tagging using Wapiti.
    This pipeline expects data produced by :class:`~.HtmlTokenizer`
    as an input and produces sequences of IOB2 tags as output.

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
        model = webstruct.create_wapiti_pipeline(
            model_filename = 'model.wapiti',
            token_features = EXAMPLE_TOKEN_FEATURES,
            train_args = '--algo l-bfgs --maxiter 50 --nthread 8 --jobsize 1 --stopwin 10',
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
    if token_features is None:
        token_features = []

    return Pipeline([
        ('fe', HtmlFeatureExtractor(token_features, global_features, min_df=min_df)),
        ('crf', WapitiCRF(model_filename, **crf_kwargs)),
    ])


def merge_top_n(chains):
    """
    Take first (most probable) as base for resulting chain
    and merge other N-1 chains one by one
    Entities in next merged chain, which has any overlap
    with entities in resulting chain, just ignored

    non-overlap
    >>> chains = [ ['B-PER', 'O'     ],
    ...            ['O'    , 'B-FUNC'] ]

    >>> merge_top_n(chains)
    ['B-PER', 'B-FUNC']

    partially overlap
    >>> chains = [ ['B-PER', 'I-PER', 'O'    ],
    ...            ['O'    , 'B-PER', 'I-PER'] ]

    >>> merge_top_n(chains)
    ['B-PER', 'I-PER', 'O']

    fully overlap
    >>> chains = [ ['B-PER', 'I-PER'],
    ...            ['B-ORG', 'I-ORG'] ]

    >>> merge_top_n(chains)
    ['B-PER', 'I-PER']
    """
    ret = copy.copy(chains[0])
    for chain in chains[1:]:
        encoder = IobEncoder()

        for items, tag in encoder.iter_group(enumerate(chain)):

            is_tagged = False
            idx = 0
            while not is_tagged and idx < len(items):
                item = items[idx]
                idx = idx + 1
                is_tagged = ret[item] != 'O'

            if is_tagged:
                continue

            for item in items:
                ret[item] = chain[item]
    return ret


class WapitiCRF(BaseSequenceClassifier):
    """
    Class for training and applying Wapiti CRF models.

    For training it relies on calling original Wapiti binary (via
    subprocess), so "wapiti" binary must be available if you need "fit"
    method.

    Trained model is saved in an external file; its filename is a first
    parameter to constructor. This file is created and overwritten by
    :meth:`WapitiCRF.fit`; it must exist for :meth:`WapitiCRF.transform`
    to work.

    For prediction WapitiCRF relies on python-wapiti_ library.

    .. _python-wapiti: https://github.com/adsva/python-wapiti
    """

    WAPITI_CMD = 'wapiti'
    """ Command used to start wapiti """

    def __init__(self, model_filename=None, train_args=None,
                 feature_template="# Label unigrams and bigrams:\n*\n",
                 unigrams_scope="u", tempdir=None, unlink_temp=True,
                 verbose=True, feature_encoder=None, dev_size=0,
                 top_n=1):

        self.modelfile = FileResource(
            filename=model_filename,
            keep_tempfiles=not unlink_temp,
            suffix='.wapiti',
            prefix='model',
        )

        if train_args is None:
            train_args = '--algo l-bfgs --maxiter 50 --compact --nthread 8 --jobsize 1 --stopwin 15'
        if isinstance(train_args, (list, tuple)):
            self.train_args = train_args
        else:
            self.train_args = shlex.split(train_args)

        self.feature_template = feature_template
        self.unigrams_scope = unigrams_scope
        self.tempdir = tempdir
        self.unlink_temp = unlink_temp
        self.verbose = verbose
        self.dev_size = dev_size
        self._wapiti_model = None
        self.feature_encoder = feature_encoder or WapitiFeatureEncoder()
        self.top_n = top_n
        super(WapitiCRF, self).__init__()

    def fit(self, X, y, X_dev=None, y_dev=None, out_dev=None):
        """
        Train a model.

        Parameters
        ----------
        X : list of lists of dicts
            Feature dicts for several documents.

        y : a list of lists of strings
            Labels for several documents.

        X_dev : (optional) list of lists of feature dicts
            Data used for testing and as a stopping criteria.

        y_dev : (optional) list of lists of labels
            Labels corresponding to X_dev.

        out_dev : (optional) string
            Path to a file where tagged development data will be written.

        """
        self.modelfile.refresh()
        self._wapiti_model = None
        self.feature_encoder.reset()
        self.feature_encoder.fit(X, y)

        if any([X_dev, y_dev, out_dev]):
            if X_dev is None or y_dev is None:
                raise ValueError("Pass both X_dev and y_dev to use the development data")
        elif self.dev_size > 0:
            # Use a part of training data to help with stopping.
            # It means less data is used for training.
            X_dev, y_dev = X[:self.dev_size], y[:self.dev_size]
            X, y = X[self.dev_size:], y[self.dev_size:]

        dev_fn = None
        to_unlink = []
        try:
            train_fn = self._create_wapiti_data_file(X, y)
            to_unlink.append(train_fn)

            if X_dev is not None:
                dev_fn = self._create_wapiti_data_file(X_dev, y_dev)
                if out_dev is None:
                    _, out_dev = tempfile.mkstemp(dir=self.tempdir, suffix=".txt", prefix="wapiti-dev-data")
                    to_unlink.append(out_dev)

            template_fn = self._create_wapiti_feature_template_file()
            to_unlink.append(template_fn)

            # run wapiti training
            args = ['train', '--pattern', template_fn] + self.train_args
            if dev_fn:
                args += ['--devel', dev_fn]
            args += [train_fn, self.modelfile.name]
            self.run_wapiti(args)

            # do a final check on development data
            if dev_fn:
                args = ['label', '-m', self.modelfile.name, '--check', dev_fn, out_dev]
                self.run_wapiti(args)

        finally:
            if self.unlink_temp:
                for filename in to_unlink:
                    os.unlink(filename)

        return self


    def predict(self, X):
        """
        Make a prediction.

        Parameters
        ----------
        X : list of lists
            feature dicts

        Returns
        -------
        y : list of lists
            predicted labels

        """
        model = self._get_python_wapiti_model()
        model.options.nbest = self.top_n
        sequences = self._to_wapiti_sequences(X)
        result = list()
        for idx, seq in enumerate(sequences):
            prediction = model.label_sequence(seq).decode(model.encoding).splitlines()
            words = len(X[idx])
            chains = [None] * self.top_n
            for i in range(self.top_n):
                start = (words + 1) * i
                chains[i] = prediction[start:start + words]
            result.append(merge_top_n(chains))
        return result

    def run_wapiti(self, args):
        """ Run ``wapiti`` binary in a subprocess """
        return run_command([self.WAPITI_CMD] + args, self.verbose)

    def _get_python_wapiti_model(self):
        if self._wapiti_model is None:
            self._load_model()
        return self._wapiti_model

    def _load_model(self):
        import wapiti
        if self.modelfile.name is None:
            raise ValueError("model filename is unknown, can't load model")
        self._wapiti_model = wapiti.Model(model=self.modelfile.name)

    def _to_wapiti_sequences(self, X, y=None):
        X = self.feature_encoder.transform(X)
        if y is None:
            return ["\n".join(lines) for lines in X]
        else:
            return [
                self._to_train_sequence(lines, tags) for lines, tags in zip(X, y)
            ]

    def _create_wapiti_data_file(self, X, y=None):
        """
        Create a file with input data for wapiti. Return a resulting file name;
        caller should unlink the file.
        """
        with tempfile.NamedTemporaryFile('wb', prefix="wapiti-data-", suffix=".txt", dir=self.tempdir, delete=False) as fp:
            for seq in self._to_wapiti_sequences(X, y):
                fp.write(seq.encode('utf8'))
                fp.write(b"\n\n")
        return fp.name

    def _create_wapiti_feature_template_file(self):
        # create feature template
        with tempfile.NamedTemporaryFile('wb', prefix="feature-template-", suffix=".txt", dir=self.tempdir, delete=False) as fp:
            template = self.feature_encoder.prepare_template(self.feature_template)
            fp.write(template.encode('utf8'))

            if self.unigrams_scope is not None:
                unigram_template = self.feature_encoder.unigram_features_template(self.unigrams_scope)
                fp.write(b"\n")
                fp.write(unigram_template.encode('utf8'))
        return fp.name

    def _to_train_sequence(self, wapiti_lines, tags):
        return "\n".join(["%s %s" %(line, tag) for line, tag in zip(wapiti_lines, tags)])

    def __getstate__(self):
        dct = self.__dict__.copy()
        dct['_wapiti_model'] = None
        return dct



class WapitiFeatureEncoder(BaseEstimator, TransformerMixin):
    """
    Utility class for preparing Wapiti templates and
    converting sequences of dicts with features to the format Wapiti_
    understands.
    """
    def __init__(self, move_to_front=('token',)):
        self.move_to_front = tuple(move_to_front)
        self.feature_names_ = None
        self.vocabulary_ = None

    def fit(self, X, y=None):
        """
        X should be a list of lists of dicts with features.
        It can be obtained, for example, using
        :class:`~.HtmlFeatureExtractor`.
        """
        return self.partial_fit(X)

    def partial_fit(self, X, y=None):
        keys = set(self.feature_names_ or set())
        move_to_front = set(self.move_to_front)

        for feature_dicts in X:
            keys = (keys | get_combined_keys(feature_dicts)) - move_to_front

        self.feature_names_ = self.move_to_front + tuple(keys)
        self.vocabulary_ = dict((f, i) for i, f in enumerate(self.feature_names_))
        return self

    def transform_single(self, feature_dicts):
        """
        Transform a sequence of dicts ``feature_dicts``
        to a list of Wapiti data file lines.
        """
        lines = []
        for dct in feature_dicts:
            line = ' '.join(_tostr(dct.get(key)) for key in self.feature_names_)
            lines.append(line)
        return lines

    def transform(self, X):
        return [self.transform_single(feature_dicts) for feature_dicts in X]

    def prepare_template(self, template):
        r"""
        Prepare Wapiti template by replacing feature names with feature
        column indices inside ``%x[row,col]`` macros. Indices are compatible
        with :meth:`WapitiFeatureEncoder.transform` output.

            >>> we = WapitiFeatureEncoder(['token', 'tag'])
            >>> seq_features = [{'token': 'the', 'tag': 'DT'}, {'token': 'dog', 'tag': 'NN'}]
            >>> we.fit([seq_features])
            WapitiFeatureEncoder(move_to_front=('token', 'tag'))
            >>> we.prepare_template('*:Pos-1 L=%x[-1, tag]\n*:Suf-2 X=%m[ 0,token,".?.?$"]')
            '*:Pos-1 L=%x[-1,1]\n*:Suf-2 X=%m[0,0,".?.?$"]'

        Check these links for more info about template format:

        * http://wapiti.limsi.fr/manual.html
        * http://crfpp.googlecode.com/svn/trunk/doc/index.html#templ

        """
        return prepare_wapiti_template(template, self.vocabulary_)

    def unigram_features_template(self, scope='*'):
        """
        Return Wapiti template with unigram features for each of
        known features.

            >>> we = WapitiFeatureEncoder(['token', 'tag'])
            >>> seq_features = [{'token': 'the', 'tag': 'DT'}, {'token': 'dog', 'tag': 'NN'}]
            >>> we.fit([seq_features])
            WapitiFeatureEncoder(move_to_front=('token', 'tag'))
            >>> print(we.unigram_features_template())
            <BLANKLINE>
            # Unigrams for all custom features
            *feat:token=%x[0,0]
            *feat:tag=%x[0,1]
            <BLANKLINE>
            >>> print(we.unigram_features_template('u'))
            <BLANKLINE>
            # Unigrams for all custom features
            ufeat:token=%x[0,0]
            ufeat:tag=%x[0,1]
            <BLANKLINE>
        """
        lines = ['\n# Unigrams for all custom features']
        for col, name in enumerate(self.feature_names_):
            line = '{scope}feat:{name}=%x[0,{col}]'.format(scope=scope, name=name, col=col)
            lines.append(line)
        return "\n".join(lines) + '\n'

    def reset(self):
        self.feature_names_ = None



WAPITI_MACRO_PATTERN = re.compile(r'''
    (?P<macro>%[xXtTmM])
    \[
    \s*(?P<offset>[-]?\d+)\s*
    ,
    \s*(?P<column>[^\],\s]+)\s*  # identifier: anything but closing bracket or comma
    (?P<rest>[\],])              # closing bracket or comma
    ''', re.VERBOSE | re.UNICODE
)

def prepare_wapiti_template(template, vocabulary):
    r"""
    Prepare Wapiti template by replacing feature names with feature
    column indices inside ``%x[row,col]`` macros::

        >>> vocab = {'token': 0, 'tag': 1}
        >>> prepare_wapiti_template('*:Pos-1 L=%x[-1, tag]\n*:Suf-2 X=%m[ 0,token,".?.?$"]', vocab)
        '*:Pos-1 L=%x[-1,1]\n*:Suf-2 X=%m[0,0,".?.?$"]'

    It understands which lines are comments::

        >>> prepare_wapiti_template('*:Pos-1 L=%x[-1, tag]\n# *:Suf-2 X=%m[ 0,token,".?.?$"]', vocab)
        '*:Pos-1 L=%x[-1,1]\n# *:Suf-2 X=%m[ 0,token,".?.?$"]'

    Check these links for more info about template format:

    * http://wapiti.limsi.fr/manual.html
    * http://crfpp.googlecode.com/svn/trunk/doc/index.html#templ

    """
    def repl(m):
        column = m.group('column')
        if not column.isdigit():
            column = vocabulary[column]
        return "{0[macro]}[{0[offset]},{1}{0[rest]}".format(m.groupdict(), column)

    lines = [
        (WAPITI_MACRO_PATTERN.sub(repl, line) if not _wapiti_line_is_comment(line) else line)
        for line in template.splitlines()
    ]

    return "\n".join(lines)


def _tostr(val):
    """
    >>> _tostr('foo')
    'foo'
    >>> isinstance(_tostr(u'foo'), six.text_type)
    True
    >>> _tostr(10)
    '10'
    >>> _tostr(True)
    '1'
    >>> _tostr(False)
    '0'
    """
    if isinstance(val, six.string_types):
        return val
    if isinstance(val, bool):
        return str(int(val))
    return str(val)


def _wapiti_line_is_comment(line):
    return line.strip().startswith('#')
