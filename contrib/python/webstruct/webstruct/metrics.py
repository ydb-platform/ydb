# -*- coding: utf-8 -*-
"""
:mod:`webstruct.metrics` contains metric functions that can be used for
model developmenton: on their own or as scoring functions for
scikit-learn's `cross-validation`_ and `model selection`_.

.. _cross-validation: http://scikit-learn.org/stable/modules/cross_validation.html
.. _model selection: http://scikit-learn.org/stable/tutorial/statistical_inference/model_selection.html

"""
from __future__ import absolute_import
from itertools import chain
from functools import partial
import numpy as np
from sklearn.metrics import classification_report

# steal from seqlearn
def bio_f_score(y_true, y_pred):
    """F-score for BIO-tagging scheme, as used by CoNLL.

    This F-score variant is used for evaluating named-entity recognition and
    related problems, where the goal is to predict segments of interest within
    sequences and mark these as a "B" (begin) tag followed by zero or more "I"
    (inside) tags. A true positive is then defined as a BI* segment in both
    y_true and y_pred, with false positives and false negatives defined
    similarly.

    Support for tags schemes with classes (e.g. "B-NP") are limited: reported
    scores may be too high for inconsistent labelings.

    Parameters
    ----------
    y_true : array-like of strings, shape (n_samples,)
        Ground truth labeling.

    y_pred : array-like of strings, shape (n_samples,)
        Sequence classifier's predictions.

    Returns
    -------
    f : float
        F-score.
    """

    if len(y_true) != len(y_pred):
        msg = "Sequences not of the same length ({} != {})."""
        raise ValueError(msg.format(len(y_true), len(y_pred)))

    y_true = np.asarray(y_true)
    y_pred = np.asarray(y_pred)

    is_b = partial(np.char.startswith, prefix="B")

    where = np.where
    t_starts = where(is_b(y_true))[0]
    p_starts = where(is_b(y_pred))[0]

    # These lengths are off-by-one because we skip the "B", but that's ok.
    # http://stackoverflow.com/q/17929499/166749
    t_lengths = np.diff(where(is_b(np.r_[y_true[y_true != 'O'], ['B']]))[0])
    p_lengths = np.diff(where(is_b(np.r_[y_pred[y_pred != 'O'], ['B']]))[0])

    t_segments = set(zip(t_starts, t_lengths, y_true[t_starts]))
    p_segments = set(zip(p_starts, p_lengths, y_pred[p_starts]))

    # tp = len(t_segments & p_segments)
    # fn = len(t_segments - p_segments)
    # fp = len(p_segments - t_segments)
    tp = sum(x in t_segments for x in p_segments)
    fn = sum(x not in p_segments for x in t_segments)
    fp = sum(x not in t_segments for x in p_segments)

    if tp == 0:
        # special-cased like this in CoNLL evaluation
        return 0.

    precision = tp / float(tp + fp)
    recall = tp / float(tp + fn)

    return 2. * precision * recall / (precision + recall)


def avg_bio_f1_score(y_true, y_pred):
    """
    Macro-averaged F1 score of lists of BIO-encoded sequences
    ``y_true`` and ``y_pred``.

    A named entity in a sequence from ``y_pred`` is considered
    correct only if it is an exact match of the corresponding entity
    in the ``y_true``.

    It requires https://github.com/larsmans/seqlearn to work.
    """
    return sum(map(bio_f_score, y_true, y_pred)) / len(y_true)


def bio_classification_report(y_true, y_pred):
    """
    Classification report for a list of BIO-encoded sequences.
    It computes token-level metrics and discards "O" labels.
    """
    y_true_combined = list(chain.from_iterable(y_true))
    y_pred_combined = list(chain.from_iterable(y_pred))
    tagset = (set(y_true_combined) | set(y_pred_combined)) - {'O'}
    return classification_report(
        y_true_combined,
        y_pred_combined,
        labels = sorted(tagset, key=lambda tag: tag.split('-', 1)[::-1])
    )
