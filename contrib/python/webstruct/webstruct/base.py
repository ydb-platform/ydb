from __future__ import absolute_import
from sklearn.base import BaseEstimator, TransformerMixin
from webstruct.metrics import avg_bio_f1_score


class BaseSequenceClassifier(BaseEstimator, TransformerMixin):

    def score(self, X, y):
        """
        Macro-averaged F1 score of lists of BIO-encoded sequences
        ``y_true`` and ``y_pred``.

        A named entity in a sequence from ``y_pred`` is considered
        correct only if it is an exact match of the corresponding entity
        in the ``y_true``.
        """
        y_pred = self.predict(X)
        return avg_bio_f1_score(y, y_pred)
