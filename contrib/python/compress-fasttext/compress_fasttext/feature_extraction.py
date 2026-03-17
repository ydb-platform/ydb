from typing import List

import numpy as np
from sklearn.base import BaseEstimator, TransformerMixin
from compress_fasttext.models import CompressedFastTextKeyedVectors


def tokenize(text) -> List[str]:
    """ Keep only alphanumeric character and split the text by space"""
    return ''.join(x for x in text if x.isalnum() or x.isspace()).split() or [' ']


class FastTextTransformer(BaseEstimator, TransformerMixin):
    """ Convert texts into their mean fastText vectors """
    def __init__(self, model, tokenizer=tokenize, normalize=True):
        if isinstance(model, str):
            model = CompressedFastTextKeyedVectors.load(model)
        self.model = model
        self.tokenizer = tokenizer
        self.normalize = normalize

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        result = np.stack([
            np.mean([self.model[w] for w in self.tokenizer(text)], 0)
            for text in X
        ])
        if self.normalize:
            norm = np.maximum(1e-10, (result ** 2).sum(1, keepdims=True) ** 0.5)
            result /= norm
        return result
