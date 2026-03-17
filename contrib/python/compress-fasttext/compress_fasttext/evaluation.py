import os
import re
import numpy as np

from collections import Counter


def vocabulary_from_files(path):
    wc = Counter()
    for fn in os.listdir(path):
        if not fn.endswith('txt'):
            continue
        with open(path + fn, 'r', encoding='utf-8') as f:
            for line in f.readlines():
                for w in re.sub('[^а-яёa-z]', ' ', line.lower()).split():
                    w2 = w.strip().replace('ё', 'е')
                    if len(w2) > 1:  # one-letter words are not included in the model I compress, so I skip them
                        wc[w2] += 1
    return wc


def cosine(x, y, eps=1e-10):
    x = x.astype(np.float32)
    y = y.astype(np.float32)
    den = (np.dot(x, x) * np.dot(y, y))**0.5
    return np.dot(x, y) / (den + eps)


def vecs_similarity(old_vecs, new_vecs, corpus):
    return np.mean([cosine(old_vecs[w], new_vecs[w]) for w in corpus])


def make_evaluator(baseline_model, path):
    vocab = vocabulary_from_files(path)
    return lambda model: vecs_similarity(baseline_model, model, vocab.keys())
