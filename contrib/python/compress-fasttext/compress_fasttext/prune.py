# based on Andrey Vasnetsov code: https://gist.github.com/generall/68fddb87ae1845d6f54c958ed3d0addb

import numpy as np

from collections import defaultdict
from copy import deepcopy
from .utils import ft_ngram_hashes


def count_buckets(ft, words, new_ngrams_size):
    new_to_old_buckets = defaultdict(set)
    old_hash_count = defaultdict(int)
    for word in words:
        old_hashes = ft_ngram_hashes(word=word, minn=ft.min_n, maxn=ft.max_n, num_buckets=ft.bucket)
        new_hashes = ft_ngram_hashes(word=word, minn=ft.min_n, maxn=ft.max_n, num_buckets=new_ngrams_size)
        for old_hash in old_hashes:
            old_hash_count[old_hash] += 1  # calculate frequency of ngrams for proper weighting

        for old_hash, new_hash in zip(old_hashes, new_hashes):
            new_to_old_buckets[new_hash].add(old_hash)
    return new_to_old_buckets, old_hash_count


def fasttext_like_init(n, dim=300, random_state=42):
    rand_obj = np.random
    rand_obj.seed(random_state)
    lo, hi = -1.0 / dim, 1.0 / dim
    new_ngrams = rand_obj.uniform(lo, hi, (n, dim)).astype(np.float32)
    return new_ngrams


def prune_ngrams(ft, new_ngrams_size, random_state=42):
    """ Reduce the size of fasttext ngrams matrix by collapsing some hashes together """
    new_to_old_buckets, old_hash_count = count_buckets(
        ft, ft.key_to_index.keys(), new_ngrams_size
    )

    # initialize new buckets just like in fasttext
    new_ngrams = fasttext_like_init(n=new_ngrams_size, dim=ft.vectors_ngrams.shape[1], random_state=random_state)

    # fill new buckets with the weighted average old vectors
    for new_hash, old_buckets in new_to_old_buckets.items():
        total_sum = sum(old_hash_count[old_hash] for old_hash in old_buckets)
        new_vector = np.zeros(ft.vector_size, dtype=np.float32)
        for old_hash in old_buckets:
            weight = old_hash_count[old_hash] / total_sum
            new_vector += ft.vectors_ngrams[old_hash] * weight
        new_ngrams[new_hash] = new_vector

    return new_ngrams


def prune_vocab(ft, new_vocab_size=1_000):
    sorted_vocab = sorted(ft.key_to_index.items(), key=lambda x: ft.get_vecattr(x[0], 'count'), reverse=True)
    top_vocab_list = deepcopy(sorted_vocab[:new_vocab_size])
    top_vector_ids = []
    for new_index, (word, idx) in enumerate(top_vocab_list):
        top_vector_ids.append(idx)
    top_vectors = ft.vectors[top_vector_ids, :]
    return dict(top_vocab_list), top_vectors


class RowSparseMatrix:
    """ This data structure returns 0 for empty rows """
    def __init__(self, mapping, compressed, nrows, ncols, dtype):
        self.mapping = mapping
        self.compressed = compressed
        self.nrows = nrows
        self.ncols = ncols
        self.dtype = dtype

    def __getitem__(self, item):
        if item not in self.mapping:
            return np.zeros(self.ncols, dtype=self.dtype)
        return self.compressed[self.mapping[item]]

    def unpack(self):
        result = np.zeros(shape=self.shape, dtype=self.dtype)
        for old_index, new_index in self.mapping.items():
            result[old_index] = self[new_index].astype(self.dtype)
        return result

    def __add__(self, other):
        return self.unpack() + other

    def __sub__(self, other):
        return self.unpack() - other

    def __mul__(self, other):
        return self.unpack() * other

    def __truediv__(self, other):
        return self.unpack() / other

    def __pow__(self, other):
        return self.unpack() ** other

    def sqrt(self):
        return self ** 0.5

    @property
    def shape(self):
        return self.nrows, self.ncols

    @classmethod
    def from_big(cls, row_indices, big_matrix, dtype=np.float32):
        nrows, ncols = big_matrix.shape
        dtype = dtype
        compressed = big_matrix[row_indices].astype(dtype)
        mapping = {old_id: new_id for new_id, old_id in enumerate(row_indices)}
        return cls(mapping, compressed, nrows, ncols, dtype)

    @classmethod
    def from_small(cls, row_indices, small_matrix, nrows, dtype=np.float32):
        nrows, ncols = nrows, small_matrix.shape[1]
        dtype = dtype
        compressed = small_matrix
        mapping = {old_id: new_id for new_id, old_id in enumerate(row_indices)}
        return cls(mapping, compressed, nrows, ncols, dtype)
