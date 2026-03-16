from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import os.path
import numpy as np
import tempfile
import cv2
from opensfm import context
from library.python import resource

class BagOfWords:
    def __init__(self, words, frequencies):
        self.words = words
        self.frequencies = frequencies
        self.weights = np.log(frequencies.sum() / frequencies)
        FLANN_INDEX_KDTREE = 1
        flann_params = dict(algorithm=FLANN_INDEX_KDTREE,
                            trees=8,
                            checks=300)
        self.index = context.flann_Index(words, flann_params)

    def map_to_words(self, descriptors, k, matcher_type='FLANN'):
        if matcher_type == 'FLANN':
            params = {str('checks'): 200}
            idx, dist = self.index.knnSearch(descriptors, k, params=params)
        else:
            matcher = cv2.DescriptorMatcher_create(matcher_type)
            matches = matcher.knnMatch(descriptors, self.words, k=k)
            idx = [[int(n.trainIdx) for n in m] for m in matches]
            idx = np.array(idx).astype(np.int32)
        return idx

    def histogram(self, words):
        h = np.bincount(words, minlength=len(self.words)) * self.weights
        return h / h.sum()

    def bow_distance(self, w1, w2, h1=None, h2=None):
        if h1 is None:
            h1 = self.histogram(w1)
        if h2 is None:
            h2 = self.histogram(w2)
        return np.fabs(h1 - h2).sum()


def load_bow_words_and_frequencies(config):
    if config['bow_file'] == 'bow_hahog_root_uchar_10000.npz':
        assert config['feature_type'] == 'HAHOG'
        assert config['feature_root']
        assert config['hahog_normalize_to_uchar']

    with tempfile.TemporaryFile() as f:
        f.write(resource.find('bow_hahog_10000'))
        f.seek(0)
        bow = np.load(f)

    return bow['words'], bow['frequencies']


def load_vlad_words_and_frequencies(config):
    if config['vlad_file'] == 'bow_hahog_root_uchar_64.npz':
        assert config['feature_type'] == 'HAHOG'
        assert config['feature_root']
        assert config['hahog_normalize_to_uchar']

    with tempfile.TemporaryFile() as f:
        f.write(resource.find('bow_hahog_64'))
        f.seek(0)
        vlad = np.load(f)

    return vlad['words'], vlad['frequencies']


def load_bows(config):
    words, frequencies = load_bow_words_and_frequencies(config)
    return BagOfWords(words, frequencies)
