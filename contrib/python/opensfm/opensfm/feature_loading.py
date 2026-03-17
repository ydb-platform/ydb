from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import logging

import numpy as np
from repoze.lru import LRUCache

from opensfm import features as ft


logger = logging.getLogger(__name__)


class FeatureLoader(object):
    def __init__(self):
        self.points_cache = LRUCache(1000)
        self.colors_cache = LRUCache(1000)
        self.features_cache = LRUCache(200)
        self.words_cache = LRUCache(200)
        self.masks_cache = LRUCache(1000)
        self.index_cache = LRUCache(200)
        self.masked_index_cache = LRUCache(200)

    def clear_cache(self):
        self.points_cache.clear()
        self.colors_cache.clear()
        self.features_cache.clear()
        self.words_cache.clear()
        self.masks_cache.clear()

    def load_mask(self, data, image, points=None):
        masks = self.masks_cache.get(image)
        if masks is None:
            if points is None:
                points, _ = self.load_points_colors(data, image, masked=False)
            if points is None:
                return None
            masks = data.load_features_mask(image, points[:, :2])
            self.masks_cache.put(image, masks)
        return masks

    def load_points_colors(self, data, image, masked=False):
        points = self.points_cache.get(image)
        colors = self.colors_cache.get(image)
        if points is None or colors is None:
            points, _, colors = self._load_features_nocache(data, image)
            self.points_cache.put(image, points)
            self.colors_cache.put(image, colors)
        if masked:
            mask = self.load_mask(data, image, points)
            if mask is not None:
                points = points[mask]
                colors = colors[mask]
        return points, colors

    def load_points_features_colors(self, data, image, masked=False):
        points = self.points_cache.get(image)
        features = self.features_cache.get(image)
        colors = self.colors_cache.get(image)
        if points is None or features is None or colors is None:
            points, features, colors = self._load_features_nocache(data, image)
            self.points_cache.put(image, points)
            self.features_cache.put(image, features)
            self.colors_cache.put(image, colors)
        if masked:
            mask = self.load_mask(data, image, points)
            if mask is not None:
                points = points[mask]
                features = features[mask]
                colors = colors[mask]
        return points, features, colors

    def load_features_index(self, data, image, masked=False):
        cache = self.masked_index_cache if masked else self.index_cache
        cached = cache.get(image)
        if cached is None:
            _, features, _ = self.load_points_features_colors(data, image,
                                                              masked)
            index = ft.build_flann_index(features, data.config)
            cache.put(image, (features, index))
        else:
            features, index = cached
        return index

    def load_words(self, data, image, masked):
        words = self.words_cache.get(image)
        if words is None:
            words = data.load_words(image)
            self.words_cache.put(image, words)
        if masked and words is not None:
            mask = self.load_mask(data, image)
            if mask is not None:
                words = words[mask]
        return words

    def _load_features_nocache(self, data, image):
        points, features, colors = data.load_features(image)
        if points is None:
            logger.error('Could not load features for image {}'.format(image))
        else:
            points = np.array(points[:, :3], dtype=float)
        return points, features, colors
