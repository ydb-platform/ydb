import logging
import sys

import numpy as np
import networkx as nx

from collections import defaultdict
from itertools import combinations
from six import iteritems
from opensfm.unionfind import UnionFind


logger = logging.getLogger(__name__)


def load_features(dataset, images):
    logging.info('reading features')
    features = {}
    colors = {}
    for im in images:
        p, f, c = dataset.load_features(im)
        features[im] = p[:, :3]
        colors[im] = c
    return features, colors


def load_matches(dataset, images):
    matches = {}
    for im1 in images:
        try:
            im1_matches = dataset.load_matches(im1)
        except IOError:
            continue
        for im2 in im1_matches:
            if im2 in images:
                matches[im1, im2] = im1_matches[im2]
    return matches


def create_tracks_graph(features, colors, matches, config):
    """Link matches into tracks."""
    logger.debug('Merging features onto tracks')
    uf = UnionFind()
    for im1, im2 in matches:
        for f1, f2 in matches[im1, im2]:
            uf.union((im1, f1), (im2, f2))

    sets = {}
    for i in uf:
        p = uf[i]
        if p in sets:
            sets[p].append(i)
        else:
            sets[p] = [i]

    min_length = config['min_track_length']
    tracks = [t for t in sets.values() if _good_track(t, min_length)]
    logger.debug('Good tracks: {}'.format(len(tracks)))

    tracks_graph = nx.Graph()
    for track_id, track in enumerate(tracks):
        for image, featureid in track:
            if image not in features:
                continue
            x, y, s = features[image][featureid]
            r, g, b = colors[image][featureid]
            tracks_graph.add_node(str(image), bipartite=0)
            tracks_graph.add_node(str(track_id), bipartite=1)
            tracks_graph.add_edge(str(image),
                                  str(track_id),
                                  feature=(float(x), float(y)),
                                  feature_scale=float(s),
                                  feature_id=int(featureid),
                                  feature_color=(float(r), float(g), float(b)))

    return tracks_graph


def tracks_and_images(graph):
    """List of tracks and images in the graph."""
    tracks, images = [], []
    for n in graph.nodes(data=True):
        if n[1]['bipartite'] == 0:
            images.append(n[0])
        else:
            tracks.append(n[0])
    return tracks, images


def common_tracks(graph, im1, im2):
    """List of tracks observed in both images.

    Args:
        graph: tracks graph
        im1: name of the first image
        im2: name of the second image

    Returns:
        tuple: tracks, feature from first image, feature from second image
    """
    t1, t2 = graph[im1], graph[im2]
    tracks, p1, p2 = [], [], []
    for track in t1:
        if track in t2:
            p1.append(t1[track]['feature'])
            p2.append(t2[track]['feature'])
            tracks.append(track)
    p1 = np.array(p1)
    p2 = np.array(p2)
    return tracks, p1, p2


def all_common_tracks(graph, tracks, include_features=True, min_common=50):
    """List of tracks observed by each image pair.

    Args:
        graph: tracks graph
        tracks: list of track identifiers
        include_features: whether to include the features from the images
        min_common: the minimum number of tracks the two images need to have
            in common

    Returns:
        tuple: im1, im2 -> tuple: tracks, features from first image, features
        from second image
    """
    track_dict = defaultdict(list)
    for track in tracks:
        track_images = sorted(graph[track].keys())
        for im1, im2 in combinations(track_images, 2):
            track_dict[im1, im2].append(track)

    common_tracks = {}
    for k, v in iteritems(track_dict):
        if len(v) < min_common:
            continue
        im1, im2 = k
        if include_features:
            p1 = np.array([graph[im1][tr]['feature'] for tr in v])
            p2 = np.array([graph[im2][tr]['feature'] for tr in v])
            common_tracks[im1, im2] = (v, p1, p2)
        else:
            common_tracks[im1, im2] = v
    return common_tracks


def _good_track(track, min_length):
    if len(track) < min_length:
        return False
    images = [f[0] for f in track]
    if len(images) != len(set(images)):
        return False
    return True


TRACKS_VERSION = 1
TRACKS_HEADER = u'OPENSFM_TRACKS_VERSION'


def load_tracks_graph(fileobj):
    """ Load a tracks graph from file object """
    version = _tracks_file_version(fileobj)
    return getattr(sys.modules[__name__], '_load_tracks_graph_v%d' % version)(fileobj)


def save_tracks_graph(fileobj, graph):
    """ Save a tracks graph to some file object """
    fileobj.write((TRACKS_HEADER + u'_v%d\n') % TRACKS_VERSION)
    getattr(sys.modules[__name__], '_save_tracks_graph_v%d' % TRACKS_VERSION)(fileobj, graph)


def _tracks_file_version(fileobj):
    """ Extract tracks file version by reading header.

    Return 0 version if no vrsion/header was red
    """
    current_position = fileobj.tell()
    line = fileobj.readline()
    if line.startswith(TRACKS_HEADER):
        version = int(line.split('_v')[1])
    else:
        fileobj.seek(current_position)
        version = 0
    return version


def _load_tracks_graph_v0(fileobj):
    """ Tracks graph file base version reading

    Uses some default scale for compliancy
    """
    default_scale = 0.004  # old default reprojection_sd config
    g = nx.Graph()
    for line in fileobj:
        image, track, observation, x, y, R, G, B = line.split('\t')
        g.add_node(image, bipartite=0)
        g.add_node(track, bipartite=1)
        g.add_edge(
            image, track,
            feature=(float(x), float(y)),
            feature_scale=float(default_scale),
            feature_id=int(observation),
            feature_color=(float(R), float(G), float(B)))
    return g


def _save_tracks_graph_v0(fileobj, graph):
    """ Tracks graph file base version saving """
    for node, data in graph.nodes(data=True):
        if data['bipartite'] == 0:
            image = node
            for track, data in graph[image].items():
                x, y = data['feature']
                fid = data['feature_id']
                r, g, b = data['feature_color']
                fileobj.write(u'%s\t%s\t%d\t%g\t%g\t%g\t%g\t%g\n' % (
                    str(image), str(track), fid, x, y, r, g, b))


def _load_tracks_graph_v1(fileobj):
    """ Version 1 of tracks graph file loading

    Feature scale was added
    """
    g = nx.Graph()
    for line in fileobj:
        image, track, observation, x, y, scale, R, G, B = line.split('\t')
        g.add_node(image, bipartite=0)
        g.add_node(track, bipartite=1)
        g.add_edge(
            image, track,
            feature=(float(x), float(y)),
            feature_scale=float(scale),
            feature_id=int(observation),
            feature_color=(float(R), float(G), float(B)))
    return g


def _save_tracks_graph_v1(fileobj, graph):
    """ Version 1 of tracks graph file saving

    Feature scale was added
    """
    for node, data in graph.nodes(data=True):
        if data['bipartite'] == 0:
            image = node
            for track, data in graph[image].items():
                x, y = data['feature']
                s = data['feature_scale']
                fid = data['feature_id']
                r, g, b = data['feature_color']
                fileobj.write(u'%s\t%s\t%d\t%g\t%g\t%g\t%g\t%g\t%g\n' % (
                    str(image), str(track), fid, x, y, s, r, g, b))
