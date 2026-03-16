import logging
from timeit import default_timer as timer

from networkx.algorithms import bipartite

from opensfm import dataset
from opensfm import io
from opensfm import tracking

logger = logging.getLogger(__name__)


class Command:
    name = 'create_tracks'
    help = "Link matches pair-wise matches into tracks"

    def add_arguments(self, parser):
        parser.add_argument('dataset', help='dataset to process')

    def run(self, args):
        data = dataset.DataSet(args.dataset)

        start = timer()
        features, colors = tracking.load_features(data, data.images())
        features_end = timer()
        matches = tracking.load_matches(data, data.images())
        matches_end = timer()
        graph = tracking.create_tracks_graph(features, colors, matches,
                                             data.config)
        tracks_end = timer()
        data.save_tracks_graph(graph)
        end = timer()

        with open(data.profile_log(), 'a') as fout:
            fout.write('create_tracks: {0}\n'.format(end - start))

        self.write_report(data,
                          graph,
                          features_end - start,
                          matches_end - features_end,
                          tracks_end - matches_end)

    def write_report(self, data, graph,
                     features_time, matches_time, tracks_time):
        tracks, images = tracking.tracks_and_images(graph)
        image_graph = bipartite.weighted_projected_graph(graph, images)
        view_graph = []
        for im1 in data.images():
            for im2 in data.images():
                if im1 in image_graph and im2 in image_graph[im1]:
                    weight = image_graph[im1][im2]['weight']
                    view_graph.append((im1, im2, weight))

        report = {
            "wall_times": {
                "load_features": features_time,
                "load_matches": matches_time,
                "compute_tracks": tracks_time,
            },
            "wall_time": features_time + matches_time + tracks_time,
            "num_images": len(images),
            "num_tracks": len(tracks),
            "view_graph": view_graph
        }
        data.save_report(io.json_dumps(report), 'tracks.json')
