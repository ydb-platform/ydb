import logging

from opensfm import dataset
from opensfm import dense

logger = logging.getLogger(__name__)


class Command:
    name = 'compute_depthmaps'
    help = "Compute depthmap"

    def add_arguments(self, parser):
        parser.add_argument('dataset', help='dataset to process')
        parser.add_argument('--interactive',
                            help='plot results as they are being computed',
                            action='store_true')

    def run(self, args):
        data = dataset.DataSet(args.dataset)
        data.config['interactive'] = args.interactive
        reconstructions = data.load_undistorted_reconstruction()
        graph = data.load_undistorted_tracks_graph()

        dense.compute_depthmaps(data, graph, reconstructions[0])
