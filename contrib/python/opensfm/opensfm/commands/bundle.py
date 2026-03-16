import logging
import time

import opensfm.reconstruction as orec
from opensfm import dataset

logger = logging.getLogger(__name__)


class Command:
    name = 'bundle'
    help = "Bundle a reconstruction"

    def add_arguments(self, parser):
        parser.add_argument('dataset', help="dataset to process")
        parser.add_argument(
            '--input',
            help="file name of the reconstruction to bundle")
        parser.add_argument(
            '--output',
            help="file name where to store the bundled reconstruction")

    def run(self, args):
        start = time.time()
        data = dataset.DataSet(args.dataset)
        graph = data.load_tracks_graph()
        reconstructions = data.load_reconstruction(args.input)
        camera_priors = data.load_camera_models()
        gcp = data.load_ground_control_points()

        for reconstruction in reconstructions:
            orec.bundle(graph, reconstruction, camera_priors, gcp, data.config)

        end = time.time()
        with open(data.profile_log(), 'a') as fout:
            fout.write('bundle: {0}\n'.format(end - start))
        data.save_reconstruction(reconstructions, args.output)
