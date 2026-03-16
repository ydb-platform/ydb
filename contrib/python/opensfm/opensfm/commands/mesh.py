import logging
import time

from opensfm import dataset
from opensfm import mesh
from opensfm import types

logger = logging.getLogger(__name__)


class Command:
    name = 'mesh'
    help = "Add delaunay meshes to the reconstruction"

    def add_arguments(self, parser):
        parser.add_argument('dataset', help='dataset to process')

    def run(self, args):
        start = time.time()
        data = dataset.DataSet(args.dataset)
        graph = data.load_tracks_graph()
        reconstructions = data.load_reconstruction()

        for i, r in enumerate(reconstructions):
            for shot in r.shots.values():
                if shot.id in graph:
                    vertices, faces = mesh.triangle_mesh(shot.id, r, graph,
                                                         data)
                    shot.mesh = types.ShotMesh()
                    shot.mesh.vertices = vertices
                    shot.mesh.faces = faces

        data.save_reconstruction(reconstructions,
                                 filename='reconstruction.meshed.json',
                                 minify=True)

        end = time.time()
        with open(data.profile_log(), 'a') as fout:
            fout.write('mesh: {0}\n'.format(end - start))
