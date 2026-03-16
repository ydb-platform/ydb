from __future__ import unicode_literals

import logging
import os

from opensfm import dataset
from opensfm import transformations as tf
from opensfm import io
from opensfm import types
from six import iteritems

logger = logging.getLogger(__name__)

class Command:
    name = 'export_visualsfm'
    help = "Export reconstruction to NVM_V3 format from VisualSfM"

    def add_arguments(self, parser):
        parser.add_argument('dataset', help='dataset to process')
        parser.add_argument('--undistorted',
                            action='store_true',
                            help='export the undistorted reconstruction')
        parser.add_argument('--points',
                            action='store_true',
                            help='export points')

    def run(self, args):
        data = dataset.DataSet(args.dataset)
        if args.undistorted:
            reconstructions = data.load_undistorted_reconstruction()
            graph = data.load_undistorted_tracks_graph()
        else:
            reconstructions = data.load_reconstruction()
            graph = data.load_tracks_graph()

        if reconstructions:
            self.export(reconstructions[0], graph, data, args.points)

    def export(self, reconstruction, graph, data, with_points):
        lines = ['NVM_V3', '', str(len(reconstruction.shots))]
        shot_size_cache = {}
        shot_index = {}
        i = 0

        for shot in reconstruction.shots.values():
            q = tf.quaternion_from_matrix(shot.pose.get_rotation_matrix())
            o = shot.pose.get_origin()

            shot_size_cache[shot.id] = data.undistorted_image_size(shot.id)
            shot_index[shot.id] = i
            i += 1

            if type(shot.camera) == types.BrownPerspectiveCamera:
                # Will aproximate Brown model, not optimal
                focal_normalized = shot.camera.focal_x
            else:
                focal_normalized = shot.camera.focal

            words = [
                self.image_path(shot.id, data),
                focal_normalized * max(shot_size_cache[shot.id]),
                q[0], q[1], q[2], q[3],
                o[0], o[1], o[2],
                '0', '0',
            ]
            lines.append(' '.join(map(str, words)))
        
        if with_points:
            lines.append('')
            points = reconstruction.points
            lines.append(str(len(points)))

            for point_id, point in iteritems(points):
                shots = reconstruction.shots
                coord = point.coordinates
                color = list(map(int, point.color))

                view_list = graph[point_id]
                view_line = []

                for shot_key, view in iteritems(view_list):
                    if shot_key in shots.keys():
                        v = view['feature']
                        x = (0.5 + v[0]) * shot_size_cache[shot_key][1]
                        y = (0.5 + v[1]) * shot_size_cache[shot_key][0]
                        view_line.append(' '.join(
                            map(str, [shot_index[shot_key], view['feature_id'], x, y])))
                
                lines.append(' '.join(map(str, coord)) + ' ' + 
                             ' '.join(map(str, color)) + ' ' + 
                             str(len(view_line)) + ' ' + ' '.join(view_line))
        else:
            lines += ['0', '']

        lines += ['0', '', '0']

        with io.open_wt(data.data_path + '/reconstruction.nvm') as fout:
            fout.write('\n'.join(lines))

    def image_path(self, image, data):
        """Path to the undistorted image relative to the dataset path."""
        path = data._undistorted_image_file(image)
        return os.path.relpath(path, data.data_path)
