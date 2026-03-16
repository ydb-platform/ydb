import os
import logging

import numpy as np
import pyproj

from opensfm import dataset
from opensfm import io

logger = logging.getLogger(__name__)


class Command:
    name = 'export_geocoords'
    help = "Export reconstructions in geographic coordinates"

    def add_arguments(self, parser):
        parser.add_argument('dataset', help='dataset to process')
        parser.add_argument(
            '--proj',
            help='PROJ.4 projection string',
            required=True)
        parser.add_argument(
            '--transformation',
            help='Print cooordinate transformation matrix',
            action='store_true',
            default=False)
        parser.add_argument(
            '--image-positions',
            help='Export image positions',
            action='store_true',
            default=False)
        parser.add_argument(
            '--reconstruction',
            help='Export reconstruction.json',
            action='store_true',
            default=False)
        parser.add_argument(
            '--dense',
            help='Export dense point cloud (depthmaps/merged.ply)',
            action='store_true',
            default=False)
        parser.add_argument(
            '--output',
            help='Path of the output file relative to the dataset'
        )

    def run(self, args):
        if not (args.transformation or
                args.image_positions or
                args.reconstruction or
                args.dense):
            print('Nothing to do. At least on of the options: ')
            print(' --transformation, --image-positions, --reconstruction, --dense')

        data = dataset.DataSet(args.dataset)
        reference = data.load_reference()

        projection = pyproj.Proj(args.proj)
        transformation = self._get_transformation(reference, projection)

        if args.transformation:
            output = args.output or 'geocoords_transformation.txt'
            output_path = os.path.join(data.data_path, output)
            self._write_transformation(transformation, output_path)

        if args.image_positions:
            reconstructions = data.load_reconstruction()
            output = args.output or 'image_geocoords.tsv'
            output_path = os.path.join(data.data_path, output)
            self._transform_image_positions(reconstructions, transformation,
                                            output_path)

        if args.reconstruction:
            reconstructions = data.load_reconstruction()
            for r in reconstructions:
                self._transform_reconstruction(r, transformation)
            output = args.output or 'reconstruction.geocoords.json'
            data.save_reconstruction(reconstructions, output)

        if args.dense:
            output = args.output or 'depthmaps/merged.geocoords.ply'
            self._transform_dense_point_cloud(data, transformation, output)

    def _get_transformation(self, reference, projection):
        """Get the linear transform from reconstruction coords to geocoords."""
        p = [[1, 0, 0],
             [0, 1, 0],
             [0, 0, 1],
             [0, 0, 0]]
        q = [self._transform(point, reference, projection) for point in p]

        transformation = np.array([
            [q[0][0] - q[3][0], q[1][0] - q[3][0], q[2][0] - q[3][0], q[3][0]],
            [q[0][1] - q[3][1], q[1][1] - q[3][1], q[2][1] - q[3][1], q[3][1]],
            [q[0][2] - q[3][2], q[1][2] - q[3][2], q[2][2] - q[3][2], q[3][2]],
            [0, 0, 0, 1]
        ])
        return transformation

    def _write_transformation(self, transformation, filename):
        """Write the 4x4 matrix transformation to a text file."""
        with io.open_wt(filename) as fout:
            for row in transformation:
                fout.write(u' '.join(map(str, row)))
                fout.write(u'\n')

    def _transform(self, point, reference, projection):
        """Transform on point from local coords to a proj4 projection."""
        lat, lon, altitude = reference.to_lla(point[0], point[1], point[2])
        easting, northing = projection(lon, lat)
        return [easting, northing, altitude]

    def _transform_image_positions(self, reconstructions, transformation, output):
        A, b = transformation[:3, :3], transformation[:3, 3]

        rows = ['Image\tX\tY\tZ']
        for r in reconstructions:
            for shot in r.shots.values():
                o = shot.pose.get_origin()
                to = np.dot(A, o) + b
                row = [shot.id, to[0], to[1], to[2]]
                rows.append('\t'.join(map(str, row)))

        text = '\n'.join(rows + [''])
        with open(output, 'w') as fout:
            fout.write(text)

    def _transform_reconstruction(self, reconstruction, transformation):
        """Apply a transformation to a reconstruction in-place."""
        A, b = transformation[:3, :3], transformation[:3, 3]
        A1 = np.linalg.inv(A)
        b1 = -np.dot(A1, b)

        for shot in reconstruction.shots.values():
            R = shot.pose.get_rotation_matrix()
            t = shot.pose.translation
            shot.pose.set_rotation_matrix(np.dot(R, A1))
            shot.pose.translation = list(np.dot(R, b1) + t)

        for point in reconstruction.points.values():
            point.coordinates = list(np.dot(A, point.coordinates) + b)

    def _transform_dense_point_cloud(self, data, transformation, output):
        """Apply a transformation to the merged point cloud."""
        A, b = transformation[:3, :3], transformation[:3, 3]
        input_path = os.path.join(data._depthmap_path(), 'merged.ply')
        output_path = os.path.join(data.data_path, output)
        with io.open_rt(input_path) as fin:
            with io.open_wt(output_path) as fout:
                for i, line in enumerate(fin):
                    if i < 13:
                        fout.write(line)
                    else:
                        x, y, z, nx, ny, nz, red, green, blue = line.split()
                        x, y, z = np.dot(A, map(float, [x, y, z])) + b
                        nx, ny, nz = np.dot(A, map(float, [nx, ny, nz]))
                        fout.write(
                            "{} {} {} {} {} {} {} {} {}\n".format(
                                x, y, z, nx, ny, nz, red, green, blue))
