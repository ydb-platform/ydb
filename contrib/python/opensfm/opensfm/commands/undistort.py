import logging

import cv2
import networkx as nx
import numpy as np
from six import iteritems

from opensfm import dataset
from opensfm import features
from opensfm import log
from opensfm import transformations as tf
from opensfm import types
from opensfm.context import parallel_map


logger = logging.getLogger(__name__)


class Command:
    name = 'undistort'
    help = "Save radially undistorted images"

    def add_arguments(self, parser):
        parser.add_argument('dataset', help='dataset to process')

    def run(self, args):
        data = dataset.DataSet(args.dataset)
        reconstructions = data.load_reconstruction()
        graph = data.load_tracks_graph() if data.tracks_exists() else None

        if reconstructions:
            self.undistort_reconstruction(graph, reconstructions[0], data)

    def undistort_reconstruction(self, graph, reconstruction, data):
        urec = types.Reconstruction()
        urec.points = reconstruction.points
        ugraph = nx.Graph()

        logger.debug('Undistorting the reconstruction')
        undistorted_shots = {}
        for shot in reconstruction.shots.values():
            if shot.camera.projection_type == 'perspective':
                camera = perspective_camera_from_perspective(shot.camera)
                subshots = [get_shot_with_different_camera(shot, camera)]
            elif shot.camera.projection_type == 'brown':
                camera = perspective_camera_from_brown(shot.camera)
                subshots = [get_shot_with_different_camera(shot, camera)]
            elif shot.camera.projection_type == 'fisheye':
                camera = perspective_camera_from_fisheye(shot.camera)
                subshots = [get_shot_with_different_camera(shot, camera)]
            elif shot.camera.projection_type in ['equirectangular', 'spherical']:
                subshot_width = int(data.config['depthmap_resolution'])
                subshots = perspective_views_of_a_panorama(shot, subshot_width)

            for subshot in subshots:
                urec.add_camera(subshot.camera)
                urec.add_shot(subshot)
                if graph:
                    add_subshot_tracks(graph, ugraph, shot, subshot)
            undistorted_shots[shot.id] = subshots

        data.save_undistorted_reconstruction([urec])
        if graph:
            data.save_undistorted_tracks_graph(ugraph)

        arguments = []
        for shot in reconstruction.shots.values():
            arguments.append((shot, undistorted_shots[shot.id], data))

        processes = data.config['processes']
        parallel_map(undistort_image_and_masks, arguments, processes)


def undistort_image_and_masks(arguments):
    shot, undistorted_shots, data = arguments
    log.setup()
    logger.debug('Undistorting image {}'.format(shot.id))

    # Undistort image
    image = data.load_image(shot.id)
    if image is not None:
        max_size = data.config['undistorted_image_max_size']
        undistorted = undistort_image(shot, undistorted_shots, image,
                                      cv2.INTER_AREA, max_size)
        for k, v in undistorted.items():
            data.save_undistorted_image(k, v)

    # Undistort mask
    mask = data.load_mask(shot.id)
    if mask is not None:
        undistorted = undistort_image(shot, undistorted_shots, mask,
                                      cv2.INTER_NEAREST, 1e9)
        for k, v in undistorted.items():
            data.save_undistorted_mask(k, v)

    # Undistort segmentation
    segmentation = data.load_segmentation(shot.id)
    if segmentation is not None:
        undistorted = undistort_image(shot, undistorted_shots, segmentation,
                                      cv2.INTER_NEAREST, 1e9)
        for k, v in undistorted.items():
            data.save_undistorted_segmentation(k, v)

    # Undistort detections
    detection = data.load_detection(shot.id)
    if detection is not None:
        undistorted = undistort_image(shot, undistorted_shots, detection,
                                      cv2.INTER_NEAREST, 1e9)
        for k, v in undistorted.items():
            data.save_undistorted_detection(k, v)


def undistort_image(shot, undistorted_shots, original, interpolation,
                    max_size):
    """Undistort an image into a set of undistorted ones.

    Args:
        shot: the distorted shot
        undistorted_shots: the set of undistorted shots covering the
            distorted shot field of view. That is 1 for most camera
            types and 6 for equirectangular cameras.
        original: the original distorted image array.
        interpolation: the opencv interpolation flag to use.
        max_size: maximum size of the undistorted image.
    """
    if original is None:
        return

    projection_type = shot.camera.projection_type
    if projection_type in ['perspective', 'brown', 'fisheye']:
        undistort_function = {
            'perspective': undistort_perspective_image,
            'brown': undistort_brown_image,
            'fisheye': undistort_fisheye_image,
        }
        new_camera = undistorted_shots[0].camera
        uf = undistort_function[projection_type]
        undistorted = uf(original, shot.camera, new_camera, interpolation)
        return {shot.id: scale_image(undistorted, max_size)}
    elif projection_type in ['equirectangular', 'spherical']:
        subshot_width = undistorted_shots[0].camera.width
        width = 4 * subshot_width
        height = width // 2
        image = cv2.resize(original, (width, height), interpolation=interpolation)
        mint = cv2.INTER_LINEAR if interpolation == cv2.INTER_AREA else interpolation
        res = {}
        for subshot in undistorted_shots:
            undistorted = render_perspective_view_of_a_panorama(
                image, shot, subshot, mint)
            res[subshot.id] = scale_image(undistorted, max_size)
        return res
    else:
        raise NotImplementedError(
            'Undistort not implemented for projection type: {}'.format(
                shot.camera.projection_type))


def scale_image(image, max_size):
    """Scale an image not to exceed max_size."""
    height, width = image.shape[:2]
    factor = max_size / float(max(height, width))
    if factor >= 1:
        return image
    width = int(round(width * factor))
    height = int(round(height * factor))
    return cv2.resize(image, (width, height), interpolation=cv2.INTER_NEAREST)


def undistort_perspective_image(image, camera, new_camera, interpolation):
    """Remove radial distortion from a perspective image."""
    height, width = image.shape[:2]
    K = camera.get_K_in_pixel_coordinates(width, height)
    distortion = np.array([camera.k1, camera.k2, 0, 0])
    new_K = new_camera.get_K_in_pixel_coordinates(width, height)
    map1, map2 = cv2.initUndistortRectifyMap(
        K, distortion, None, new_K, (width, height), cv2.CV_32FC1)
    return cv2.remap(image, map1, map2, interpolation)


def undistort_brown_image(image, camera, new_camera, interpolation):
    """Remove radial distortion from a brown image."""
    height, width = image.shape[:2]
    K = camera.get_K_in_pixel_coordinates(width, height)
    distortion = np.array([camera.k1, camera.k2, camera.p1, camera.p2, camera.k3])
    new_K = new_camera.get_K_in_pixel_coordinates(width, height)
    map1, map2 = cv2.initUndistortRectifyMap(
        K, distortion, None, new_K, (width, height), cv2.CV_32FC1)
    return cv2.remap(image, map1, map2, interpolation)


def undistort_fisheye_image(image, camera, new_camera, interpolation):
    """Remove radial distortion from a fisheye image."""
    height, width = image.shape[:2]
    K = camera.get_K_in_pixel_coordinates(width, height)
    distortion = np.array([camera.k1, camera.k2, 0, 0])
    new_K = new_camera.get_K_in_pixel_coordinates(width, height)
    map1, map2 = cv2.fisheye.initUndistortRectifyMap(
        K, distortion, None, new_K, (width, height), cv2.CV_32FC1)
    return cv2.remap(image, map1, map2, interpolation)


def get_shot_with_different_camera(shot, camera):
    """Copy shot and replace camera."""
    ushot = types.Shot()
    ushot.id = shot.id
    ushot.camera = camera
    ushot.pose = shot.pose
    ushot.metadata = shot.metadata
    return ushot


def perspective_camera_from_perspective(distorted):
    """Create an undistorted camera from a distorted."""
    camera = types.PerspectiveCamera()
    camera.id = distorted.id
    camera.width = distorted.width
    camera.height = distorted.height
    camera.focal = distorted.focal
    camera.k1 = camera.k2 = 0.0
    return camera


def perspective_camera_from_brown(brown):
    """Create a perspective camera froma a Brown camera."""
    camera = types.PerspectiveCamera()
    camera.id = brown.id
    camera.width = brown.width
    camera.height = brown.height
    camera.focal = (brown.focal_x + brown.focal_y) / 2.0
    camera.k1 = camera.k2 = 0.0
    return camera


def perspective_camera_from_fisheye(fisheye):
    """Create a perspective camera from a fisheye."""
    camera = types.PerspectiveCamera()
    camera.id = fisheye.id
    camera.width = fisheye.width
    camera.height = fisheye.height
    camera.focal = fisheye.focal
    camera.k1 = camera.k2 = 0.0
    return camera


def perspective_views_of_a_panorama(spherical_shot, width):
    """Create 6 perspective views of a panorama."""
    camera = types.PerspectiveCamera()
    camera.id = 'perspective_panorama_camera'
    camera.width = width
    camera.height = width
    camera.focal = 0.5
    camera.k1 = camera.k2 = 0.0

    names = ['front', 'left', 'back', 'right', 'top', 'bottom']
    rotations = [
        tf.rotation_matrix(-0 * np.pi / 2, (0, 1, 0)),
        tf.rotation_matrix(-1 * np.pi / 2, (0, 1, 0)),
        tf.rotation_matrix(-2 * np.pi / 2, (0, 1, 0)),
        tf.rotation_matrix(-3 * np.pi / 2, (0, 1, 0)),
        tf.rotation_matrix(-np.pi / 2, (1, 0, 0)),
        tf.rotation_matrix(+np.pi / 2, (1, 0, 0)),
    ]
    shots = []
    for name, rotation in zip(names, rotations):
        shot = types.Shot()
        shot.id = '{}_perspective_view_{}'.format(spherical_shot.id, name)
        shot.camera = camera
        R = np.dot(rotation[:3, :3], spherical_shot.pose.get_rotation_matrix())
        o = spherical_shot.pose.get_origin()
        shot.pose = types.Pose()
        shot.pose.set_rotation_matrix(R)
        shot.pose.set_origin(o)
        shots.append(shot)
    return shots


def render_perspective_view_of_a_panorama(image, panoshot, perspectiveshot,
                                          interpolation=cv2.INTER_LINEAR,
                                          borderMode=cv2.BORDER_WRAP):
    """Render a perspective view of a panorama."""
    # Get destination pixel coordinates
    dst_shape = (perspectiveshot.camera.height, perspectiveshot.camera.width)
    dst_y, dst_x = np.indices(dst_shape).astype(np.float32)
    dst_pixels_denormalized = np.column_stack([dst_x.ravel(), dst_y.ravel()])

    dst_pixels = features.normalized_image_coordinates(
        dst_pixels_denormalized,
        perspectiveshot.camera.width,
        perspectiveshot.camera.height)

    # Convert to bearing
    dst_bearings = perspectiveshot.camera.pixel_bearing_many(dst_pixels)

    # Rotate to panorama reference frame
    rotation = np.dot(panoshot.pose.get_rotation_matrix(),
                      perspectiveshot.pose.get_rotation_matrix().T)
    rotated_bearings = np.dot(dst_bearings, rotation.T)

    # Project to panorama pixels
    src_x, src_y = panoshot.camera.project((rotated_bearings[:, 0],
                                            rotated_bearings[:, 1],
                                            rotated_bearings[:, 2]))
    src_pixels = np.column_stack([src_x.ravel(), src_y.ravel()])

    src_pixels_denormalized = features.denormalized_image_coordinates(
        src_pixels, image.shape[1], image.shape[0])

    src_pixels_denormalized.shape = dst_shape + (2,)

    # Sample color
    x = src_pixels_denormalized[..., 0].astype(np.float32)
    y = src_pixels_denormalized[..., 1].astype(np.float32)
    colors = cv2.remap(image, x, y, interpolation, borderMode=borderMode)

    return colors


def add_subshot_tracks(graph, ugraph, shot, subshot):
    """Add shot tracks to the undistorted graph."""
    if shot.id not in graph:
        return

    if shot.camera.projection_type in ['equirectangular', 'spherical']:
        add_pano_subshot_tracks(graph, ugraph, shot, subshot)
    else:
        ugraph.add_node(subshot.id, bipartite=0)
        for track_id, edge in iteritems(graph[shot.id]):
            ugraph.add_node(track_id, bipartite=1)
            ugraph.add_edge(
                subshot.id, track_id,
                feature=edge['feature'],
                feature_scale=edge['feature_scale'],
                feature_id=edge['feature_id'],
                feature_color=edge['feature_color'])


def add_pano_subshot_tracks(graph, ugraph, panoshot, perspectiveshot):
    """Add edges between subshots and visible tracks."""
    ugraph.add_node(perspectiveshot.id, bipartite=0)
    for track in graph[panoshot.id]:
        edge = graph[panoshot.id][track]
        feature = edge['feature']
        bearing = panoshot.camera.pixel_bearing(feature)
        rotation = np.dot(perspectiveshot.pose.get_rotation_matrix(),
                          panoshot.pose.get_rotation_matrix().T)

        rotated_bearing = np.dot(bearing, rotation.T)
        if rotated_bearing[2] <= 0:
            continue

        perspective_feature = perspectiveshot.camera.project(rotated_bearing)
        if (perspective_feature[0] < -0.5 or
                perspective_feature[0] > 0.5 or
                perspective_feature[1] < -0.5 or
                perspective_feature[1] > 0.5):
            continue

        ugraph.add_node(track, bipartite=1)
        ugraph.add_edge(perspectiveshot.id,
                        track,
                        feature=perspective_feature,
                        feature_scale=edge['feature_scale'],
                        feature_id=edge['feature_id'],
                        feature_color=edge['feature_color'])
