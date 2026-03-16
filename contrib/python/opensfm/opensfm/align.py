"""Tools to align a reconstruction to GPS and GCP data."""

import logging
import math

import numpy as np

from opensfm import csfm
from opensfm import multiview
from opensfm import transformations as tf

logger = logging.getLogger(__name__)


def align_reconstruction(reconstruction, gcp, config):
    """Align a reconstruction with GPS and GCP data."""
    res = align_reconstruction_similarity(reconstruction, gcp, config)
    if res:
        s, A, b = res
        apply_similarity(reconstruction, s, A, b)


def apply_similarity(reconstruction, s, A, b):
    """Apply a similarity (y = s A x + b) to a reconstruction.

    :param reconstruction: The reconstruction to transform.
    :param s: The scale (a scalar)
    :param A: The rotation matrix (3x3)
    :param b: The translation vector (3)
    """
    # Align points.
    for point in reconstruction.points.values():
        Xp = s * A.dot(point.coordinates) + b
        point.coordinates = Xp.tolist()

    # Align cameras.
    for shot in reconstruction.shots.values():
        R = shot.pose.get_rotation_matrix()
        t = np.array(shot.pose.translation)
        Rp = R.dot(A.T)
        tp = -Rp.dot(b) + s * t
        shot.pose.set_rotation_matrix(Rp)
        shot.pose.translation = list(tp)


def align_reconstruction_similarity(reconstruction, gcp, config):
    """Align reconstruction with GPS and GCP data.

    Config parameter `align_method` can be used to choose the alignment method.
    Accepted values are
     - navie: does a direct 3D-3D fit
     - orientation_prior: assumes a particular camera orientation
    """
    align_method = config['align_method']
    if align_method == 'auto':
        align_method = detect_alignment_constraints(config, reconstruction, gcp)
    if align_method == 'orientation_prior':
        res = align_reconstruction_orientation_prior_similarity(reconstruction, config, gcp)
    elif align_method == 'naive':
        res = align_reconstruction_naive_similarity(config, reconstruction, gcp)

    s, A, b = res
    if (s == 0) or np.isnan(A).any() or np.isnan(b).any():
        logger.warning('Computation of alignment similarity (%s) is degenerate.' % align_method)
        return None
    return res


def alignment_constraints(config, reconstruction, gcp):
    """ Gather alignment constraints to be used by checking bundle_use_gcp and bundle_use_gps. """

    X, Xp = [], []

    # Get Ground Control Point correspondences
    if gcp and config['bundle_use_gcp']:
        triangulated, measured = triangulate_all_gcp(reconstruction, gcp)
        X.extend(triangulated)
        Xp.extend(measured)

    # Get camera center correspondences
    if config['bundle_use_gps']:
        for shot in reconstruction.shots.values():
            X.append(shot.pose.get_origin())
            Xp.append(shot.metadata.gps_position)

    return X, Xp


def detect_alignment_constraints(config, reconstruction, gcp):
    """ Automatically pick the best alignment method, depending
    if alignment data such as GPS/GCP is aligned on a single-line or not.

    """

    X, Xp = alignment_constraints(config, reconstruction, gcp)
    if len(X) < 3:
        return 'orientation_prior'

    X = np.array(X)
    X = X - np.average(X, axis=0)
    evalues, _ = np.linalg.eig(X.T.dot(X))

    evalues = np.array(sorted(evalues))
    ratio_1st_2nd = math.fabs(evalues[2]/evalues[1])

    epsilon_abs = 1e-10
    epsilon_ratio = 5e3
    is_line = sum(evalues < epsilon_abs) > 1 or ratio_1st_2nd > epsilon_ratio
    if is_line:
        logger.warning('Shots and/or GCPs are aligned on a single-line. Using %s prior',
                       config['align_orientation_prior'])
        return 'orientation_prior'
    else:
        logger.info('Shots and/or GCPs are well-conditionned. Using naive 3D-3D alignment.')
        return 'naive'


def align_reconstruction_naive_similarity(config, reconstruction, gcp):
    """Align with GPS and GCP data using direct 3D-3D matches."""
    X, Xp = alignment_constraints(config, reconstruction, gcp)

    if len(X) == 0:
        return 1.0, np.identity(3), np.zeros((3))

    # Translation-only case, either : 
    #  - a single value
    #  - identical values
    same_values = (np.linalg.norm(np.std(Xp, axis=0)) < 1e-10)
    single_value = (len(X) == 1)
    if single_value:
        logger.warning('Only 1 constraints. Using translation-only alignment.')
    if same_values:
        logger.warning('GPS/GCP data seems to have identical values. Using translation-only alignment.')
    if same_values or single_value:
        t = np.array(Xp[0]) - np.array(X[0])
        return 1.0, np.identity(3), t

    # Will be up to some unknown rotation
    if len(X) == 2:
        logger.warning('Only 2 constraints. Will be up to some unknown rotation.')
        X.append(X[1])
        Xp.append(Xp[1])

    # Compute similarity Xp = s A X + b
    X = np.array(X)
    Xp = np.array(Xp)
    T = tf.superimposition_matrix(X.T, Xp.T, scale=True)

    A, b = T[:3, :3], T[:3, 3]
    s = np.linalg.det(A)**(1. / 3)
    A /= s
    return s, A, b


def align_reconstruction_orientation_prior_similarity(reconstruction, config, gcp):
    """Align with GPS data assuming particular a camera orientation.

    In some cases, using 3D-3D matches directly fails to find proper
    orientation of the world.  That happends mainly when all cameras lie
    close to a straigh line.

    In such cases, we can impose a particular orientation of the cameras
    to improve the orientation of the alignment.  The config parameter
    `align_orientation_prior` can be used to specify such orientation.
    Accepted values are:
     - no_roll: assumes horizon is horizontal on the images
     - horizontal: assumes cameras are looking towards the horizon
     - vertical: assumes cameras are looking down towards the ground
    """
    X, Xp = alignment_constraints(config, reconstruction, gcp)
    X = np.array(X)
    Xp = np.array(Xp)

    if len(X) < 1:
        return 1.0, np.identity(3), np.zeros((3))

    p = estimate_ground_plane(reconstruction, config)
    Rplane = multiview.plane_horizontalling_rotation(p)
    X = Rplane.dot(X.T).T

    # Estimate 2d similarity to align to GPS
    single_shot = len(X) < 2
    same_shots = (X.std(axis=0).max() < 1e-8 or     # All points are the same.
                  Xp.std(axis=0).max() < 0.01)      # All GPS points are the same.
    if single_shot or same_shots:
        s = 1.0
        A = Rplane
        b = Xp.mean(axis=0) - X.mean(axis=0)
    else:
        T = tf.affine_matrix_from_points(X.T[:2], Xp.T[:2], shear=False)
        s = np.linalg.det(T[:2, :2])**0.5
        A = np.eye(3)
        A[:2, :2] = T[:2, :2] / s
        A = A.dot(Rplane)
        b = np.array([
            T[0, 2],
            T[1, 2],
            Xp[:, 2].mean() - s * X[:, 2].mean()  # vertical alignment
        ])
    return s, A, b


def estimate_ground_plane(reconstruction, config):
    """Estimate ground plane orientation.
    
    It assumes cameras are all at a similar height and uses the
    align_orientation_prior option to enforce cameras to look
    horizontally or vertically.
    """
    orientation_type = config['align_orientation_prior']
    onplane, verticals = [], []
    for shot in reconstruction.shots.values():
        R = shot.pose.get_rotation_matrix()
        x, y, z = get_horizontal_and_vertical_directions(
            R, shot.metadata.orientation)
        if orientation_type == 'no_roll':
            onplane.append(x)
            verticals.append(-y)
        elif orientation_type == 'horizontal':
            onplane.append(x)
            onplane.append(z)
            verticals.append(-y)
        elif orientation_type == 'vertical':
            onplane.append(x)
            onplane.append(y)
            verticals.append(-z)

    ground_points = []
    for shot in reconstruction.shots.values():
        ground_points.append(shot.pose.get_origin())
    ground_points = np.array(ground_points)
    ground_points -= ground_points.mean(axis=0)
    
    plane = multiview.fit_plane(ground_points, onplane, verticals)
    return plane
    

def get_horizontal_and_vertical_directions(R, orientation):
    """Get orientation vectors from camera rotation matrix and orientation tag.

    Return a 3D vectors pointing to the positive XYZ directions of the image.
    X points to the right, Y to the bottom, Z to the front.
    """
    # See http://sylvana.net/jpegcrop/exif_orientation.html
    if orientation == 1:
        return R[0, :], R[1, :], R[2, :]
    if orientation == 2:
        return -R[0, :], R[1, :], -R[2, :]
    if orientation == 3:
        return -R[0, :], -R[1, :], R[2, :]
    if orientation == 4:
        return R[0, :], -R[1, :], R[2, :]
    if orientation == 5:
        return R[1, :], R[0, :], -R[2, :]
    if orientation == 6:
        return -R[1, :], R[0, :], R[2, :]
    if orientation == 7:
        return -R[1, :], -R[0, :], -R[2, :]
    if orientation == 8:
        return R[1, :], -R[0, :], R[2, :]
    logger.error('unknown orientation {0}. Using 1 instead'.format(orientation))
    return R[0, :], R[1, :], R[2, :]


def triangulate_single_gcp(reconstruction, observations):
    """Triangulate one Ground Control Point."""
    reproj_threshold = 0.004
    min_ray_angle_degrees = 2.0

    os, bs = [], []
    for o in observations:
        if o.shot_id in reconstruction.shots:
            shot = reconstruction.shots[o.shot_id]
            os.append(shot.pose.get_origin())
            b = shot.camera.pixel_bearing(np.asarray(o.projection))
            r = shot.pose.get_rotation_matrix().T
            bs.append(r.dot(b))

    if len(os) >= 2:
        thresholds = len(os) * [reproj_threshold]
        angle = np.radians(min_ray_angle_degrees)
        e, X = csfm.triangulate_bearings_midpoint(os, bs, thresholds, angle)
        return X


def triangulate_all_gcp(reconstruction, gcp):
    """Group and triangulate Ground Control Points seen in 2+ images."""
    triangulated, measured = [], []
    for point in gcp:
        x = triangulate_single_gcp(reconstruction, point.observations)
        if x is not None:
            triangulated.append(x)
            measured.append(point.coordinates)
    return triangulated, measured
