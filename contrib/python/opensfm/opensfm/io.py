from __future__ import absolute_import
from __future__ import unicode_literals
from __future__ import division
from __future__ import print_function

import errno
import io
import json
import logging
import os
import sys

import cv2
import numpy as np
import pyproj
from PIL import Image
from six import iteritems

from opensfm import geo
from opensfm import features
from opensfm import types
from opensfm import context


logger = logging.getLogger(__name__)


def camera_from_json(key, obj):
    """
    Read camera from a json object
    """
    pt = obj.get('projection_type', 'perspective')
    if pt == 'perspective':
        camera = types.PerspectiveCamera()
        camera.id = key
        camera.width = obj.get('width', 0)
        camera.height = obj.get('height', 0)
        camera.focal = obj['focal']
        camera.k1 = obj.get('k1', 0.0)
        camera.k2 = obj.get('k2', 0.0)
        return camera
    if pt == 'brown':
        camera = types.BrownPerspectiveCamera()
        camera.id = key
        camera.width = obj.get('width', 0)
        camera.height = obj.get('height', 0)
        camera.focal_x = obj['focal_x']
        camera.focal_y = obj['focal_y']
        camera.c_x = obj.get('c_x', 0.0)
        camera.c_y = obj.get('c_y', 0.0)
        camera.k1 = obj.get('k1', 0.0)
        camera.k2 = obj.get('k2', 0.0)
        camera.p1 = obj.get('p1', 0.0)
        camera.p2 = obj.get('p2', 0.0)
        camera.k3 = obj.get('k3', 0.0)
        return camera
    elif pt == 'fisheye':
        camera = types.FisheyeCamera()
        camera.id = key
        camera.width = obj.get('width', 0)
        camera.height = obj.get('height', 0)
        camera.focal = obj['focal']
        camera.k1 = obj.get('k1', 0.0)
        camera.k2 = obj.get('k2', 0.0)
        return camera
    elif pt == 'dual':
        camera = types.DualCamera()
        camera.id = key
        camera.width = obj.get('width', 0)
        camera.height = obj.get('height', 0)
        camera.focal = obj['focal']
        camera.k1 = obj.get('k1', 0.0)
        camera.k2 = obj.get('k2', 0.0)
        camera.transition = obj.get('transition', 0.5)
        return camera
    elif pt in ['equirectangular', 'spherical']:
        camera = types.SphericalCamera()
        camera.id = key
        camera.width = obj['width']
        camera.height = obj['height']
        return camera
    else:
        raise NotImplementedError


def shot_from_json(key, obj, cameras):
    """
    Read shot from a json object
    """
    pose = types.Pose()
    pose.rotation = obj["rotation"]
    if "translation" in obj:
        pose.translation = obj["translation"]

    metadata = types.ShotMetadata()
    metadata.orientation = obj.get("orientation")
    metadata.capture_time = obj.get("capture_time")
    metadata.gps_dop = obj.get("gps_dop")
    metadata.gps_position = obj.get("gps_position")
    metadata.skey = obj.get("skey")

    shot = types.Shot()
    shot.id = key
    shot.metadata = metadata
    shot.pose = pose
    shot.camera = cameras.get(obj["camera"])

    if 'scale' in obj:
        shot.scale = obj['scale']
    if 'covariance' in obj:
        shot.covariance = np.array(obj['covariance'])
    if 'merge_cc' in obj:
        shot.merge_cc = obj['merge_cc']
    if 'vertices' in obj and 'faces' in obj:
        shot.mesh = types.ShotMesh()
        shot.mesh.vertices = obj['vertices']
        shot.mesh.faces = obj['faces']

    return shot


def point_from_json(key, obj):
    """
    Read a point from a json object
    """
    point = types.Point()
    point.id = key
    point.color = obj["color"]
    point.coordinates = obj["coordinates"]
    return point


def reconstruction_from_json(obj):
    """
    Read a reconstruction from a json object
    """
    reconstruction = types.Reconstruction()

    # Extract cameras
    for key, value in iteritems(obj['cameras']):
        camera = camera_from_json(key, value)
        reconstruction.add_camera(camera)

    # Extract shots
    for key, value in iteritems(obj['shots']):
        shot = shot_from_json(key, value, reconstruction.cameras)
        reconstruction.add_shot(shot)

    # Extract points
    if 'points' in obj:
        for key, value in iteritems(obj['points']):
            point = point_from_json(key, value)
            reconstruction.add_point(point)

    # Extract pano_shots
    if 'pano_shots' in obj:
        reconstruction.pano_shots = {}
        for key, value in iteritems(obj['pano_shots']):
            shot = shot_from_json(key, value, reconstruction.cameras)
            reconstruction.pano_shots[shot.id] = shot

    # Extract main and unit shots
    if 'main_shot' in obj:
        reconstruction.main_shot = obj['main_shot']
    if 'unit_shot' in obj:
        reconstruction.unit_shot = obj['unit_shot']

    # Extract reference topocentric frame
    if 'reference_lla' in obj:
        lla = obj['reference_lla']
        reconstruction.reference = geo.TopocentricConverter(
            lla['latitude'], lla['longitude'], lla['altitude'])

    return reconstruction


def reconstructions_from_json(obj):
    """
    Read all reconstructions from a json object
    """
    return [reconstruction_from_json(i) for i in obj]


def cameras_from_json(obj):
    """
    Read cameras from a json object
    """
    cameras = {}
    for key, value in iteritems(obj):
        cameras[key] = camera_from_json(key, value)
    return cameras


def camera_to_json(camera):
    """
    Write camera to a json object
    """
    if camera.projection_type == 'perspective':
        return {
            'projection_type': camera.projection_type,
            'width': camera.width,
            'height': camera.height,
            'focal': camera.focal,
            'k1': camera.k1,
            'k2': camera.k2,
        }
    elif camera.projection_type == 'brown':
        return {
            'projection_type': camera.projection_type,
            'width': camera.width,
            'height': camera.height,
            'focal_x': camera.focal_x,
            'focal_y': camera.focal_y,
            'c_x': camera.c_x,
            'c_y': camera.c_y,
            'k1': camera.k1,
            'k2': camera.k2,
            'p1': camera.p1,
            'p2': camera.p2,
            'k3': camera.k3,
        }
    elif camera.projection_type == 'fisheye':
        return {
            'projection_type': camera.projection_type,
            'width': camera.width,
            'height': camera.height,
            'focal': camera.focal,
            'k1': camera.k1,
            'k2': camera.k2,
        }
    elif camera.projection_type == 'dual':
        return {
            'projection_type': camera.projection_type,
            'width': camera.width,
            'height': camera.height,
            'focal': camera.focal,
            'k1': camera.k1,
            'k2': camera.k2,
            'transition': camera.transition
        }
    elif camera.projection_type in ['equirectangular', 'spherical']:
        return {
            'projection_type': camera.projection_type,
            'width': camera.width,
            'height': camera.height
        }
    else:
        raise NotImplementedError


def shot_to_json(shot):
    """
    Write shot to a json object
    """
    obj = {
        'rotation': list(shot.pose.rotation),
        'translation': list(shot.pose.translation),
        'camera': shot.camera.id
    }
    if shot.metadata is not None:
        if shot.metadata.orientation is not None:
            obj['orientation'] = shot.metadata.orientation
        if shot.metadata.capture_time is not None:
            obj['capture_time'] = shot.metadata.capture_time
        if shot.metadata.gps_dop is not None:
            obj['gps_dop'] = shot.metadata.gps_dop
        if shot.metadata.gps_position is not None:
            obj['gps_position'] = shot.metadata.gps_position
        if shot.metadata.accelerometer is not None:
            obj['accelerometer'] = shot.metadata.accelerometer
        if shot.metadata.compass is not None:
            obj['compass'] = shot.metadata.compass
        if shot.metadata.skey is not None:
            obj['skey'] = shot.metadata.skey
    if shot.mesh is not None:
        obj['vertices'] = shot.mesh.vertices
        obj['faces'] = shot.mesh.faces
    if hasattr(shot, 'scale'):
        obj['scale'] = shot.scale
    if hasattr(shot, 'covariance'):
        obj['covariance'] = shot.covariance.tolist()
    if hasattr(shot, 'merge_cc'):
        obj['merge_cc'] = shot.merge_cc
    return obj


def point_to_json(point):
    """
    Write a point to a json object
    """
    return {
        'color': list(point.color),
        'coordinates': list(point.coordinates)
    }


def reconstruction_to_json(reconstruction):
    """
    Write a reconstruction to a json object
    """
    obj = {
        "cameras": {},
        "shots": {},
        "points": {}
    }

    # Extract cameras
    for camera in reconstruction.cameras.values():
        obj['cameras'][camera.id] = camera_to_json(camera)

    # Extract shots
    for shot in reconstruction.shots.values():
        obj['shots'][shot.id] = shot_to_json(shot)

    # Extract points
    for point in reconstruction.points.values():
        obj['points'][point.id] = point_to_json(point)

    # Extract pano_shots
    if hasattr(reconstruction, 'pano_shots'):
        obj['pano_shots'] = {}
        for shot in reconstruction.pano_shots.values():
            obj['pano_shots'][shot.id] = shot_to_json(shot)

    # Extract main and unit shots
    if hasattr(reconstruction, 'main_shot'):
        obj['main_shot'] = reconstruction.main_shot
    if hasattr(reconstruction, 'unit_shot'):
        obj['unit_shot'] = reconstruction.unit_shot

    # Extract reference topocentric frame
    if reconstruction.reference:
        ref = reconstruction.reference
        obj['reference_lla'] = {
            'latitude': ref.lat,
            'longitude': ref.lon,
            'altitude': ref.alt,
        }

    return obj


def reconstructions_to_json(reconstructions):
    """
    Write all reconstructions to a json object
    """
    return [reconstruction_to_json(i) for i in reconstructions]


def cameras_to_json(cameras):
    """
    Write cameras to a json object
    """
    obj = {}
    for camera in cameras.values():
        obj[camera.id] = camera_to_json(camera)
    return obj


def _read_gcp_list_lines(lines, projection, reference, exif):
    points = {}
    for line in lines:
        words = line.split(None, 5)
        easting, northing, alt, pixel_x, pixel_y = map(float, words[:5])
        shot_id = words[5].strip()
        key = (easting, northing, alt)

        if key in points:
            point = points[key]
        else:
            # Convert 3D coordinates
            if np.isnan(alt):
                alt = 0
                has_altitude = False
            else:
                has_altitude = True
            if projection is not None:
                lon, lat = projection(easting, northing, inverse=True)
            else:
                lon, lat = easting, northing

            point = types.GroundControlPoint()
            point.id = "unnamed-%d" % len(points)
            point.lla = np.array([lat, lon, alt])
            point.has_altitude = has_altitude

            if reference:
                x, y, z = reference.to_topocentric(lat, lon, alt)
                point.coordinates = np.array([x, y, z])
            else:
                point.coordinates = None

            points[key] = point

        # Convert 2D coordinates
        d = exif[shot_id]
        coordinates = features.normalized_image_coordinates(
            np.array([[pixel_x, pixel_y]]), d['width'], d['height'])[0]

        o = types.GroundControlPointObservation()
        o.shot_id = shot_id
        o.projection = coordinates
        point.observations.append(o)

    return list(points.values())


def _parse_utm_projection_string(line):
    """Convert strings like 'WGS84 UTM 32N' to a proj4 definition."""
    words = line.lower().split()
    assert len(words) == 3
    zone = line.split()[2].upper()
    if zone[-1] == 'N':
        zone_number = int(zone[:-1])
        zone_hemisphere = 'north'
    elif zone[-1] == 'S':
        zone_number = int(zone[:-1])
        zone_hemisphere = 'south'
    else:
        zone_number = int(zone)
        zone_hemisphere = 'north'
    s = '+proj=utm +zone={} +{} +ellps=WGS84 +datum=WGS84 +units=m +no_defs'
    return s.format(zone_number, zone_hemisphere)


def _parse_projection(line):
    """Build a proj4 from the GCP format line."""
    if line.strip() == 'WGS84':
        return None
    elif line.upper().startswith('WGS84 UTM'):
        return pyproj.Proj(_parse_utm_projection_string(line))
    elif '+proj' in line:
        return pyproj.Proj(line)
    else:
        raise ValueError("Un-supported geo system definition: {}".format(line))


def _valid_gcp_line(line):
    stripped = line.strip()
    return stripped and stripped[0] != '#'


def read_gcp_list(fileobj, reference, exif):
    """Read a ground control points from a gcp_list.txt file.

    It requires the points to be in the WGS84 lat, lon, alt format.
    If reference is None, topocentric data won't be initialized.
    """
    all_lines = fileobj.readlines()
    lines = iter(filter(_valid_gcp_line, all_lines))
    projection = _parse_projection(next(lines))
    points = _read_gcp_list_lines(lines, projection, reference, exif)
    return points


def read_ground_control_points(fileobj, reference):
    """Read ground control points from json file.

    Returns list of types.GroundControlPoint.
    """
    obj = json_load(fileobj)

    points = []
    for point_dict in obj['points']:
        point = types.GroundControlPoint()
        point.id = point_dict['id']
        point.lla = point_dict.get('position')
        if point.lla:
            point.has_altitude = ('altitude' in point.lla)
            if reference:
                point.coordinates = reference.to_topocentric(
                    point.lla['latitude'],
                    point.lla['longitude'],
                    point.lla.get('altitude', 0))
            else:
                point.coordinates = None

        point.observations = []
        for o_dict in point_dict['observations']:
            o = types.GroundControlPointObservation()
            o.shot_id = o_dict['shot_id']
            if 'projection' in o_dict:
                o.projection = np.array(o_dict['projection'])
            point.observations.append(o)
        points.append(point)
    return points


def write_ground_control_points(gcp, fileobj, reference):
    """Write ground control points to json file."""
    obj = {"points": []}

    for point in gcp:
        point_obj = {}
        point_obj['id'] = point.id
        if point.lla:
            point_obj['position'] = {
                'latitude': point.lla['latitude'],
                'longitude': point.lla['longitude'],
            }
            if point.has_altitude:
                point_obj['position']['altitude'] = point.lla['altitude']
        elif point.coordinates:
            lat, lon, alt = reference.to_lla(*point.coordinates)
            point_obj['position'] = {
                'latitude': lat,
                'longitude': lon,
            }
            if point.has_altitude:
                point_obj['position']['altitude'] = alt

        point_obj['observations'] = []
        for observation in point.observations:
            point_obj['observations'].append({
                'shot_id': observation.shot_id,
                'projection': tuple(observation.projection),
            })

        obj['points'].append(point_obj)

    json_dump(obj, fileobj)


def mkdir_p(path):
    '''Make a directory including parent directories.
    '''
    try:
        os.makedirs(path)
    except os.error as exc:
        if exc.errno != errno.EEXIST or not os.path.isdir(path):
            raise


def open_wt(path):
    """Open a file in text mode for writing utf-8."""
    return io.open(path, 'w', encoding='utf-8')


def open_rt(path):
    """Open a file in text mode for reading utf-8."""
    return io.open(path, 'r', encoding='utf-8')


def _json_dump_python_2_pached(
        obj, fp, skipkeys=False, ensure_ascii=True, check_circular=True,
        allow_nan=True, cls=None, indent=None, separators=None,
        encoding='utf-8', default=None, sort_keys=False, **kw):
    """Serialize ``obj`` as a JSON formatted stream to ``fp`` (a
    ``.write()``-supporting file-like object).

    If ``skipkeys`` is true then ``dict`` keys that are not basic types
    (``str``, ``unicode``, ``int``, ``long``, ``float``, ``bool``, ``None``)
    will be skipped instead of raising a ``TypeError``.

    If ``ensure_ascii`` is true (the default), all non-ASCII characters in the
    output are escaped with ``\\uXXXX`` sequences, and the result is a ``str``
    instance consisting of ASCII characters only.  If ``ensure_ascii`` is
    ``False``, some chunks written to ``fp`` may be ``unicode`` instances.
    This usually happens because the input contains unicode strings or the
    ``encoding`` parameter is used. Unless ``fp.write()`` explicitly
    understands ``unicode`` (as in ``codecs.getwriter``) this is likely to
    cause an error.

    If ``check_circular`` is false, then the circular reference check
    for container types will be skipped and a circular reference will
    result in an ``OverflowError`` (or worse).

    If ``allow_nan`` is false, then it will be a ``ValueError`` to
    serialize out of range ``float`` values (``nan``, ``inf``, ``-inf``)
    in strict compliance of the JSON specification, instead of using the
    JavaScript equivalents (``NaN``, ``Infinity``, ``-Infinity``).

    If ``indent`` is a non-negative integer, then JSON array elements and
    object members will be pretty-printed with that indent level. An indent
    level of 0 will only insert newlines. ``None`` is the most compact
    representation.  Since the default item separator is ``', '``,  the
    output might include trailing whitespace when ``indent`` is specified.
    You can use ``separators=(',', ': ')`` to avoid this.

    If ``separators`` is an ``(item_separator, dict_separator)`` tuple
    then it will be used instead of the default ``(', ', ': ')`` separators.
    ``(',', ':')`` is the most compact JSON representation.

    ``encoding`` is the character encoding for str instances, default is UTF-8.

    ``default(obj)`` is a function that should return a serializable version
    of obj or raise TypeError. The default simply raises TypeError.

    If *sort_keys* is ``True`` (default: ``False``), then the output of
    dictionaries will be sorted by key.

    To use a custom ``JSONEncoder`` subclass (e.g. one that overrides the
    ``.default()`` method to serialize additional types), specify it with
    the ``cls`` kwarg; otherwise ``JSONEncoder`` is used.

    """
    # cached encoder
    if (not skipkeys and ensure_ascii and
            check_circular and allow_nan and
            cls is None and indent is None and separators is None and
            encoding == 'utf-8' and default is None and not sort_keys and
            not kw):
        iterable = json._default_encoder.iterencode(obj)
    else:
        if cls is None:
            cls = json.JSONEncoder
        iterable = cls(
            skipkeys=skipkeys, ensure_ascii=ensure_ascii,
            check_circular=check_circular, allow_nan=allow_nan, indent=indent,
            separators=separators, encoding=encoding,
            default=default, sort_keys=sort_keys, **kw).iterencode(obj)
    # could accelerate with writelines in some versions of Python, at
    # a debuggability cost
    for chunk in iterable:
        fp.write(unicode(chunk))  # Convert chunks to unicode before writing


def json_dump_kwargs(minify=False):
    if minify:
        indent, separators = None, (',', ':')
    else:
        indent, separators = 4, None
    return dict(indent=indent, ensure_ascii=False,
                separators=separators)


def json_dump(data, fout, minify=False):
    kwargs = json_dump_kwargs(minify)
    if sys.version_info >= (3, 0):
        return json.dump(data, fout, **kwargs)
    else:
        # Python 2 json decoders can unpredictably return str or unicode
        # We use a patched json.dump function to always convert to unicode
        # See https://bugs.python.org/issue13769
        return _json_dump_python_2_pached(data, fout, **kwargs)


def json_dumps(data, minify=False):
    kwargs = json_dump_kwargs(minify)
    if sys.version_info >= (3, 0):
        return json.dumps(data, **kwargs)
    else:
        # Python 2 json decoders can unpredictably return str or unicode.
        # We use always convert to unicode
        # See https://bugs.python.org/issue13769
        return unicode(json.dumps(data, **kwargs))


def json_load(fp):
    return json.load(fp)


def json_loads(text):
    return json.loads(text)


def imread(filename, grayscale=False, unchanged=False):
    """Load image as an array ignoring EXIF orientation."""
    if context.OPENCV3:
        if grayscale:
            flags = cv2.IMREAD_GRAYSCALE
        elif unchanged:
            flags = cv2.IMREAD_UNCHANGED
        else:
            flags = cv2.IMREAD_COLOR

        try:
            flags |= cv2.IMREAD_IGNORE_ORIENTATION
        except AttributeError:
            logger.warning(
                "OpenCV version {} does not support loading images without "
                "rotating them according to EXIF. Please upgrade OpenCV to "
                "version 3.2 or newer.".format(cv2.__version__))
    else:
        if grayscale:
            flags = cv2.CV_LOAD_IMAGE_GRAYSCALE
        elif unchanged:
            flags = cv2.CV_LOAD_IMAGE_UNCHANGED
        else:
            flags = cv2.CV_LOAD_IMAGE_COLOR

    image = cv2.imread(filename, flags)

    if image is None:
        raise IOError("Unable to load image {}".format(filename))

    if len(image.shape) == 3:
        image[:, :, :3] = image[:, :, [2, 1, 0]]  # Turn BGR to RGB (or BGRA to RGBA)
    return image


def imwrite(filename, image):
    """Write an image to a file"""
    if len(image.shape) == 3:
        image[:, :, :3] = image[:, :, [2, 1, 0]]  # Turn RGB to BGR (or RGBA to BGRA)
    cv2.imwrite(filename, image)


def image_size(filename):
    """Height and width of an image."""
    try:
        with Image.open(filename) as img:
            width, height = img.size
            return height, width
    except:
        # Slower fallback
        image = imread(filename)
        return image.shape[:2]


# Bundler

def export_bundler(image_list, reconstructions, track_graph, bundle_file_path,
                   list_file_path):
    """
    Generate a reconstruction file that is consistent with Bundler's format
    """

    mkdir_p(bundle_file_path)
    mkdir_p(list_file_path)

    for j, reconstruction in enumerate(reconstructions):
        lines = []
        lines.append("# Bundle file v0.3")
        points = reconstruction.points
        shots = reconstruction.shots
        num_point = len(points)
        num_shot = len(image_list)
        lines.append(' '.join(map(str, [num_shot, num_point])))
        shots_order = {key: i for i, key in enumerate(image_list)}

        # cameras
        for shot_id in image_list:
            if shot_id in shots:
                shot = shots[shot_id]
                camera = shot.camera
                if type(camera) == types.BrownPerspectiveCamera:
                    # Will aproximate Brown model, not optimal
                    focal_normalized = camera.focal_x
                else:
                    focal_normalized = camera.focal
                scale = max(camera.width, camera.height)
                focal = focal_normalized * scale
                k1 = camera.k1
                k2 = camera.k2
                R = shot.pose.get_rotation_matrix()
                t = np.array(shot.pose.translation)
                R[1], R[2] = -R[1], -R[2]  # Reverse y and z
                t[1], t[2] = -t[1], -t[2]
                lines.append(' '.join(map(str, [focal, k1, k2])))
                for i in range(3):
                    lines.append(' '.join(map(str, R[i])))
                t = ' '.join(map(str, t))
                lines.append(t)
            else:
                for i in range(5):
                    lines.append("0 0 0")

        # tracks
        for point_id, point in iteritems(points):
            coord = point.coordinates
            color = list(map(int, point.color))
            view_list = track_graph[point_id]
            lines.append(' '.join(map(str, coord)))
            lines.append(' '.join(map(str, color)))
            view_line = []
            for shot_key, view in iteritems(view_list):
                if shot_key in shots.keys():
                    v = view['feature']
                    shot_index = shots_order[shot_key]
                    camera = shots[shot_key].camera
                    scale = max(camera.width, camera.height)
                    x = v[0] * scale
                    y = -v[1] * scale
                    view_line.append(' '.join(
                        map(str, [shot_index, view['feature_id'], x, y])))

            lines.append(str(len(view_line)) + ' ' + ' '.join(view_line))

        bundle_file = os.path.join(bundle_file_path,
                                   'bundle_r' + str(j).zfill(3) + '.out')
        with open_wt(bundle_file) as fout:
            fout.writelines('\n'.join(lines) + '\n')

        list_file = os.path.join(list_file_path,
                                 'list_r' + str(j).zfill(3) + '.out')
        with open_wt(list_file) as fout:
            fout.writelines('\n'.join(map(str, image_list)))


def import_bundler(data_path, bundle_file, list_file, track_file,
                   reconstruction_file=None):
    """
    Reconstruction and tracks graph from Bundler's output
    """

    # Init OpenSfM working folder.
    mkdir_p(data_path)

    # Copy image list.
    list_dir = os.path.dirname(list_file)
    with open_rt(list_file) as fin:
        lines = fin.read().splitlines()
    ordered_shots = []
    image_list = []
    for line in lines:
        image_path = os.path.join(list_dir, line.split()[0])
        rel_to_data = os.path.relpath(image_path, data_path)
        image_list.append(rel_to_data)
        ordered_shots.append(os.path.basename(image_path))
    with open_wt(os.path.join(data_path, 'image_list.txt')) as fout:
        fout.write('\n'.join(image_list) + '\n')

    # Check for bundle_file
    if not bundle_file or not os.path.isfile(bundle_file):
        return None

    with open_rt(bundle_file) as fin:
        lines = fin.readlines()
    offset = 1 if '#' in lines[0] else 0

    # header
    num_shot, num_point = map(int, lines[offset].split(' '))
    offset += 1

    # initialization
    reconstruction = types.Reconstruction()

    # cameras
    for i in range(num_shot):
        # Creating a model for each shot.
        shot_key = ordered_shots[i]
        focal, k1, k2 = map(float, lines[offset].rstrip('\n').split(' '))

        if focal > 0:
            im = imread(os.path.join(data_path, image_list[i]))
            height, width = im.shape[0:2]
            camera = types.PerspectiveCamera()
            camera.id = 'camera_' + str(i)
            camera.width = width
            camera.height = height
            camera.focal = focal / max(width, height)
            camera.k1 = k1
            camera.k2 = k2
            reconstruction.add_camera(camera)

            # Shots
            rline = []
            for k in range(3):
                rline += lines[offset + 1 + k].rstrip('\n').split(' ')
            R = ' '.join(rline)
            t = lines[offset + 4].rstrip('\n').split(' ')
            R = np.array(list(map(float, R.split()))).reshape(3, 3)
            t = np.array(list(map(float, t)))
            R[1], R[2] = -R[1], -R[2]  # Reverse y and z
            t[1], t[2] = -t[1], -t[2]

            shot = types.Shot()
            shot.id = shot_key
            shot.camera = camera
            shot.pose = types.Pose()
            shot.pose.set_rotation_matrix(R)
            shot.pose.translation = t
            reconstruction.add_shot(shot)
        else:
            logger.warning('ignoring failed image {}'.format(shot_key))
        offset += 5

    # tracks
    track_lines = []
    for i in range(num_point):
        coordinates = lines[offset].rstrip('\n').split(' ')
        color = lines[offset + 1].rstrip('\n').split(' ')
        point = types.Point()
        point.id = i
        point.coordinates = list(map(float, coordinates))
        point.color = list(map(int, color))
        reconstruction.add_point(point)

        view_line = lines[offset + 2].rstrip('\n').split(' ')

        num_view, view_list = int(view_line[0]), view_line[1:]

        for k in range(num_view):
            shot_key = ordered_shots[int(view_list[4 * k])]
            if shot_key in reconstruction.shots:
                camera = reconstruction.shots[shot_key].camera
                scale = max(camera.width, camera.height)
                v = '{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}'.format(
                    shot_key,
                    i,
                    view_list[4 * k + 1],
                    float(view_list[4 * k + 2]) / scale,
                    -float(view_list[4 * k + 3]) / scale,
                    point.color[0],
                    point.color[1],
                    point.color[2]
                )
                track_lines.append(v)
        offset += 3

    # save track file
    with open_wt(track_file) as fout:
        fout.writelines('\n'.join(track_lines))

    # save reconstruction
    if reconstruction_file is not None:
        with open_wt(reconstruction_file) as fout:
            obj = reconstructions_to_json([reconstruction])
            json_dump(obj, fout)
    return reconstruction


# PLY

def ply_header(count_vertices, with_normals=False):
    if with_normals:
        header = [
            "ply",
            "format ascii 1.0",
            "element vertex {}".format(count_vertices),
            "property float x",
            "property float y",
            "property float z",
            "property float nx",
            "property float ny",
            "property float nz",
            "property uchar diffuse_red",
            "property uchar diffuse_green",
            "property uchar diffuse_blue",
            "end_header",
        ]
    else:
        header = [
            "ply",
            "format ascii 1.0",
            "element vertex {}".format(count_vertices),
            "property float x",
            "property float y",
            "property float z",
            "property uchar diffuse_red",
            "property uchar diffuse_green",
            "property uchar diffuse_blue",
            "end_header",
        ]
    return header


def points_to_ply_string(vertices):
    header = ply_header(len(vertices))
    return '\n'.join(header + vertices + [''])


def ply_to_points(filename):
    points, normals, colors = [], [], []
    with open(filename, 'r') as fin:
        line = fin.readline()
        while 'end_header' not in line:
            line = fin.readline()
        line = fin.readline()
        while line != '':
            line = fin.readline()
            tokens = line.rstrip().split(' ')
            if len(tokens) == 6 or len(tokens) == 7: # XYZ and RGB(A)
                x, y, z, r, g, b = tokens[0:6]
                nx, ny, nz = 0, 0, 0
            elif len(tokens) > 7:                    # XYZ + Normal + RGB
                x, y, z = tokens[0:3]
                nx, ny, nz = tokens[3:6]
                r, g, b = tokens[6:9]
            else:
                break
            points.append([float(x), float(y), float(z)])
            normals.append([float(nx), float(ny), float(nz)])
            colors.append([int(r), int(g), int(b)])
    return np.array(points), np.array(normals), np.array(colors)


def reconstruction_to_ply(reconstruction, no_cameras=False, no_points=False):
    """Export reconstruction points as a PLY string."""
    vertices = []

    if not no_points:
        for point in reconstruction.points.values():
            p, c = point.coordinates, point.color
            s = "{} {} {} {} {} {}".format(
                p[0], p[1], p[2], int(c[0]), int(c[1]), int(c[2]))
            vertices.append(s)

    if not no_cameras:
        for shot in reconstruction.shots.values():
            o = shot.pose.get_origin()
            R = shot.pose.get_rotation_matrix()
            for axis in range(3):
                c = 255 * np.eye(3)[axis]
                for depth in np.linspace(0, 2, 10):
                    p = o + depth * R[axis]
                    s = "{} {} {} {} {} {}".format(
                        p[0], p[1], p[2], int(c[0]), int(c[1]), int(c[2]))
                    vertices.append(s)
    return points_to_ply_string(vertices)
