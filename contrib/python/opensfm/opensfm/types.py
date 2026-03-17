"""Basic types for building a reconstruction."""

import numpy as np
import cv2
import math


class Pose(object):
    """Defines the pose parameters of a camera.

    The extrinsic parameters are defined by a 3x1 rotation vector which
    maps the camera rotation respect to the origin frame (rotation) and
    a 3x1 translation vector which maps the camera translation respect
    to the origin frame (translation).

    Attributes:
        rotation (vector): the rotation vector.
        translation (vector): the rotation vector.
    """

    def __init__(self, rotation=np.zeros(3), translation=np.zeros(3)):
        self.rotation = rotation
        self.translation = translation

    @property
    def rotation(self):
        """Rotation in angle-axis format."""
        return self._rotation

    @rotation.setter
    def rotation(self, value):
        self._rotation = np.asarray(value, dtype=float)

    @property
    def translation(self):
        """Translation vector."""
        return self._translation

    @translation.setter
    def translation(self, value):
        self._translation = np.asarray(value, dtype=float)

    def transform(self, point):
        """Transform a point from world to this pose coordinates."""
        return self.get_rotation_matrix().dot(point) + self.translation

    def transform_many(self, points):
        """Transform points from world coordinates to this pose."""
        return points.dot(self.get_rotation_matrix().T) + self.translation

    def transform_inverse(self, point):
        """Transform a point from this pose to world coordinates."""
        return self.get_rotation_matrix().T.dot(point - self.translation)

    def transform_inverse_many(self, points):
        """Transform points from this pose to world coordinates."""
        return (points - self.translation).dot(self.get_rotation_matrix())

    def get_rotation_matrix(self):
        """Get rotation as a 3x3 matrix."""
        return cv2.Rodrigues(self.rotation)[0]

    def set_rotation_matrix(self, rotation_matrix, permissive=False):
        """Set rotation as a 3x3 matrix.

        >>> pose = Pose()
        >>> pose.rotation = np.array([0., 1., 2.])
        >>> R = pose.get_rotation_matrix()
        >>> pose.set_rotation_matrix(R)
        >>> np.allclose(pose.rotation, [0., 1., 2.])
        True

        >>> pose.set_rotation_matrix([[3,-4, 1], [ 5, 3,-7], [-9, 2, 6]])
        Traceback (most recent call last):
        ...
        ValueError: Not orthogonal

        >>> pose.set_rotation_matrix([[0, 0, 1], [-1, 0, 0], [0, 1, 0]])
        Traceback (most recent call last):
        ...
        ValueError: Determinant not 1
        """
        R = np.array(rotation_matrix, dtype=float)
        if not permissive:
          if not np.isclose(np.linalg.det(R), 1):
              raise ValueError("Determinant not 1")
          if not np.allclose(np.linalg.inv(R), R.T):
              raise ValueError("Not orthogonal")
        self.rotation = cv2.Rodrigues(R)[0].ravel()

    def get_origin(self):
        """The origin of the pose in world coordinates."""
        return -self.get_rotation_matrix().T.dot(self.translation)

    def set_origin(self, origin):
        """Set the origin of the pose in world coordinates.

        >>> pose = Pose()
        >>> pose.rotation = np.array([0., 1., 2.])
        >>> origin = [1., 2., 3.]
        >>> pose.set_origin(origin)
        >>> np.allclose(origin, pose.get_origin())
        True
        """
        self.translation = -self.get_rotation_matrix().dot(origin)

    def get_Rt(self):
        """Get pose as a 3x4 matrix (R|t)."""
        Rt = np.empty((3, 4))
        Rt[:, :3] = self.get_rotation_matrix()
        Rt[:, 3] = self.translation
        return Rt

    def compose(self, other):
        """Get the composition of this pose with another.

        composed = self * other
        """
        selfR = self.get_rotation_matrix()
        otherR = other.get_rotation_matrix()
        R = np.dot(selfR, otherR)
        t = selfR.dot(other.translation) + self.translation
        res = Pose()
        res.set_rotation_matrix(R)
        res.translation = t
        return res

    def inverse(self):
        """Get the inverse of this pose."""
        inverse = Pose()
        R = self.get_rotation_matrix()
        inverse.set_rotation_matrix(R.T)
        inverse.translation = -R.T.dot(self.translation)
        return inverse


class ShotMetadata(object):
    """Defines GPS data from a taken picture.

    Attributes:
        orientation (int): the exif orientation tag (1-8).
        capture_time (real): the capture time.
        gps_dop (real): the GPS dop.
        gps_position (vector): the GPS position.
    """

    def __init__(self):
        self.orientation = None
        self.gps_dop = None
        self.gps_position = None
        self.accelerometer = None
        self.compass = None
        self.capture_time = None
        self.skey = None


class ShotMesh(object):
    """Triangular mesh of points visible in a shot

    Attributes:
        vertices: (list of vectors) mesh vertices
        faces: (list of triplets) triangles' topology
    """

    def __init__(self):
        self.vertices = None
        self.faces = None


class Camera(object):
    """Abstract camera class.

    A camera is unique defined for its identification description (id),
    the projection type (projection_type) and its internal calibration
    parameters, which depend on the particular Camera sub-class.

    Attributes:
        id (str): camera description.
        projection_type (str): projection type.
    """

    pass


class PerspectiveCamera(Camera):
    """Define a perspective camera.

    Attributes:
        width (int): image width.
        height (int): image height.
        focal (real): estimated focal lenght.
        k1 (real): estimated first distortion parameter.
        k2 (real): estimated second distortion parameter.
    """

    def __init__(self):
        """Defaut constructor."""
        self.id = None
        self.projection_type = 'perspective'
        self.width = None
        self.height = None
        self.focal = None
        self.k1 = None
        self.k2 = None

    def __repr__(self):
        return '{}({!r}, {!r}, {!r}, {!r}, {!r}, {!r}, {!r})'.format(
            self.__class__.__name__,
            self.id, self.projection_type, self.width, self.height,
            self.focal, self.k1, self.k2)

    def project(self, point):
        """Project a 3D point in camera coordinates to the image plane."""
        # Normalized image coordinates
        xn = point[0] / point[2]
        yn = point[1] / point[2]

        # Radial distortion
        r2 = xn * xn + yn * yn
        distortion = 1.0 + r2 * (self.k1 + self.k2 * r2)

        return np.array([self.focal * distortion * xn,
                         self.focal * distortion * yn])

    def project_many(self, points):
        """Project 3D points in camera coordinates to the image plane."""
        distortion = np.array([self.k1, self.k2, 0, 0, 0])
        K, R, t = self.get_K(), np.zeros(3), np.zeros(3)
        pixels, _ = cv2.projectPoints(points, R, t, K, distortion)
        return pixels.reshape((-1, 2))

    def pixel_bearing(self, pixel):
        """Unit vector pointing to the pixel viewing direction."""
        point = np.asarray(pixel).reshape((1, 1, 2))
        distortion = np.array([self.k1, self.k2, 0., 0.])
        x, y = cv2.undistortPoints(point, self.get_K(), distortion).flat
        l = np.sqrt(x * x + y * y + 1.0)
        return np.array([x / l, y / l, 1.0 / l])

    def pixel_bearing_many(self, pixels):
        """Unit vectors pointing to the pixel viewing directions."""
        points = pixels.reshape((-1, 1, 2)).astype(np.float64)
        distortion = np.array([self.k1, self.k2, 0., 0.])
        up = cv2.undistortPoints(points, self.get_K(), distortion)
        up = up.reshape((-1, 2))
        x = up[:, 0]
        y = up[:, 1]
        l = np.sqrt(x * x + y * y + 1.0)
        return np.column_stack((x / l, y / l, 1.0 / l))

    def pixel_bearings(self, pixels):
        """Deprecated: use pixel_bearing_many."""
        return self.pixel_bearing_many(pixels)

    def back_project(self, pixel, depth):
        """Project a pixel to a fronto-parallel plane at a given depth."""
        bearing = self.pixel_bearing(pixel)
        scale = depth / bearing[2]
        return scale * bearing

    def back_project_many(self, pixels, depths):
        """Project pixels to fronto-parallel planes at given depths."""
        bearings = self.pixel_bearing_many(pixels)
        scales = depths / bearings[:, 2]
        return scales[:, np.newaxis] * bearings

    def get_K(self):
        """The calibration matrix."""
        return np.array([[self.focal, 0., 0.],
                         [0., self.focal, 0.],
                         [0., 0., 1.]])

    def get_K_in_pixel_coordinates(self, width=None, height=None):
        """The calibration matrix that maps to pixel coordinates.

        Coordinates (0,0) correspond to the center of the top-left pixel,
        and (width - 1, height - 1) to the center of bottom-right pixel.

        You can optionally pass the width and height of the image, in case
        you are using a resized versior of the original image.
        """
        w = width or self.width
        h = height or self.height
        f = self.focal * max(w, h)
        return np.array([[f, 0, 0.5 * (w - 1)],
                         [0, f, 0.5 * (h - 1)],
                         [0, 0, 1.0]])


class BrownPerspectiveCamera(Camera):
    """Define a perspective camera.

    Attributes:
        width (int): image width.
        height (int): image height.
        focal_x (real): estimated focal length for the X axis.
        focal_y (real): estimated focal length for the Y axis.
        c_x (real): estimated principal point X.
        c_y (real): estimated principal point Y.
        k1 (real): estimated first radial distortion parameter.
        k2 (real): estimated second radial distortion parameter.
        p1 (real): estimated first tangential distortion parameter.
        p2 (real): estimated second tangential distortion parameter.
        k3 (real): estimated third radial distortion parameter.
    """

    def __init__(self):
        """Defaut constructor."""
        self.id = None
        self.projection_type = 'brown'
        self.width = None
        self.height = None
        self.focal_x = None
        self.focal_y = None
        self.c_x = None
        self.c_y = None
        self.k1 = None
        self.k2 = None
        self.p1 = None
        self.p2 = None
        self.k3 = None

    def __repr__(self):
        return '{}({})'.format(self.__class__.__name__, self.__dict__)

    def project(self, point):
        """Project a 3D point in camera coordinates to the image plane."""
        # Normalized image coordinates
        xn = point[0] / point[2]
        yn = point[1] / point[2]

        # Radial and tangential distortion
        r2 = xn * xn + yn * yn
        radial_distortion = 1.0 + r2 * (self.k1 + r2 * (self.k2 + r2 * self.k3))
        x_tangential_distortion = 2 * self.p1 * xn * yn + self.p2 * (r2 + 2 * xn * xn)
        x_distorted = xn * radial_distortion + x_tangential_distortion
        y_tangential_distortion = self.p1 * (r2 + 2 * yn * yn) + 2 * self.p2 * xn * yn
        y_distorted = yn * radial_distortion + y_tangential_distortion

        return np.array([self.focal_x * x_distorted + self.c_x,
                         self.focal_y * y_distorted + self.c_y])

    def project_many(self, points):
        """Project 3D points in camera coordinates to the image plane."""
        distortion = np.array([self.k1, self.k2, self.p1, self.p2, self.k3])
        K, R, t = self.get_K(), np.zeros(3), np.zeros(3)
        pixels, _ = cv2.projectPoints(points, R, t, K, distortion)
        return pixels.reshape((-1, 2))

    def pixel_bearing(self, pixel):
        """Unit vector pointing to the pixel viewing direction."""
        point = np.asarray(pixel).reshape((1, 1, 2))
        distortion = np.array([self.k1, self.k2, self.p1, self.p2, self.k3])
        x, y = cv2.undistortPoints(point, self.get_K(), distortion).flat
        l = np.sqrt(x * x + y * y + 1.0)
        return np.array([x / l, y / l, 1.0 / l])

    def pixel_bearing_many(self, pixels):
        """Unit vector pointing to the pixel viewing directions."""
        points = pixels.reshape((-1, 1, 2)).astype(np.float64)
        distortion = np.array([self.k1, self.k2, self.p1, self.p2, self.k3])
        up = cv2.undistortPoints(points, self.get_K(), distortion)
        up = up.reshape((-1, 2))
        x = up[:, 0]
        y = up[:, 1]
        l = np.sqrt(x * x + y * y + 1.0)
        return np.column_stack((x / l, y / l, 1.0 / l))

    def pixel_bearings(self, pixels):
        """Deprecated: use pixel_bearing_many."""
        return self.pixel_bearing_many(pixels)

    def back_project(self, pixel, depth):
        """Project a pixel to a fronto-parallel plane at a given depth."""
        bearing = self.pixel_bearing(pixel)
        scale = depth / bearing[2]
        return scale * bearing

    def back_project_many(self, pixels, depths):
        """Project pixels to fronto-parallel planes at given depths."""
        bearings = self.pixel_bearing_many(pixels)
        scales = depths / bearings[:, 2]
        return scales[:, np.newaxis] * bearings

    def get_K(self):
        """The calibration matrix."""
        return np.array([[self.focal_x, 0., self.c_x],
                         [0., self.focal_y, self.c_y],
                         [0., 0., 1.]])

    def get_K_in_pixel_coordinates(self, width=None, height=None):
        """The calibration matrix that maps to pixel coordinates.

        Coordinates (0,0) correspond to the center of the top-left pixel,
        and (width - 1, height - 1) to the center of bottom-right pixel.

        You can optionally pass the width and height of the image, in case
        you are using a resized versior of the original image.
        """
        w = width or self.width
        h = height or self.height
        s = max(w, h)
        normalized_to_pixel = np.array([
            [s, 0, (w - 1) / 2.0],
            [0, s, (h - 1) / 2.0],
            [0, 0, 1],
        ])
        return np.dot(normalized_to_pixel, self.get_K())


class FisheyeCamera(Camera):
    """Define a fisheye camera.

    Attributes:
        width (int): image width.
        height (int): image height.
        focal (real): estimated focal lenght.
        k1 (real): estimated first distortion parameter.
        k2 (real): estimated second distortion parameter.
    """

    def __init__(self):
        """Defaut constructor."""
        self.id = None
        self.projection_type = 'fisheye'
        self.width = None
        self.height = None
        self.focal = None
        self.k1 = None
        self.k2 = None

    def project(self, point):
        """Project a 3D point in camera coordinates to the image plane."""
        x, y, z = point
        l = np.sqrt(x**2 + y**2)
        theta = np.arctan2(l, z)
        theta_d = theta * (1.0 + theta**2 * (self.k1 + theta**2 * self.k2))
        s = self.focal * theta_d / l
        return np.array([s * x, s * y])

    def project_many(self, points):
        """Project 3D points in camera coordinates to the image plane."""
        points = points.reshape((-1, 1, 3)).astype(np.float64)
        distortion = np.array([self.k1, self.k2, 0., 0.])
        K, R, t = self.get_K(), np.zeros(3), np.zeros(3)
        pixels, _ = cv2.fisheye.projectPoints(points, R, t, K, distortion)
        return pixels.reshape((-1, 2))

    def pixel_bearing(self, pixel):
        """Unit vector pointing to the pixel viewing direction."""
        point = np.asarray(pixel).reshape((1, 1, 2))
        distortion = np.array([self.k1, self.k2, 0., 0.])
        x, y = cv2.fisheye.undistortPoints(point, self.get_K(), distortion).flat
        l = np.sqrt(x * x + y * y + 1.0)
        return np.array([x / l, y / l, 1.0 / l])

    def pixel_bearing_many(self, pixels):
        """Unit vector pointing to the pixel viewing directions."""
        points = pixels.reshape((-1, 1, 2)).astype(np.float64)
        distortion = np.array([self.k1, self.k2, 0., 0.])
        up = cv2.fisheye.undistortPoints(points, self.get_K(), distortion)
        up = up.reshape((-1, 2))
        x = up[:, 0]
        y = up[:, 1]
        l = np.sqrt(x * x + y * y + 1.0)
        return np.column_stack((x / l, y / l, 1.0 / l))

    def pixel_bearings(self, pixels):
        """Deprecated: use pixel_bearing_many."""
        return self.pixel_bearing_many(pixels)

    def back_project(self, pixel, depth):
        """Project a pixel to a fronto-parallel plane at a given depth."""
        bearing = self.pixel_bearing(pixel)
        scale = depth / bearing[2]
        return scale * bearing

    def back_project_many(self, pixels, depths):
        """Project pixels to fronto-parallel planes at given depths."""
        bearings = self.pixel_bearing_many(pixels)
        scales = depths / bearings[:, 2]
        return scales[:, np.newaxis] * bearings

    def get_K(self):
        """The calibration matrix."""
        return np.array([[self.focal, 0., 0.],
                         [0., self.focal, 0.],
                         [0., 0., 1.]])

    def get_K_in_pixel_coordinates(self, width=None, height=None):
        """The calibration matrix that maps to pixel coordinates.

        Coordinates (0,0) correspond to the center of the top-left pixel,
        and (width - 1, height - 1) to the center of bottom-right pixel.

        You can optionally pass the width and height of the image, in case
        you are using a resized version of the original image.
        """
        w = width or self.width
        h = height or self.height
        f = self.focal * max(w, h)
        return np.array([[f, 0, 0.5 * (w - 1)],
                         [0, f, 0.5 * (h - 1)],
                         [0, 0, 1.0]])


class DualCamera(Camera):
    """Define a camera that seamlessly transition
        between fisheye and perspective camera.

    Attributes:
        width (int): image width.
        height (int): image height.
        focal (real): estimated focal lenght.
        k1 (real): estimated first distortion parameter.
        k2 (real): estimated second distortion parameter.
        focal_prior (real): prior focal lenght.
        k1_prior (real): prior first distortion parameter.
        k2_prior (real): prior second distortion parameter.
        transition (real): parametrize between perpective (1.0) and fisheye (0.0)
    """
    def __init__(self, projection_type='unknown'):
        """Defaut constructor."""
        self.id = None
        self.projection_type = 'dual'
        self.width = None
        self.height = None
        self.focal = None
        self.k1 = None
        self.k2 = None
        if projection_type == 'perspective':
            self.transition = 1.0
        elif projection_type == 'fisheye':
            self.transition = 0.0
        else:
            self.transition = 0.5

    def project(self, point):
        """Project a 3D point in camera coordinates to the image plane."""
        x, y, z = point
        l = np.sqrt(x**2 + y**2)
        theta = np.arctan2(l, z)
        x_fish = theta / l * x
        y_fish = theta / l * y

        x_persp = point[0] / point[2]
        y_persp = point[1] / point[2]

        x_dual = self.transition*x_persp + (1.0 - self.transition)*x_fish
        y_dual = self.transition*y_persp + (1.0 - self.transition)*y_fish

        r2 = x_dual * x_dual + y_dual * y_dual
        distortion = 1.0 + r2 * (self.k1 + self.k2 * r2)

        return np.array([self.focal * distortion * x_dual,
                         self.focal * distortion * y_dual])

    def project_many(self, points):
        """Project 3D points in camera coordinates to the image plane."""
        projected = []
        for point in points:
            projected.append(self.project(point))
        return np.array(projected)

    def pixel_bearing(self, pixel):
        """Unit vector pointing to the pixel viewing direction."""

        point = np.asarray(pixel).reshape((1, 1, 2))
        distortion = np.array([self.k1, self.k2, 0., 0.])
        no_K = np.array([[1., 0., 0.],
                         [0., 1., 0.],
                         [0., 0., 1.]])

        point /= self.focal
        x_u, y_u = cv2.undistortPoints(point, no_K, distortion).flat
        r = np.sqrt(x_u**2 + y_u**2)

        # inverse iteration for finding theta from r
        theta_fish = r
        theta_persp = np.arctan2(r, 1.0)
        theta_0 = self.transition*theta_persp + (1.0 - self.transition)*theta_fish
        for i in range(3):
            r_0 = self.transition*math.tan(theta_0) + (1.0 - self.transition)*theta_0
            secant = 1.0/math.cos(theta_0)
            d_theta = (self.transition*secant**2 - self.transition + 1)
            theta_0 = (r - r_0)/d_theta + theta_0

        s = math.tan(theta_0)/(self.transition*math.tan(theta_0) + (1.0 - self.transition)*theta_0)
        x_dual = x_u*s
        y_dual = y_u*s

        l = np.sqrt(x_dual * x_dual + y_dual * y_dual + 1.0)
        return np.array([x_dual / l, y_dual / l, 1.0 / l])

    def pixel_bearing_many(self, pixels):
        """Unit vector pointing to the pixel viewing directions."""
        points = pixels.reshape((-1, 1, 2)).astype(np.float64)
        distortion = np.array([self.k1, self.k2, 0., 0.])
        no_K = np.array([[1., 0., 0.],
                         [0., 1., 0.],
                         [0., 0., 1.]])

        undistorted = cv2.undistortPoints(points, no_K, distortion)
        undistorted = undistorted.reshape((-1, 2))
        r = np.sqrt(undistorted[:, 0]**2 + undistorted[:, 1]**2)

        # inverse iteration for finding theta from r
        theta_fish = r
        theta_persp = np.arctan2(r, 1.0)
        theta_0 = self.transition*theta_persp + (1.0 - self.transition)*theta_fish
        for i in range(3):
            r_0 = self.transition*np.tan(theta_0) + (1.0 - self.transition)*theta_0
            secant = 1.0/np.cos(theta_0)
            d_theta = (self.transition*secant**2 - self.transition + 1)
            theta_0 = (r - r_0)/d_theta + theta_0

        s = np.tan(theta_0)/(self.transition*np.tan(theta_0) + (1.0 - self.transition)*theta_0)
        x_dual = undistorted[:, 0]*s
        y_dual = undistorted[:, 1]*s

        l = np.sqrt(x_dual * x_dual + y_dual * y_dual + 1.0)
        return np.column_stack([x_dual / l, y_dual / l, 1.0 / l])

    def pixel_bearings(self, pixels):
        """Deprecated: use pixel_bearing_many."""
        return self.pixel_bearing_many(pixels)

    def back_project(self, pixel, depth):
        """Project a pixel to a fronto-parallel plane at a given depth."""
        bearing = self.pixel_bearing(pixel)
        scale = depth / bearing[2]
        return scale * bearing

    def back_project_many(self, pixels, depths):
        """Project pixels to fronto-parallel planes at given depths."""
        bearings = self.pixel_bearing_many(pixels)
        scales = depths / bearings[:, 2]
        return scales[:, np.newaxis] * bearings

    def get_K(self):
        """The calibration matrix."""
        return np.array([[self.focal, 0., 0.],
                         [0., self.focal, 0.],
                         [0., 0., 1.]])

    def get_K_in_pixel_coordinates(self, width=None, height=None):
        """The calibration matrix that maps to pixel coordinates.

        Coordinates (0,0) correspond to the center of the top-left pixel,
        and (width - 1, height - 1) to the center of bottom-right pixel.

        You can optionally pass the width and height of the image, in case
        you are using a resized version of the original image.
        """
        w = width or self.width
        h = height or self.height
        f = self.focal * max(w, h)
        return np.array([[f, 0, 0.5 * (w - 1)],
                         [0, f, 0.5 * (h - 1)],
                         [0, 0, 1.0]])


class SphericalCamera(Camera):
    """A spherical camera generating equirectangular projections.

    Attributes:
        width (int): image width.
        height (int): image height.
    """

    def __init__(self):
        """Defaut constructor."""
        self.id = None
        self.projection_type = 'equirectangular'
        self.width = None
        self.height = None

    def project(self, point):
        """Project a 3D point in camera coordinates to the image plane."""
        x, y, z = point
        lon = np.arctan2(x, z)
        lat = np.arctan2(-y, np.sqrt(x**2 + z**2))
        return np.array([lon / (2 * np.pi), -lat / (2 * np.pi)])

    def project_many(self, points):
        """Project 3D points in camera coordinates to the image plane."""
        x, y, z = points.T
        lon = np.arctan2(x, z)
        lat = np.arctan2(-y, np.sqrt(x**2 + z**2))
        return np.column_stack([lon / (2 * np.pi), -lat / (2 * np.pi)])

    def pixel_bearing(self, pixel):
        """Unit vector pointing to the pixel viewing direction."""
        lon = pixel[0] * 2 * np.pi
        lat = -pixel[1] * 2 * np.pi
        x = np.cos(lat) * np.sin(lon)
        y = -np.sin(lat)
        z = np.cos(lat) * np.cos(lon)
        return np.array([x, y, z])

    def pixel_bearing_many(self, pixels):
        """Unit vector pointing to the pixel viewing directions."""
        lon = pixels[:, 0] * 2 * np.pi
        lat = -pixels[:, 1] * 2 * np.pi
        x = np.cos(lat) * np.sin(lon)
        y = -np.sin(lat)
        z = np.cos(lat) * np.cos(lon)
        return np.column_stack([x, y, z]).astype(float)

    def pixel_bearings(self, pixels):
        """Deprecated: use pixel_bearing_many."""
        return self.pixel_bearing_many(pixels)


class Shot(object):
    """Defines a shot in a reconstructed scene.

    A shot here is refered as a unique view inside the scene defined by
    the image filename (id), the used camera with its refined internal
    parameters (camera), the fully camera pose respect to the scene origin
    frame (pose) and the GPS data obtained in the moment that the picture
    was taken (metadata).

    Attributes:
        id (str): picture filename.
        camera (Camera): camera.
        pose (Pose): extrinsic parameters.
        metadata (ShotMetadata): GPS, compass, capture time, etc.
    """

    def __init__(self):
        """Defaut constructor."""
        self.id = None
        self.camera = None
        self.pose = None
        self.metadata = None
        self.mesh = None

    def project(self, point):
        """Project a 3D point to the image plane."""
        camera_point = self.pose.transform(point)
        return self.camera.project(camera_point)

    def project_many(self, points):
        """Project 3D points to the image plane."""
        camera_point = self.pose.transform_many(points)
        return self.camera.project_many(camera_point)

    def back_project(self, pixel, depth):
        """Project a pixel to a fronto-parallel plane at a given depth.

        The plane is defined by z = depth in the shot reference frame.
        """
        point_in_cam_coords = self.camera.back_project(pixel, depth)
        return self.pose.transform_inverse(point_in_cam_coords)

    def back_project_many(self, pixels, depths):
        """Project pixels to fronto-parallel planes at given depths.
        The planes are defined by z = depth in the shot reference frame.
        """
        points_in_cam_coords = self.camera.back_project_many(pixels, depths)
        return self.pose.transform_inverse_many(points_in_cam_coords)

    def viewing_direction(self):
        """The viewing direction of the shot.

        That is the positive camera Z axis in world coordinates.
        """
        return self.pose.get_rotation_matrix().T.dot([0, 0, 1])


class Point(object):
    """Defines a 3D point.

    Attributes:
        id (int): identification number.
        color (list(int)): list containing the RGB values.
        coordinates (list(real)): list containing the 3D position.
        reprojection_errors (dict(real)): the reprojection error per shot.
    """

    def __init__(self):
        """Defaut constructor"""
        self.id = None
        self.color = None
        self.coordinates = None
        self.reprojection_errors = {}


class GroundControlPoint(object):
    """A ground control point with its observations.

    Attributes:
        lla: latitue, longitude and altitude
        coordinates: x, y, z coordinates in topocentric reference frame
        has_altitude: true if z coordinate is known
        observations: list of observations of the point on images
    """

    def __init__(self):
        self.id = None
        self.lla = None
        self.coordinates = None
        self.has_altitude = None
        self.observations = []


class GroundControlPointObservation(object):
    """A ground control point observation.

    Attributes:
        shot_id: the shot where the point is observed
        projection: 2d coordinates of the observation
    """

    def __init__(self):
        self.shot_id = None
        self.projection = None


class Reconstruction(object):
    """Defines the reconstructed scene.

    Attributes:
      cameras (Dict(Camera)): List of cameras.
      shots (Dict(Shot)): List of reconstructed shots.
      points (Dict(Point)): List of reconstructed points.
      reference (TopocentricConverter): Topocentric reference converter.
    """

    def __init__(self):
        """Defaut constructor"""
        self.cameras = {}
        self.shots = {}
        self.points = {}
        self.reference = None

    def add_camera(self, camera):
        """Add a camera in the list

        :param camera: The camera.
        """
        self.cameras[camera.id] = camera

    def get_camera(self, id):
        """Return a camera by id.

        :return: If exists returns the camera, otherwise None.
        """
        return self.cameras.get(id)

    def add_shot(self, shot):
        """Add a shot in the list

        :param shot: The shot.
        """
        self.shots[shot.id] = shot

    def get_shot(self, id):
        """Return a shot by id.

        :return: If exists returns the shot, otherwise None.
        """
        return self.shots.get(id)

    def add_point(self, point):
        """Add a point in the list

        :param point: The point.
        """
        self.points[point.id] = point

    def get_point(self, id):
        """Return a point by id.

        :return: If exists returns the point, otherwise None.
        """
        return self.points.get(id)
