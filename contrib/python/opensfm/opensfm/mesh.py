#!/usr/bin/env python3
import itertools
import numpy as np
import scipy.spatial
import logging

logger = logging.getLogger(__name__)


def triangle_mesh(shot_id, r, graph, data):
    '''
    Create triangle meshes in a list
    '''
    if shot_id not in r.shots or shot_id not in graph:
        return [], []

    shot = r.shots[shot_id]

    if shot.camera.projection_type in ['perspective', 'brown']:
        return triangle_mesh_perspective(shot_id, r, graph)
    elif shot.camera.projection_type == 'fisheye':
        return triangle_mesh_fisheye(shot_id, r, graph)
    elif shot.camera.projection_type == 'dual':
        return triangle_mesh_fisheye(shot_id, r, graph)
    elif shot.camera.projection_type in ['equirectangular', 'spherical']:
        return triangle_mesh_equirectangular(shot_id, r, graph)
    else:
        raise NotImplementedError


def triangle_mesh_perspective(shot_id, r, graph):
    shot = r.shots[shot_id]
    cam = shot.camera

    dx = float(cam.width) / 2 / max(cam.width, cam.height)
    dy = float(cam.height) / 2 / max(cam.width, cam.height)
    pixels = [[-dx, -dy], [-dx, dy], [dx, dy], [dx, -dy]]
    vertices = [None for i in range(4)]
    for track_id, edge in graph[shot_id].items():
        if track_id in r.points:
            point = r.points[track_id]
            pixel = shot.project(point.coordinates)
            nonans = not np.isnan(pixel).any()
            if nonans and -dx <= pixel[0] <= dx and -dy <= pixel[1] <= dy:
                vertices.append(point.coordinates)
                pixels.append(pixel.tolist())

    try:
        tri = scipy.spatial.Delaunay(pixels)
    except Exception as e:
        logger.error('Delaunay triangulation failed for input: {}'.format(
            repr(pixels)))
        raise e

    sums = [0., 0., 0., 0.]
    depths = [0., 0., 0., 0.]
    for t in tri.simplices:
        for i in range(4):
            if i in t:
                for j in t:
                    if j >= 4:
                        depths[i] += shot.pose.transform(vertices[j])[2]
                        sums[i] += 1
    for i in range(4):
        if sums[i] > 0:
            d = depths[i] / sums[i]
        else:
            d = 50.0
        vertices[i] = back_project_no_distortion(shot, pixels[i], d).tolist()

    faces = tri.simplices.tolist()
    return vertices, faces


def back_project_no_distortion(shot, pixel, depth):
    '''
    Back-project a pixel of a perspective camera ignoring its radial distortion
    '''
    K = shot.camera.get_K()
    K1 = np.linalg.inv(K)
    p = np.dot(K1, [pixel[0], pixel[1], 1])
    p *= depth / p[2]
    return shot.pose.transform_inverse(p)


def triangle_mesh_fisheye(shot_id, r, graph):
    shot = r.shots[shot_id]

    bearings = []
    vertices = []

    # Add boundary vertices
    num_circle_points = 20
    for i in range(num_circle_points):
        a = 2 * np.pi * float(i) / num_circle_points
        point = 30 * np.array([np.cos(a), np.sin(a), 0])
        bearing = point / np.linalg.norm(point)
        point = shot.pose.transform_inverse(point)
        vertices.append(point.tolist())
        bearings.append(bearing)

    # Add a single vertex in front of the camera
    point = 30 * np.array([0, 0, 1])
    bearing = 0.3 * point / np.linalg.norm(point)
    point = shot.pose.transform_inverse(point)
    vertices.append(point.tolist())
    bearings.append(bearing)

    # Add reconstructed points
    for track_id, edge in graph[shot_id].items():
        if track_id in r.points:
            point = r.points[track_id].coordinates
            direction = shot.pose.transform(point)
            pixel = direction / np.linalg.norm(direction)
            if not np.isnan(pixel).any():
                vertices.append(point)
                bearings.append(pixel.tolist())

    # Triangulate
    tri = scipy.spatial.ConvexHull(bearings)
    faces = tri.simplices.tolist()

    # Remove faces having only boundary vertices
    def good_face(face):
        return (face[0] >= num_circle_points or
                face[1] >= num_circle_points or
                face[2] >= num_circle_points)

    faces = filter(good_face, faces)

    return vertices, faces


def triangle_mesh_equirectangular(shot_id, r, graph):
    shot = r.shots[shot_id]

    bearings = []
    vertices = []

    # Add vertices to ensure that the camera is inside the convex hull
    # of the points
    for point in itertools.product([-1, 1], repeat=3):  # vertices of a cube
        bearing = 0.3 * np.array(point) / np.linalg.norm(point)
        bearings.append(bearing)
        point = shot.pose.transform_inverse(bearing)
        vertices.append(point.tolist())

    for track_id, edge in graph[shot_id].items():
        if track_id in r.points:
            point = r.points[track_id].coordinates
            direction = shot.pose.transform(point)
            pixel = direction / np.linalg.norm(direction)
            if not np.isnan(pixel).any():
                vertices.append(point)
                bearings.append(pixel.tolist())

    tri = scipy.spatial.ConvexHull(bearings)
    faces = tri.simplices.tolist()

    return vertices, faces
