from __future__ import division

import cv2
import numpy as np

from opensfm import transformations


def rotation_from_angle_axis(angle_axis):
    return cv2.Rodrigues(np.asarray(angle_axis))[0]


def rotation_from_ptr(pan, tilt, roll):
    """Camera rotation matrix from pan, tilt and roll."""
    R1 = rotation_from_angle_axis([0.0, 0.0, roll])
    R2 = rotation_from_angle_axis([tilt + np.pi/2, 0.0, 0.0])
    R3 = rotation_from_angle_axis([0.0, 0.0, pan])
    return R1.dot(R2).dot(R3)


def ptr_from_rotation(rotation_matrix):
    """Pan tilt and roll from camera rotation matrix"""
    pan = pan_from_rotation(rotation_matrix)
    tilt = tilt_from_rotation(rotation_matrix)
    roll = roll_from_rotation(rotation_matrix)
    return pan, tilt, roll


def pan_from_rotation(rotation_matrix):
    Rt_ez = np.dot(rotation_matrix.T, [0, 0, 1])
    return np.arctan2(Rt_ez[0], Rt_ez[1])


def tilt_from_rotation(rotation_matrix):
    Rt_ez = np.dot(rotation_matrix.T, [0, 0, 1])
    l = np.linalg.norm(Rt_ez[:2])
    return np.arctan2(-Rt_ez[2], l)


def roll_from_rotation(rotation_matrix):
    Rt_ex = np.dot(rotation_matrix.T, [1, 0, 0])
    Rt_ez = np.dot(rotation_matrix.T, [0, 0, 1])
    a = np.cross(Rt_ez, [0, 0, 1])
    a /= np.linalg.norm(a)
    b = np.cross(Rt_ex, a)
    return np.arcsin(np.dot(Rt_ez, b))


def rotation_from_ptr_v2(pan, tilt, roll):
    """Camera rotation matrix from pan, tilt and roll.
    
    This is the implementation used in the Single Image Calibration code.
    """
    tilt += np.pi / 2
    return transformations.euler_matrix(pan, tilt, roll, 'szxz')[:3, :3]


def ptr_from_rotation_v2(rotation_matrix):
    """Pan tilt and roll from camera rotation matrix.

    This is the implementation used in the Single Image Calibration code.
    """
    T = np.identity(4)
    T[:3, :3] = rotation_matrix
    pan, tilt, roll = transformations.euler_from_matrix(T, 'szxz')
    return pan, tilt - np.pi / 2, roll
