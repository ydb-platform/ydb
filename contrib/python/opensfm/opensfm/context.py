# -*- coding: utf-8 -*-
import logging
import os
import resource
import sys

import cv2
from joblib import Parallel, parallel_backend, delayed


logger = logging.getLogger(__name__)


abspath = os.path.abspath(os.path.dirname(__file__))
SENSOR = os.path.join(abspath, 'data', 'sensor_data.json')
BOW_PATH = os.path.join(abspath, 'data', 'bow')


# Handle different OpenCV versions
OPENCV3 = int(cv2.__version__.split('.')[0]) >= 3

if hasattr(cv2, 'flann_Index'):
    flann_Index = cv2.flann_Index
elif hasattr(cv2, 'flann') and hasattr(cv2.flann, 'Index'):
    flann_Index = cv2.flann.Index
else:
    logger.warning('Unable to find flann Index')
    flann_Index = None


# Parallel processes
def parallel_map(func, args, num_proc, max_batch_size=1):
    """Run function for all arguments using multiple processes."""
    num_proc = min(num_proc, len(args))
    if num_proc <= 1:
        return list(map(func, args))
    else:
        with parallel_backend('loky', n_jobs=num_proc):
            batch_size = max(1, int(len(args)/(num_proc*2)))
            batch_size = min(batch_size, max_batch_size) if max_batch_size else batch_size
            return Parallel(batch_size=batch_size)(delayed(func)(arg) for arg in args)


# Memory usage
if sys.platform == 'darwin':
    rusage_unit = 1
else:
    rusage_unit = 1024


def memory_available():
    """Available memory in MB.

    Only works on linux and returns None otherwise.
    """
    lines = os.popen('free -t -m').readlines()
    if not lines:
        return None
    available_mem = int(lines[1].split()[6])
    return available_mem


def processes_that_fit_in_memory(desired, per_process):
    """Amount of parallel BoW process that fit in memory."""
    available_mem = memory_available()
    if available_mem is not None:
        fittable = max(1, int(available_mem / per_process))
        return min(desired, fittable)
    else:
        return desired


def current_memory_usage():
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * rusage_unit
