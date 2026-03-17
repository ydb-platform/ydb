import os
import numpy as np

# Putting this in cython was giving strange bugs for different versions
# of cython which seemed to indicate troubles with the __file__ variable
# not being defined. Keeping it in pure python makes it more reliable
this_dir = os.path.dirname(__file__)
import importlib.resources
POS = np.loadtxt((importlib.resources.files(__package__) / "orb_descriptor_positions.txt").open(mode='rb'), dtype=np.int8)
POS0 = np.ascontiguousarray(POS[:, :2])
POS1 = np.ascontiguousarray(POS[:, 2:])
