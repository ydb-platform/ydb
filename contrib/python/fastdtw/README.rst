fastdtw
-------

Python implementation of `FastDTW
<http://cs.fit.edu/~pkc/papers/tdm04.pdf>`_ [1]_, which is an approximate Dynamic Time Warping (DTW) algorithm that provides optimal or near-optimal alignments with an O(N) time and memory complexity.

Install
-------

::

  pip install fastdtw

Example
-------

::
  
  import numpy as np
  from scipy.spatial.distance import euclidean

  from fastdtw import fastdtw

  x = np.array([[1,1], [2,2], [3,3], [4,4], [5,5]])
  y = np.array([[2,2], [3,3], [4,4]])
  distance, path = fastdtw(x, y, dist=euclidean)
  print(distance)

References
----------

.. [1] Stan Salvador, and Philip Chan. "FastDTW: Toward accurate dynamic time warping in linear time and space." Intelligent Data Analysis 11.5 (2007): 561-580.
