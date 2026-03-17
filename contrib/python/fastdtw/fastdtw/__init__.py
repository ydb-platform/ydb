try:
    from ._fastdtw import fastdtw, dtw
except ImportError:
    from .fastdtw import fastdtw, dtw
