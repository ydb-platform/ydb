from thinc.compat import enable_tensorflow

try:
    enable_tensorflow()
except ImportError:
    pass
