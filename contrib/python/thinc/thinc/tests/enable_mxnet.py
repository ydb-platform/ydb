from thinc.compat import enable_mxnet

try:
    enable_mxnet()
except ImportError:
    pass
