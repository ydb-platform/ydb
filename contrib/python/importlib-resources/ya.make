PY23_LIBRARY()

LICENSE(Apache-2.0)

# The code is not taken from upstream. An analogue of version 1.0.2 has been implemented. See CONTRIB-1203
VERSION(1.0.2)

PY_SRCS(
    TOP_LEVEL
    importlib_resources/__init__.py
)

NO_LINT()

END()
