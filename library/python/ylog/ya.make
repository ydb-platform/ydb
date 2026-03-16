PY23_LIBRARY()


VERSION(0.53)

PEERDIR(
    contrib/python/six
)

PY_SRCS(
    TOP_LEVEL
    ylog/__init__.py
    ylog/format.py
    ylog/handlers.py
    ylog/context.py
    ylog/utils.py

    ylog/local_data/__init__.py
    ylog/local_data/base.py
    ylog/local_data/contextvar.py
    ylog/local_data/thread.py
)

END()
