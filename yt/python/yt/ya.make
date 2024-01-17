PY23_LIBRARY()

PEERDIR(
    contrib/python/simplejson
    contrib/python/six
    yt/python/yt/type_info 
)

IF(LINUX)
    PEERDIR(
        library/python/prctl
    )
ENDIF()

PY_SRCS(
    NAMESPACE yt

    __init__.py
    common.py
    json_wrapper.py
    logger.py
    logger_config.py
    subprocess_wrapper.py
)

END()
