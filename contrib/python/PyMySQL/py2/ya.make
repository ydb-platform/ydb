PY2_LIBRARY()

LICENSE(MIT)

VERSION(0.10.1)

PEERDIR(
    contrib/python/cryptography
)

PY_SRCS(
    TOP_LEVEL
    pymysql/__init__.py
    pymysql/_auth.py
    pymysql/_compat.py
    pymysql/_socketio.py
    pymysql/charset.py
    pymysql/connections.py
    pymysql/constants/__init__.py
    pymysql/constants/CLIENT.py
    pymysql/constants/COMMAND.py
    pymysql/constants/CR.py
    pymysql/constants/ER.py
    pymysql/constants/FIELD_TYPE.py
    pymysql/constants/FLAG.py
    pymysql/constants/SERVER_STATUS.py
    pymysql/converters.py
    pymysql/cursors.py
    pymysql/err.py
    pymysql/optionfile.py
    pymysql/protocol.py
    pymysql/times.py
    pymysql/util.py
)

NO_LINT()

RESOURCE_FILES(
    PREFIX contrib/python/PyMySQL/py2/
    .dist-info/METADATA
    .dist-info/top_level.txt
)

END()
