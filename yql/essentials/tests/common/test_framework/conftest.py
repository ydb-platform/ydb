try:
    from yql_http_file_server import yql_http_file_server
except ImportError:
    yql_http_file_server = None

try:
    from solomon_runner import solomon
except ImportError:
    solomon = None

# bunch of useless statements for linter happiness
# (otherwise it complains about unused names)
assert yql_http_file_server is yql_http_file_server
assert solomon is solomon
