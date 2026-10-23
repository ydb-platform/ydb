PY3_PROGRAM(mirror3dc-verify)

PY_MAIN(verify)

PY_SRCS(
    TOP_LEVEL
    model_common.py
    realms.py
    verify.py
)

PEERDIR(
    library/python/resource
)

# Keep the graph and exact generator sources available for the verification manifest.
RESOURCE(
    graph.json mirror3dc/graph.json
    model_common.py mirror3dc/model_common.py
    realms.py mirror3dc/realms.py
    verify.py mirror3dc/verify.py
)

END()
