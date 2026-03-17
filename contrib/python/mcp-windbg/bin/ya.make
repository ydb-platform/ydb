PY3_PROGRAM(mcp_windbg)

VERSION(Service-proxy-version)

LICENSE(MIT)

PEERDIR(
    contrib/python/mcp-windbg
)

PY_MAIN(mcp_windbg:main)

NO_CHECK_IMPORTS()

END()
