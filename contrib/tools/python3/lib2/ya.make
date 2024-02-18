LIBRARY()

PROVIDES(python)

LICENSE(Python-2.0)

PEERDIR(
    contrib/tools/python3
    contrib/tools/python3/Lib
    contrib/tools/python3/Modules
)

SUPPRESSIONS(lsan.supp)

END()
