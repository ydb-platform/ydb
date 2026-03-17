from devtools.yamaker.project import CMakeNinjaNixProject

ngtcp2 = CMakeNinjaNixProject(
    license="MIT",
    owners=["g:balancer", "g:cpp-contrib"],
    nixattr="ls-qpack",
    arcdir="contrib/libs/ls-qpack",
    install_targets=[
        "ls-qpack",
    ],
    disable_includes=[
        "XXH_HEADER_NAME",
        "LSQPACK_ENC_LOGGER_HEADER",
        "LSQPACK_DEC_LOGGER_HEADER",
    ],
)
