from devtools.yamaker.project import CMakeNinjaNixProject

google_crc32c = CMakeNinjaNixProject(
    owners=["g:cpp-contrib"],
    arcdir="contrib/restricted/crc32c",
    nixattr="crc32c",
    build_targets=["crc32c"],
    platform_dispatchers=["include/crc32c/crc32c_config.h"],
)
