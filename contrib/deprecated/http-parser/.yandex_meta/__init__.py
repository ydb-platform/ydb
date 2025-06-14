from devtools.yamaker.project import NixProject


http_parser = NixProject(
    owners=["g:mds", "g:cpp-contrib"],
    arcdir="contrib/deprecated/http-parser",
    nixattr="http-parser",
    # By default maximium header size allowed is 80Kb. To remove the effective limit
    # on the size of the header, we define the macro to a very large number (0x7fffffff).
    cflags=["-DHTTP_MAX_HEADER_SIZE=0x7fffffff"],
)
