from devtools.yamaker.project import GNUMakeNixProject

hiredis = GNUMakeNixProject(
    owners=["g:cpp-contrib"],
    arcdir="contrib/libs/hiredis",
    nixattr="hiredis",
    makeflags=["USE_SSL=1"],
    copy_sources=[
        "adapters",
        "examples",
        "win32.h",
    ],
    disable_includes=["testhelp.h"],
    install_targets=["hiredis", "hiredis_ssl"],
    put={"hiredis": "."},
    put_with={"hiredis": ["hiredis_ssl"]},
)
