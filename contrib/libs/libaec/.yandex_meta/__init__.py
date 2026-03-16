from devtools.yamaker.project import CMakeNinjaNixProject

libaec = CMakeNinjaNixProject(
    arcdir="contrib/libs/libaec",
    nixattr="libaec",
    put={"aec": "."},
    addincl_global={".": {"./include"}},
    ignore_targets=["graec", "sz"],
    copy_sources=["include/libaec.h"],
)
