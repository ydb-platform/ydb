from devtools.yamaker.project import CMakeNinjaNixProject

sundials = CMakeNinjaNixProject(
    arcdir="contrib/libs/sundials",
    nixattr="sundials",
    build_targets=["libsundials_cvodes.so", "libsundials_idas.so"],
    copy_top_sources_except=["INSTALL_GUIDE.pdf"],
    addincl_global={".": {"./include"}},
    put_with={"sundials_cvodes": ["sundials_idas"]},
)
