from devtools.yamaker.project import CMakeNinjaNixProject


qhull = CMakeNinjaNixProject(
    arcdir="contrib/libs/qhull",
    nixattr="qhull",
    install_targets=["qhull_r"],
    install_subdir="src",
    copy_top_sources_except=["REGISTER.TXT", "QHULL-GO.lnk"],
)
