from devtools.yamaker.project import CMakeNinjaNixProject

libgeotiff = CMakeNinjaNixProject(
    nixattr="libgeotiff",
    arcdir="contrib/libs/libgeotiff",
    build_targets=["libgeotiff.so"],
    disable_includes=[
        "geo_units.inc",
        "old_datum.inc",
        "old_ellipse.inc",
        "old_gcs.inc",
        "old_pcs.inc",
        "old_pm.inc",
        "old_proj.inc",
    ],
    addincl_global={".": {"."}},
)
